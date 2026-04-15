//! Time-lag estimation.
//!
//! Two modes:
//!
//! 1. **`Message`** — read the actual message at the committed offset via a
//!    pooled BaseConsumer and use its produce timestamp. Exact, but the
//!    pool is a heavyweight native-memory consumer on large clusters
//!    (each `BaseConsumer` is a full librdkafka client with its own
//!    metadata cache and background threads).
//! 2. **`Rate`** — estimate time lag from the observed rate of change of
//!    high watermarks. No consumer pool, no FFI, pure CPU. Default since
//!    Tier 3; see [`crate::collector::rate_sampler`] for the math.
//!
//! Both modes present the same interface to `ClusterManager`:
//! [`TimestampSampler::compute_time_lags`] takes an `OffsetsSnapshot` and
//! returns `HashMap<(group_id, TopicPartition), TimestampData>` — a synthetic
//! `timestamp_ms` is produced for rate mode (`now_ms - estimated_lag_secs *
//! 1000`) so the downstream `LagCalculator` doesn't need to care which
//! backend produced the number.

use crate::collector::lag_calculator::TimestampData;
use crate::collector::offset_collector::OffsetsSnapshot;
use crate::collector::rate_sampler::RateSampler;
use crate::error::Result;
use crate::kafka::client::TopicPartition;
use crate::kafka::TimestampConsumer;
use dashmap::DashMap;
use futures::future::join_all;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::{debug, warn};

#[derive(Debug, Clone)]
struct CachedTimestamp {
    timestamp_ms: i64,
    offset: i64,
    cached_at: Instant,
}

/// Message-mode internal state.
struct MessageSamplerInner {
    consumer: TimestampConsumer,
    cache: DashMap<(String, TopicPartition), CachedTimestamp>,
    cache_ttl: Duration,
}

pub struct MessageSampler {
    inner: Arc<MessageSamplerInner>,
}

impl Clone for MessageSampler {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl MessageSampler {
    fn new(consumer: TimestampConsumer, cache_ttl: Duration) -> Self {
        Self {
            inner: Arc::new(MessageSamplerInner {
                consumer,
                cache: DashMap::new(),
                cache_ttl,
            }),
        }
    }

    fn get_timestamp(
        &self,
        group_id: &str,
        tp: &TopicPartition,
        offset: i64,
    ) -> Result<Option<i64>> {
        let key = (group_id.to_string(), tp.clone());

        // Cache hit if TTL not exceeded AND offset unchanged (consumer didn't move).
        if let Some(cached) = self.inner.cache.get(&key) {
            if cached.cached_at.elapsed() < self.inner.cache_ttl && cached.offset == offset {
                return Ok(Some(cached.timestamp_ms));
            }
        }

        let fetch_result = self.inner.consumer.fetch_timestamp(tp, offset)?;
        if let Some(ref ts) = fetch_result {
            self.inner.cache.insert(
                key,
                CachedTimestamp {
                    timestamp_ms: ts.timestamp_ms,
                    offset,
                    cached_at: Instant::now(),
                },
            );
        }
        Ok(fetch_result.map(|r| r.timestamp_ms))
    }

    fn recycle_pool(&self) -> Result<()> {
        self.inner.consumer.recycle_pool()
    }

    fn clear_stale_entries(&self) {
        let now = Instant::now();
        let ttl = self.inner.cache_ttl;
        self.inner
            .cache
            .retain(|_, v| now.duration_since(v.cached_at) < ttl);
    }

    fn cache_size(&self) -> usize {
        self.inner.cache.len()
    }
}

/// Unified sampler handle. Cheap to `clone()` (Arc bump inside).
#[derive(Clone)]
pub enum TimestampSampler {
    Message(MessageSampler),
    Rate(Arc<RateSampler>),
}

impl TimestampSampler {
    /// Build a message-mode sampler. Takes ownership of the
    /// `TimestampConsumer` pool (which is only constructed in message
    /// mode, so we don't pay for the pool's memory in rate mode).
    pub fn new_message(consumer: TimestampConsumer, cache_ttl: Duration) -> Self {
        Self::Message(MessageSampler::new(consumer, cache_ttl))
    }

    /// Build a rate-mode sampler.
    pub fn new_rate(sampler: RateSampler) -> Self {
        Self::Rate(Arc::new(sampler))
    }

    /// Only meaningful in message mode; no-op for rate mode.
    pub fn recycle_pool(&self) -> Result<()> {
        match self {
            Self::Message(s) => s.recycle_pool(),
            Self::Rate(_) => Ok(()),
        }
    }

    /// Only meaningful in message mode; no-op for rate mode.
    pub fn clear_stale_entries(&self) {
        if let Self::Message(s) = self {
            s.clear_stale_entries();
        }
    }

    /// Diagnostic count: message-mode cache entries or rate-mode tracked
    /// partitions.
    pub fn cache_size(&self) -> usize {
        match self {
            Self::Message(s) => s.cache_size(),
            Self::Rate(s) => s.tracked_partitions(),
        }
    }

    /// Compute per-(group, partition) timestamps for everything in `snapshot`
    /// with `lag > 0`. Returns a map shaped for direct consumption by
    /// `LagCalculator::calculate`.
    ///
    /// - Message mode: spawn up to `max_concurrent_fetches` blocking FFI
    ///   tasks via the consumer pool. Unchanged behavior from Tier 2.
    /// - Rate mode: record this cycle's watermarks into the history ring
    ///   buffer, then synthesize `timestamp_ms = now_ms - estimate_secs *
    ///   1000` for each laggy partition where a reliable rate is available.
    ///   `max_concurrent_fetches` is ignored.
    pub async fn compute_time_lags(
        &self,
        snapshot: &OffsetsSnapshot,
        now_ms: i64,
        max_concurrent_fetches: usize,
    ) -> HashMap<(String, TopicPartition), TimestampData> {
        match self {
            Self::Message(s) => {
                compute_time_lags_message(s, snapshot, max_concurrent_fetches).await
            }
            Self::Rate(s) => compute_time_lags_rate(s, snapshot, now_ms),
        }
    }
}

fn compute_time_lags_rate(
    sampler: &RateSampler,
    snapshot: &OffsetsSnapshot,
    now_ms: i64,
) -> HashMap<(String, TopicPartition), TimestampData> {
    // First: add this cycle's watermarks to history (and prune stale
    // partitions). Only after that can we compute rate for the current
    // cycle reliably.
    sampler.record_watermarks(&snapshot.watermarks);

    let mut out = HashMap::new();
    for group in &snapshot.groups {
        for (tp, committed_offset) in &group.offsets {
            let high = snapshot
                .get_watermark(tp)
                .map(|(_, h)| h)
                .unwrap_or(*committed_offset);
            let lag = high - *committed_offset;
            if lag <= 0 {
                // 0 lag → 0 seconds. Downstream builds Some(0.0) naturally;
                // don't need to populate.
                continue;
            }
            if let Some(secs) = sampler.estimate_lag_seconds(tp, lag) {
                let synthetic_ts_ms = now_ms - (secs * 1000.0) as i64;
                out.insert(
                    (group.group_id.clone(), tp.clone()),
                    TimestampData {
                        timestamp_ms: synthetic_ts_ms,
                    },
                );
            }
            // If estimate is None (insufficient history / idle partition),
            // leave the entry absent. LagCalculator handles missing entries
            // by emitting the metric as None.
        }
    }
    debug!(
        tracked_partitions = sampler.tracked_partitions(),
        emitted = out.len(),
        "Rate-mode time-lag computation complete"
    );
    out
}

async fn compute_time_lags_message(
    sampler: &MessageSampler,
    snapshot: &OffsetsSnapshot,
    max_concurrent_fetches: usize,
) -> HashMap<(String, TopicPartition), TimestampData> {
    let mut requests: Vec<(String, TopicPartition, i64)> = Vec::new();
    for group in &snapshot.groups {
        for (tp, committed_offset) in &group.offsets {
            let high = snapshot
                .get_watermark(tp)
                .map(|(_, h)| h)
                .unwrap_or(*committed_offset);
            if high - *committed_offset > 0 {
                requests.push((group.group_id.clone(), tp.clone(), *committed_offset));
            }
        }
    }
    if requests.is_empty() {
        return HashMap::new();
    }

    debug!(
        request_count = requests.len(),
        max_concurrent = max_concurrent_fetches,
        "Fetching per-partition message timestamps (message mode)"
    );

    let semaphore = Arc::new(Semaphore::new(max_concurrent_fetches.max(1)));
    let mut handles = Vec::with_capacity(requests.len());
    for (group_id, tp, offset) in requests {
        let permit = Arc::clone(&semaphore);
        let sampler = sampler.clone();
        handles.push(tokio::spawn(async move {
            let permit_guard: OwnedSemaphorePermit =
                permit.acquire_owned().await.expect("semaphore closed");
            let result = tokio::task::spawn_blocking(move || {
                let _p = permit_guard;
                let result = sampler.get_timestamp(&group_id, &tp, offset);
                ((group_id, tp), result)
            })
            .await;
            result
        }));
    }

    let results = join_all(handles).await;
    let mut out = HashMap::new();
    for result in results {
        match result {
            Ok(Ok(((group_id, tp), Ok(Some(ts))))) => {
                out.insert((group_id, tp), TimestampData { timestamp_ms: ts });
            }
            Ok(Ok(((_group_id, _tp), Ok(None)))) => {
                // No message available at offset; skip.
            }
            Ok(Ok(((group_id, tp), Err(e)))) => {
                warn!(
                    group = %group_id,
                    topic = %tp.topic,
                    partition = tp.partition,
                    error = %e,
                    "Message timestamp fetch failed"
                );
            }
            Ok(Err(e)) => {
                warn!(error = %e, "Message timestamp blocking task panicked");
            }
            Err(e) => {
                warn!(error = %e, "Message timestamp task panicked");
            }
        }
    }
    out
}

impl std::fmt::Debug for TimestampSampler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Message(s) => f
                .debug_struct("TimestampSampler::Message")
                .field("cache_size", &s.cache_size())
                .finish(),
            Self::Rate(s) => f
                .debug_struct("TimestampSampler::Rate")
                .field("tracked_partitions", &s.tracked_partitions())
                .finish(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cached_timestamp_ttl_expiry_check() {
        let cached = CachedTimestamp {
            timestamp_ms: 1000,
            offset: 100,
            cached_at: Instant::now() - Duration::from_secs(120),
        };
        let cache_ttl = Duration::from_secs(60);
        assert!(cached.cached_at.elapsed() >= cache_ttl);
    }

    #[test]
    fn rate_mode_synthesizes_timestamp_from_lag_estimate() {
        use crate::collector::offset_collector::{GroupSnapshot, MemberSnapshot};
        use std::collections::HashSet;

        let sampler =
            TimestampSampler::new_rate(RateSampler::new(5, Duration::from_secs(600), 0.01));

        // Prime the rate sampler's history: two watermark observations
        // separated by a small sleep so the rate is well-defined.
        let tp_key = TopicPartition::new("t", 0);

        let mut watermarks = HashMap::new();
        watermarks.insert(tp_key.clone(), (0i64, 100i64));
        let snap1 = OffsetsSnapshot {
            cluster_name: "c".into(),
            groups: vec![],
            watermarks: watermarks.clone(),
            compacted_topics: HashSet::new(),
            timestamp_ms: 0,
        };
        let _ = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(sampler.compute_time_lags(&snap1, 0, 1));

        std::thread::sleep(Duration::from_millis(100));

        // New cycle: hwm moved by 1000 → ~10k msg/sec. Consumer committed
        // at 500 → lag 600. Expected estimated secs: 600 / 10000 = 0.06s.
        let mut watermarks2 = HashMap::new();
        watermarks2.insert(tp_key.clone(), (0i64, 1100i64));
        let mut offsets = HashMap::new();
        offsets.insert(tp_key.clone(), 500i64);
        let snap2 = OffsetsSnapshot {
            cluster_name: "c".into(),
            groups: vec![GroupSnapshot {
                group_id: "g".into(),
                state: "Stable".into(),
                members: vec![] as Vec<MemberSnapshot>,
                offsets,
            }],
            watermarks: watermarks2,
            compacted_topics: HashSet::new(),
            timestamp_ms: 0,
        };
        let now_ms = 1_000_000_000i64;
        let out = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(sampler.compute_time_lags(&snap2, now_ms, 1));

        let ts = out
            .get(&("g".to_string(), tp_key))
            .expect("should produce a synthetic timestamp");
        // Synthesized ts = now_ms - estimated_secs*1000. Estimated secs
        // should be small but positive; allow a generous window for CI
        // sleep jitter.
        let lag_ms = now_ms - ts.timestamp_ms;
        assert!(
            (20..=300).contains(&lag_ms),
            "synthetic lag_ms out of sanity range: {lag_ms}"
        );
    }
}
