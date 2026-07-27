//! Time-lag estimation.
//!
//! Two modes:
//!
//! 1. **`Message`** — read the actual message at the committed offset via a
//!    pooled `BaseConsumer` and use its produce timestamp. Exact, but the
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

    /// Scan back from the committed offset to the last readable data record, for a
    /// consumer wedged at the last stable offset (behind an open transaction).
    /// Transaction control markers just below the committed offset are never
    /// delivered, so a read there returns `None`; walk back past them — bounded,
    /// with a short per-probe timeout — to the data record. Bypasses the cache
    /// (the committed offset is frozen, so nothing useful is cached for it).
    fn scan_back_for_stable_timestamp(
        &self,
        tp: &TopicPartition,
        committed: i64,
        low: i64,
    ) -> Option<i64> {
        for offset in fallback_scan_offsets(committed, low) {
            if let Ok(Some(r)) =
                self.inner
                    .consumer
                    .fetch_timestamp_within(tp, offset, FALLBACK_PROBE_TIMEOUT)
            {
                return Some(r.timestamp_ms);
            }
        }
        None
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
    ///   tasks via the consumer pool. When `skip_data_loss_partitions` is set,
    ///   partitions whose committed offset is below the low watermark are not
    ///   fetched (retention deleted the committed message).
    /// - Rate mode: record this cycle's watermarks into the history ring
    ///   buffer, then synthesize `timestamp_ms = now_ms - estimate_secs *
    ///   1000` for each laggy partition where a reliable rate is available.
    ///   `max_concurrent_fetches` and `skip_data_loss_partitions` are ignored.
    pub async fn compute_time_lags(
        &self,
        snapshot: &OffsetsSnapshot,
        now_ms: i64,
        max_concurrent_fetches: usize,
        skip_data_loss_partitions: bool,
    ) -> HashMap<(String, TopicPartition), TimestampData> {
        match self {
            Self::Message(s) => {
                compute_time_lags_message(
                    s,
                    snapshot,
                    now_ms,
                    max_concurrent_fetches,
                    skip_data_loss_partitions,
                )
                .await
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

    // Take the history lock once and materialize per-partition rates —
    // avoids O(groups × partitions) lock acquisitions on large clusters.
    let rates = sampler.rates_snapshot();

    let mut out = HashMap::new();
    for group in &snapshot.groups {
        for (tp, committed_offset) in &group.offsets {
            let high = snapshot
                .get_watermark(tp)
                .map_or(*committed_offset, |(_, h)| h);
            let lag = high - *committed_offset;
            if lag <= 0 {
                // 0 lag → 0 seconds. Downstream builds Some(0.0) naturally;
                // don't need to populate.
                continue;
            }
            if let Some(&rate) = rates.get(tp) {
                let secs = lag as f64 / rate;
                let synthetic_ts_ms = now_ms - (secs * 1000.0) as i64;
                out.insert(
                    (group.group_id.clone(), tp.clone()),
                    TimestampData {
                        timestamp_ms: synthetic_ts_ms,
                    },
                );
            }
            // Partitions absent from `rates` have no reliable estimate
            // (insufficient history / idle / retention rewind). Leave the
            // entry out; LagCalculator emits the metric as None.
        }
    }
    debug!(
        tracked_partitions = sampler.tracked_partitions(),
        rates_available = rates.len(),
        emitted = out.len(),
        "Rate-mode time-lag computation complete"
    );
    out
}

async fn compute_time_lags_message(
    sampler: &MessageSampler,
    snapshot: &OffsetsSnapshot,
    now_ms: i64,
    max_concurrent_fetches: usize,
    skip_data_loss_partitions: bool,
) -> HashMap<(String, TopicPartition), TimestampData> {
    let mut requests: Vec<(String, TopicPartition, i64)> = Vec::new();
    // Low watermark per requested (group, tp): lets a `None` fetch fall back to
    // the last stable message without ever reading below the retention floor.
    let mut low_by_key: HashMap<(String, TopicPartition), i64> = HashMap::new();
    for group in &snapshot.groups {
        for (tp, committed_offset) in &group.offsets {
            let (low, high) = snapshot
                .get_watermark(tp)
                .unwrap_or((*committed_offset, *committed_offset));
            // Retention has deleted the committed message when the committed
            // offset is below the low watermark. A fetch there can only time
            // out or read the wrong (earliest) message, so skip it when asked:
            // the lag calculator still reports the partition, with offset lag
            // and time lag both 0 and `data_loss_detected` set (magnitude in
            // `messages_lost` / `retention_margin`).
            if skip_data_loss_partitions && *committed_offset < low {
                continue;
            }
            if high - *committed_offset > 0 {
                let key = (group.group_id.clone(), tp.clone());
                low_by_key.insert(key, low);
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

    let mut out = HashMap::new();

    // Pass 1: the committed offset itself.
    let mut fallbacks: Vec<(String, TopicPartition, i64, i64)> = Vec::new();
    for (group_id, tp, committed, ts) in
        fetch_timestamps_at(sampler, requests, max_concurrent_fetches).await
    {
        match ts {
            Some(ts) => {
                out.insert((group_id, tp), TimestampData { timestamp_ms: ts });
            }
            None => {
                let low = low_by_key
                    .get(&(group_id.clone(), tp.clone()))
                    .copied()
                    .unwrap_or(committed);
                if committed < low {
                    // Committed offset is below the low watermark: the committed
                    // message was purged (retention / Streams deleteRecords
                    // caught up to it between the offset snapshot and this fetch
                    // — a data-loss race the snapshot-time skip check missed).
                    // Matches the `committed < low_watermark` data-loss test in
                    // the lag calculator; report 0. A committed offset exactly at
                    // `low` is still readable, so it is NOT data loss — it falls
                    // through to the bounded scan below (which finds nothing and
                    // warns).
                    out.insert(
                        (group_id, tp),
                        TimestampData {
                            timestamp_ms: now_ms,
                        },
                    );
                } else {
                    // Committed offset is above the low watermark but unreadable:
                    // it is the first uncommitted offset (the last stable offset),
                    // e.g. a consumer wedged behind an open/hung transaction while
                    // the high watermark keeps advancing. Don't drop the series;
                    // pass 2 scans back to the last readable data record so the
                    // partition still reports its real, growing time lag.
                    fallbacks.push((group_id, tp, committed, low));
                }
            }
        }
    }

    // Pass 2: for partitions whose committed offset was unreadable (wedged behind
    // an open transaction), scan back to the last readable data record, skipping
    // any transaction control markers. A miss here is unexpected — we could not
    // read any data record near the committed offset — so it is warned, not
    // silently dropped.
    if !fallbacks.is_empty() {
        debug!(
            fallback_count = fallbacks.len(),
            "Scanning back to last readable data record for wedged partitions"
        );
        let semaphore = Arc::new(Semaphore::new(max_concurrent_fetches.max(1)));
        let mut handles = Vec::with_capacity(fallbacks.len());
        for (group_id, tp, committed, low) in fallbacks {
            let permit = Arc::clone(&semaphore);
            let sampler = sampler.clone();
            handles.push(tokio::spawn(async move {
                let permit_guard: OwnedSemaphorePermit =
                    permit.acquire_owned().await.expect("semaphore closed");
                tokio::task::spawn_blocking(move || {
                    let _p = permit_guard;
                    let ts = sampler.scan_back_for_stable_timestamp(&tp, committed, low);
                    (group_id, tp, ts)
                })
                .await
            }));
        }
        for result in join_all(handles).await {
            match result {
                Ok(Ok((group_id, tp, Some(ts)))) => {
                    out.insert((group_id, tp), TimestampData { timestamp_ms: ts });
                }
                Ok(Ok((group_id, tp, None))) => {
                    warn!(
                        group = %group_id,
                        topic = %tp.topic,
                        partition = tp.partition,
                        "No readable data record near committed offset; time lag unavailable this cycle"
                    );
                }
                Ok(Err(e)) => warn!(error = %e, "Fallback scan blocking task panicked"),
                Err(e) => warn!(error = %e, "Fallback scan task panicked"),
            }
        }
    }
    out
}

/// Max offsets to walk back looking for the last readable data record when the
/// committed offset is unreadable. Bounds a pathological run of transaction
/// control markers (each occupies an offset but is never delivered to consumers).
const FALLBACK_SCAN_MAX: i64 = 8;

/// Short per-probe timeout for the fallback scan. A readable data record returns
/// in well under this; a miss (a control marker or the last stable offset) blocks
/// until the timeout, so keep it short to avoid tying up a fetch slot.
const FALLBACK_PROBE_TIMEOUT: Duration = Duration::from_secs(2);

/// Offsets to probe, nearest-first, when the committed message is unreadable: the
/// committed offset is the first uncommitted message, and one or more transaction
/// control markers may sit just below it. Walks back from `committed - 1`,
/// stopping at the retention floor or after [`FALLBACK_SCAN_MAX`] steps. Empty
/// when the committed offset is on the retention floor.
fn fallback_scan_offsets(committed: i64, low: i64) -> Vec<i64> {
    let floor = low.max(0);
    (1..=FALLBACK_SCAN_MAX)
        .map(|k| committed - k)
        .take_while(|&o| o >= floor)
        .collect()
}

/// Fetch message timestamps for each `(group, tp, offset)` request, bounded by
/// `max_concurrent`. Fetch errors and task panics are logged and surface as a
/// `None` timestamp so a failed row never silently disappears; the returned offset
/// echoes the request so callers can react per row.
async fn fetch_timestamps_at(
    sampler: &MessageSampler,
    requests: Vec<(String, TopicPartition, i64)>,
    max_concurrent: usize,
) -> Vec<(String, TopicPartition, i64, Option<i64>)> {
    let semaphore = Arc::new(Semaphore::new(max_concurrent.max(1)));
    let mut handles = Vec::with_capacity(requests.len());
    for (group_id, tp, offset) in requests {
        let permit = Arc::clone(&semaphore);
        let sampler = sampler.clone();
        handles.push(tokio::spawn(async move {
            let permit_guard: OwnedSemaphorePermit =
                permit.acquire_owned().await.expect("semaphore closed");
            tokio::task::spawn_blocking(move || {
                let _p = permit_guard;
                let ts = match sampler.get_timestamp(&group_id, &tp, offset) {
                    Ok(ts) => ts,
                    Err(e) => {
                        warn!(
                            group = %group_id,
                            topic = %tp.topic,
                            partition = tp.partition,
                            offset,
                            error = %e,
                            "Message timestamp fetch failed"
                        );
                        None
                    }
                };
                (group_id, tp, offset, ts)
            })
            .await
        }));
    }

    let mut out = Vec::with_capacity(handles.len());
    for result in join_all(handles).await {
        match result {
            Ok(Ok(entry)) => out.push(entry),
            Ok(Err(e)) => warn!(error = %e, "Message timestamp blocking task panicked"),
            Err(e) => warn!(error = %e, "Message timestamp task panicked"),
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
    fn fallback_scan_offsets_walks_back_bounded_by_floor() {
        // Wedged consumer well above the low watermark: probe committed-1 down to
        // committed-FALLBACK_SCAN_MAX, nearest first.
        assert_eq!(
            fallback_scan_offsets(775060, 771303),
            vec![775059, 775058, 775057, 775056, 775055, 775054, 775053, 775052]
        );
        // Stops at the retention floor.
        assert_eq!(fallback_scan_offsets(771305, 771303), vec![771304, 771303]);
        // Committed on the retention floor: nothing stable below it.
        assert!(fallback_scan_offsets(771303, 771303).is_empty());
        // committed=1, low=0 -> offset 0 is the earliest readable record.
        assert_eq!(fallback_scan_offsets(1, 0), vec![0]);
        assert!(fallback_scan_offsets(0, 0).is_empty());
    }

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
            .block_on(sampler.compute_time_lags(&snap1, 0, 1, false));

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
            .block_on(sampler.compute_time_lags(&snap2, now_ms, 1, false));

        let ts = out
            .get(&("g".to_string(), tp_key))
            .expect("should produce a synthetic timestamp");
        // Synthesized ts = now_ms - estimated_secs*1000. The exact value
        // depends on how long the OS scheduler actually delayed the
        // thread, which is unstable under CI contention. Only assert
        // sign/order-of-magnitude, not a tight window.
        let lag_ms = now_ms - ts.timestamp_ms;
        assert!(
            lag_ms > 0 && lag_ms < 60_000,
            "synthetic lag_ms out of sanity range: {lag_ms}"
        );
    }
}
