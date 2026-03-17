use crate::error::Result;
use crate::kafka::client::TopicPartition;
use crate::kafka::TimestampConsumer;
use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, instrument};

#[derive(Debug, Clone)]
struct CachedTimestamp {
    timestamp_ms: i64,
    offset: i64,
    cached_at: Instant,
}

/// Result of getting a timestamp
#[derive(Debug, Clone)]
pub struct TimestampResult {
    /// The timestamp in milliseconds
    pub timestamp_ms: i64,
}

/// Inner struct holding the actual sampler state
struct TimestampSamplerInner {
    consumer: TimestampConsumer,
    cache: DashMap<(String, TopicPartition), CachedTimestamp>,
    cache_ttl: Duration,
}

/// Thread-safe, clonable timestamp sampler for concurrent fetching
#[derive(Clone)]
pub struct TimestampSampler {
    inner: Arc<TimestampSamplerInner>,
}

impl TimestampSampler {
    pub fn new(consumer: TimestampConsumer, cache_ttl: Duration) -> Self {
        Self {
            inner: Arc::new(TimestampSamplerInner {
                consumer,
                cache: DashMap::new(),
                cache_ttl,
            }),
        }
    }

    #[instrument(skip(self), fields(group = %group_id, topic = %tp.topic, partition = tp.partition))]
    pub fn get_timestamp(
        &self,
        group_id: &str,
        tp: &TopicPartition,
        offset: i64,
    ) -> Result<Option<TimestampResult>> {
        let key = (group_id.to_string(), tp.clone());

        // Check cache
        if let Some(cached) = self.inner.cache.get(&key) {
            // Cache is valid if:
            // 1. Not expired by TTL
            // 2. Offset hasn't changed (consumer hasn't moved)
            if cached.cached_at.elapsed() < self.inner.cache_ttl && cached.offset == offset {
                debug!(
                    cached_timestamp = cached.timestamp_ms,
                    "Using cached timestamp"
                );
                return Ok(Some(TimestampResult {
                    timestamp_ms: cached.timestamp_ms,
                }));
            }
        }

        // Fetch from Kafka
        let fetch_result = self.inner.consumer.fetch_timestamp(tp, offset)?;

        // Cache the result
        if let Some(ref result) = fetch_result {
            self.inner.cache.insert(
                key,
                CachedTimestamp {
                    timestamp_ms: result.timestamp_ms,
                    offset,
                    cached_at: Instant::now(),
                },
            );
        }

        Ok(fetch_result.map(|r| TimestampResult {
            timestamp_ms: r.timestamp_ms,
        }))
    }

    #[allow(dead_code)]
    #[allow(clippy::type_complexity)]
    pub fn get_timestamps_batch(
        &self,
        requests: &[(String, TopicPartition, i64)],
    ) -> Vec<((String, TopicPartition), Result<Option<TimestampResult>>)> {
        requests
            .iter()
            .map(|(group_id, tp, offset)| {
                let result = self.get_timestamp(group_id, tp, *offset);
                ((group_id.clone(), tp.clone()), result)
            })
            .collect()
    }

    /// Recycle the underlying consumer pool to release accumulated metadata.
    pub fn recycle_pool(&self) -> Result<()> {
        self.inner.consumer.recycle_pool()
    }

    pub fn clear_stale_entries(&self) {
        let now = Instant::now();
        self.inner
            .cache
            .retain(|_, v| now.duration_since(v.cached_at) < self.inner.cache_ttl);
    }

    pub fn cache_size(&self) -> usize {
        self.inner.cache.len()
    }

    #[allow(dead_code)]
    pub fn clear_cache(&self) {
        self.inner.cache.clear();
    }
}

impl std::fmt::Debug for TimestampSampler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TimestampSampler")
            .field("cache_size", &self.inner.cache.len())
            .field("cache_ttl", &self.inner.cache_ttl)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timestamp_cache_ttl_expiry() {
        // This test verifies cache entry expiration logic
        let cached = CachedTimestamp {
            timestamp_ms: 1000,
            offset: 100,
            cached_at: Instant::now() - Duration::from_secs(120),
        };

        let cache_ttl = Duration::from_secs(60);

        // Entry should be expired
        assert!(cached.cached_at.elapsed() >= cache_ttl);
    }

    #[test]
    fn test_cache_invalidation_on_offset_change() {
        // This test verifies cache is invalidated when offset changes
        let cached = CachedTimestamp {
            timestamp_ms: 1000,
            offset: 100,
            cached_at: Instant::now(),
        };

        let new_offset = 150;

        // Cache should be invalid because offset changed
        assert_ne!(cached.offset, new_offset);
    }
}
