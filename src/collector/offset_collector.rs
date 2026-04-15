use crate::config::{CompiledFilters, Granularity, PerformanceConfig};
use crate::error::Result;
use crate::kafka::client::{KafkaClient, TopicPartition};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::{debug, instrument, warn};

/// Per-topic TTL cache for `cleanup.policy=compact` detection.
///
/// `cleanup.policy` is set at topic creation and almost never changes, so
/// caching it with a long TTL saves a per-cycle `DescribeConfigs` over the
/// monitored topic set. On each cycle the collector asks the cache which
/// topics still need to be queried fresh, then merges the cached-true
/// entries with whatever DescribeConfigs returns.
struct CompactedTopicsCache {
    ttl: Duration,
    // (is_compacted, fetched_at). Mutex is fine — accessed only from
    // `collect_parallel` on the cluster's single collection task.
    entries: Mutex<HashMap<String, (bool, Instant)>>,
}

impl CompactedTopicsCache {
    fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            entries: Mutex::new(HashMap::new()),
        }
    }

    /// Split `monitored_topics` into:
    ///   - `cached_compacted` — topics the cache says are compacted (fresh)
    ///   - `to_fetch` — topics with no fresh cache entry (need DescribeConfigs)
    fn partition<'a>(&self, monitored_topics: &'a [String]) -> (HashSet<String>, Vec<&'a str>) {
        let now = Instant::now();
        let entries = self.entries.lock().unwrap_or_else(|p| p.into_inner());

        let mut cached_compacted = HashSet::new();
        let mut to_fetch: Vec<&str> = Vec::new();
        for topic in monitored_topics {
            match entries.get(topic) {
                Some((is_compacted, fetched_at)) if now.duration_since(*fetched_at) < self.ttl => {
                    if *is_compacted {
                        cached_compacted.insert(topic.clone());
                    }
                }
                _ => to_fetch.push(topic.as_str()),
            }
        }
        (cached_compacted, to_fetch)
    }

    /// Update the cache with a fresh `DescribeConfigs` result. For each topic
    /// in `fetched_topics`, record whether it appeared in `compacted_result`.
    fn update(&self, fetched_topics: &[&str], compacted_result: &HashSet<String>) {
        let now = Instant::now();
        let mut entries = self.entries.lock().unwrap_or_else(|p| p.into_inner());
        for topic in fetched_topics {
            let is_compacted = compacted_result.contains(*topic);
            entries.insert((*topic).to_string(), (is_compacted, now));
        }
    }

    /// Drop cache entries for topics no longer being monitored.
    fn prune_to(&self, monitored_topics: &[String]) {
        let keep: HashSet<&str> = monitored_topics.iter().map(|s| s.as_str()).collect();
        let mut entries = self.entries.lock().unwrap_or_else(|p| p.into_inner());
        entries.retain(|k, _| keep.contains(k.as_str()));
    }
}

pub struct OffsetCollector {
    client: Arc<KafkaClient>,
    filters: CompiledFilters,
    performance: PerformanceConfig,
    granularity: Granularity,
    compacted_cache: CompactedTopicsCache,
}

#[derive(Debug, Clone)]
pub struct OffsetsSnapshot {
    pub cluster_name: String,
    pub groups: Vec<GroupSnapshot>,
    pub watermarks: HashMap<TopicPartition, (i64, i64)>,
    /// Topics configured with `cleanup.policy=compact`. Populated by
    /// `collect_parallel` for the monitored topic set. Used by the lag
    /// calculator to suppress data-loss warnings on compacted topics (where
    /// low_watermark ahead of committed_offset is expected, not a loss).
    pub compacted_topics: HashSet<String>,
    #[allow(dead_code)]
    pub timestamp_ms: i64,
}

#[derive(Debug, Clone)]
pub struct GroupSnapshot {
    pub group_id: String,
    pub state: String,
    pub members: Vec<MemberSnapshot>,
    pub offsets: HashMap<TopicPartition, i64>,
}

#[derive(Debug, Clone)]
pub struct MemberSnapshot {
    pub member_id: String,
    pub client_id: String,
    pub client_host: String,
    pub assignments: Vec<TopicPartition>,
}

impl OffsetCollector {
    pub fn with_performance(
        client: Arc<KafkaClient>,
        filters: CompiledFilters,
        performance: PerformanceConfig,
        granularity: Granularity,
    ) -> Self {
        let compacted_cache = CompactedTopicsCache::new(performance.compacted_topics_cache_ttl);
        Self {
            client,
            filters,
            performance,
            granularity,
            compacted_cache,
        }
    }

    /// Collect offsets using batched Admin API calls. Drives everything from a
    /// single filtered topic-set so internal / blacklisted topics never enter
    /// the watermark + compacted-topic-config path.
    ///
    /// Per-cycle RPC count:
    ///   - 2 batched ListOffsets calls (EARLIEST + LATEST), routed per leader
    ///     broker internally — O(brokers) broker round trips regardless of
    ///     partition count.
    ///   - `ceil(groups / 100)` batched DescribeConsumerGroups calls.
    ///   - 1 ListConsumerGroupOffsets call per group (`PER_CALL_CHUNK = 1`
    ///     in `fetch_all_group_offsets_batched` because librdkafka 2.12
    ///     rejects multi-group calls at the client layer; fanned out via
    ///     `max_concurrent_groups`).
    ///   - 1 DescribeConfigs call restricted to monitored topics.
    #[instrument(skip(self), fields(cluster = %self.client.cluster_name()))]
    pub async fn collect_parallel(&self) -> Result<OffsetsSnapshot> {
        let start = std::time::Instant::now();

        // List all consumer groups (single metadata call).
        let all_groups = self.client.list_consumer_groups()?;
        debug!(
            total_groups = all_groups.len(),
            "Listed all consumer groups"
        );

        let filtered_groups: Vec<_> = all_groups
            .iter()
            .filter(|g| self.filters.matches_group(&g.group_id))
            .collect();
        debug!(
            filtered_groups = filtered_groups.len(),
            "Filtered consumer groups"
        );

        let group_ids: Vec<&str> = filtered_groups
            .iter()
            .map(|g| g.group_id.as_str())
            .collect();

        // Describe filtered groups via batched FFI. Skip member-assignment
        // parsing unless we actually emit per-partition member labels
        // (granularity = partition).
        let parse_assignments = matches!(self.granularity, Granularity::Partition);
        let descriptions = self
            .client
            .describe_consumer_groups(&group_ids, parse_assignments)?;

        // Compute the monitored partition + topic set once from a single
        // metadata fetch. Topic filter is applied here, BEFORE any
        // partition-touching operation — this keeps `__consumer_offsets`
        // (50 partitions by default) and blacklisted topics out of the hot
        // path entirely.
        let (monitored_partitions, monitored_topics) = self.list_monitored_partitions()?;
        debug!(
            partitions = monitored_partitions.len(),
            topics = monitored_topics.len(),
            "Computed monitored topic + partition set"
        );

        // Watermarks via batched ListOffsets (two blocking FFI calls). Move
        // `monitored_partitions` into the blocking closure — no subsequent
        // use in this function, and cloning an O(partitions) Vec every cycle
        // is wasted work on large clusters.
        let watermarks = {
            let client = Arc::clone(&self.client);
            tokio::task::spawn_blocking(move || {
                client.fetch_watermarks_for_partitions(&monitored_partitions)
            })
            .await
            .map_err(|e| {
                crate::error::KlagError::Admin(format!("watermark task panicked: {e}"))
            })??
        };
        debug!(
            partitions = watermarks.len(),
            "Fetched watermarks (batched)"
        );

        // Group offsets via batched multi-group ListConsumerGroupOffsets.
        // `NULL` partitions → broker returns every committed partition per
        // group; we then filter the (much smaller) response by topic.
        let group_offsets = self.fetch_all_group_offsets_batched(&group_ids).await;

        // Compacted-topic lookup — TTL-cached per topic. `cleanup.policy`
        // almost never changes after topic creation, so most cycles only
        // refresh new topics (or nothing at all in steady state).
        let (mut compacted_topics, to_fetch) = self.compacted_cache.partition(&monitored_topics);
        if !to_fetch.is_empty() {
            debug!(
                to_fetch = to_fetch.len(),
                cached = compacted_topics.len(),
                "Compacted-topic cache partial miss — refreshing"
            );
            let to_fetch_owned: Vec<String> = to_fetch.iter().map(|s| s.to_string()).collect();
            match self
                .client
                .fetch_compacted_topics_for(&to_fetch_owned)
                .await
            {
                Ok(freshly_compacted) => {
                    self.compacted_cache.update(&to_fetch, &freshly_compacted);
                    compacted_topics.extend(freshly_compacted);
                }
                Err(e) => warn!(error = %e, "Failed to refresh compacted topics"),
            }
        } else {
            debug!(
                cached = compacted_topics.len(),
                "Compacted-topic cache fully hit — no DescribeConfigs RPC"
            );
        }
        // Drop cache entries for topics no longer monitored (filter change,
        // topic deletion) so memory doesn't grow unboundedly.
        self.compacted_cache.prune_to(&monitored_topics);

        // Build group snapshots
        let mut groups = Vec::with_capacity(descriptions.len());
        for desc in descriptions {
            let offsets = group_offsets
                .get(&desc.group_id)
                .cloned()
                .unwrap_or_default();

            let filtered_offsets: HashMap<TopicPartition, i64> = offsets
                .into_iter()
                .filter(|(tp, _)| self.filters.matches_topic(&tp.topic))
                .collect();

            let members = desc
                .members
                .into_iter()
                .map(|m| MemberSnapshot {
                    member_id: m.member_id,
                    client_id: m.client_id,
                    client_host: m.client_host,
                    assignments: m.assignments,
                })
                .collect();

            groups.push(GroupSnapshot {
                group_id: desc.group_id,
                state: desc.state,
                members,
                offsets: filtered_offsets,
            });
        }

        let elapsed = start.elapsed();
        debug!(
            elapsed_ms = elapsed.as_millis(),
            monitored_topics = monitored_topics.len(),
            compacted_topics = compacted_topics.len(),
            "Batched collection completed"
        );

        Ok(OffsetsSnapshot {
            cluster_name: self.client.cluster_name().to_string(),
            groups,
            watermarks,
            compacted_topics,
            timestamp_ms: chrono_timestamp_ms(),
        })
    }

    /// Compute the partition list this collector should monitor by applying
    /// the topic whitelist/blacklist to the current cluster metadata. Runs
    /// before any partition-touching Admin API call.
    fn list_monitored_partitions(&self) -> Result<(Vec<TopicPartition>, Vec<String>)> {
        let metadata = self.client.fetch_metadata()?;
        let mut partitions = Vec::new();
        let mut topics = Vec::new();
        for topic in metadata.topics() {
            let name = topic.name();
            if !self.filters.matches_topic(name) {
                continue;
            }
            topics.push(name.to_string());
            for p in topic.partitions() {
                partitions.push(TopicPartition::new(name, p.id()));
            }
        }
        Ok((partitions, topics))
    }

    /// Fetch offsets for all groups via batched Admin API.
    ///
    /// librdkafka 2.12's `rd_kafka_ListConsumerGroupOffsets` rejects calls with
    /// more than one group per call ("Exactly one ListConsumerGroupOffsets must
    /// be passed") even though the Kafka protocol supports multi-group
    /// ListOffsetFetch (KIP-709). We therefore issue one FFI call per group,
    /// fanned out with bounded concurrency via `max_concurrent_groups`.
    ///
    /// The win vs. the prior path is not "multi-group in one call" but:
    ///   - `NULL` partition list passed to each request → broker returns only
    ///     committed partitions; no 19K-entry partition-list clone per group.
    ///   - No per-group `spawn` of a 19K-entry Vec clone.
    ///
    /// Once librdkafka lifts the single-group restriction we can increase the
    /// inner `chunk_size` without code changes beyond this constant.
    async fn fetch_all_group_offsets_batched(
        &self,
        group_ids: &[&str],
    ) -> HashMap<String, HashMap<TopicPartition, i64>> {
        use crate::kafka::admin::list_consumer_group_offsets_batched;

        if group_ids.is_empty() {
            return HashMap::new();
        }

        // librdkafka constraint: one group per FFI call.
        const PER_CALL_CHUNK: usize = 1;

        let offset_timeout = self.performance.offset_fetch_timeout;
        let max_concurrent = self.performance.max_concurrent_groups;

        debug!(
            groups = group_ids.len(),
            per_call_chunk = PER_CALL_CHUNK,
            max_concurrent = max_concurrent,
            "Fetching group offsets (one call per group, fanned out)"
        );

        let semaphore = Arc::new(Semaphore::new(max_concurrent));
        let client = Arc::clone(&self.client);

        let mut handles = Vec::with_capacity(group_ids.len());
        for gid in group_ids {
            let gid = gid.to_string();
            let permit = semaphore.clone();
            let client_clone = Arc::clone(&client);
            handles.push(tokio::spawn(async move {
                let _permit: OwnedSemaphorePermit =
                    permit.acquire_owned().await.expect("semaphore closed");
                // Return the group id alongside the result so failure logs
                // can report which group broke.
                let result = tokio::task::spawn_blocking({
                    let gid = gid.clone();
                    move || {
                        list_consumer_group_offsets_batched(
                            &client_clone.admin_handle(),
                            &[gid.as_str()],
                            offset_timeout,
                            PER_CALL_CHUNK,
                        )
                    }
                })
                .await;
                (gid, result)
            }));
        }

        let results = futures::future::join_all(handles).await;

        let mut merged: HashMap<String, HashMap<TopicPartition, i64>> = HashMap::new();
        for r in results {
            match r {
                Ok((_gid, Ok(Ok(map)))) => merged.extend(map),
                Ok((gid, Ok(Err(e)))) => {
                    warn!(group = %gid, error = %e, "Group-offset call failed")
                }
                Ok((gid, Err(e))) => {
                    warn!(group = %gid, error = %e, "Group-offset call task panicked")
                }
                Err(e) => warn!(error = %e, "Group-offset join error"),
            }
        }
        merged
    }
}

impl OffsetsSnapshot {
    #[allow(dead_code)]
    pub fn filtered_groups(&self) -> Vec<&str> {
        self.groups.iter().map(|g| g.group_id.as_str()).collect()
    }

    pub fn get_watermark(&self, tp: &TopicPartition) -> Option<(i64, i64)> {
        self.watermarks.get(tp).copied()
    }

    #[allow(dead_code)]
    pub fn get_high_watermark(&self, tp: &TopicPartition) -> Option<i64> {
        self.watermarks.get(tp).map(|(_, high)| *high)
    }
}

fn chrono_timestamp_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_offsets_snapshot_filtered_groups() {
        let snapshot = OffsetsSnapshot {
            cluster_name: "test".to_string(),
            groups: vec![
                GroupSnapshot {
                    group_id: "group1".to_string(),
                    state: "Stable".to_string(),
                    members: vec![],
                    offsets: HashMap::new(),
                },
                GroupSnapshot {
                    group_id: "group2".to_string(),
                    state: "Stable".to_string(),
                    members: vec![],
                    offsets: HashMap::new(),
                },
            ],
            watermarks: HashMap::new(),
            compacted_topics: HashSet::new(),
            timestamp_ms: 0,
        };

        let groups = snapshot.filtered_groups();
        assert_eq!(groups.len(), 2);
        assert!(groups.contains(&"group1"));
        assert!(groups.contains(&"group2"));
    }

    #[test]
    fn compacted_cache_empty_cache_requests_all() {
        let cache = CompactedTopicsCache::new(Duration::from_secs(60));
        let topics = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let (cached, to_fetch) = cache.partition(&topics);
        assert!(cached.is_empty());
        assert_eq!(to_fetch, vec!["a", "b", "c"]);
    }

    #[test]
    fn compacted_cache_hit_skips_fetch() {
        let cache = CompactedTopicsCache::new(Duration::from_secs(60));
        let topics = vec!["a".to_string(), "b".to_string()];
        let mut compacted = HashSet::new();
        compacted.insert("a".to_string());

        // First partition call: everything needs fetching.
        let (_cached, to_fetch) = cache.partition(&topics);
        assert_eq!(to_fetch.len(), 2);

        // Update with the fetch result.
        cache.update(&to_fetch, &compacted);

        // Second partition call: everything cached.
        let (cached, to_fetch) = cache.partition(&topics);
        assert_eq!(to_fetch.len(), 0);
        assert_eq!(cached.len(), 1);
        assert!(cached.contains("a"));
    }

    #[test]
    fn compacted_cache_expired_entries_re_fetched() {
        let cache = CompactedTopicsCache::new(Duration::from_millis(50));
        let topics = vec!["a".to_string()];
        let mut compacted = HashSet::new();
        compacted.insert("a".to_string());

        let (_cached, to_fetch) = cache.partition(&topics);
        cache.update(&to_fetch, &compacted);

        // Cached entry is fresh.
        let (cached, to_fetch) = cache.partition(&topics);
        assert!(cached.contains("a"));
        assert!(to_fetch.is_empty());

        // Wait for TTL to expire.
        std::thread::sleep(Duration::from_millis(70));

        // Cached entry is stale — partition() returns it for re-fetching.
        let (cached, to_fetch) = cache.partition(&topics);
        assert!(cached.is_empty());
        assert_eq!(to_fetch, vec!["a"]);
    }

    #[test]
    fn compacted_cache_prune_removes_unseen_topics() {
        let cache = CompactedTopicsCache::new(Duration::from_secs(60));
        let initial = vec!["a".to_string(), "b".to_string()];
        let mut compacted = HashSet::new();
        compacted.insert("a".to_string());
        let (_cached, to_fetch) = cache.partition(&initial);
        cache.update(&to_fetch, &compacted);

        // Only "a" is still monitored. "b"'s cache entry should be dropped.
        let remaining = vec!["a".to_string()];
        cache.prune_to(&remaining);

        let entries = cache.entries.lock().unwrap();
        assert!(entries.contains_key("a"));
        assert!(!entries.contains_key("b"));
    }
}
