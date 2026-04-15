use crate::config::{CompiledFilters, PerformanceConfig};
use crate::error::Result;
use crate::kafka::client::{KafkaClient, TopicPartition};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::{debug, instrument, warn};

pub struct OffsetCollector {
    client: Arc<KafkaClient>,
    filters: CompiledFilters,
    performance: PerformanceConfig,
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
    ) -> Self {
        Self {
            client,
            filters,
            performance,
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

        // Describe filtered groups via batched FFI (one chunked call).
        let descriptions = self.client.describe_consumer_groups(&group_ids)?;

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

        // Compacted-topic lookup restricted to monitored topics (huge saving
        // vs. the prior full-cluster DescribeConfigs).
        let compacted_topics = self
            .client
            .fetch_compacted_topics_for(&monitored_topics)
            .await
            .unwrap_or_else(|e| {
                warn!(error = %e, "Failed to fetch compacted topics");
                HashSet::new()
            });

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
}
