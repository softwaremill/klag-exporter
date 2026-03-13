use crate::config::{CompiledFilters, PerformanceConfig};
use crate::error::Result;
use crate::kafka::client::{KafkaClient, TopicPartition};
use std::collections::HashMap;
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
    /// Create a new OffsetCollector with default performance config.
    /// Prefer `with_performance` for large clusters.
    #[allow(dead_code)]
    pub fn new(client: Arc<KafkaClient>, filters: CompiledFilters) -> Self {
        let performance = client.performance().clone();
        Self {
            client,
            filters,
            performance,
        }
    }

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

    /// Collect offsets sequentially (legacy method).
    /// For large clusters, use `collect_parallel` instead.
    #[allow(dead_code)]
    #[instrument(skip(self), fields(cluster = %self.client.cluster_name()))]
    pub fn collect(&self) -> Result<OffsetsSnapshot> {
        let start = std::time::Instant::now();

        // List all consumer groups
        let all_groups = self.client.list_consumer_groups()?;
        debug!(
            total_groups = all_groups.len(),
            "Listed all consumer groups"
        );

        // Filter groups
        let filtered_groups: Vec<_> = all_groups
            .iter()
            .filter(|g| self.filters.matches_group(&g.group_id))
            .collect();
        debug!(
            filtered_groups = filtered_groups.len(),
            "Filtered consumer groups"
        );

        // Get group descriptions
        let group_ids: Vec<&str> = filtered_groups
            .iter()
            .map(|g| g.group_id.as_str())
            .collect();
        let descriptions = self.client.describe_consumer_groups(&group_ids)?;

        // Fetch watermarks for all topics
        let watermarks = self.client.fetch_all_watermarks()?;
        debug!(partitions = watermarks.len(), "Fetched watermarks");

        // Build group snapshots
        let wm_keys: Vec<TopicPartition> = watermarks.keys().cloned().collect();
        let mut groups = Vec::with_capacity(descriptions.len());
        for desc in descriptions {
            let offsets = self.client.list_consumer_group_offsets(
                &desc.group_id,
                &wm_keys,
                self.performance.offset_fetch_timeout,
            )?;

            // Filter offsets by topic whitelist/blacklist
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

        // Filter watermarks by topic
        let filtered_watermarks: HashMap<TopicPartition, (i64, i64)> = watermarks
            .into_iter()
            .filter(|(tp, _)| self.filters.matches_topic(&tp.topic))
            .collect();

        let elapsed = start.elapsed();
        debug!(elapsed_ms = elapsed.as_millis(), "Collection completed");

        Ok(OffsetsSnapshot {
            cluster_name: self.client.cluster_name().to_string(),
            groups,
            watermarks: filtered_watermarks,
            timestamp_ms: chrono_timestamp_ms(),
        })
    }

    /// Collect offsets with parallel watermark and group offset fetching.
    /// This is more efficient for large clusters with many groups and partitions.
    #[instrument(skip(self), fields(cluster = %self.client.cluster_name()))]
    pub async fn collect_parallel(&self) -> Result<OffsetsSnapshot> {
        let start = std::time::Instant::now();

        // List all consumer groups (single call, cannot parallelize)
        let all_groups = self.client.list_consumer_groups()?;
        debug!(
            total_groups = all_groups.len(),
            "Listed all consumer groups"
        );

        // Filter groups
        let filtered_groups: Vec<_> = all_groups
            .iter()
            .filter(|g| self.filters.matches_group(&g.group_id))
            .collect();
        debug!(
            filtered_groups = filtered_groups.len(),
            "Filtered consumer groups"
        );

        // Get group descriptions (still sequential as this is a metadata call)
        let group_ids: Vec<&str> = filtered_groups
            .iter()
            .map(|g| g.group_id.as_str())
            .collect();
        let descriptions = self.client.describe_consumer_groups(&group_ids)?;

        // Fetch watermarks in parallel
        let watermarks = self.client.fetch_all_watermarks_parallel().await?;
        debug!(
            partitions = watermarks.len(),
            "Fetched watermarks (parallel)"
        );

        // Fetch group offsets in parallel
        let group_offsets = self
            .fetch_all_group_offsets_parallel(&descriptions, &watermarks)
            .await;

        // Build group snapshots
        let mut groups = Vec::with_capacity(descriptions.len());
        for desc in descriptions {
            let offsets = group_offsets
                .get(&desc.group_id)
                .cloned()
                .unwrap_or_default();

            // Filter offsets by topic whitelist/blacklist
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

        // Filter watermarks by topic
        let filtered_watermarks: HashMap<TopicPartition, (i64, i64)> = watermarks
            .into_iter()
            .filter(|(tp, _)| self.filters.matches_topic(&tp.topic))
            .collect();

        let elapsed = start.elapsed();
        debug!(
            elapsed_ms = elapsed.as_millis(),
            "Parallel collection completed"
        );

        Ok(OffsetsSnapshot {
            cluster_name: self.client.cluster_name().to_string(),
            groups,
            watermarks: filtered_watermarks,
            timestamp_ms: chrono_timestamp_ms(),
        })
    }

    /// Fetch offsets for all groups in parallel with bounded concurrency.
    /// Uses the Admin API (ListConsumerGroupOffsets) through the shared AdminClient — no per-group consumers needed.
    async fn fetch_all_group_offsets_parallel(
        &self,
        descriptions: &[crate::kafka::client::GroupDescription],
        watermarks: &HashMap<TopicPartition, (i64, i64)>,
    ) -> HashMap<String, HashMap<TopicPartition, i64>> {
        let max_concurrent = self.performance.max_concurrent_groups;
        let offset_timeout = self.performance.offset_fetch_timeout;

        debug!(
            groups = descriptions.len(),
            max_concurrent = max_concurrent,
            "Fetching group offsets in parallel via Admin API"
        );

        let semaphore = Arc::new(Semaphore::new(max_concurrent));
        let client = Arc::clone(&self.client);
        let wm_keys: Vec<TopicPartition> = watermarks.keys().cloned().collect();

        let mut handles = Vec::with_capacity(descriptions.len());

        for desc in descriptions {
            let group_id = desc.group_id.clone();
            let permit = semaphore.clone();
            let client_clone = Arc::clone(&client);
            let partitions = wm_keys.clone();
            let timeout = offset_timeout;

            let handle = tokio::spawn(async move {
                let permit_guard: OwnedSemaphorePermit =
                    permit.acquire_owned().await.expect("semaphore closed");

                tokio::task::spawn_blocking(move || {
                    let _permit = permit_guard;

                    let offsets =
                        client_clone.list_consumer_group_offsets(&group_id, &partitions, timeout);

                    (group_id, offsets)
                })
                .await
            });

            handles.push(handle);
        }

        let results = futures::future::join_all(handles).await;

        let mut all_offsets = HashMap::new();
        for result in results {
            match result {
                Ok(Ok((group_id, Ok(offsets)))) => {
                    all_offsets.insert(group_id, offsets);
                }
                Ok(Ok((group_id, Err(e)))) => {
                    warn!(group = group_id, error = %e, "Failed to fetch group offsets");
                    all_offsets.insert(group_id, HashMap::new());
                }
                Ok(Err(e)) => {
                    warn!(error = %e, "Group offset fetch blocking task panicked");
                }
                Err(e) => {
                    warn!(error = %e, "Group offset fetch task panicked");
                }
            }
        }

        all_offsets
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
            timestamp_ms: 0,
        };

        let groups = snapshot.filtered_groups();
        assert_eq!(groups.len(), 2);
        assert!(groups.contains(&"group1"));
        assert!(groups.contains(&"group2"));
    }
}
