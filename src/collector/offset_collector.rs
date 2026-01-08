use crate::config::CompiledFilters;
use crate::error::Result;
use crate::kafka::client::{KafkaClient, TopicPartition};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, instrument, warn};

pub struct OffsetCollector {
    client: Arc<KafkaClient>,
    filters: CompiledFilters,
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
    #[allow(dead_code)]
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
    pub fn new(client: Arc<KafkaClient>, filters: CompiledFilters) -> Self {
        Self { client, filters }
    }

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
        let mut groups = Vec::with_capacity(descriptions.len());
        for desc in descriptions {
            let offsets = self.fetch_group_offsets(&desc.group_id, &watermarks)?;

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

    fn fetch_group_offsets(
        &self,
        group_id: &str,
        watermarks: &HashMap<TopicPartition, (i64, i64)>,
    ) -> Result<HashMap<TopicPartition, i64>> {
        // We need to create a consumer for this specific group to fetch committed offsets
        use rdkafka::config::ClientConfig;
        use rdkafka::consumer::{BaseConsumer, Consumer};
        use rdkafka::TopicPartitionList;

        let _cluster = self.client.cluster_name();
        let mut client_config = ClientConfig::new();

        // Get bootstrap servers from the existing client's metadata
        let metadata = self.client.fetch_metadata()?;

        // Build bootstrap servers from metadata brokers
        let bootstrap_servers: Vec<String> = metadata
            .brokers()
            .iter()
            .map(|b| format!("{}:{}", b.host(), b.port()))
            .collect();

        if bootstrap_servers.is_empty() {
            return Ok(HashMap::new());
        }

        client_config
            .set("bootstrap.servers", bootstrap_servers.join(","))
            .set("group.id", group_id)
            .set("enable.auto.commit", "false");

        let consumer: BaseConsumer = match client_config.create() {
            Ok(c) => c,
            Err(e) => {
                warn!(group = group_id, error = %e, "Failed to create consumer for group");
                return Ok(HashMap::new());
            }
        };

        // Build topic partition list from watermarks
        let mut tpl = TopicPartitionList::new();
        for tp in watermarks.keys() {
            tpl.add_partition(&tp.topic, tp.partition);
        }

        // Fetch committed offsets
        let committed = match consumer.committed_offsets(tpl, std::time::Duration::from_secs(10)) {
            Ok(c) => c,
            Err(e) => {
                warn!(group = group_id, error = %e, "Failed to fetch committed offsets");
                return Ok(HashMap::new());
            }
        };

        let mut offsets = HashMap::new();
        for elem in committed.elements() {
            if let rdkafka::Offset::Offset(offset) = elem.offset() {
                offsets.insert(TopicPartition::new(elem.topic(), elem.partition()), offset);
            }
        }

        Ok(offsets)
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
