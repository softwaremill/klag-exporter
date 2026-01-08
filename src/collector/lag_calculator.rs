use crate::collector::offset_collector::{GroupSnapshot, OffsetsSnapshot};
use crate::kafka::client::TopicPartition;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone)]
pub struct LagMetrics {
    #[allow(dead_code)]
    pub cluster_name: String,
    pub partition_metrics: Vec<PartitionLagMetric>,
    pub group_metrics: Vec<GroupLagMetric>,
    pub topic_metrics: Vec<TopicLagMetric>,
    pub partition_offsets: Vec<PartitionOffsetMetric>,
    pub poll_time_ms: u64,
    /// Number of partitions where log compaction was detected
    pub compaction_detected_count: u64,
    /// Number of partitions where retention deletion was detected
    pub retention_detected_count: u64,
}

#[derive(Debug, Clone)]
pub struct PartitionLagMetric {
    pub cluster_name: String,
    pub group_id: String,
    pub topic: String,
    pub partition: i32,
    pub member_host: String,
    pub consumer_id: String,
    pub client_id: String,
    pub committed_offset: i64,
    pub lag: i64,
    pub lag_seconds: Option<f64>,
    /// Whether compaction was detected for this partition's timestamp fetch
    pub compaction_detected: bool,
    /// Whether retention deletion was detected (committed_offset < low_watermark)
    pub retention_detected: bool,
}

#[derive(Debug, Clone)]
pub struct GroupLagMetric {
    pub cluster_name: String,
    pub group_id: String,
    pub max_lag: i64,
    pub max_lag_seconds: Option<f64>,
    pub sum_lag: i64,
}

#[derive(Debug, Clone)]
pub struct TopicLagMetric {
    pub cluster_name: String,
    pub group_id: String,
    pub topic: String,
    pub sum_lag: i64,
}

#[derive(Debug, Clone)]
pub struct PartitionOffsetMetric {
    pub cluster_name: String,
    pub topic: String,
    pub partition: i32,
    pub earliest_offset: i64,
    pub latest_offset: i64,
}

pub struct LagCalculator;

/// Timestamp data for a partition
#[derive(Debug, Clone)]
pub struct TimestampData {
    pub timestamp_ms: i64,
}

impl LagCalculator {
    pub fn calculate(
        snapshot: &OffsetsSnapshot,
        timestamps: &HashMap<(String, TopicPartition), TimestampData>,
        now_ms: i64,
        poll_time_ms: u64,
        compacted_topics: &HashSet<String>,
    ) -> LagMetrics {
        let mut partition_metrics = Vec::new();
        let mut group_metrics = Vec::new();
        let mut topic_metrics = Vec::new();

        // Partition offset metrics (independent of groups)
        let partition_offsets: Vec<PartitionOffsetMetric> = snapshot
            .watermarks
            .iter()
            .map(|(tp, (low, high))| PartitionOffsetMetric {
                cluster_name: snapshot.cluster_name.clone(),
                topic: tp.topic.clone(),
                partition: tp.partition,
                earliest_offset: *low,
                latest_offset: *high,
            })
            .collect();

        // Process each consumer group
        for group in &snapshot.groups {
            let mut group_max_lag: i64 = 0;
            let mut group_max_lag_seconds: Option<f64> = Some(0.0); // Always emit, default to 0
            let mut group_sum_lag: i64 = 0;
            let mut topic_lags: HashMap<String, i64> = HashMap::new();

            // Build member assignment map for partition -> member lookup
            let member_map = build_member_map(group);

            for (tp, committed_offset) in &group.offsets {
                let (low_watermark, high_watermark) =
                    snapshot.get_watermark(tp).unwrap_or((0, *committed_offset));

                // Calculate lag, clamped to 0 for race conditions
                let lag = (high_watermark - committed_offset).max(0);

                // Detect retention deletion: committed offset is before the low watermark
                let retention_detected = *committed_offset < low_watermark;

                // Look up member info for this partition
                let (member_host, consumer_id, client_id) = member_map
                    .get(tp)
                    .map(|m| {
                        (
                            m.client_host.to_string(),
                            m.member_id.to_string(),
                            m.client_id.to_string(),
                        )
                    })
                    .unwrap_or_else(|| (String::new(), String::new(), String::new()));

                // Calculate time lag if timestamp available
                let ts_data = timestamps.get(&(group.group_id.clone(), tp.clone()));
                let lag_seconds = if lag > 0 {
                    ts_data
                        .map(|td| ((now_ms - td.timestamp_ms) as f64) / 1000.0)
                        .map(|s| s.max(0.0))
                        // For retention-affected partitions without timestamp, still emit metric
                        // so it shows up in dashboards with retention_detected label
                        .or(if retention_detected { Some(0.0) } else { None })
                } else {
                    Some(0.0)
                };
                // Compaction detected if topic has cleanup.policy=compact
                let compaction_detected = compacted_topics.contains(&tp.topic);

                partition_metrics.push(PartitionLagMetric {
                    cluster_name: snapshot.cluster_name.clone(),
                    group_id: group.group_id.clone(),
                    topic: tp.topic.clone(),
                    partition: tp.partition,
                    member_host,
                    consumer_id,
                    client_id,
                    committed_offset: *committed_offset,
                    lag,
                    lag_seconds,
                    compaction_detected,
                    retention_detected,
                });

                // Update aggregates
                group_sum_lag += lag;
                if lag > group_max_lag {
                    group_max_lag = lag;
                }
                // Track max time lag separately - use the max of all available time lags.
                // This ensures max_lag_seconds reflects the worst case we CAN measure,
                // even if the partition with highest offset lag has no timestamp.
                if let Some(secs) = lag_seconds {
                    group_max_lag_seconds =
                        Some(group_max_lag_seconds.map_or(secs, |current| current.max(secs)));
                }

                *topic_lags.entry(tp.topic.clone()).or_insert(0) += lag;
            }

            // Add group-level metrics
            group_metrics.push(GroupLagMetric {
                cluster_name: snapshot.cluster_name.clone(),
                group_id: group.group_id.clone(),
                max_lag: group_max_lag,
                max_lag_seconds: group_max_lag_seconds,
                sum_lag: group_sum_lag,
            });

            // Add topic-level metrics
            for (topic, sum_lag) in topic_lags {
                topic_metrics.push(TopicLagMetric {
                    cluster_name: snapshot.cluster_name.clone(),
                    group_id: group.group_id.clone(),
                    topic,
                    sum_lag,
                });
            }
        }

        // Count compaction and retention detections from partition metrics
        let compaction_detected_count = partition_metrics
            .iter()
            .filter(|m| m.compaction_detected)
            .count() as u64;
        let retention_detected_count = partition_metrics
            .iter()
            .filter(|m| m.retention_detected)
            .count() as u64;

        LagMetrics {
            cluster_name: snapshot.cluster_name.clone(),
            partition_metrics,
            group_metrics,
            topic_metrics,
            partition_offsets,
            poll_time_ms,
            compaction_detected_count,
            retention_detected_count,
        }
    }
}

struct MemberRef<'a> {
    member_id: &'a str,
    client_id: &'a str,
    client_host: &'a str,
}

fn build_member_map(group: &GroupSnapshot) -> HashMap<TopicPartition, MemberRef<'_>> {
    let mut map = HashMap::new();

    for member in &group.members {
        for assignment in &member.assignments {
            map.insert(
                assignment.clone(),
                MemberRef {
                    member_id: &member.member_id,
                    client_id: &member.client_id,
                    client_host: &member.client_host,
                },
            );
        }
    }

    map
}

impl LagMetrics {
    pub fn iter_partition_metrics(&self) -> impl Iterator<Item = &PartitionLagMetric> {
        self.partition_metrics.iter()
    }

    pub fn iter_group_metrics(&self) -> impl Iterator<Item = &GroupLagMetric> {
        self.group_metrics.iter()
    }

    pub fn iter_topic_metrics(&self) -> impl Iterator<Item = &TopicLagMetric> {
        self.topic_metrics.iter()
    }

    pub fn iter_partition_offsets(&self) -> impl Iterator<Item = &PartitionOffsetMetric> {
        self.partition_offsets.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collector::offset_collector::MemberSnapshot;

    fn make_snapshot() -> OffsetsSnapshot {
        let mut watermarks = HashMap::new();
        watermarks.insert(TopicPartition::new("topic1", 0), (0, 100));
        watermarks.insert(TopicPartition::new("topic1", 1), (0, 200));
        watermarks.insert(TopicPartition::new("topic2", 0), (0, 50));

        let mut offsets = HashMap::new();
        offsets.insert(TopicPartition::new("topic1", 0), 90);
        offsets.insert(TopicPartition::new("topic1", 1), 150);
        offsets.insert(TopicPartition::new("topic2", 0), 50);

        OffsetsSnapshot {
            cluster_name: "test-cluster".to_string(),
            groups: vec![GroupSnapshot {
                group_id: "test-group".to_string(),
                state: "Stable".to_string(),
                members: vec![MemberSnapshot {
                    member_id: "member-1".to_string(),
                    client_id: "client-1".to_string(),
                    client_host: "host-1".to_string(),
                    assignments: vec![
                        TopicPartition::new("topic1", 0),
                        TopicPartition::new("topic1", 1),
                    ],
                }],
                offsets,
            }],
            watermarks,
            timestamp_ms: 1000000,
        }
    }

    #[test]
    fn test_lag_calculator_offset_lag() {
        let snapshot = make_snapshot();
        let timestamps = HashMap::new();
        let now_ms = 1000000;

        let metrics =
            LagCalculator::calculate(&snapshot, &timestamps, now_ms, 100, &HashSet::new());

        // topic1 partition 0: 100 - 90 = 10
        // topic1 partition 1: 200 - 150 = 50
        // topic2 partition 0: 50 - 50 = 0
        let p0 = metrics
            .partition_metrics
            .iter()
            .find(|m| m.topic == "topic1" && m.partition == 0)
            .unwrap();
        assert_eq!(p0.lag, 10);

        let p1 = metrics
            .partition_metrics
            .iter()
            .find(|m| m.topic == "topic1" && m.partition == 1)
            .unwrap();
        assert_eq!(p1.lag, 50);

        let p2 = metrics
            .partition_metrics
            .iter()
            .find(|m| m.topic == "topic2" && m.partition == 0)
            .unwrap();
        assert_eq!(p2.lag, 0);
    }

    #[test]
    fn test_lag_calculator_time_lag() {
        let snapshot = make_snapshot();
        let mut timestamps = HashMap::new();
        // Message at offset 90 was produced at time 900000 (100 seconds ago)
        timestamps.insert(
            ("test-group".to_string(), TopicPartition::new("topic1", 0)),
            TimestampData {
                timestamp_ms: 900000,
            },
        );

        let now_ms = 1000000;
        let metrics =
            LagCalculator::calculate(&snapshot, &timestamps, now_ms, 100, &HashSet::new());

        let p0 = metrics
            .partition_metrics
            .iter()
            .find(|m| m.topic == "topic1" && m.partition == 0)
            .unwrap();

        assert_eq!(p0.lag_seconds, Some(100.0));
        assert!(!p0.compaction_detected);
    }

    #[test]
    fn test_lag_calculator_handles_negative_lag() {
        let mut watermarks = HashMap::new();
        watermarks.insert(TopicPartition::new("topic1", 0), (0, 100));

        let mut offsets = HashMap::new();
        // Committed offset > high watermark (race condition)
        offsets.insert(TopicPartition::new("topic1", 0), 110);

        let snapshot = OffsetsSnapshot {
            cluster_name: "test".to_string(),
            groups: vec![GroupSnapshot {
                group_id: "test-group".to_string(),
                state: "Stable".to_string(),
                members: vec![],
                offsets,
            }],
            watermarks,
            timestamp_ms: 0,
        };

        let metrics = LagCalculator::calculate(&snapshot, &HashMap::new(), 0, 100, &HashSet::new());

        let p0 = metrics
            .partition_metrics
            .iter()
            .find(|m| m.partition == 0)
            .unwrap();

        // Lag should be clamped to 0
        assert_eq!(p0.lag, 0);
    }

    #[test]
    fn test_lag_calculator_max_lag_aggregation() {
        let snapshot = make_snapshot();
        let metrics = LagCalculator::calculate(&snapshot, &HashMap::new(), 0, 100, &HashSet::new());

        let group_metric = metrics
            .group_metrics
            .iter()
            .find(|m| m.group_id == "test-group")
            .unwrap();

        // Max lag should be 50 (from topic1 partition 1)
        assert_eq!(group_metric.max_lag, 50);
    }

    #[test]
    fn test_lag_calculator_sum_lag_aggregation() {
        let snapshot = make_snapshot();
        let metrics = LagCalculator::calculate(&snapshot, &HashMap::new(), 0, 100, &HashSet::new());

        let group_metric = metrics
            .group_metrics
            .iter()
            .find(|m| m.group_id == "test-group")
            .unwrap();

        // Sum lag: 10 + 50 + 0 = 60
        assert_eq!(group_metric.sum_lag, 60);
    }

    #[test]
    fn test_topic_sum_lag() {
        let snapshot = make_snapshot();
        let metrics = LagCalculator::calculate(&snapshot, &HashMap::new(), 0, 100, &HashSet::new());

        let topic1_metric = metrics
            .topic_metrics
            .iter()
            .find(|m| m.topic == "topic1")
            .unwrap();

        // topic1 sum lag: 10 + 50 = 60
        assert_eq!(topic1_metric.sum_lag, 60);
    }

    #[test]
    fn test_partition_offset_metrics() {
        let snapshot = make_snapshot();
        let metrics = LagCalculator::calculate(&snapshot, &HashMap::new(), 0, 100, &HashSet::new());

        assert_eq!(metrics.partition_offsets.len(), 3);

        let topic1_p0 = metrics
            .partition_offsets
            .iter()
            .find(|m| m.topic == "topic1" && m.partition == 0)
            .unwrap();

        assert_eq!(topic1_p0.earliest_offset, 0);
        assert_eq!(topic1_p0.latest_offset, 100);
    }
}
