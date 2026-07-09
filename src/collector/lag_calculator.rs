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
    /// Number of partitions where data loss occurred (committed offset < low watermark)
    pub data_loss_partition_count: u64,
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
    /// Whether data loss occurred (`committed_offset` < `low_watermark`)
    pub data_loss_detected: bool,
    /// Number of messages lost to retention (`low_watermark` - `committed_offset` when positive)
    pub messages_lost: i64,
    /// Offset distance to deletion boundary (`committed_offset` - `low_watermark`)
    pub retention_margin: i64,
    /// Percentage of retention window occupied by lag (0=caught up, 100=at boundary, >100=data loss)
    pub lag_retention_ratio: f64,
}

#[derive(Debug, Clone)]
pub struct GroupLagMetric {
    pub cluster_name: String,
    pub group_id: String,
    pub max_lag: i64,
    pub max_lag_seconds: Option<f64>,
    pub sum_lag: i64,
    pub state: i32,
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

/// Encode consumer group state as an integer.
///
/// The string values come from Kafka's `DescribeGroups` response and match
/// `org.apache.kafka.common.ConsumerGroupState` exactly.
///
/// Classic protocol states: `PreparingRebalance`, `CompletingRebalance`, Stable, Dead, Empty
/// KIP-848 protocol states: Assigning, Reconciling, Stable, Dead, Empty
///
/// Mapping:
///   Unknown=0, PreparingRebalance=1, CompletingRebalance=2, Stable=3,
///   Dead=4, Empty=5, Assigning=6, Reconciling=7
pub fn encode_group_state(state: &str) -> i32 {
    match state {
        "PreparingRebalance" => 1,
        "CompletingRebalance" => 2,
        "Stable" => 3,
        "Dead" => 4,
        "Empty" => 5,
        "Assigning" => 6,
        "Reconciling" => 7,
        _ => 0,
    }
}

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
        time_lag_enabled: bool,
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
                topic: tp.topic.to_string(),
                partition: tp.partition,
                earliest_offset: *low,
                latest_offset: *high,
            })
            .collect();

        // Process each consumer group
        for group in &snapshot.groups {
            let mut group_max_lag: i64 = 0;
            let mut group_max_lag_seconds: Option<f64> =
                if time_lag_enabled { Some(0.0) } else { None };
            let mut group_sum_lag: i64 = 0;
            let mut topic_lags: HashMap<String, i64> = HashMap::new();

            // Build member assignment map for partition -> member lookup
            let member_map = build_member_map(group);

            for (tp, committed_offset) in &group.offsets {
                let (low_watermark, high_watermark) =
                    snapshot.get_watermark(tp).unwrap_or((0, *committed_offset));

                // Calculate lag, clamped to 0 for race conditions
                let lag = (high_watermark - committed_offset).max(0);

                // Data loss detection: committed offset is before the low watermark
                let data_loss_detected = *committed_offset < low_watermark;
                let messages_lost = (low_watermark - *committed_offset).max(0);

                // Prevention metrics
                let retention_margin = *committed_offset - low_watermark; // negative if data loss

                let retention_window = high_watermark - low_watermark;
                let lag_retention_ratio = if retention_window > 0 {
                    let current_lag = high_watermark - *committed_offset;
                    (current_lag as f64 / retention_window as f64) * 100.0
                } else {
                    0.0 // empty partition, no ratio
                };

                // Look up member info for this partition
                let (member_host, consumer_id, client_id) = member_map.get(tp).map_or_else(
                    || (String::new(), String::new(), String::new()),
                    |m| {
                        (
                            m.client_host.to_string(),
                            m.member_id.to_string(),
                            m.client_id.to_string(),
                        )
                    },
                );

                // Calculate time lag if timestamp available
                let ts_data = timestamps.get(&(group.group_id.clone(), tp.clone()));
                let lag_seconds = if !time_lag_enabled {
                    None
                } else if lag > 0 {
                    ts_data
                        .map(|td| ((now_ms - td.timestamp_ms) as f64) / 1000.0)
                        .map(|s| s.max(0.0))
                        // For data-loss-affected partitions without timestamp, still emit metric
                        // so it shows up in dashboards with data_loss_detected label
                        .or(if data_loss_detected { Some(0.0) } else { None })
                } else {
                    Some(0.0)
                };
                // Compaction detected if topic has cleanup.policy=compact.
                // `compacted_topics` is a HashSet<String>; deref the Arc<str>
                // to &str so the lookup uses str's Hash/Eq.
                let compaction_detected = compacted_topics.contains(&*tp.topic);

                partition_metrics.push(PartitionLagMetric {
                    cluster_name: snapshot.cluster_name.clone(),
                    group_id: group.group_id.clone(),
                    topic: tp.topic.to_string(),
                    partition: tp.partition,
                    member_host,
                    consumer_id,
                    client_id,
                    committed_offset: *committed_offset,
                    lag,
                    lag_seconds,
                    compaction_detected,
                    data_loss_detected,
                    messages_lost,
                    retention_margin,
                    lag_retention_ratio,
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

                *topic_lags.entry(tp.topic.to_string()).or_insert(0) += lag;
            }

            // Add group-level metrics
            group_metrics.push(GroupLagMetric {
                cluster_name: snapshot.cluster_name.clone(),
                group_id: group.group_id.clone(),
                max_lag: group_max_lag,
                max_lag_seconds: group_max_lag_seconds,
                sum_lag: group_sum_lag,
                state: encode_group_state(&group.state),
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

        // Count compaction and data loss detections from partition metrics
        let compaction_detected_count = partition_metrics
            .iter()
            .filter(|m| m.compaction_detected)
            .count() as u64;
        let data_loss_partition_count = partition_metrics
            .iter()
            .filter(|m| m.data_loss_detected)
            .count() as u64;

        LagMetrics {
            cluster_name: snapshot.cluster_name.clone(),
            partition_metrics,
            group_metrics,
            topic_metrics,
            partition_offsets,
            poll_time_ms,
            compaction_detected_count,
            data_loss_partition_count,
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
            compacted_topics: HashSet::new(),
            timestamp_ms: 1000000,
        }
    }

    #[test]
    fn test_lag_calculator_offset_lag() {
        let snapshot = make_snapshot();
        let timestamps = HashMap::new();
        let now_ms = 1000000;

        let metrics =
            LagCalculator::calculate(&snapshot, &timestamps, now_ms, 100, &HashSet::new(), true);

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
            LagCalculator::calculate(&snapshot, &timestamps, now_ms, 100, &HashSet::new(), true);

        let p0 = metrics
            .partition_metrics
            .iter()
            .find(|m| m.topic == "topic1" && m.partition == 0)
            .unwrap();

        assert_eq!(p0.lag_seconds, Some(100.0));
        assert!(!p0.compaction_detected);
    }

    #[test]
    fn disabled_time_lag_omits_all_lag_seconds() {
        // timestamp_sampling.enabled = false ⇒ seconds-of-lag is never
        // derived. Every partition (caught-up AND lagging) and every group
        // must carry `None` so the `*_lag_seconds` families are omitted
        // rather than emitted as a misleading `0`. make_snapshot() has both
        // lagging (topic1) and caught-up (topic2/0) partitions.
        let snapshot = make_snapshot();
        let metrics =
            LagCalculator::calculate(&snapshot, &HashMap::new(), 0, 100, &HashSet::new(), false);

        assert!(
            metrics
                .partition_metrics
                .iter()
                .all(|m| m.lag_seconds.is_none()),
            "no partition may expose lag_seconds when time-lag sampling is disabled"
        );
        assert!(
            metrics
                .group_metrics
                .iter()
                .all(|m| m.max_lag_seconds.is_none()),
            "no group may expose max_lag_seconds when time-lag sampling is disabled"
        );
        // Offset-lag itself is unaffected — it does not depend on sampling.
        assert!(metrics.partition_metrics.iter().any(|m| m.lag > 0));
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
            compacted_topics: HashSet::new(),
            timestamp_ms: 0,
        };

        let metrics =
            LagCalculator::calculate(&snapshot, &HashMap::new(), 0, 100, &HashSet::new(), true);

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
        let metrics =
            LagCalculator::calculate(&snapshot, &HashMap::new(), 0, 100, &HashSet::new(), true);

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
        let metrics =
            LagCalculator::calculate(&snapshot, &HashMap::new(), 0, 100, &HashSet::new(), true);

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
        let metrics =
            LagCalculator::calculate(&snapshot, &HashMap::new(), 0, 100, &HashSet::new(), true);

        let topic1_metric = metrics
            .topic_metrics
            .iter()
            .find(|m| m.topic == "topic1")
            .unwrap();

        // topic1 sum lag: 10 + 50 = 60
        assert_eq!(topic1_metric.sum_lag, 60);
    }

    #[test]
    fn test_encode_group_state_classic_protocol() {
        assert_eq!(encode_group_state("PreparingRebalance"), 1);
        assert_eq!(encode_group_state("CompletingRebalance"), 2);
        assert_eq!(encode_group_state("Stable"), 3);
        assert_eq!(encode_group_state("Dead"), 4);
        assert_eq!(encode_group_state("Empty"), 5);
    }

    #[test]
    fn test_encode_group_state_kip848_protocol() {
        assert_eq!(encode_group_state("Assigning"), 6);
        assert_eq!(encode_group_state("Reconciling"), 7);
    }

    #[test]
    fn test_encode_group_state_unknown_fallback() {
        assert_eq!(encode_group_state("Unknown"), 0);
        assert_eq!(encode_group_state("SomeFutureState"), 0);
        assert_eq!(encode_group_state(""), 0);
    }

    #[test]
    fn test_lag_calculator_group_state() {
        let snapshot = make_snapshot();
        let metrics =
            LagCalculator::calculate(&snapshot, &HashMap::new(), 0, 100, &HashSet::new(), true);

        let group_metric = metrics
            .group_metrics
            .iter()
            .find(|m| m.group_id == "test-group")
            .unwrap();

        assert_eq!(group_metric.state, 3); // "Stable" -> 3
    }

    #[test]
    fn test_partition_offset_metrics() {
        let snapshot = make_snapshot();
        let metrics =
            LagCalculator::calculate(&snapshot, &HashMap::new(), 0, 100, &HashSet::new(), true);

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
