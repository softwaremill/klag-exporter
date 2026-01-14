use crate::metrics::registry::MetricsRegistry;
use std::sync::Arc;

pub struct PrometheusExporter {
    registry: Arc<MetricsRegistry>,
}

impl PrometheusExporter {
    pub fn new(registry: Arc<MetricsRegistry>) -> Self {
        Self { registry }
    }

    pub fn render_metrics(&self) -> String {
        self.registry.render_prometheus()
    }
}

impl Clone for PrometheusExporter {
    fn clone(&self) -> Self {
        Self {
            registry: Arc::clone(&self.registry),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collector::lag_calculator::{
        GroupLagMetric, LagMetrics, PartitionLagMetric, PartitionOffsetMetric, TopicLagMetric,
    };

    #[test]
    fn test_prometheus_exporter_render() {
        let registry = Arc::new(MetricsRegistry::new());

        let metrics = LagMetrics {
            cluster_name: "test".to_string(),
            partition_metrics: vec![PartitionLagMetric {
                cluster_name: "test".to_string(),
                group_id: "group1".to_string(),
                topic: "topic1".to_string(),
                partition: 0,
                member_host: "host1".to_string(),
                consumer_id: "consumer1".to_string(),
                client_id: "client1".to_string(),
                committed_offset: 100,
                lag: 10,
                lag_seconds: Some(5.0),
                compaction_detected: false,
                data_loss_detected: false,
                messages_lost: 0,
                retention_margin: 100,
                lag_retention_ratio: 9.09,
            }],
            group_metrics: vec![GroupLagMetric {
                cluster_name: "test".to_string(),
                group_id: "group1".to_string(),
                max_lag: 10,
                max_lag_seconds: Some(5.0),
                sum_lag: 10,
            }],
            topic_metrics: vec![TopicLagMetric {
                cluster_name: "test".to_string(),
                group_id: "group1".to_string(),
                topic: "topic1".to_string(),
                sum_lag: 10,
            }],
            partition_offsets: vec![PartitionOffsetMetric {
                cluster_name: "test".to_string(),
                topic: "topic1".to_string(),
                partition: 0,
                earliest_offset: 0,
                latest_offset: 110,
            }],
            poll_time_ms: 100,
            compaction_detected_count: 0,
            data_loss_partition_count: 0,
        };

        registry.update("test", metrics);

        let exporter = PrometheusExporter::new(registry);
        let output = exporter.render_metrics();

        assert!(output.contains("kafka_consumergroup_group_lag"));
        assert!(output.contains("kafka_consumergroup_group_lag_seconds"));
        assert!(output.contains("kafka_partition_latest_offset"));
    }
}
