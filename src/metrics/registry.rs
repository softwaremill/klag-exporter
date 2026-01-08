use crate::collector::lag_calculator::LagMetrics;
use crate::config::Granularity;
use crate::metrics::definitions::{
    HELP_COMPACTION_DETECTED, HELP_GROUP_LAG, HELP_GROUP_LAG_SECONDS, HELP_GROUP_MAX_LAG,
    HELP_GROUP_MAX_LAG_SECONDS, HELP_GROUP_OFFSET, HELP_GROUP_SUM_LAG, HELP_GROUP_TOPIC_SUM_LAG,
    HELP_LAST_UPDATE_TIMESTAMP, HELP_PARTITION_EARLIEST_OFFSET, HELP_PARTITION_LATEST_OFFSET,
    HELP_POLL_TIME_MS, HELP_RETENTION_DETECTED, HELP_SCRAPE_DURATION_SECONDS, HELP_UP,
    LABEL_CLIENT_ID, LABEL_CLUSTER_NAME, LABEL_COMPACTION_DETECTED, LABEL_CONSUMER_ID, LABEL_GROUP,
    LABEL_MEMBER_HOST, LABEL_PARTITION, LABEL_RETENTION_DETECTED, LABEL_TOPIC,
    METRIC_COMPACTION_DETECTED, METRIC_GROUP_LAG, METRIC_GROUP_LAG_SECONDS, METRIC_GROUP_MAX_LAG,
    METRIC_GROUP_MAX_LAG_SECONDS, METRIC_GROUP_OFFSET, METRIC_GROUP_SUM_LAG,
    METRIC_GROUP_TOPIC_SUM_LAG, METRIC_LAST_UPDATE_TIMESTAMP, METRIC_PARTITION_EARLIEST_OFFSET,
    METRIC_PARTITION_LATEST_OFFSET, METRIC_POLL_TIME_MS, METRIC_RETENTION_DETECTED,
    METRIC_SCRAPE_DURATION_SECONDS, METRIC_UP,
};
use crate::metrics::types::{Labels, MetricPoint, OtelDataPoint, OtelMetric};
use dashmap::DashMap;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Default staleness threshold: 3x the typical poll interval
const DEFAULT_STALENESS_THRESHOLD: Duration = Duration::from_secs(90);

pub struct MetricsRegistry {
    metrics: DashMap<String, Vec<MetricPoint>>,
    last_update: DashMap<String, Instant>,
    last_update_timestamp: DashMap<String, u64>, // Unix timestamp in seconds
    healthy: AtomicBool,
    last_scrape_duration_ms: AtomicU64,
    staleness_threshold: Duration,
}

impl MetricsRegistry {
    pub fn new() -> Self {
        Self::with_staleness_threshold(DEFAULT_STALENESS_THRESHOLD)
    }

    pub fn with_staleness_threshold(staleness_threshold: Duration) -> Self {
        Self {
            metrics: DashMap::new(),
            last_update: DashMap::new(),
            last_update_timestamp: DashMap::new(),
            healthy: AtomicBool::new(true),
            last_scrape_duration_ms: AtomicU64::new(0),
            staleness_threshold,
        }
    }

    /// Update metrics with default options (partition granularity, no custom labels)
    #[allow(dead_code)]
    pub fn update(&self, cluster: &str, lag_metrics: LagMetrics) {
        self.update_with_options(
            cluster,
            lag_metrics,
            Granularity::Partition,
            &HashMap::new(),
        )
    }

    /// Update metrics with granularity control and custom labels
    pub fn update_with_options(
        &self,
        cluster: &str,
        lag_metrics: LagMetrics,
        granularity: Granularity,
        custom_labels: &HashMap<String, String>,
    ) {
        let mut points = Vec::new();

        // Helper to add custom labels to a labels map
        let add_custom_labels = |labels: &mut Labels| {
            for (k, v) in custom_labels {
                labels.insert(k.clone(), v.clone());
            }
        };

        // Partition offset metrics (always at partition level)
        for m in lag_metrics.iter_partition_offsets() {
            let mut labels = Labels::new();
            labels.insert(LABEL_CLUSTER_NAME.to_string(), m.cluster_name.clone());
            labels.insert(LABEL_TOPIC.to_string(), m.topic.clone());
            labels.insert(LABEL_PARTITION.to_string(), m.partition.to_string());
            add_custom_labels(&mut labels);

            points.push(MetricPoint::gauge(
                METRIC_PARTITION_LATEST_OFFSET,
                labels.clone(),
                m.latest_offset as f64,
                HELP_PARTITION_LATEST_OFFSET,
            ));

            points.push(MetricPoint::gauge(
                METRIC_PARTITION_EARLIEST_OFFSET,
                labels,
                m.earliest_offset as f64,
                HELP_PARTITION_EARLIEST_OFFSET,
            ));
        }

        // Consumer group metrics - respect granularity setting
        match granularity {
            Granularity::Partition => {
                // Output partition-level consumer group metrics
                for m in lag_metrics.iter_partition_metrics() {
                    let mut labels = Labels::new();
                    labels.insert(LABEL_CLUSTER_NAME.to_string(), m.cluster_name.clone());
                    labels.insert(LABEL_GROUP.to_string(), m.group_id.clone());
                    labels.insert(LABEL_TOPIC.to_string(), m.topic.clone());
                    labels.insert(LABEL_PARTITION.to_string(), m.partition.to_string());
                    labels.insert(LABEL_MEMBER_HOST.to_string(), m.member_host.clone());
                    labels.insert(LABEL_CONSUMER_ID.to_string(), m.consumer_id.clone());
                    labels.insert(LABEL_CLIENT_ID.to_string(), m.client_id.clone());
                    add_custom_labels(&mut labels);

                    points.push(MetricPoint::gauge(
                        METRIC_GROUP_OFFSET,
                        labels.clone(),
                        m.committed_offset as f64,
                        HELP_GROUP_OFFSET,
                    ));

                    points.push(MetricPoint::gauge(
                        METRIC_GROUP_LAG,
                        labels.clone(),
                        m.lag as f64,
                        HELP_GROUP_LAG,
                    ));

                    if let Some(lag_seconds) = m.lag_seconds {
                        // Add compaction_detected and retention_detected labels to lag_seconds metric
                        labels.insert(
                            LABEL_COMPACTION_DETECTED.to_string(),
                            m.compaction_detected.to_string(),
                        );
                        labels.insert(
                            LABEL_RETENTION_DETECTED.to_string(),
                            m.retention_detected.to_string(),
                        );
                        points.push(MetricPoint::gauge(
                            METRIC_GROUP_LAG_SECONDS,
                            labels,
                            lag_seconds,
                            HELP_GROUP_LAG_SECONDS,
                        ));
                    }
                }
            }
            Granularity::Topic => {
                // Skip partition-level metrics, only output topic aggregates
            }
        }

        // Group-level aggregate metrics (always output)
        for m in lag_metrics.iter_group_metrics() {
            let mut labels = Labels::new();
            labels.insert(LABEL_CLUSTER_NAME.to_string(), m.cluster_name.clone());
            labels.insert(LABEL_GROUP.to_string(), m.group_id.clone());
            add_custom_labels(&mut labels);

            points.push(MetricPoint::gauge(
                METRIC_GROUP_MAX_LAG,
                labels.clone(),
                m.max_lag as f64,
                HELP_GROUP_MAX_LAG,
            ));

            points.push(MetricPoint::gauge(
                METRIC_GROUP_SUM_LAG,
                labels.clone(),
                m.sum_lag as f64,
                HELP_GROUP_SUM_LAG,
            ));

            if let Some(max_lag_seconds) = m.max_lag_seconds {
                points.push(MetricPoint::gauge(
                    METRIC_GROUP_MAX_LAG_SECONDS,
                    labels,
                    max_lag_seconds,
                    HELP_GROUP_MAX_LAG_SECONDS,
                ));
            }
        }

        // Topic-level metrics (always output)
        for m in lag_metrics.iter_topic_metrics() {
            let mut labels = Labels::new();
            labels.insert(LABEL_CLUSTER_NAME.to_string(), m.cluster_name.clone());
            labels.insert(LABEL_GROUP.to_string(), m.group_id.clone());
            labels.insert(LABEL_TOPIC.to_string(), m.topic.clone());
            add_custom_labels(&mut labels);

            points.push(MetricPoint::gauge(
                METRIC_GROUP_TOPIC_SUM_LAG,
                labels,
                m.sum_lag as f64,
                HELP_GROUP_TOPIC_SUM_LAG,
            ));
        }

        // Poll time metric
        let mut poll_labels = Labels::new();
        poll_labels.insert(LABEL_CLUSTER_NAME.to_string(), cluster.to_string());
        add_custom_labels(&mut poll_labels);
        points.push(MetricPoint::gauge(
            METRIC_POLL_TIME_MS,
            poll_labels.clone(),
            lag_metrics.poll_time_ms as f64,
            HELP_POLL_TIME_MS,
        ));

        // Compaction detected metric
        points.push(MetricPoint::gauge(
            METRIC_COMPACTION_DETECTED,
            poll_labels.clone(),
            lag_metrics.compaction_detected_count as f64,
            HELP_COMPACTION_DETECTED,
        ));

        // Retention detected metric
        points.push(MetricPoint::gauge(
            METRIC_RETENTION_DETECTED,
            poll_labels,
            lag_metrics.retention_detected_count as f64,
            HELP_RETENTION_DETECTED,
        ));

        // Store metrics and update timestamps
        let now = Instant::now();
        let unix_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        self.metrics.insert(cluster.to_string(), points);
        self.last_update.insert(cluster.to_string(), now);
        self.last_update_timestamp
            .insert(cluster.to_string(), unix_timestamp);
    }

    pub fn set_healthy(&self, healthy: bool) {
        self.healthy.store(healthy, Ordering::SeqCst);
    }

    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::SeqCst)
    }

    pub fn set_scrape_duration_ms(&self, duration_ms: u64) {
        self.last_scrape_duration_ms
            .store(duration_ms, Ordering::SeqCst);
    }

    pub fn get_scrape_duration_seconds(&self) -> f64 {
        self.last_scrape_duration_ms.load(Ordering::SeqCst) as f64 / 1000.0
    }

    /// Check if any cluster's data is stale (older than staleness_threshold)
    #[allow(dead_code)]
    pub fn is_data_stale(&self) -> bool {
        let now = Instant::now();
        for entry in self.last_update.iter() {
            if now.duration_since(*entry.value()) > self.staleness_threshold {
                return true;
            }
        }
        false
    }

    /// Get the age of the oldest data across all clusters
    #[allow(dead_code)]
    pub fn get_max_data_age(&self) -> Option<Duration> {
        let now = Instant::now();
        self.last_update
            .iter()
            .map(|entry| now.duration_since(*entry.value()))
            .max()
    }

    pub fn render_prometheus(&self) -> String {
        self.render_prometheus_with_staleness_check(true)
    }

    /// Render Prometheus metrics, optionally filtering out stale clusters
    pub fn render_prometheus_with_staleness_check(&self, filter_stale: bool) -> String {
        let mut output = String::new();
        let mut seen_metrics: HashSet<String> = HashSet::new();
        let now = Instant::now();

        // Collect all points, optionally filtering out stale clusters
        let all_points: Vec<MetricPoint> = self
            .metrics
            .iter()
            .filter(|entry| {
                if !filter_stale {
                    return true;
                }
                // Check if this cluster's data is fresh
                if let Some(last_update) = self.last_update.get(entry.key()) {
                    now.duration_since(*last_update) <= self.staleness_threshold
                } else {
                    false // No timestamp means stale
                }
            })
            .flat_map(|entry| entry.value().clone())
            .collect();

        // Group metrics by name for proper HELP/TYPE output
        let mut by_name: HashMap<String, Vec<MetricPoint>> = HashMap::new();
        for point in all_points {
            by_name.entry(point.name.clone()).or_default().push(point);
        }

        // Sort metric names for consistent output
        let mut names: Vec<_> = by_name.keys().cloned().collect();
        names.sort();

        for name in names {
            let points = &by_name[&name];
            if points.is_empty() {
                continue;
            }

            // Output HELP and TYPE once per metric
            if !seen_metrics.contains(&name) {
                let first = &points[0];
                output.push_str(&format!("# HELP {} {}\n", name, first.help));
                output.push_str(&format!("# TYPE {} {}\n", name, first.metric_type.as_str()));
                seen_metrics.insert(name.clone());
            }

            // Output all data points
            for point in points {
                let labels_str = render_labels(&point.labels);
                output.push_str(&format!(
                    "{}{} {}\n",
                    point.name,
                    labels_str,
                    point.value.as_f64()
                ));
            }
        }

        // Add scrape duration metric
        let scrape_duration = self.get_scrape_duration_seconds();
        output.push_str(&format!(
            "# HELP {} {}\n",
            METRIC_SCRAPE_DURATION_SECONDS, HELP_SCRAPE_DURATION_SECONDS
        ));
        output.push_str(&format!(
            "# TYPE {} gauge\n",
            METRIC_SCRAPE_DURATION_SECONDS
        ));
        output.push_str(&format!(
            "{} {:.6}\n",
            METRIC_SCRAPE_DURATION_SECONDS, scrape_duration
        ));

        // Add exporter health metric
        output.push_str(&format!("# HELP {} {}\n", METRIC_UP, HELP_UP));
        output.push_str(&format!("# TYPE {} gauge\n", METRIC_UP));
        output.push_str(&format!(
            "{} {}\n",
            METRIC_UP,
            if self.is_healthy() { 1 } else { 0 }
        ));

        // Add last update timestamp metric per cluster
        if !self.last_update_timestamp.is_empty() {
            output.push_str(&format!(
                "# HELP {} {}\n",
                METRIC_LAST_UPDATE_TIMESTAMP, HELP_LAST_UPDATE_TIMESTAMP
            ));
            output.push_str(&format!("# TYPE {} gauge\n", METRIC_LAST_UPDATE_TIMESTAMP));
            for entry in self.last_update_timestamp.iter() {
                let cluster = entry.key();
                let timestamp = entry.value();
                // Only include non-stale clusters if filtering is enabled
                if filter_stale {
                    if let Some(last_update) = self.last_update.get(cluster) {
                        if now.duration_since(*last_update) > self.staleness_threshold {
                            continue;
                        }
                    }
                }
                output.push_str(&format!(
                    "{}{{{}=\"{}\"}} {}\n",
                    METRIC_LAST_UPDATE_TIMESTAMP, LABEL_CLUSTER_NAME, cluster, timestamp
                ));
            }
        }

        output
    }

    pub fn get_otel_metrics(&self) -> Vec<OtelMetric> {
        let mut otel_metrics: HashMap<String, OtelMetric> = HashMap::new();
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        for entry in self.metrics.iter() {
            for point in entry.value() {
                let metric = otel_metrics
                    .entry(point.name.clone())
                    .or_insert_with(|| OtelMetric {
                        name: point.name.clone(),
                        description: point.help.to_string(),
                        unit: "1".to_string(),
                        data_points: Vec::new(),
                    });

                metric.data_points.push(OtelDataPoint {
                    attributes: point.labels.clone(),
                    value: point.value.as_f64(),
                    timestamp_ms: now_ms,
                });
            }
        }

        // Add scrape duration metric
        let scrape_duration = self.get_scrape_duration_seconds();
        otel_metrics.insert(
            METRIC_SCRAPE_DURATION_SECONDS.to_string(),
            OtelMetric {
                name: METRIC_SCRAPE_DURATION_SECONDS.to_string(),
                description: HELP_SCRAPE_DURATION_SECONDS.to_string(),
                unit: "s".to_string(),
                data_points: vec![OtelDataPoint {
                    attributes: HashMap::new(),
                    value: scrape_duration,
                    timestamp_ms: now_ms,
                }],
            },
        );

        // Add up metric
        otel_metrics.insert(
            METRIC_UP.to_string(),
            OtelMetric {
                name: METRIC_UP.to_string(),
                description: HELP_UP.to_string(),
                unit: "1".to_string(),
                data_points: vec![OtelDataPoint {
                    attributes: HashMap::new(),
                    value: if self.is_healthy() { 1.0 } else { 0.0 },
                    timestamp_ms: now_ms,
                }],
            },
        );

        otel_metrics.into_values().collect()
    }

    pub fn remove_cluster(&self, cluster: &str) {
        self.metrics.remove(cluster);
        self.last_update.remove(cluster);
        self.last_update_timestamp.remove(cluster);
    }

    pub fn cluster_count(&self) -> usize {
        self.metrics.len()
    }
}

fn render_labels(labels: &Labels) -> String {
    if labels.is_empty() {
        return String::new();
    }

    let mut pairs: Vec<_> = labels.iter().collect();
    pairs.sort_by_key(|(k, _)| *k);

    let label_str = pairs
        .into_iter()
        .map(|(k, v)| format!("{}=\"{}\"", k, escape_label_value(v)))
        .collect::<Vec<_>>()
        .join(",");

    format!("{{{}}}", label_str)
}

fn escape_label_value(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collector::lag_calculator::{
        GroupLagMetric, PartitionLagMetric, PartitionOffsetMetric, TopicLagMetric,
    };

    fn make_lag_metrics() -> LagMetrics {
        LagMetrics {
            cluster_name: "test-cluster".to_string(),
            partition_metrics: vec![PartitionLagMetric {
                cluster_name: "test-cluster".to_string(),
                group_id: "test-group".to_string(),
                topic: "test-topic".to_string(),
                partition: 0,
                member_host: "host1".to_string(),
                consumer_id: "consumer-1".to_string(),
                client_id: "client-1".to_string(),
                committed_offset: 100,
                lag: 10,
                lag_seconds: Some(5.5),
                compaction_detected: false,
                retention_detected: false,
            }],
            group_metrics: vec![GroupLagMetric {
                cluster_name: "test-cluster".to_string(),
                group_id: "test-group".to_string(),
                max_lag: 10,
                max_lag_seconds: Some(5.5),
                sum_lag: 10,
            }],
            topic_metrics: vec![TopicLagMetric {
                cluster_name: "test-cluster".to_string(),
                group_id: "test-group".to_string(),
                topic: "test-topic".to_string(),
                sum_lag: 10,
            }],
            partition_offsets: vec![PartitionOffsetMetric {
                cluster_name: "test-cluster".to_string(),
                topic: "test-topic".to_string(),
                partition: 0,
                earliest_offset: 0,
                latest_offset: 110,
            }],
            poll_time_ms: 50,
            compaction_detected_count: 0,
            retention_detected_count: 0,
        }
    }

    #[test]
    fn test_metrics_registry_update_replaces() {
        let registry = MetricsRegistry::new();

        let metrics1 = make_lag_metrics();
        registry.update("test-cluster", metrics1);

        let output1 = registry.render_prometheus();
        assert!(output1.contains("kafka_consumergroup_group_lag"));

        // Update with new metrics
        let mut metrics2 = make_lag_metrics();
        metrics2.partition_metrics[0].lag = 20;
        registry.update("test-cluster", metrics2);

        let output2 = registry.render_prometheus();
        assert!(output2.contains("20")); // New lag value
    }

    #[test]
    fn test_prometheus_format_gauge() {
        let registry = MetricsRegistry::new();
        let metrics = make_lag_metrics();
        registry.update("test-cluster", metrics);

        let output = registry.render_prometheus();

        assert!(output.contains("# TYPE kafka_consumergroup_group_lag gauge"));
        assert!(output.contains("kafka_consumergroup_group_lag{"));
    }

    #[test]
    fn test_prometheus_format_labels() {
        let registry = MetricsRegistry::new();
        let metrics = make_lag_metrics();
        registry.update("test-cluster", metrics);

        let output = registry.render_prometheus();

        assert!(output.contains("cluster_name=\"test-cluster\""));
        assert!(output.contains("group=\"test-group\""));
        assert!(output.contains("topic=\"test-topic\""));
    }

    #[test]
    fn test_escape_label_value() {
        assert_eq!(escape_label_value("simple"), "simple");
        assert_eq!(escape_label_value("with\"quote"), "with\\\"quote");
        assert_eq!(escape_label_value("with\\backslash"), "with\\\\backslash");
        assert_eq!(escape_label_value("with\nnewline"), "with\\nnewline");
    }

    #[test]
    fn test_metrics_registry_evicts_disappeared_groups() {
        let registry = MetricsRegistry::new();

        let metrics = make_lag_metrics();
        registry.update("cluster1", metrics);

        assert_eq!(registry.cluster_count(), 1);

        registry.remove_cluster("cluster1");
        assert_eq!(registry.cluster_count(), 0);
    }

    #[test]
    fn test_up_metric() {
        let registry = MetricsRegistry::new();
        registry.set_healthy(true);

        let output = registry.render_prometheus();
        assert!(output.contains("kafka_lag_exporter_up 1"));

        registry.set_healthy(false);
        let output = registry.render_prometheus();
        assert!(output.contains("kafka_lag_exporter_up 0"));
    }

    #[test]
    fn test_scrape_duration_metric() {
        let registry = MetricsRegistry::new();
        registry.set_scrape_duration_ms(1500);

        let output = registry.render_prometheus();
        assert!(output.contains("kafka_lag_exporter_scrape_duration_seconds"));
        assert!(output.contains("1.5"));
    }

    #[test]
    fn test_granularity_topic_skips_partition_metrics() {
        let registry = MetricsRegistry::new();
        let metrics = make_lag_metrics();

        registry.update_with_options("test-cluster", metrics, Granularity::Topic, &HashMap::new());

        let output = registry.render_prometheus();

        // Should have topic-level metrics
        assert!(output.contains("kafka_consumergroup_group_max_lag"));
        assert!(output.contains("kafka_consumergroup_group_topic_sum_lag"));

        // Should NOT have partition-level consumer group metrics
        assert!(!output.contains("kafka_consumergroup_group_lag{"));
        assert!(!output.contains("kafka_consumergroup_group_offset{"));
    }

    #[test]
    fn test_custom_labels_added() {
        let registry = MetricsRegistry::new();
        let metrics = make_lag_metrics();

        let mut custom_labels = HashMap::new();
        custom_labels.insert("environment".to_string(), "production".to_string());
        custom_labels.insert("datacenter".to_string(), "us-west-2".to_string());

        registry.update_with_options(
            "test-cluster",
            metrics,
            Granularity::Partition,
            &custom_labels,
        );

        let output = registry.render_prometheus();

        assert!(output.contains("environment=\"production\""));
        assert!(output.contains("datacenter=\"us-west-2\""));
    }
}
