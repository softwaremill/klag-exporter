use crate::config::OtelConfig;
use crate::error::Result;
use crate::metrics::registry::MetricsRegistry;
use opentelemetry::global;
use opentelemetry::KeyValue;
use opentelemetry_otlp::{MetricExporter, WithExportConfig};
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::Resource;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};

pub async fn run_otel_exporter(
    registry: Arc<MetricsRegistry>,
    config: OtelConfig,
    mut shutdown: broadcast::Receiver<()>,
) -> Result<()> {
    info!(endpoint = %config.endpoint, "Starting OpenTelemetry exporter");

    // Initialize OTLP exporter
    let meter_provider = match init_meter_provider(&config) {
        Ok(provider) => provider,
        Err(e) => {
            error!(error = %e, "Failed to initialize OpenTelemetry meter provider");
            return Err(crate::error::KlagError::Otel(e.to_string()));
        }
    };

    global::set_meter_provider(meter_provider.clone());
    let meter = global::meter("klag-exporter");

    // Create observable gauges for all metrics
    let registry_clone = Arc::clone(&registry);
    let _partition_latest = meter
        .f64_observable_gauge("kafka_partition_latest_offset")
        .with_description("Latest offset for a partition")
        .with_callback({
            let reg = Arc::clone(&registry_clone);
            move |observer| {
                for metric in reg.get_otel_metrics() {
                    if metric.name == "kafka_partition_latest_offset" {
                        for dp in &metric.data_points {
                            let attrs: Vec<KeyValue> = dp
                                .attributes
                                .iter()
                                .map(|(k, v)| KeyValue::new(k.clone(), v.clone()))
                                .collect();
                            observer.observe(dp.value, &attrs);
                        }
                    }
                }
            }
        })
        .build();

    let registry_clone = Arc::clone(&registry);
    let _partition_earliest = meter
        .f64_observable_gauge("kafka_partition_earliest_offset")
        .with_description("Earliest offset for a partition")
        .with_callback({
            let reg = Arc::clone(&registry_clone);
            move |observer| {
                for metric in reg.get_otel_metrics() {
                    if metric.name == "kafka_partition_earliest_offset" {
                        for dp in &metric.data_points {
                            let attrs: Vec<KeyValue> = dp
                                .attributes
                                .iter()
                                .map(|(k, v)| KeyValue::new(k.clone(), v.clone()))
                                .collect();
                            observer.observe(dp.value, &attrs);
                        }
                    }
                }
            }
        })
        .build();

    let registry_clone = Arc::clone(&registry);
    let _group_offset = meter
        .f64_observable_gauge("kafka_consumergroup_group_offset")
        .with_description("Current committed offset of a consumer group for a partition")
        .with_callback({
            let reg = Arc::clone(&registry_clone);
            move |observer| {
                for metric in reg.get_otel_metrics() {
                    if metric.name == "kafka_consumergroup_group_offset" {
                        for dp in &metric.data_points {
                            let attrs: Vec<KeyValue> = dp
                                .attributes
                                .iter()
                                .map(|(k, v)| KeyValue::new(k.clone(), v.clone()))
                                .collect();
                            observer.observe(dp.value, &attrs);
                        }
                    }
                }
            }
        })
        .build();

    let registry_clone = Arc::clone(&registry);
    let _group_lag = meter
        .f64_observable_gauge("kafka_consumergroup_group_lag")
        .with_description("Current lag of a consumer group for a partition")
        .with_callback({
            let reg = Arc::clone(&registry_clone);
            move |observer| {
                for metric in reg.get_otel_metrics() {
                    if metric.name == "kafka_consumergroup_group_lag" {
                        for dp in &metric.data_points {
                            let attrs: Vec<KeyValue> = dp
                                .attributes
                                .iter()
                                .map(|(k, v)| KeyValue::new(k.clone(), v.clone()))
                                .collect();
                            observer.observe(dp.value, &attrs);
                        }
                    }
                }
            }
        })
        .build();

    let registry_clone = Arc::clone(&registry);
    let _group_lag_seconds = meter
        .f64_observable_gauge("kafka_consumergroup_group_lag_seconds")
        .with_description("Time lag in seconds for a consumer group partition")
        .with_callback({
            let reg = Arc::clone(&registry_clone);
            move |observer| {
                for metric in reg.get_otel_metrics() {
                    if metric.name == "kafka_consumergroup_group_lag_seconds" {
                        for dp in &metric.data_points {
                            let attrs: Vec<KeyValue> = dp
                                .attributes
                                .iter()
                                .map(|(k, v)| KeyValue::new(k.clone(), v.clone()))
                                .collect();
                            observer.observe(dp.value, &attrs);
                        }
                    }
                }
            }
        })
        .build();

    let registry_clone = Arc::clone(&registry);
    let _group_max_lag = meter
        .f64_observable_gauge("kafka_consumergroup_group_max_lag")
        .with_description("Maximum lag across all partitions for a consumer group")
        .with_callback({
            let reg = Arc::clone(&registry_clone);
            move |observer| {
                for metric in reg.get_otel_metrics() {
                    if metric.name == "kafka_consumergroup_group_max_lag" {
                        for dp in &metric.data_points {
                            let attrs: Vec<KeyValue> = dp
                                .attributes
                                .iter()
                                .map(|(k, v)| KeyValue::new(k.clone(), v.clone()))
                                .collect();
                            observer.observe(dp.value, &attrs);
                        }
                    }
                }
            }
        })
        .build();

    let registry_clone = Arc::clone(&registry);
    let _group_sum_lag = meter
        .f64_observable_gauge("kafka_consumergroup_group_sum_lag")
        .with_description("Sum of lag across all partitions for a consumer group")
        .with_callback({
            let reg = Arc::clone(&registry_clone);
            move |observer| {
                for metric in reg.get_otel_metrics() {
                    if metric.name == "kafka_consumergroup_group_sum_lag" {
                        for dp in &metric.data_points {
                            let attrs: Vec<KeyValue> = dp
                                .attributes
                                .iter()
                                .map(|(k, v)| KeyValue::new(k.clone(), v.clone()))
                                .collect();
                            observer.observe(dp.value, &attrs);
                        }
                    }
                }
            }
        })
        .build();

    let registry_clone = Arc::clone(&registry);
    let _group_max_lag_seconds = meter
        .f64_observable_gauge("kafka_consumergroup_group_max_lag_seconds")
        .with_description("Maximum time lag in seconds across all partitions")
        .with_callback({
            let reg = Arc::clone(&registry_clone);
            move |observer| {
                for metric in reg.get_otel_metrics() {
                    if metric.name == "kafka_consumergroup_group_max_lag_seconds" {
                        for dp in &metric.data_points {
                            let attrs: Vec<KeyValue> = dp
                                .attributes
                                .iter()
                                .map(|(k, v)| KeyValue::new(k.clone(), v.clone()))
                                .collect();
                            observer.observe(dp.value, &attrs);
                        }
                    }
                }
            }
        })
        .build();

    let registry_clone = Arc::clone(&registry);
    let _group_topic_sum_lag = meter
        .f64_observable_gauge("kafka_consumergroup_group_topic_sum_lag")
        .with_description("Sum of lag for a consumer group per topic")
        .with_callback({
            let reg = Arc::clone(&registry_clone);
            move |observer| {
                for metric in reg.get_otel_metrics() {
                    if metric.name == "kafka_consumergroup_group_topic_sum_lag" {
                        for dp in &metric.data_points {
                            let attrs: Vec<KeyValue> = dp
                                .attributes
                                .iter()
                                .map(|(k, v)| KeyValue::new(k.clone(), v.clone()))
                                .collect();
                            observer.observe(dp.value, &attrs);
                        }
                    }
                }
            }
        })
        .build();

    let registry_clone = Arc::clone(&registry);
    let _poll_time = meter
        .f64_observable_gauge("kafka_consumergroup_poll_time_ms")
        .with_description("Time taken to collect consumer group metrics in milliseconds")
        .with_callback({
            let reg = Arc::clone(&registry_clone);
            move |observer| {
                for metric in reg.get_otel_metrics() {
                    if metric.name == "kafka_consumergroup_poll_time_ms" {
                        for dp in &metric.data_points {
                            let attrs: Vec<KeyValue> = dp
                                .attributes
                                .iter()
                                .map(|(k, v)| KeyValue::new(k.clone(), v.clone()))
                                .collect();
                            observer.observe(dp.value, &attrs);
                        }
                    }
                }
            }
        })
        .build();

    let registry_clone = Arc::clone(&registry);
    let _scrape_duration = meter
        .f64_observable_gauge("kafka_lag_exporter_scrape_duration_seconds")
        .with_description("Duration of the last metrics collection in seconds")
        .with_callback({
            let reg = Arc::clone(&registry_clone);
            move |observer| {
                for metric in reg.get_otel_metrics() {
                    if metric.name == "kafka_lag_exporter_scrape_duration_seconds" {
                        for dp in &metric.data_points {
                            let attrs: Vec<KeyValue> = dp
                                .attributes
                                .iter()
                                .map(|(k, v)| KeyValue::new(k.clone(), v.clone()))
                                .collect();
                            observer.observe(dp.value, &attrs);
                        }
                    }
                }
            }
        })
        .build();

    let _exporter_up = meter
        .f64_observable_gauge("kafka_lag_exporter_up")
        .with_description("Whether the Kafka lag exporter is healthy")
        .with_callback({
            let reg = Arc::clone(&registry);
            move |observer| {
                for metric in reg.get_otel_metrics() {
                    if metric.name == "kafka_lag_exporter_up" {
                        for dp in &metric.data_points {
                            let attrs: Vec<KeyValue> = dp
                                .attributes
                                .iter()
                                .map(|(k, v)| KeyValue::new(k.clone(), v.clone()))
                                .collect();
                            observer.observe(dp.value, &attrs);
                        }
                    }
                }
            }
        })
        .build();

    let registry_clone = Arc::clone(&registry);
    let _messages_lost = meter
        .f64_observable_gauge("kafka_consumergroup_group_messages_lost")
        .with_description("Number of messages deleted by retention before consumer processed them")
        .with_callback({
            let reg = Arc::clone(&registry_clone);
            move |observer| {
                for metric in reg.get_otel_metrics() {
                    if metric.name == "kafka_consumergroup_group_messages_lost" {
                        for dp in &metric.data_points {
                            let attrs: Vec<KeyValue> = dp
                                .attributes
                                .iter()
                                .map(|(k, v)| KeyValue::new(k.clone(), v.clone()))
                                .collect();
                            observer.observe(dp.value, &attrs);
                        }
                    }
                }
            }
        })
        .build();

    let registry_clone = Arc::clone(&registry);
    let _retention_margin = meter
        .f64_observable_gauge("kafka_consumergroup_group_retention_margin")
        .with_description("Offset distance between consumer position and deletion boundary")
        .with_callback({
            let reg = Arc::clone(&registry_clone);
            move |observer| {
                for metric in reg.get_otel_metrics() {
                    if metric.name == "kafka_consumergroup_group_retention_margin" {
                        for dp in &metric.data_points {
                            let attrs: Vec<KeyValue> = dp
                                .attributes
                                .iter()
                                .map(|(k, v)| KeyValue::new(k.clone(), v.clone()))
                                .collect();
                            observer.observe(dp.value, &attrs);
                        }
                    }
                }
            }
        })
        .build();

    let registry_clone = Arc::clone(&registry);
    let _lag_retention_ratio = meter
        .f64_observable_gauge("kafka_consumergroup_group_lag_retention_ratio")
        .with_description("Percentage of retention window occupied by consumer lag")
        .with_callback({
            let reg = Arc::clone(&registry_clone);
            move |observer| {
                for metric in reg.get_otel_metrics() {
                    if metric.name == "kafka_consumergroup_group_lag_retention_ratio" {
                        for dp in &metric.data_points {
                            let attrs: Vec<KeyValue> = dp
                                .attributes
                                .iter()
                                .map(|(k, v)| KeyValue::new(k.clone(), v.clone()))
                                .collect();
                            observer.observe(dp.value, &attrs);
                        }
                    }
                }
            }
        })
        .build();

    let _data_loss_partitions = meter
        .f64_observable_gauge("kafka_lag_exporter_data_loss_partitions_total")
        .with_description("Number of partitions where data loss occurred")
        .with_callback({
            let reg = Arc::clone(&registry);
            move |observer| {
                for metric in reg.get_otel_metrics() {
                    if metric.name == "kafka_lag_exporter_data_loss_partitions_total" {
                        for dp in &metric.data_points {
                            let attrs: Vec<KeyValue> = dp
                                .attributes
                                .iter()
                                .map(|(k, v)| KeyValue::new(k.clone(), v.clone()))
                                .collect();
                            observer.observe(dp.value, &attrs);
                        }
                    }
                }
            }
        })
        .build();

    info!("OpenTelemetry metrics registered, waiting for shutdown...");

    // Wait for shutdown signal
    let _ = shutdown.recv().await;
    info!("OpenTelemetry exporter shutting down");

    // Shutdown meter provider
    if let Err(e) = meter_provider.shutdown() {
        warn!(error = %e, "Failed to shutdown OpenTelemetry meter provider");
    }

    Ok(())
}

fn init_meter_provider(
    config: &OtelConfig,
) -> std::result::Result<SdkMeterProvider, Box<dyn std::error::Error + Send + Sync>> {
    use opentelemetry_sdk::metrics::PeriodicReader;
    use opentelemetry_sdk::runtime;

    // Build the exporter
    let exporter = MetricExporter::builder()
        .with_tonic()
        .with_endpoint(&config.endpoint)
        .with_timeout(Duration::from_secs(10))
        .build()?;

    // Create periodic reader
    let reader = PeriodicReader::builder(exporter, runtime::Tokio)
        .with_interval(config.export_interval)
        .build();

    // Build the provider
    let provider = SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(Resource::new(vec![KeyValue::new(
            "service.name",
            "klag-exporter",
        )]))
        .build();

    debug!(endpoint = %config.endpoint, "OpenTelemetry meter provider initialized");
    Ok(provider)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: Integration test for OTel exporter requires a running collector
    // This unit test just verifies the registry can be queried without panics
    #[test]
    fn test_otel_metrics_from_registry() {
        let registry = MetricsRegistry::new();
        registry.set_healthy(true);
        registry.set_scrape_duration_ms(100);

        let metrics = registry.get_otel_metrics();

        // Should have at least the up and scrape duration metrics
        assert!(!metrics.is_empty());

        let metric_names: Vec<_> = metrics.iter().map(|m| m.name.as_str()).collect();
        assert!(metric_names.contains(&"kafka_lag_exporter_up"));
        assert!(metric_names.contains(&"kafka_lag_exporter_scrape_duration_seconds"));
    }
}
