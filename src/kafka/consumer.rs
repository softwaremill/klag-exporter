use crate::config::ClusterConfig;
use crate::error::{KlagError, Result};
use crate::kafka::client::TopicPartition;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::Offset;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tracing::{debug, instrument, warn};

/// Result of fetching a timestamp from Kafka
#[derive(Debug, Clone)]
pub struct TimestampFetchResult {
    /// The timestamp in milliseconds
    pub timestamp_ms: i64,
}

/// Factory for creating consumers. Creates a fresh consumer for each fetch
/// to avoid state conflicts when fetching timestamps concurrently.
pub struct TimestampConsumer {
    config: ClusterConfig,
    cluster_name: String,
    fetch_timeout: Duration,
    consumer_counter: AtomicU64,
}

impl TimestampConsumer {
    pub fn new(config: &ClusterConfig) -> Result<Self> {
        Ok(Self {
            config: config.clone(),
            cluster_name: config.name.clone(),
            fetch_timeout: Duration::from_secs(5),
            consumer_counter: AtomicU64::new(0),
        })
    }

    /// Create a fresh consumer for a single fetch operation.
    /// Each consumer gets a unique client.id to avoid broker-side conflicts.
    fn create_consumer(&self) -> Result<BaseConsumer> {
        let counter = self.consumer_counter.fetch_add(1, Ordering::Relaxed);

        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", &self.config.bootstrap_servers)
            .set(
                "client.id",
                format!("klag-exporter-ts-{}-{}", self.config.name, counter),
            )
            .set(
                "group.id",
                format!("klag-exporter-ts-internal-{}", self.config.name),
            )
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            // Small fetch size for timestamp sampling
            .set("fetch.max.bytes", "1048576")
            .set("max.partition.fetch.bytes", "262144");

        for (key, value) in &self.config.consumer_properties {
            client_config.set(key, value);
        }

        client_config.create().map_err(KlagError::Kafka)
    }

    #[instrument(skip(self), fields(cluster = %self.cluster_name, topic = %tp.topic, partition = tp.partition, offset = offset))]
    pub fn fetch_timestamp(
        &self,
        tp: &TopicPartition,
        offset: i64,
    ) -> Result<Option<TimestampFetchResult>> {
        use rdkafka::TopicPartitionList;

        // Create a fresh consumer for this fetch to avoid state conflicts
        let consumer = self.create_consumer()?;

        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(&tp.topic, tp.partition, Offset::Offset(offset))
            .map_err(KlagError::Kafka)?;

        consumer.assign(&tpl).map_err(KlagError::Kafka)?;

        // Poll for message - no seek needed since assign with offset already positions
        match consumer.poll(self.fetch_timeout) {
            Some(result) => match result {
                Ok(msg) => {
                    let timestamp = msg.timestamp().to_millis();

                    debug!(
                        topic = tp.topic,
                        partition = tp.partition,
                        requested_offset = offset,
                        actual_offset = msg.offset(),
                        timestamp = ?timestamp,
                        "Fetched message timestamp"
                    );

                    Ok(timestamp.map(|ts| TimestampFetchResult { timestamp_ms: ts }))
                }
                Err(e) => {
                    warn!(
                        topic = tp.topic,
                        partition = tp.partition,
                        error = %e,
                        "Failed to fetch message"
                    );
                    Err(KlagError::Kafka(e))
                }
            },
            None => {
                debug!(
                    topic = tp.topic,
                    partition = tp.partition,
                    offset = offset,
                    "No message available at offset (may be beyond high watermark)"
                );
                Ok(None)
            }
        }
        // Consumer is dropped here, cleaning up resources
    }

    #[allow(dead_code)]
    pub fn fetch_timestamps_batch(
        &self,
        requests: &[(TopicPartition, i64)],
    ) -> Vec<(TopicPartition, Result<Option<TimestampFetchResult>>)> {
        requests
            .iter()
            .map(|(tp, offset)| {
                let result = self.fetch_timestamp(tp, *offset);
                (tp.clone(), result)
            })
            .collect()
    }
}

impl std::fmt::Debug for TimestampConsumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TimestampConsumer")
            .field("cluster", &self.cluster_name)
            .finish()
    }
}
