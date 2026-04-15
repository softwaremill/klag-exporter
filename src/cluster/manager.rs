use crate::collector::lag_calculator::{LagCalculator, TimestampData};
use crate::collector::offset_collector::OffsetCollector;
use crate::collector::timestamp_sampler::TimestampSampler;
use crate::config::{ClusterConfig, ExporterConfig, Granularity};
use crate::error::Result;
use crate::kafka::client::{KafkaClient, TopicPartition};
use crate::kafka::TimestampConsumer;
use crate::leadership::LeadershipStatus;
use crate::metrics::registry::MetricsRegistry;
use futures::future::join_all;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, OwnedSemaphorePermit, Semaphore};
use tracing::{debug, error, info, instrument, warn};

/// Default timeout for a single collection cycle (should be less than poll_interval)
const DEFAULT_COLLECTION_TIMEOUT: Duration = Duration::from_secs(60);

pub struct ClusterManager {
    cluster_name: String,
    cluster_labels: HashMap<String, String>,
    client: Arc<KafkaClient>,
    offset_collector: OffsetCollector,
    timestamp_sampler: Option<TimestampSampler>,
    registry: Arc<MetricsRegistry>,
    poll_interval: Duration,
    max_backoff: Duration,
    granularity: Granularity,
    max_concurrent_fetches: usize,
    cache_cleanup_interval: Duration,
    collection_timeout: Duration,
    client_recycle_interval: u64,
}

impl ClusterManager {
    pub fn new(
        config: ClusterConfig,
        registry: Arc<MetricsRegistry>,
        exporter_config: &ExporterConfig,
    ) -> Result<Self> {
        let cluster_name = config.name.clone();
        let cluster_labels = config.labels.clone();
        let filters = config.compile_filters()?;
        let performance = exporter_config.performance.clone();

        let client = Arc::new(KafkaClient::with_performance(&config, performance.clone())?);
        let offset_collector =
            OffsetCollector::with_performance(Arc::clone(&client), filters, performance.clone());

        let timestamp_sampler = if exporter_config.timestamp_sampling.enabled {
            let ts_consumer = TimestampConsumer::with_pool_size(
                &config,
                exporter_config.timestamp_sampling.max_concurrent_fetches,
            )?;
            Some(TimestampSampler::new(
                ts_consumer,
                exporter_config.timestamp_sampling.cache_ttl,
            ))
        } else {
            None
        };

        info!(
            cluster = cluster_name,
            timestamp_sampling = exporter_config.timestamp_sampling.enabled,
            poll_interval = ?exporter_config.poll_interval,
            granularity = ?exporter_config.granularity,
            max_concurrent_fetches = exporter_config.timestamp_sampling.max_concurrent_fetches,
            custom_labels = ?cluster_labels,
            "Created cluster manager"
        );

        // Collection timeout should be less than poll_interval to avoid overlap
        let collection_timeout = if exporter_config.poll_interval > Duration::from_secs(10) {
            exporter_config.poll_interval - Duration::from_secs(5)
        } else {
            DEFAULT_COLLECTION_TIMEOUT.min(exporter_config.poll_interval)
        };

        Ok(Self {
            cluster_name,
            cluster_labels,
            client,
            offset_collector,
            timestamp_sampler,
            registry,
            poll_interval: exporter_config.poll_interval,
            max_backoff: Duration::from_secs(300),
            granularity: exporter_config.granularity,
            max_concurrent_fetches: exporter_config.timestamp_sampling.max_concurrent_fetches,
            cache_cleanup_interval: exporter_config.timestamp_sampling.cache_ttl * 2,
            collection_timeout,
            client_recycle_interval: exporter_config.performance.client_recycle_interval,
        })
    }

    #[instrument(skip(self, shutdown, leadership), fields(cluster = %self.cluster_name))]
    pub async fn run(self, mut shutdown: broadcast::Receiver<()>, leadership: LeadershipStatus) {
        info!(cluster = %self.cluster_name, "Starting collection loop");

        let mut interval = tokio::time::interval(self.poll_interval);
        let mut cache_cleanup_interval = tokio::time::interval(self.cache_cleanup_interval);
        let mut consecutive_errors = 0u32;
        let mut current_backoff = Duration::from_secs(1);
        let mut was_leader = leadership.is_leader();
        let mut cycle_count: u64 = 0;

        if !was_leader {
            info!(
                cluster = %self.cluster_name,
                "Starting in standby mode - waiting for leadership"
            );
        }

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    // Check leadership status before collecting
                    let is_leader = leadership.is_leader();

                    // Log leadership transitions
                    if is_leader != was_leader {
                        if is_leader {
                            info!(cluster = %self.cluster_name, "Acquired leadership - starting collection");
                        } else {
                            info!(cluster = %self.cluster_name, "Lost leadership - pausing collection");
                            // Clear metrics when losing leadership to avoid stale data
                            self.registry.remove_cluster(&self.cluster_name);
                        }
                        was_leader = is_leader;
                    }

                    // Skip collection if not leader
                    if !is_leader {
                        debug!(cluster = %self.cluster_name, "Standby mode - skipping collection");
                        continue;
                    }

                    // Wrap collect_once with a timeout to prevent hangs
                    let collection_result = tokio::time::timeout(
                        self.collection_timeout,
                        self.collect_once()
                    ).await;

                    match collection_result {
                        Ok(Ok(())) => {
                            consecutive_errors = 0;
                            current_backoff = Duration::from_secs(1);
                            self.registry.set_healthy(true);

                            // Periodically recycle Kafka clients to release
                            // accumulated librdkafka internal metadata.
                            // Run in spawn_blocking since client creation/teardown
                            // involves thread startup and rd_kafka_destroy().
                            if self.client_recycle_interval > 0 {
                                cycle_count += 1;
                                if cycle_count >= self.client_recycle_interval {
                                    cycle_count = 0;
                                    let client = Arc::clone(&self.client);
                                    let sampler = self.timestamp_sampler.clone();
                                    let cluster = self.cluster_name.clone();
                                    let _ = tokio::task::spawn_blocking(move || {
                                        if let Err(e) = client.recycle() {
                                            warn!(
                                                cluster = %cluster,
                                                error = %e,
                                                "Failed to recycle Kafka clients"
                                            );
                                        }
                                        if let Some(ref sampler) = sampler {
                                            if let Err(e) = sampler.recycle_pool() {
                                                warn!(
                                                    cluster = %cluster,
                                                    error = %e,
                                                    "Failed to recycle timestamp consumer pool"
                                                );
                                            }
                                        }
                                    }).await;
                                }
                            }
                        }
                        Ok(Err(e)) => {
                            consecutive_errors += 1;
                            error!(
                                cluster = %self.cluster_name,
                                error = %e,
                                consecutive_errors = consecutive_errors,
                                "Collection failed"
                            );

                            if consecutive_errors >= 3 {
                                self.registry.set_healthy(false);

                                let backoff = current_backoff.min(self.max_backoff);
                                warn!(
                                    cluster = %self.cluster_name,
                                    backoff_secs = backoff.as_secs(),
                                    "Applying backoff due to consecutive errors"
                                );

                                tokio::time::sleep(backoff).await;
                                current_backoff = (current_backoff * 2).min(self.max_backoff);
                            }
                        }
                        Err(_timeout) => {
                            consecutive_errors += 1;
                            error!(
                                cluster = %self.cluster_name,
                                timeout_secs = self.collection_timeout.as_secs(),
                                consecutive_errors = consecutive_errors,
                                "Collection timed out"
                            );

                            if consecutive_errors >= 3 {
                                self.registry.set_healthy(false);
                            }
                        }
                    }
                }
                _ = cache_cleanup_interval.tick() => {
                    // Periodic cache cleanup (only if leader, to save resources)
                    if leadership.is_leader() {
                        if let Some(ref sampler) = self.timestamp_sampler {
                            let before = sampler.cache_size();
                            sampler.clear_stale_entries();
                            let after = sampler.cache_size();
                            if before != after {
                                debug!(
                                    cluster = %self.cluster_name,
                                    before = before,
                                    after = after,
                                    "Cleaned up stale cache entries"
                                );
                            }
                        }
                    }
                }
                _ = shutdown.recv() => {
                    info!(cluster = %self.cluster_name, "Received shutdown signal");
                    break;
                }
            }
        }

        // Cleanup
        self.registry.remove_cluster(&self.cluster_name);
        info!(cluster = %self.cluster_name, "Collection loop stopped");
    }

    #[instrument(skip(self), fields(cluster = %self.cluster_name))]
    async fn collect_once(&self) -> Result<()> {
        let start = Instant::now();

        // Collect offsets, watermarks, and compacted-topic set in one batched pass.
        let snapshot = self.offset_collector.collect_parallel().await?;
        let compacted_topics = snapshot.compacted_topics.clone();

        debug!(
            cluster = %self.cluster_name,
            groups = snapshot.groups.len(),
            partitions = snapshot.watermarks.len(),
            compacted_topics = compacted_topics.len(),
            "Collected offsets"
        );

        // Collect timestamps if enabled (with concurrency limit)
        let timestamps = if let Some(ref sampler) = self.timestamp_sampler {
            self.collect_timestamps_concurrent(sampler, &snapshot).await
        } else {
            HashMap::new()
        };

        // Calculate lag metrics
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let poll_time_ms = start.elapsed().as_millis() as u64;
        let lag_metrics = LagCalculator::calculate(
            &snapshot,
            &timestamps,
            now_ms,
            poll_time_ms,
            &compacted_topics,
        );

        // Update registry with granularity and custom labels
        self.registry.update_with_options(
            &self.cluster_name,
            lag_metrics,
            self.granularity,
            &self.cluster_labels,
        );

        // Record scrape duration
        let scrape_duration_ms = start.elapsed().as_millis() as u64;
        self.registry.set_scrape_duration_ms(scrape_duration_ms);

        debug!(
            cluster = %self.cluster_name,
            elapsed_ms = scrape_duration_ms,
            timestamp_cache_size = self.timestamp_sampler.as_ref().map(|s| s.cache_size()).unwrap_or(0),
            "Collection cycle completed"
        );

        Ok(())
    }

    async fn collect_timestamps_concurrent(
        &self,
        sampler: &TimestampSampler,
        snapshot: &crate::collector::offset_collector::OffsetsSnapshot,
    ) -> HashMap<(String, TopicPartition), TimestampData> {
        // Build list of requests for partitions with lag
        let mut requests: Vec<(String, TopicPartition, i64)> = Vec::new();

        for group in &snapshot.groups {
            for (tp, committed_offset) in &group.offsets {
                let high_watermark = snapshot.get_high_watermark(tp).unwrap_or(*committed_offset);
                let lag = high_watermark - committed_offset;

                if lag > 0 {
                    requests.push((group.group_id.clone(), tp.clone(), *committed_offset));
                }
            }
        }

        if requests.is_empty() {
            return HashMap::new();
        }

        debug!(
            cluster = %self.cluster_name,
            request_count = requests.len(),
            max_concurrent = self.max_concurrent_fetches,
            "Fetching timestamps in parallel"
        );

        // Use semaphore to limit concurrency
        let semaphore = Arc::new(Semaphore::new(self.max_concurrent_fetches));
        let mut handles = Vec::with_capacity(requests.len());

        // Spawn tasks with proper semaphore-based backpressure
        for (group_id, tp, offset) in requests {
            let semaphore_clone = semaphore.clone();
            let sampler_clone = sampler.clone();

            // Spawn an async task that properly acquires the semaphore before spawning blocking work
            let handle = tokio::spawn(async move {
                // Acquire permit - this properly awaits until one is available
                let permit: OwnedSemaphorePermit = semaphore_clone
                    .acquire_owned()
                    .await
                    .expect("semaphore closed");

                // Now spawn the blocking task with the permit held
                let result = tokio::task::spawn_blocking(move || {
                    let _permit = permit; // Hold permit until blocking work completes
                    let result = sampler_clone.get_timestamp(&group_id, &tp, offset);
                    ((group_id, tp), result)
                })
                .await;

                result
            });

            handles.push(handle);
        }

        // Wait for all tasks to complete in parallel
        let results = join_all(handles).await;

        // Collect results (nested Result from tokio::spawn -> spawn_blocking)
        let mut timestamps = HashMap::new();
        for result in results {
            match result {
                Ok(Ok(((group_id, tp), Ok(Some(ts_result))))) => {
                    timestamps.insert(
                        (group_id, tp),
                        TimestampData {
                            timestamp_ms: ts_result.timestamp_ms,
                        },
                    );
                }
                Ok(Ok(((group_id, tp), Ok(None)))) => {
                    debug!(
                        group = group_id,
                        topic = tp.topic,
                        partition = tp.partition,
                        "No timestamp available"
                    );
                }
                Ok(Ok(((group_id, tp), Err(e)))) => {
                    warn!(
                        group = group_id,
                        topic = tp.topic,
                        partition = tp.partition,
                        error = %e,
                        "Failed to fetch timestamp"
                    );
                }
                Ok(Err(e)) => {
                    warn!(error = %e, "Timestamp fetch blocking task panicked");
                }
                Err(e) => {
                    warn!(error = %e, "Timestamp fetch task panicked");
                }
            }
        }

        timestamps
    }
}

impl std::fmt::Debug for ClusterManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterManager")
            .field("cluster_name", &self.cluster_name)
            .field("poll_interval", &self.poll_interval)
            .field("granularity", &self.granularity)
            .finish()
    }
}
