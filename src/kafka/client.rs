use crate::config::{ClusterConfig, PerformanceConfig};
use crate::error::{KlagError, Result};
use rdkafka::admin::{AdminClient, AdminOptions, ResourceSpecifier};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::groups::GroupList;
use rdkafka::metadata::Metadata;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{debug, info, instrument, warn};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TopicPartition {
    /// `Arc<str>` rather than `String` so multiple partitions of the same
    /// topic can share one heap allocation for the topic name. On clusters
    /// with many partitions per topic (and whenever a `HashMap<TopicPartition,
    /// _>` is cloned) this meaningfully cuts heap churn.
    pub topic: Arc<str>,
    pub partition: i32,
}

impl TopicPartition {
    /// Build a fresh `TopicPartition`. Accepts anything that can produce an
    /// `Arc<str>`: `&str`, `String`, `Arc<str>`, `&Arc<str>`, etc. Callers
    /// that already hold a shared `Arc<str>` (e.g., from a per-cycle topic
    /// interner) should pass `Arc::clone(&shared)` to avoid a fresh alloc.
    pub fn new(topic: impl Into<Arc<str>>, partition: i32) -> Self {
        Self {
            topic: topic.into(),
            partition,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConsumerGroupInfo {
    pub group_id: String,
    #[allow(dead_code)]
    pub protocol_type: String,
    #[allow(dead_code)]
    pub state: String,
}

#[derive(Debug, Clone)]
pub struct GroupMemberInfo {
    pub member_id: String,
    pub client_id: String,
    pub client_host: String,
    pub assignments: Vec<TopicPartition>,
}

#[derive(Debug, Clone)]
pub struct GroupDescription {
    pub group_id: String,
    pub state: String,
    #[allow(dead_code)]
    pub protocol_type: String,
    #[allow(dead_code)]
    pub protocol: String,
    pub members: Vec<GroupMemberInfo>,
}

pub struct KafkaClient {
    admin: Mutex<Arc<AdminClient<DefaultClientContext>>>,
    consumer: Mutex<Arc<BaseConsumer>>,
    config: ClusterConfig,
    timeout: Duration,
    performance: PerformanceConfig,
}

impl KafkaClient {
    /// Create a new `KafkaClient` with default performance config.
    /// Prefer `with_performance` for large clusters.
    #[allow(dead_code)]
    pub fn new(config: &ClusterConfig) -> Result<Self> {
        Self::with_performance(config, PerformanceConfig::default())
    }

    pub fn with_performance(
        config: &ClusterConfig,
        performance: PerformanceConfig,
    ) -> Result<Self> {
        let timeout = performance.kafka_timeout;
        let (admin, consumer) = Self::create_clients(config)?;

        Ok(Self {
            admin: Mutex::new(Arc::new(admin)),
            consumer: Mutex::new(Arc::new(consumer)),
            config: config.clone(),
            timeout,
            performance,
        })
    }

    /// Create fresh `AdminClient` + `BaseConsumer` pair.
    fn create_clients(
        config: &ClusterConfig,
    ) -> Result<(AdminClient<DefaultClientContext>, BaseConsumer)> {
        let mut client_config = ClientConfig::new();
        client_config.set("bootstrap.servers", &config.bootstrap_servers);
        client_config.set("client.id", format!("klag-exporter-{}", config.name));

        for (key, value) in &config.consumer_properties {
            client_config.set(key, value);
        }

        let admin: AdminClient<DefaultClientContext> =
            client_config.create().map_err(KlagError::Kafka)?;

        let consumer: BaseConsumer = client_config
            .clone()
            .set(
                "group.id",
                format!("klag-exporter-internal-{}", config.name),
            )
            .set("enable.auto.commit", "false")
            // Memory tuning: reduce internal buffer sizes (monitoring, not consuming)
            .set("queued.min.messages", "100")
            .set("queued.max.messages.kbytes", "1024")
            // Reduce background metadata refresh — we call fetch_metadata() explicitly
            .set("topic.metadata.refresh.interval.ms", "600000")
            .create()
            .map_err(KlagError::Kafka)?;

        Ok((admin, consumer))
    }

    /// Get a snapshot of the current admin client (cheap Arc clone).
    fn admin(&self) -> Arc<AdminClient<DefaultClientContext>> {
        Arc::clone(
            &self
                .admin
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }

    /// Public accessor for the underlying `AdminClient` Arc. Used by batched
    /// Admin API wrappers in `kafka::admin` and by integration tests. The
    /// caller holds the Arc for the duration of the FFI call to keep the
    /// native handle valid.
    pub fn admin_handle(&self) -> Arc<AdminClient<DefaultClientContext>> {
        self.admin()
    }

    /// Get a snapshot of the current consumer (cheap Arc clone).
    fn consumer(&self) -> Arc<BaseConsumer> {
        Arc::clone(
            &self
                .consumer
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }

    /// Recycle internal Kafka clients to release accumulated librdkafka metadata.
    ///
    /// librdkafka caches `rd_kafka_topic_t` handles internally and never frees
    /// them until the client is destroyed. Periodic recycling prevents unbounded
    /// memory growth on clusters with many topics.
    pub fn recycle(&self) -> Result<()> {
        let rss_before = get_rss_kb();
        let (new_admin, new_consumer) = Self::create_clients(&self.config)?;

        *self
            .admin
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Arc::new(new_admin);
        *self
            .consumer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Arc::new(new_consumer);

        let rss_after = get_rss_kb();
        info!(
            cluster = %self.config.name,
            rss_before_kb = rss_before,
            rss_after_kb = rss_after,
            rss_reclaimed_kb = rss_before.cast_signed() - rss_after.cast_signed(),
            "Recycled Kafka clients"
        );
        Ok(())
    }

    #[allow(dead_code)]
    pub const fn performance(&self) -> &PerformanceConfig {
        &self.performance
    }

    pub fn cluster_name(&self) -> &str {
        &self.config.name
    }

    #[instrument(skip(self), fields(cluster = %self.config.name))]
    pub fn list_consumer_groups(&self) -> Result<Vec<ConsumerGroupInfo>> {
        let consumer = self.consumer();
        let group_list: GroupList = consumer
            .fetch_group_list(None, self.timeout)
            .map_err(KlagError::Kafka)?;

        let groups = group_list
            .groups()
            .iter()
            .map(|g| ConsumerGroupInfo {
                group_id: g.name().to_string(),
                protocol_type: g.protocol_type().to_string(),
                state: g.state().to_string(),
            })
            .collect();

        debug!(count = group_list.groups().len(), "Listed consumer groups");
        Ok(groups)
    }

    /// Describe consumer groups via batched FFI. Chunks are fanned out
    /// concurrently (bounded by `max_concurrent_chunks`) instead of being
    /// called serially — this was the largest remaining synchronous delay
    /// in the hot path after Tier 1.
    ///
    /// Pass `parse_assignments = false` to skip the per-member
    /// assignment-parsing step (saves O(groups × members ×
    /// partitions-per-member) work per cycle). The data is only consumed
    /// by per-partition metrics, so the default topic-granularity mode
    /// should pass `false`. `protocol_type`/`protocol` fields are not
    /// populated — they are not consumed downstream.
    #[instrument(skip(self, group_ids), fields(cluster = %self.config.name, count = group_ids.len()))]
    pub async fn describe_consumer_groups(
        &self,
        group_ids: &[&str],
        parse_assignments: bool,
        max_concurrent_chunks: usize,
    ) -> Result<Vec<GroupDescription>> {
        use crate::kafka::admin::describe_consumer_groups_batched;

        let admin = self.admin();
        let batched = describe_consumer_groups_batched(
            admin,
            group_ids,
            self.timeout,
            100,
            parse_assignments,
            max_concurrent_chunks,
            self.performance.group_fetch_retries,
        )
        .await?;

        Ok(batched
            .into_iter()
            .map(|g| GroupDescription {
                group_id: g.group_id,
                state: g.state,
                protocol_type: String::new(),
                protocol: String::new(),
                members: g
                    .members
                    .into_iter()
                    .map(|m| GroupMemberInfo {
                        member_id: m.member_id,
                        client_id: m.client_id,
                        client_host: m.client_host,
                        assignments: m.assignments,
                    })
                    .collect(),
            })
            .collect())
    }

    #[instrument(skip(self), fields(cluster = %self.config.name))]
    pub fn fetch_metadata(&self) -> Result<Metadata> {
        self.consumer()
            .fetch_metadata(None, self.timeout)
            .map_err(KlagError::Kafka)
    }

    /// Fetch low + high watermarks for an explicit partition set via batched
    /// `ListOffsets`. Two Admin API calls total (EARLIEST + LATEST), regardless
    /// of partition count. Replaces the prior per-partition `fetch_watermarks`
    /// fan-out (O(partitions) blocking calls via a semaphore).
    ///
    /// This is a blocking call — run inside `spawn_blocking` from async code.
    #[instrument(skip(self, partitions), fields(cluster = %self.config.name, count = partitions.len()))]
    pub fn fetch_watermarks_for_partitions(
        &self,
        partitions: &[TopicPartition],
    ) -> Result<HashMap<TopicPartition, (i64, i64)>> {
        use crate::kafka::admin::{list_offsets_batched, OffsetSpec};

        if partitions.is_empty() {
            return Ok(HashMap::new());
        }

        let admin = self.admin();
        let lows = list_offsets_batched(&admin, partitions, OffsetSpec::Earliest, self.timeout)?;
        let highs = list_offsets_batched(&admin, partitions, OffsetSpec::Latest, self.timeout)?;

        let mut merged = HashMap::with_capacity(partitions.len());
        for (tp, high) in highs {
            // If the EARLIEST call failed for this partition, fall back to
            // `high` as the low watermark. This is conservative: it avoids
            // a spurious data-loss alarm from the lag calculator (which
            // flags `committed_offset < low_watermark`) while still surfacing
            // valid lag numbers. A 0 default would misrepresent the earliest
            // retained offset on compacted or retention-trimmed topics.
            let low = if let Some(l) = lows.get(&tp).copied() {
                l
            } else {
                warn!(
                    topic = %tp.topic,
                    partition = tp.partition,
                    "EARLIEST watermark missing; using LATEST as conservative fallback"
                );
                high
            };
            merged.insert(tp, (low, high));
        }
        // Surface partitions where only EARLIEST returned (rare — usually both
        // complete together, but a broker-level error on LATEST could leave us
        // with low-only data).
        for (tp, low) in lows {
            merged.entry(tp).or_insert((low, low));
        }
        Ok(merged)
    }

    #[allow(dead_code)]
    pub fn admin_options(&self) -> AdminOptions {
        AdminOptions::new().request_timeout(Some(self.timeout))
    }

    /// Fetch topics that have compaction enabled (cleanup.policy contains
    /// "compact"), restricted to the supplied topic name list. Callers pass
    /// the already-filtered monitored topic set to avoid paying the
    /// full-cluster `DescribeConfigs` cost on clusters with thousands of
    /// topics.
    #[instrument(skip(self, topic_names), fields(cluster = %self.config.name, count = topic_names.len()))]
    pub async fn fetch_compacted_topics_for(
        &self,
        topic_names: &[String],
    ) -> Result<HashSet<String>> {
        if topic_names.is_empty() {
            return Ok(HashSet::new());
        }

        let resources: Vec<ResourceSpecifier> = topic_names
            .iter()
            .map(|name| ResourceSpecifier::Topic(name.as_str()))
            .collect();

        let admin = self.admin();
        let opts = self.admin_options();
        let results = admin
            .describe_configs(resources.iter(), &opts)
            .await
            .map_err(KlagError::Kafka)?;

        let mut compacted_topics = HashSet::new();

        for result in results {
            match result {
                Ok(resource) => {
                    let topic_name = match &resource.specifier {
                        rdkafka::admin::OwnedResourceSpecifier::Topic(name) => name.clone(),
                        _ => continue,
                    };
                    for entry in resource.entries {
                        if entry.name == "cleanup.policy" {
                            if let Some(value) = entry.value {
                                if value.contains("compact") {
                                    compacted_topics.insert(topic_name.clone());
                                }
                            }
                        }
                    }
                }
                Err(err) => {
                    warn!(error = %err, "Failed to describe config for resource");
                }
            }
        }

        debug!(
            count = compacted_topics.len(),
            "Identified compacted topics"
        );

        Ok(compacted_topics)
    }
}

/// Read current process RSS in kilobytes. Returns 0 on failure or non-Linux.
/// Assumes 4 KB page size (standard for `x86_64` Linux where /proc/self/statm exists).
fn get_rss_kb() -> u64 {
    std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|s| s.split_whitespace().nth(1)?.parse::<u64>().ok())
        .map_or(0, |pages| pages * 4)
}

impl std::fmt::Debug for KafkaClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaClient")
            .field("cluster", &self.config.name)
            .finish()
    }
}
