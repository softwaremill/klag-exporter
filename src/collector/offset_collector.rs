use crate::config::{CompiledFilters, Granularity, PerformanceConfig};
use crate::error::Result;
use crate::kafka::client::{ConsumerGroupInfo, KafkaClient, TopicPartition};
use crate::retry::{full_jitter_backoff, retry_with_backoff};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::{debug, instrument, warn};

/// Per-topic TTL cache for `cleanup.policy=compact` detection.
///
/// `cleanup.policy` is set at topic creation and almost never changes, so
/// caching it with a long TTL saves a per-cycle `DescribeConfigs` over the
/// monitored topic set. On each cycle the collector asks the cache which
/// topics still need to be queried fresh, then merges the cached-true
/// entries with whatever `DescribeConfigs` returns.
struct CompactedTopicsCache {
    ttl: Duration,
    // (is_compacted, fetched_at). Mutex is fine — accessed only from
    // `collect_parallel` on the cluster's single collection task.
    entries: Mutex<HashMap<String, (bool, Instant)>>,
}

impl CompactedTopicsCache {
    fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            entries: Mutex::new(HashMap::new()),
        }
    }

    /// Split `monitored_topics` into:
    ///   - `cached_compacted` — topics the cache says are compacted (fresh)
    ///   - `to_fetch` — topics with no fresh cache entry (need `DescribeConfigs`)
    fn partition<'a>(&self, monitored_topics: &'a [String]) -> (HashSet<String>, Vec<&'a str>) {
        let now = Instant::now();
        let entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        let mut cached_compacted = HashSet::new();
        let mut to_fetch: Vec<&str> = Vec::new();
        for topic in monitored_topics {
            match entries.get(topic) {
                Some((is_compacted, fetched_at)) if now.duration_since(*fetched_at) < self.ttl => {
                    if *is_compacted {
                        cached_compacted.insert(topic.clone());
                    }
                }
                _ => to_fetch.push(topic.as_str()),
            }
        }
        (cached_compacted, to_fetch)
    }

    /// Update the cache with a fresh `DescribeConfigs` result. For each topic
    /// in `fetched_topics`, record whether it appeared in `compacted_result`.
    fn update(&self, fetched_topics: &[&str], compacted_result: &HashSet<String>) {
        let now = Instant::now();
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for topic in fetched_topics {
            let is_compacted = compacted_result.contains(*topic);
            entries.insert((*topic).to_string(), (is_compacted, now));
        }
    }

    /// Drop cache entries for topics no longer being monitored.
    fn prune_to(&self, monitored_topics: &[String]) {
        let keep: HashSet<&str> = monitored_topics
            .iter()
            .map(std::string::String::as_str)
            .collect();
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        entries.retain(|k, _| keep.contains(k.as_str()));
    }
}

/// Shared handle to the filtered monitored-topic set. Arc-wrapped so cache
/// hits are cheap pointer bumps rather than Vec clones.
type MonitoredSet = (Arc<Vec<TopicPartition>>, Arc<Vec<String>>);

/// Cached result of `list_monitored_partitions`. Re-fetching cluster metadata
/// and re-running the topic filter is expensive on large clusters (thousands
/// of topics), and the result is stable across many poll cycles. A fresh
/// cache entry lets the collector skip both.
struct MetadataCache {
    ttl: Duration,
    entry: Mutex<Option<(MonitoredSet, Instant)>>,
}

impl MetadataCache {
    const fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            entry: Mutex::new(None),
        }
    }

    fn get(&self) -> Option<MonitoredSet> {
        if self.ttl.is_zero() {
            return None;
        }
        let guard = self
            .entry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (set, at) = guard.as_ref()?;
        if at.elapsed() < self.ttl {
            Some((Arc::clone(&set.0), Arc::clone(&set.1)))
        } else {
            None
        }
    }

    /// Store fresh values and return Arc clones. When `ttl == 0` the cache
    /// is treated as disabled: we don't store the entry (so subsequent
    /// `get()` calls still return None) but we still wrap in Arcs for a
    /// uniform return shape.
    fn set(&self, partitions: Vec<TopicPartition>, topics: Vec<String>) -> MonitoredSet {
        let set: MonitoredSet = (Arc::new(partitions), Arc::new(topics));
        if !self.ttl.is_zero() {
            let mut guard = self
                .entry
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *guard = Some(((Arc::clone(&set.0), Arc::clone(&set.1)), Instant::now()));
        }
        set
    }
}

/// TTL cache for the output of `list_consumer_groups`. The cluster's
/// consumer-group roster is stable outside of deployments / scaling events,
/// so re-fetching it every poll cycle is wasted work. Same shape as
/// `MetadataCache`: one entry for the whole list, Arc-wrapped so cache
/// hits don't deep-clone the Vec.
struct ConsumerGroupsCache {
    ttl: Duration,
    entry: Mutex<Option<(Arc<Vec<ConsumerGroupInfo>>, Instant)>>,
}

impl ConsumerGroupsCache {
    const fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            entry: Mutex::new(None),
        }
    }

    fn get(&self) -> Option<Arc<Vec<ConsumerGroupInfo>>> {
        if self.ttl.is_zero() {
            return None;
        }
        let guard = self
            .entry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (groups, at) = guard.as_ref()?;
        if at.elapsed() < self.ttl {
            Some(Arc::clone(groups))
        } else {
            None
        }
    }

    fn set(&self, groups: Vec<ConsumerGroupInfo>) -> Arc<Vec<ConsumerGroupInfo>> {
        let arc = Arc::new(groups);
        if !self.ttl.is_zero() {
            let mut guard = self
                .entry
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *guard = Some((Arc::clone(&arc), Instant::now()));
        }
        arc
    }
}

pub struct OffsetCollector {
    client: Arc<KafkaClient>,
    filters: CompiledFilters,
    performance: PerformanceConfig,
    granularity: Granularity,
    compacted_cache: CompactedTopicsCache,
    metadata_cache: MetadataCache,
    consumer_groups_cache: ConsumerGroupsCache,
}

/// Per-phase wall-clock timings collected during one `collect_parallel`
/// run. Emitted as a single structured debug log at the end of the cycle
/// so operators can see where time goes without enabling trace logging.
#[derive(Default)]
struct PhaseTimings {
    list_groups_ms: u64,
    describe_groups_ms: u64,
    metadata_ms: u64,
    watermarks_ms: u64,
    group_offsets_ms: u64,
    compacted_ms: u64,
}

#[derive(Debug, Clone)]
pub struct OffsetsSnapshot {
    pub cluster_name: String,
    pub groups: Vec<GroupSnapshot>,
    pub watermarks: HashMap<TopicPartition, (i64, i64)>,
    /// Topics configured with `cleanup.policy=compact`. Populated by
    /// `collect_parallel` for the monitored topic set. Used by the lag
    /// calculator to suppress data-loss warnings on compacted topics (where
    /// `low_watermark` ahead of `committed_offset` is expected, not a loss).
    pub compacted_topics: HashSet<String>,
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
    pub fn with_performance(
        client: Arc<KafkaClient>,
        filters: CompiledFilters,
        performance: PerformanceConfig,
        granularity: Granularity,
    ) -> Self {
        let compacted_cache = CompactedTopicsCache::new(performance.compacted_topics_cache_ttl);
        let metadata_cache = MetadataCache::new(performance.metadata_cache_ttl);
        let consumer_groups_cache = ConsumerGroupsCache::new(performance.consumer_groups_cache_ttl);
        Self {
            client,
            filters,
            performance,
            granularity,
            compacted_cache,
            metadata_cache,
            consumer_groups_cache,
        }
    }

    /// Collect offsets using batched Admin API calls. Drives everything from a
    /// single filtered topic-set so internal / blacklisted topics never enter
    /// the watermark + compacted-topic-config path.
    ///
    /// Per-cycle RPC count:
    ///   - 2 batched `ListOffsets` calls (EARLIEST + LATEST), routed per leader
    ///     broker internally — O(brokers) broker round trips regardless of
    ///     partition count.
    ///   - `ceil(groups / 100)` batched `DescribeConsumerGroups` calls.
    ///   - 1 `ListConsumerGroupOffsets` call per group (`PER_CALL_CHUNK = 1`
    ///     in `fetch_all_group_offsets_batched` because librdkafka 2.12
    ///     rejects multi-group calls at the client layer; fanned out via
    ///     `max_concurrent_groups`).
    ///   - 1 `DescribeConfigs` call restricted to monitored topics.
    #[instrument(skip(self), fields(cluster = %self.client.cluster_name()))]
    pub async fn collect_parallel(&self) -> Result<OffsetsSnapshot> {
        let start = std::time::Instant::now();

        // Per-phase timings, emitted as a single structured log line at
        // the end of the cycle. Lets operators see where wall-clock time
        // goes (list_groups vs describe vs watermarks vs offsets vs
        // compacted-configs) without turning on trace-level logging.
        let mut timings = PhaseTimings::default();

        // List all consumer groups (single metadata call).
        // TTL-cached: steady-state (stable group roster) cycles skip this
        // RPC entirely. New groups appear within one TTL of creation.
        let phase_start = std::time::Instant::now();
        let all_groups = if let Some(cached) = self.consumer_groups_cache.get() {
            debug!(total_groups = cached.len(), "Consumer groups cache hit");
            cached
        } else {
            let fresh = self.client.list_consumer_groups()?;
            debug!(
                total_groups = fresh.len(),
                "Listed all consumer groups (fresh)"
            );
            self.consumer_groups_cache.set(fresh)
        };
        timings.list_groups_ms = phase_start.elapsed().as_millis() as u64;

        let filtered_groups: Vec<_> = all_groups
            .iter()
            .filter(|g| self.filters.matches_group(&g.group_id))
            .collect();
        debug!(
            filtered_groups = filtered_groups.len(),
            "Filtered consumer groups"
        );

        let group_ids: Vec<&str> = filtered_groups
            .iter()
            .map(|g| g.group_id.as_str())
            .collect();

        // Describe filtered groups via batched FFI. Skip member-assignment
        // parsing unless we actually emit per-partition member labels
        // (granularity = partition).
        let parse_assignments = matches!(self.granularity, Granularity::Partition);
        let phase_start = std::time::Instant::now();
        let descriptions = self
            .client
            .describe_consumer_groups(
                &group_ids,
                parse_assignments,
                self.performance.max_concurrent_groups,
            )
            .await?;
        timings.describe_groups_ms = phase_start.elapsed().as_millis() as u64;

        // Compute the monitored partition + topic set once from a single
        // metadata fetch. Topic filter is applied here, BEFORE any
        // partition-touching operation — this keeps `__consumer_offsets`
        // (50 partitions by default) and blacklisted topics out of the hot
        // path entirely.
        let phase_start = std::time::Instant::now();
        let (monitored_partitions, monitored_topics) = self.list_monitored_partitions()?;
        timings.metadata_ms = phase_start.elapsed().as_millis() as u64;
        debug!(
            partitions = monitored_partitions.len(),
            topics = monitored_topics.len(),
            "Computed monitored topic + partition set"
        );

        // Watermarks via batched ListOffsets (two blocking FFI calls). Move
        // `monitored_partitions` into the blocking closure — no subsequent
        // use in this function, and cloning an O(partitions) Vec every cycle
        // is wasted work on large clusters.
        let phase_start = std::time::Instant::now();
        let watermarks = {
            let client = Arc::clone(&self.client);
            tokio::task::spawn_blocking(move || {
                client.fetch_watermarks_for_partitions(&monitored_partitions)
            })
            .await
            .map_err(|e| {
                crate::error::KlagError::Admin(format!("watermark task panicked: {e}"))
            })??
        };
        timings.watermarks_ms = phase_start.elapsed().as_millis() as u64;
        debug!(
            partitions = watermarks.len(),
            "Fetched watermarks (batched)"
        );

        // Group offsets via batched multi-group ListConsumerGroupOffsets.
        // `NULL` partitions → broker returns every committed partition per
        // group; we then filter the (much smaller) response by topic.
        let phase_start = std::time::Instant::now();
        let group_offsets = self.fetch_all_group_offsets_batched(&group_ids).await;
        timings.group_offsets_ms = phase_start.elapsed().as_millis() as u64;

        // Compacted-topic lookup — TTL-cached per topic. `cleanup.policy`
        // almost never changes after topic creation, so most cycles only
        // refresh new topics (or nothing at all in steady state).
        let phase_start = std::time::Instant::now();
        let (mut compacted_topics, to_fetch) = self.compacted_cache.partition(&monitored_topics);
        if to_fetch.is_empty() {
            debug!(
                cached = compacted_topics.len(),
                "Compacted-topic cache fully hit — no DescribeConfigs RPC"
            );
        } else {
            debug!(
                to_fetch = to_fetch.len(),
                cached = compacted_topics.len(),
                "Compacted-topic cache partial miss — refreshing"
            );
            let to_fetch_owned: Vec<String> = to_fetch
                .iter()
                .map(std::string::ToString::to_string)
                .collect();
            match self
                .client
                .fetch_compacted_topics_for(&to_fetch_owned)
                .await
            {
                Ok(freshly_compacted) => {
                    self.compacted_cache.update(&to_fetch, &freshly_compacted);
                    compacted_topics.extend(freshly_compacted);
                }
                Err(e) => warn!(error = %e, "Failed to refresh compacted topics"),
            }
        }
        // Drop cache entries for topics no longer monitored (filter change,
        // topic deletion) so memory doesn't grow unboundedly.
        self.compacted_cache.prune_to(&monitored_topics);
        timings.compacted_ms = phase_start.elapsed().as_millis() as u64;

        // Build group snapshots
        let mut groups = Vec::with_capacity(descriptions.len());
        for desc in descriptions {
            let offsets = group_offsets
                .get(&desc.group_id)
                .cloned()
                .unwrap_or_default();

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

        let elapsed = start.elapsed();
        debug!(
            elapsed_ms = elapsed.as_millis(),
            list_groups_ms = timings.list_groups_ms,
            describe_groups_ms = timings.describe_groups_ms,
            metadata_ms = timings.metadata_ms,
            watermarks_ms = timings.watermarks_ms,
            group_offsets_ms = timings.group_offsets_ms,
            compacted_ms = timings.compacted_ms,
            monitored_topics = monitored_topics.len(),
            compacted_topics = compacted_topics.len(),
            "Batched collection completed"
        );

        Ok(OffsetsSnapshot {
            cluster_name: self.client.cluster_name().to_string(),
            groups,
            watermarks,
            compacted_topics,
            timestamp_ms: chrono_timestamp_ms(),
        })
    }

    /// Compute the partition list this collector should monitor by applying
    /// the topic whitelist/blacklist to the current cluster metadata. Runs
    /// before any partition-touching Admin API call.
    ///
    /// Result is TTL-cached; a cache hit returns the previously computed
    /// (partitions, topics) pair via cheap Arc clones. This avoids both the
    /// `fetch_metadata` round trip and the O(topics) regex-filter pass on
    /// every cycle.
    fn list_monitored_partitions(&self) -> Result<MonitoredSet> {
        if let Some(cached) = self.metadata_cache.get() {
            debug!("Metadata cache hit");
            return Ok(cached);
        }

        let metadata = self.client.fetch_metadata()?;
        let mut partitions = Vec::new();
        let mut topics = Vec::new();
        for topic in metadata.topics() {
            let name = topic.name();
            if !self.filters.matches_topic(name) {
                continue;
            }
            topics.push(name.to_string());
            // Intern the topic name once per topic so the per-partition
            // TopicPartition entries share one Arc<str> allocation. On a
            // cluster with avg 10 partitions/topic this cuts topic-name
            // heap allocs by 90%.
            let topic_arc: Arc<str> = Arc::from(name);
            for p in topic.partitions() {
                partitions.push(TopicPartition::new(Arc::clone(&topic_arc), p.id()));
            }
        }
        Ok(self.metadata_cache.set(partitions, topics))
    }

    /// Fetch offsets for all groups via batched Admin API.
    ///
    /// librdkafka 2.12's `rd_kafka_ListConsumerGroupOffsets` rejects calls with
    /// more than one group per call ("Exactly one `ListConsumerGroupOffsets` must
    /// be passed") even though the Kafka protocol supports multi-group
    /// `ListOffsetFetch` (KIP-709). We therefore issue one FFI call per group,
    /// fanned out with bounded concurrency via `max_concurrent_groups`.
    ///
    /// The win vs. the prior path is not "multi-group in one call" but:
    ///   - `NULL` partition list passed to each request → broker returns only
    ///     committed partitions; no 19K-entry partition-list clone per group.
    ///   - No per-group `spawn` of a 19K-entry Vec clone.
    ///
    /// Once librdkafka lifts the single-group restriction we can increase the
    /// inner `chunk_size` without code changes beyond this constant.
    async fn fetch_all_group_offsets_batched(
        &self,
        group_ids: &[&str],
    ) -> HashMap<String, HashMap<TopicPartition, i64>> {
        use crate::kafka::admin::{list_consumer_group_offsets_batched, AdminRequestError};

        if group_ids.is_empty() {
            return HashMap::new();
        }

        // librdkafka constraint: one group per FFI call.
        const PER_CALL_CHUNK: usize = 1;

        let offset_timeout = self.performance.offset_fetch_timeout;
        let max_concurrent = self.performance.max_concurrent_groups;
        let max_retries = self.performance.group_fetch_retries;

        debug!(
            groups = group_ids.len(),
            per_call_chunk = PER_CALL_CHUNK,
            max_concurrent = max_concurrent,
            max_retries = max_retries,
            "Fetching group offsets (one call per group, fanned out)"
        );

        let semaphore = Arc::new(Semaphore::new(max_concurrent));
        let client = Arc::clone(&self.client);

        let mut handles = Vec::with_capacity(group_ids.len());
        for gid in group_ids {
            let gid: Arc<str> = Arc::from(*gid);
            let permit = semaphore.clone();
            let client_clone = Arc::clone(&client);
            handles.push(tokio::spawn(async move {
                let result = retry_with_backoff(
                    max_retries,
                    |attempt| {
                        let gid = Arc::clone(&gid);
                        let permit = Arc::clone(&permit);
                        let client = Arc::clone(&client_clone);
                        async move {
                            if attempt > 1 {
                                debug!(
                                    group = %gid,
                                    attempt = attempt,
                                    max_attempts = max_retries.saturating_add(1),
                                    "Retrying failed ListConsumerGroupOffsets request"
                                );
                            }
                            // Every attempt, including retries, acquires the
                            // same concurrency permit. Release it before any
                            // retry backoff so sleeping calls do not occupy a
                            // scarce blocking-operation slot.
                            let _permit: OwnedSemaphorePermit =
                                permit.acquire_owned().await.expect("semaphore closed");
                            let response = tokio::task::spawn_blocking(move || {
                                list_consumer_group_offsets_batched(
                                    &client.admin_handle(),
                                    &[gid.as_ref()],
                                    offset_timeout,
                                    PER_CALL_CHUNK,
                                )
                            })
                            .await
                            .map_err(|error| {
                                AdminRequestError::task(
                                    "ListConsumerGroupOffsets",
                                    format!("blocking task failed: {error}"),
                                )
                            })??;
                            response.into_single_group_result()
                        }
                    },
                    AdminRequestError::is_retriable,
                    full_jitter_backoff,
                )
                .await;
                (gid, result)
            }));
        }

        let results = futures::future::join_all(handles).await;

        let mut merged: HashMap<String, HashMap<TopicPartition, i64>> = HashMap::new();
        for r in results {
            match r {
                Ok((gid, Ok((map, attempts)))) => {
                    if attempts > 1 {
                        debug!(
                            group = %gid,
                            attempts,
                            "ListConsumerGroupOffsets recovered after retry"
                        );
                    }
                    merged.extend(map);
                }
                Ok((gid, Err(failure))) => {
                    warn!(
                        group = %gid,
                        error = %failure.error,
                        attempts = failure.attempts,
                        stop_reason = ?failure.reason,
                        "Group-offset call failed"
                    );
                }
                Err(e) => warn!(error = %e, "Group-offset join error"),
            }
        }
        merged
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
    use crate::config::ClusterConfig;
    use rdkafka::mocking::{MockCluster, MockCoordinator};
    use rdkafka::producer::DefaultProducerContext;
    use rdkafka::types::{RDKafkaApiKey, RDKafkaRespErr};

    const MOCK_GROUP: &str = "retry-test-group";

    fn mock_offset_collector(
        max_retries: usize,
    ) -> (
        MockCluster<'static, DefaultProducerContext>,
        OffsetCollector,
    ) {
        let mock_cluster = MockCluster::new(1).expect("create mock Kafka cluster");
        mock_cluster
            .coordinator(MockCoordinator::Group(MOCK_GROUP.to_string()), 1)
            .expect("set mock group coordinator");

        let cluster = ClusterConfig {
            name: "mock".to_string(),
            bootstrap_servers: mock_cluster.bootstrap_servers(),
            group_whitelist: vec![".*".to_string()],
            group_blacklist: vec![],
            topic_whitelist: vec![".*".to_string()],
            topic_blacklist: vec![],
            consumer_properties: HashMap::new(),
            labels: HashMap::new(),
        };
        let filters = cluster.compile_filters().expect("compile filters");
        let performance = PerformanceConfig {
            offset_fetch_timeout: Duration::from_secs(2),
            group_fetch_retries: max_retries,
            max_concurrent_groups: 1,
            ..PerformanceConfig::default()
        };
        let client = Arc::new(
            KafkaClient::with_performance(&cluster, performance.clone())
                .expect("create Kafka client"),
        );
        let collector =
            OffsetCollector::with_performance(client, filters, performance, Granularity::Topic);

        (mock_cluster, collector)
    }

    #[tokio::test]
    async fn group_offset_fetch_does_not_retry_when_disabled() {
        let (mock_cluster, collector) = mock_offset_collector(0);
        mock_cluster.request_errors(
            RDKafkaApiKey::OffsetFetch,
            &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_COORDINATOR_LOAD_IN_PROGRESS],
        );

        let failed = collector
            .fetch_all_group_offsets_batched(&[MOCK_GROUP])
            .await;
        assert!(
            !failed.contains_key(MOCK_GROUP),
            "a failed group should be omitted when retries are disabled"
        );

        // The injected error is consumed by the first broker request. A second
        // collection succeeds, proving that the first result was caused by the
        // lack of an application-level retry rather than bad mock setup.
        let next_collection = collector
            .fetch_all_group_offsets_batched(&[MOCK_GROUP])
            .await;
        assert!(next_collection.contains_key(MOCK_GROUP));
    }

    #[tokio::test]
    async fn group_offset_fetch_recovers_from_one_retriable_broker_error() {
        let (mock_cluster, collector) = mock_offset_collector(1);
        mock_cluster.request_errors(
            RDKafkaApiKey::OffsetFetch,
            &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_COORDINATOR_LOAD_IN_PROGRESS],
        );

        let offsets = collector
            .fetch_all_group_offsets_batched(&[MOCK_GROUP])
            .await;

        assert!(
            offsets.contains_key(MOCK_GROUP),
            "the retry should recover the successful empty-offset response"
        );
    }

    #[tokio::test]
    async fn group_offset_fetch_does_not_retry_transport_disconnect_when_disabled() {
        let (mock_cluster, collector) = mock_offset_collector(0);
        mock_cluster.request_errors(
            RDKafkaApiKey::OffsetFetch,
            &[RDKafkaRespErr::RD_KAFKA_RESP_ERR__TRANSPORT],
        );

        let failed = collector
            .fetch_all_group_offsets_batched(&[MOCK_GROUP])
            .await;
        assert!(!failed.contains_key(MOCK_GROUP));
    }

    #[tokio::test]
    async fn group_offset_fetch_recovers_from_transport_disconnect() {
        let (mock_cluster, collector) = mock_offset_collector(1);
        mock_cluster.request_errors(
            RDKafkaApiKey::OffsetFetch,
            &[RDKafkaRespErr::RD_KAFKA_RESP_ERR__TRANSPORT],
        );

        let offsets = collector
            .fetch_all_group_offsets_batched(&[MOCK_GROUP])
            .await;

        assert!(
            offsets.contains_key(MOCK_GROUP),
            "the retry should reconnect after a broker transport disconnect"
        );
    }

    #[tokio::test]
    async fn group_offset_fetch_does_not_retry_permanent_broker_errors() {
        let (mock_cluster, collector) = mock_offset_collector(1);
        mock_cluster.request_errors(
            RDKafkaApiKey::OffsetFetch,
            &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED],
        );

        let failed = collector
            .fetch_all_group_offsets_batched(&[MOCK_GROUP])
            .await;
        assert!(!failed.contains_key(MOCK_GROUP));

        // If the collector had retried the permanent error, that retry would
        // have consumed the mock broker's next (successful) response already.
        let next_collection = collector
            .fetch_all_group_offsets_batched(&[MOCK_GROUP])
            .await;
        assert!(next_collection.contains_key(MOCK_GROUP));
    }

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
            compacted_topics: HashSet::new(),
            timestamp_ms: 0,
        };

        let groups = snapshot.filtered_groups();
        assert_eq!(groups.len(), 2);
        assert!(groups.contains(&"group1"));
        assert!(groups.contains(&"group2"));
    }

    #[test]
    fn compacted_cache_empty_cache_requests_all() {
        let cache = CompactedTopicsCache::new(Duration::from_secs(60));
        let topics = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let (cached, to_fetch) = cache.partition(&topics);
        assert!(cached.is_empty());
        assert_eq!(to_fetch, vec!["a", "b", "c"]);
    }

    #[test]
    fn compacted_cache_hit_skips_fetch() {
        let cache = CompactedTopicsCache::new(Duration::from_secs(60));
        let topics = vec!["a".to_string(), "b".to_string()];
        let mut compacted = HashSet::new();
        compacted.insert("a".to_string());

        // First partition call: everything needs fetching.
        let (_cached, to_fetch) = cache.partition(&topics);
        assert_eq!(to_fetch.len(), 2);

        // Update with the fetch result.
        cache.update(&to_fetch, &compacted);

        // Second partition call: everything cached.
        let (cached, to_fetch) = cache.partition(&topics);
        assert_eq!(to_fetch.len(), 0);
        assert_eq!(cached.len(), 1);
        assert!(cached.contains("a"));
    }

    #[test]
    fn compacted_cache_expired_entries_re_fetched() {
        let cache = CompactedTopicsCache::new(Duration::from_millis(50));
        let topics = vec!["a".to_string()];
        let mut compacted = HashSet::new();
        compacted.insert("a".to_string());

        let (_cached, to_fetch) = cache.partition(&topics);
        cache.update(&to_fetch, &compacted);

        // Cached entry is fresh.
        let (cached, to_fetch) = cache.partition(&topics);
        assert!(cached.contains("a"));
        assert!(to_fetch.is_empty());

        // Wait for TTL to expire.
        std::thread::sleep(Duration::from_millis(70));

        // Cached entry is stale — partition() returns it for re-fetching.
        let (cached, to_fetch) = cache.partition(&topics);
        assert!(cached.is_empty());
        assert_eq!(to_fetch, vec!["a"]);
    }

    #[test]
    fn metadata_cache_hit_returns_cached() {
        let cache = MetadataCache::new(Duration::from_secs(60));
        assert!(cache.get().is_none(), "empty cache should miss");

        let (parts, topics) = cache.set(vec![TopicPartition::new("t", 0)], vec!["t".to_string()]);
        assert_eq!(parts.len(), 1);
        assert_eq!(topics.len(), 1);

        let cached = cache.get().expect("cache should be populated");
        assert_eq!(cached.0.len(), 1);
        assert_eq!(cached.1.len(), 1);
    }

    #[test]
    fn metadata_cache_disabled_by_zero_ttl() {
        let cache = MetadataCache::new(Duration::ZERO);
        let (_parts, _topics) = cache.set(vec![TopicPartition::new("t", 0)], vec!["t".into()]);
        assert!(
            cache.get().is_none(),
            "zero-TTL cache must never return a hit"
        );
    }

    #[test]
    fn metadata_cache_expires() {
        let cache = MetadataCache::new(Duration::from_millis(50));
        cache.set(vec![], vec![]);
        assert!(cache.get().is_some());
        std::thread::sleep(Duration::from_millis(70));
        assert!(cache.get().is_none(), "expired entry must miss");
    }

    #[test]
    fn consumer_groups_cache_hit_returns_cached() {
        let cache = ConsumerGroupsCache::new(Duration::from_secs(60));
        assert!(cache.get().is_none(), "empty cache should miss");
        let arc = cache.set(vec![ConsumerGroupInfo {
            group_id: "g".into(),
            protocol_type: String::new(),
            state: String::new(),
        }]);
        assert_eq!(arc.len(), 1);
        let cached = cache.get().expect("should hit after set");
        assert_eq!(cached.len(), 1);
    }

    #[test]
    fn consumer_groups_cache_zero_ttl_disabled() {
        let cache = ConsumerGroupsCache::new(Duration::ZERO);
        let _arc = cache.set(vec![ConsumerGroupInfo {
            group_id: "g".into(),
            protocol_type: String::new(),
            state: String::new(),
        }]);
        assert!(cache.get().is_none());
    }

    #[test]
    fn consumer_groups_cache_expires() {
        let cache = ConsumerGroupsCache::new(Duration::from_millis(50));
        cache.set(vec![]);
        assert!(cache.get().is_some());
        std::thread::sleep(Duration::from_millis(70));
        assert!(cache.get().is_none());
    }

    #[test]
    fn compacted_cache_prune_removes_unseen_topics() {
        let cache = CompactedTopicsCache::new(Duration::from_secs(60));
        let initial = vec!["a".to_string(), "b".to_string()];
        let mut compacted = HashSet::new();
        compacted.insert("a".to_string());
        let (_cached, to_fetch) = cache.partition(&initial);
        cache.update(&to_fetch, &compacted);

        // Only "a" is still monitored. "b"'s cache entry should be dropped.
        let remaining = vec!["a".to_string()];
        cache.prune_to(&remaining);

        let entries = cache.entries.lock().unwrap();
        assert!(entries.contains_key("a"));
        assert!(!entries.contains_key("b"));
    }
}
