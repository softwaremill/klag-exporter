use crate::config::{ClusterConfig, PerformanceConfig};
use crate::error::{KlagError, Result};
use rdkafka::admin::{AdminClient, AdminOptions, ResourceSpecifier};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::groups::GroupList;
use rdkafka::metadata::Metadata;
use rdkafka::TopicPartitionList;
use std::collections::{HashMap, HashSet};
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::{debug, info, instrument, warn};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: i32,
}

impl TopicPartition {
    pub fn new(topic: impl Into<String>, partition: i32) -> Self {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum OffsetPosition {
    Earliest,
    Latest,
}

pub struct KafkaClient {
    admin: Mutex<Arc<AdminClient<DefaultClientContext>>>,
    consumer: Mutex<Arc<BaseConsumer>>,
    config: ClusterConfig,
    timeout: Duration,
    performance: PerformanceConfig,
}

impl KafkaClient {
    /// Create a new KafkaClient with default performance config.
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

    /// Create fresh AdminClient + BaseConsumer pair.
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
        Arc::clone(&self.admin.lock().unwrap_or_else(|p| p.into_inner()))
    }

    /// Public accessor for the underlying AdminClient Arc. Used by batched
    /// Admin API wrappers in `kafka::admin` and by integration tests. The
    /// caller holds the Arc for the duration of the FFI call to keep the
    /// native handle valid.
    pub fn admin_handle(&self) -> Arc<AdminClient<DefaultClientContext>> {
        self.admin()
    }

    /// Get a snapshot of the current consumer (cheap Arc clone).
    fn consumer(&self) -> Arc<BaseConsumer> {
        Arc::clone(&self.consumer.lock().unwrap_or_else(|p| p.into_inner()))
    }

    /// Recycle internal Kafka clients to release accumulated librdkafka metadata.
    ///
    /// librdkafka caches `rd_kafka_topic_t` handles internally and never frees
    /// them until the client is destroyed. Periodic recycling prevents unbounded
    /// memory growth on clusters with many topics.
    pub fn recycle(&self) -> Result<()> {
        let rss_before = get_rss_kb();
        let (new_admin, new_consumer) = Self::create_clients(&self.config)?;

        *self.admin.lock().unwrap_or_else(|p| p.into_inner()) = Arc::new(new_admin);
        *self.consumer.lock().unwrap_or_else(|p| p.into_inner()) = Arc::new(new_consumer);

        let rss_after = get_rss_kb();
        info!(
            cluster = %self.config.name,
            rss_before_kb = rss_before,
            rss_after_kb = rss_after,
            rss_reclaimed_kb = rss_before as i64 - rss_after as i64,
            "Recycled Kafka clients"
        );
        Ok(())
    }

    #[allow(dead_code)]
    pub fn performance(&self) -> &PerformanceConfig {
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

    #[instrument(skip(self, group_ids), fields(cluster = %self.config.name, count = group_ids.len()))]
    pub fn describe_consumer_groups(&self, group_ids: &[&str]) -> Result<Vec<GroupDescription>> {
        let consumer = self.consumer();
        let mut descriptions = Vec::with_capacity(group_ids.len());

        for group_id in group_ids {
            let group_list = consumer
                .fetch_group_list(Some(group_id), self.timeout)
                .map_err(KlagError::Kafka)?;

            for group in group_list.groups() {
                let members = group
                    .members()
                    .iter()
                    .map(|m| {
                        let assignments =
                            parse_member_assignment(m.assignment()).unwrap_or_default();

                        GroupMemberInfo {
                            member_id: m.id().to_string(),
                            client_id: m.client_id().to_string(),
                            client_host: m.client_host().to_string(),
                            assignments,
                        }
                    })
                    .collect();

                descriptions.push(GroupDescription {
                    group_id: group.name().to_string(),
                    state: group.state().to_string(),
                    protocol_type: group.protocol_type().to_string(),
                    protocol: group.protocol().to_string(),
                    members,
                });
            }
        }

        Ok(descriptions)
    }

    /// Fetch committed offsets for a consumer group using the Admin API.
    /// Uses the existing AdminClient connection — no additional consumers/FDs needed.
    #[instrument(skip(self, partitions), fields(cluster = %self.config.name, group = %group_id))]
    pub fn list_consumer_group_offsets(
        &self,
        group_id: &str,
        partitions: &[TopicPartition],
        timeout: Duration,
    ) -> Result<HashMap<TopicPartition, i64>> {
        use rdkafka::bindings::*;

        let group_cstr = CString::new(group_id)
            .map_err(|e| KlagError::Admin(format!("Invalid group_id contains null byte: {e}")))?;
        let timeout_ms = timeout.as_millis().min(i32::MAX as u128) as i32;

        // Hold an Arc to the admin client for the duration of the FFI call
        // to keep the native rd_kafka_t handle valid.
        let admin = self.admin();

        unsafe {
            let rk = admin.inner().native_ptr();

            // Build the C topic-partition list from our partitions
            let c_tpl = rd_kafka_topic_partition_list_new(partitions.len() as i32);
            if c_tpl.is_null() {
                return Err(KlagError::Admin(
                    "Failed to create topic partition list".into(),
                ));
            }

            // RAII guard to clean up all C resources on any exit path
            struct Cleanup {
                tpl: *mut rd_kafka_topic_partition_list_t,
                request: *mut rd_kafka_ListConsumerGroupOffsets_t,
                options: *mut rd_kafka_AdminOptions_t,
                queue: *mut rd_kafka_queue_t,
                event: *mut rd_kafka_event_t,
                // CStrings kept alive for the duration of the FFI call
                _topic_cstrings: Vec<CString>,
            }
            impl Drop for Cleanup {
                fn drop(&mut self) {
                    unsafe {
                        if !self.event.is_null() {
                            rd_kafka_event_destroy(self.event);
                        }
                        if !self.queue.is_null() {
                            rd_kafka_queue_destroy(self.queue);
                        }
                        if !self.options.is_null() {
                            rd_kafka_AdminOptions_destroy(self.options);
                        }
                        if !self.request.is_null() {
                            rd_kafka_ListConsumerGroupOffsets_destroy(self.request);
                        }
                        // tpl ownership is transferred to the request via
                        // rd_kafka_ListConsumerGroupOffsets_new, which copies it.
                        // We still own the original and must free it.
                        if !self.tpl.is_null() {
                            rd_kafka_topic_partition_list_destroy(self.tpl);
                        }
                    }
                }
            }

            let mut cleanup = Cleanup {
                tpl: c_tpl,
                request: std::ptr::null_mut(),
                options: std::ptr::null_mut(),
                queue: std::ptr::null_mut(),
                event: std::ptr::null_mut(),
                _topic_cstrings: Vec::with_capacity(partitions.len()),
            };

            // Populate the partition list
            for tp in partitions {
                let topic_cstr = CString::new(tp.topic.as_str())
                    .map_err(|e| KlagError::Admin(format!("Topic name contains null byte: {e}")))?;
                cleanup._topic_cstrings.push(topic_cstr);
                let cstr_ptr = cleanup._topic_cstrings.last().unwrap().as_ptr();
                rd_kafka_topic_partition_list_add(c_tpl, cstr_ptr, tp.partition);
            }

            // Create the request object
            let request = rd_kafka_ListConsumerGroupOffsets_new(group_cstr.as_ptr(), c_tpl);
            if request.is_null() {
                return Err(KlagError::Admin(
                    "Failed to create ListConsumerGroupOffsets request".into(),
                ));
            }
            cleanup.request = request;

            // Create admin options with timeout
            let options = rd_kafka_AdminOptions_new(
                rk,
                rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_LISTCONSUMERGROUPOFFSETS,
            );
            if options.is_null() {
                return Err(KlagError::Admin("Failed to create AdminOptions".into()));
            }
            cleanup.options = options;

            let mut errstr_buf = [0 as c_char; 512];
            let err = rd_kafka_AdminOptions_set_request_timeout(
                options,
                timeout_ms,
                errstr_buf.as_mut_ptr(),
                errstr_buf.len(),
            );
            if err != rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
                let errstr = CStr::from_ptr(errstr_buf.as_ptr())
                    .to_string_lossy()
                    .to_string();
                return Err(KlagError::Admin(format!(
                    "Failed to set request timeout: {errstr}"
                )));
            }

            // Create a temporary queue for the async result
            let queue = rd_kafka_queue_new(rk);
            if queue.is_null() {
                return Err(KlagError::Admin("Failed to create queue".into()));
            }
            cleanup.queue = queue;

            // Issue the async call — caller retains ownership of request
            let mut request_ptr = request;
            rd_kafka_ListConsumerGroupOffsets(rk, &mut request_ptr, 1, options, queue);

            // Poll for the result event (blocks up to timeout)
            let event = rd_kafka_queue_poll(queue, timeout_ms);
            if event.is_null() {
                return Err(KlagError::Admin(
                    "ListConsumerGroupOffsets timed out".into(),
                ));
            }
            cleanup.event = event;

            // Verify it's the right event type
            let event_type = rd_kafka_event_type(event);
            if event_type != RD_KAFKA_EVENT_LISTCONSUMERGROUPOFFSETS_RESULT {
                return Err(KlagError::Admin(format!(
                    "Unexpected event type: {event_type}"
                )));
            }

            // Check top-level error
            let resp_err = rd_kafka_event_error(event);
            if resp_err != rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
                let err_cstr = rd_kafka_event_error_string(event);
                let err_msg = if err_cstr.is_null() {
                    "unknown error".to_string()
                } else {
                    CStr::from_ptr(err_cstr).to_string_lossy().to_string()
                };
                return Err(KlagError::Admin(format!(
                    "ListConsumerGroupOffsets failed: {err_msg}"
                )));
            }

            // Extract the result
            let result = rd_kafka_event_ListConsumerGroupOffsets_result(event);
            if result.is_null() {
                return Err(KlagError::Admin(
                    "ListConsumerGroupOffsets result is null".into(),
                ));
            }

            let mut n_groups: usize = 0;
            let groups_ptr = rd_kafka_ListConsumerGroupOffsets_result_groups(result, &mut n_groups);

            let mut offsets = HashMap::new();

            if groups_ptr.is_null() || n_groups == 0 {
                debug!(group = group_id, "No group results returned from Admin API");
                return Ok(offsets);
            }

            for i in 0..n_groups {
                let group = *groups_ptr.add(i);

                // Check per-group error
                let group_error = rd_kafka_group_result_error(group);
                if !group_error.is_null() {
                    let code = rd_kafka_error_code(group_error);
                    if code != rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
                        let err_str = rd_kafka_error_string(group_error);
                        let msg = if err_str.is_null() {
                            "unknown".to_string()
                        } else {
                            CStr::from_ptr(err_str).to_string_lossy().to_string()
                        };
                        warn!(group = group_id, error = %msg, "Group result error");
                        continue;
                    }
                }

                let result_partitions = rd_kafka_group_result_partitions(group);
                if result_partitions.is_null() {
                    continue;
                }

                let cnt = (*result_partitions).cnt;
                let elems = (*result_partitions).elems;
                if elems.is_null() {
                    continue;
                }

                for j in 0..cnt {
                    let elem = &*elems.add(j as usize);
                    // offset == -1001 (RD_KAFKA_OFFSET_INVALID) means no committed offset
                    if elem.offset >= 0 && !elem.topic.is_null() {
                        let topic = CStr::from_ptr(elem.topic).to_string_lossy().to_string();
                        offsets.insert(TopicPartition::new(topic, elem.partition), elem.offset);
                    }
                }
            }

            debug!(
                group = group_id,
                partitions = offsets.len(),
                "Fetched committed offsets via Admin API"
            );
            Ok(offsets)
        }
    }

    #[instrument(skip(self), fields(cluster = %self.config.name))]
    pub fn fetch_metadata(&self) -> Result<Metadata> {
        self.consumer()
            .fetch_metadata(None, self.timeout)
            .map_err(KlagError::Kafka)
    }

    #[allow(dead_code)]
    #[instrument(skip(self, partitions), fields(cluster = %self.config.name, position = ?position, count = partitions.len()))]
    pub fn list_offsets(
        &self,
        partitions: &[TopicPartition],
        position: OffsetPosition,
    ) -> Result<HashMap<TopicPartition, i64>> {
        let mut tpl = TopicPartitionList::new();
        for tp in partitions {
            let offset = match position {
                OffsetPosition::Earliest => rdkafka::Offset::Beginning,
                OffsetPosition::Latest => rdkafka::Offset::End,
            };
            tpl.add_partition_offset(&tp.topic, tp.partition, offset)
                .map_err(KlagError::Kafka)?;
        }

        let watermarks = self.fetch_watermarks_for_tpl(&tpl, position)?;
        Ok(watermarks)
    }

    #[allow(dead_code)]
    fn fetch_watermarks_for_tpl(
        &self,
        tpl: &TopicPartitionList,
        position: OffsetPosition,
    ) -> Result<HashMap<TopicPartition, i64>> {
        let consumer = self.consumer();
        let mut offsets = HashMap::new();

        for elem in tpl.elements() {
            let (low, high) = consumer
                .fetch_watermarks(elem.topic(), elem.partition(), self.timeout)
                .map_err(KlagError::Kafka)?;

            let offset = match position {
                OffsetPosition::Earliest => low,
                OffsetPosition::Latest => high,
            };

            offsets.insert(TopicPartition::new(elem.topic(), elem.partition()), offset);
        }

        Ok(offsets)
    }

    /// Fetch watermarks sequentially (legacy method).
    /// For large clusters, use `fetch_all_watermarks_parallel` instead.
    #[allow(dead_code)]
    pub fn fetch_all_watermarks(&self) -> Result<HashMap<TopicPartition, (i64, i64)>> {
        let metadata = self.fetch_metadata()?;
        let consumer = self.consumer();
        let mut watermarks = HashMap::new();

        for topic in metadata.topics() {
            for partition in topic.partitions() {
                match consumer.fetch_watermarks(topic.name(), partition.id(), self.timeout) {
                    Ok((low, high)) => {
                        watermarks.insert(
                            TopicPartition::new(topic.name(), partition.id()),
                            (low, high),
                        );
                    }
                    Err(e) => {
                        warn!(
                            topic = topic.name(),
                            partition = partition.id(),
                            error = %e,
                            "Failed to fetch watermarks"
                        );
                    }
                }
            }
        }

        Ok(watermarks)
    }

    /// Fetch watermarks for all partitions in parallel with bounded concurrency.
    /// This is more efficient for large clusters with many partitions.
    #[instrument(skip(self), fields(cluster = %self.config.name))]
    pub async fn fetch_all_watermarks_parallel(
        &self,
    ) -> Result<HashMap<TopicPartition, (i64, i64)>> {
        let metadata = self.fetch_metadata()?;
        let max_concurrent = self.performance.max_concurrent_watermarks;

        // Collect all topic-partition pairs
        let partitions: Vec<(String, i32)> = metadata
            .topics()
            .iter()
            .flat_map(|topic| {
                topic
                    .partitions()
                    .iter()
                    .map(move |p| (topic.name().to_string(), p.id()))
            })
            .collect();

        let total_partitions = partitions.len();
        debug!(
            cluster = %self.config.name,
            partitions = total_partitions,
            max_concurrent = max_concurrent,
            "Fetching watermarks in parallel"
        );

        // Use semaphore to limit concurrency
        let semaphore = Arc::new(Semaphore::new(max_concurrent));
        let consumer = self.consumer();
        let timeout = self.timeout;

        let mut handles = Vec::with_capacity(partitions.len());

        for (topic, partition) in partitions {
            let semaphore_clone = semaphore.clone();
            let consumer_clone = Arc::clone(&consumer);

            let handle = tokio::spawn(async move {
                let permit: OwnedSemaphorePermit = semaphore_clone
                    .acquire_owned()
                    .await
                    .expect("semaphore closed");

                tokio::task::spawn_blocking(move || {
                    let _permit = permit;

                    match consumer_clone.fetch_watermarks(&topic, partition, timeout) {
                        Ok((low, high)) => {
                            Some((TopicPartition::new(&topic, partition), (low, high)))
                        }
                        Err(e) => {
                            warn!(
                                topic = topic,
                                partition = partition,
                                error = %e,
                                "Failed to fetch watermarks"
                            );
                            None
                        }
                    }
                })
                .await
            });

            handles.push(handle);
        }

        // Wait for all tasks to complete (nested Result from tokio::spawn -> spawn_blocking)
        let results = futures::future::join_all(handles).await;

        let mut watermarks = HashMap::new();
        for result in results {
            if let Ok(Ok(Some((tp, wm)))) = result {
                watermarks.insert(tp, wm);
            }
        }

        debug!(
            cluster = %self.config.name,
            fetched = watermarks.len(),
            total = total_partitions,
            "Parallel watermark fetch completed"
        );

        Ok(watermarks)
    }

    #[allow(dead_code)]
    pub fn admin_options(&self) -> AdminOptions {
        AdminOptions::new().request_timeout(Some(self.timeout))
    }

    /// Fetch topics that have compaction enabled (cleanup.policy contains "compact")
    #[instrument(skip(self), fields(cluster = %self.config.name))]
    pub async fn fetch_compacted_topics(&self) -> Result<HashSet<String>> {
        let metadata = self.fetch_metadata()?;
        let topic_names: Vec<String> = metadata
            .topics()
            .iter()
            .map(|t| t.name().to_string())
            .collect();

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
                    // Extract topic name from OwnedResourceSpecifier
                    let topic_name = match &resource.specifier {
                        rdkafka::admin::OwnedResourceSpecifier::Topic(name) => name.clone(),
                        _ => continue, // Skip non-topic resources
                    };
                    for entry in resource.entries {
                        if entry.name == "cleanup.policy" {
                            if let Some(value) = entry.value {
                                if value.contains("compact") {
                                    debug!(topic = %topic_name, cleanup_policy = %value, "Topic has compaction enabled");
                                    compacted_topics.insert(topic_name.clone());
                                }
                            }
                        }
                    }
                }
                Err(err) => {
                    warn!(
                        error = %err,
                        "Failed to describe config for resource"
                    );
                }
            }
        }

        debug!(
            count = compacted_topics.len(),
            topics = ?compacted_topics,
            "Identified compacted topics"
        );

        Ok(compacted_topics)
    }
}

fn parse_member_assignment(assignment: Option<&[u8]>) -> Option<Vec<TopicPartition>> {
    let data = assignment?;
    if data.len() < 4 {
        return None;
    }

    let mut assignments = Vec::new();
    let mut pos = 0;

    // Skip version (2 bytes)
    if data.len() < 2 {
        return None;
    }
    pos += 2;

    // Number of topics (4 bytes)
    if data.len() < pos + 4 {
        return None;
    }
    let num_topics = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
    pos += 4;

    for _ in 0..num_topics {
        // Topic name length (2 bytes)
        if data.len() < pos + 2 {
            break;
        }
        let topic_len = i16::from_be_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2;

        // Topic name
        if data.len() < pos + topic_len {
            break;
        }
        let topic = String::from_utf8_lossy(&data[pos..pos + topic_len]).to_string();
        pos += topic_len;

        // Number of partitions (4 bytes)
        if data.len() < pos + 4 {
            break;
        }
        let num_partitions =
            i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += 4;

        // Partitions
        for _ in 0..num_partitions {
            if data.len() < pos + 4 {
                break;
            }
            let partition =
                i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
            pos += 4;

            assignments.push(TopicPartition::new(&topic, partition));
        }
    }

    Some(assignments)
}

/// Read current process RSS in kilobytes. Returns 0 on failure or non-Linux.
/// Assumes 4 KB page size (standard for x86_64 Linux where /proc/self/statm exists).
fn get_rss_kb() -> u64 {
    std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|s| s.split_whitespace().nth(1)?.parse::<u64>().ok())
        .map(|pages| pages * 4)
        .unwrap_or(0)
}

impl std::fmt::Debug for KafkaClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaClient")
            .field("cluster", &self.config.name)
            .finish()
    }
}
