use crate::config::ClusterConfig;
use crate::error::{KlagError, Result};
use rdkafka::admin::{AdminClient, AdminOptions, ResourceSpecifier};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::groups::GroupList;
use rdkafka::metadata::Metadata;
use rdkafka::TopicPartitionList;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tracing::{debug, instrument, warn};

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
    admin: AdminClient<DefaultClientContext>,
    consumer: BaseConsumer,
    config: ClusterConfig,
    timeout: Duration,
}

impl KafkaClient {
    pub fn new(config: &ClusterConfig) -> Result<Self> {
        let timeout = Duration::from_secs(30);

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
            .create()
            .map_err(KlagError::Kafka)?;

        Ok(Self {
            admin,
            consumer,
            config: config.clone(),
            timeout,
        })
    }

    pub fn cluster_name(&self) -> &str {
        &self.config.name
    }

    pub fn consumer_properties(&self) -> &HashMap<String, String> {
        &self.config.consumer_properties
    }

    #[instrument(skip(self), fields(cluster = %self.config.name))]
    pub fn list_consumer_groups(&self) -> Result<Vec<ConsumerGroupInfo>> {
        let group_list: GroupList = self
            .consumer
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
        let mut descriptions = Vec::with_capacity(group_ids.len());

        for group_id in group_ids {
            let group_list = self
                .consumer
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

    #[allow(dead_code)]
    #[instrument(skip(self), fields(cluster = %self.config.name, group = %group_id))]
    pub fn list_consumer_group_offsets(
        &self,
        group_id: &str,
    ) -> Result<HashMap<TopicPartition, i64>> {
        let metadata = self.fetch_metadata()?;
        let mut tpl = TopicPartitionList::new();

        for topic in metadata.topics() {
            for partition in topic.partitions() {
                tpl.add_partition(topic.name(), partition.id());
            }
        }

        let committed = self
            .consumer
            .committed_offsets(tpl, self.timeout)
            .map_err(KlagError::Kafka)?;

        let mut offsets = HashMap::new();
        for elem in committed.elements() {
            if let rdkafka::Offset::Offset(offset) = elem.offset() {
                offsets.insert(TopicPartition::new(elem.topic(), elem.partition()), offset);
            }
        }

        debug!(
            group = group_id,
            partitions = offsets.len(),
            "Fetched committed offsets"
        );
        Ok(offsets)
    }

    #[instrument(skip(self), fields(cluster = %self.config.name))]
    pub fn fetch_metadata(&self) -> Result<Metadata> {
        self.consumer
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
        let mut offsets = HashMap::new();

        for elem in tpl.elements() {
            let (low, high) = self
                .consumer
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

    pub fn fetch_all_watermarks(&self) -> Result<HashMap<TopicPartition, (i64, i64)>> {
        let metadata = self.fetch_metadata()?;
        let mut watermarks = HashMap::new();

        for topic in metadata.topics() {
            for partition in topic.partitions() {
                match self
                    .consumer
                    .fetch_watermarks(topic.name(), partition.id(), self.timeout)
                {
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

    #[allow(dead_code)]
    pub fn inner_admin(&self) -> &AdminClient<DefaultClientContext> {
        &self.admin
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

        let opts = self.admin_options();
        let results = self
            .admin
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

impl std::fmt::Debug for KafkaClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaClient")
            .field("cluster", &self.config.name)
            .finish()
    }
}
