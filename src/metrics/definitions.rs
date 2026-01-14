#![allow(dead_code)]

pub const METRIC_PARTITION_LATEST_OFFSET: &str = "kafka_partition_latest_offset";
pub const METRIC_PARTITION_EARLIEST_OFFSET: &str = "kafka_partition_earliest_offset";

pub const METRIC_GROUP_OFFSET: &str = "kafka_consumergroup_group_offset";
pub const METRIC_GROUP_LAG: &str = "kafka_consumergroup_group_lag";
pub const METRIC_GROUP_LAG_SECONDS: &str = "kafka_consumergroup_group_lag_seconds";

pub const METRIC_GROUP_MAX_LAG: &str = "kafka_consumergroup_group_max_lag";
pub const METRIC_GROUP_MAX_LAG_SECONDS: &str = "kafka_consumergroup_group_max_lag_seconds";
pub const METRIC_GROUP_SUM_LAG: &str = "kafka_consumergroup_group_sum_lag";
pub const METRIC_GROUP_TOPIC_SUM_LAG: &str = "kafka_consumergroup_group_topic_sum_lag";

pub const METRIC_POLL_TIME_MS: &str = "kafka_consumergroup_poll_time_ms";
pub const METRIC_SCRAPE_DURATION_SECONDS: &str = "kafka_lag_exporter_scrape_duration_seconds";
pub const METRIC_UP: &str = "kafka_lag_exporter_up";
pub const METRIC_LAST_UPDATE_TIMESTAMP: &str = "kafka_lag_exporter_last_update_timestamp_seconds";
pub const METRIC_COMPACTION_DETECTED: &str = "kafka_lag_exporter_compaction_detected_total";
pub const METRIC_DATA_LOSS_PARTITIONS: &str = "kafka_lag_exporter_data_loss_partitions_total";

// Data loss metrics (per partition)
pub const METRIC_MESSAGES_LOST: &str = "kafka_consumergroup_group_messages_lost";
pub const METRIC_RETENTION_MARGIN: &str = "kafka_consumergroup_group_retention_margin";
pub const METRIC_LAG_RETENTION_RATIO: &str = "kafka_consumergroup_group_lag_retention_ratio";

pub const LABEL_CLUSTER_NAME: &str = "cluster_name";
pub const LABEL_GROUP: &str = "group";
pub const LABEL_TOPIC: &str = "topic";
pub const LABEL_PARTITION: &str = "partition";
pub const LABEL_MEMBER_HOST: &str = "member_host";
pub const LABEL_CONSUMER_ID: &str = "consumer_id";
pub const LABEL_CLIENT_ID: &str = "client_id";
pub const LABEL_COMPACTION_DETECTED: &str = "compaction_detected";
pub const LABEL_DATA_LOSS_DETECTED: &str = "data_loss_detected";

pub const HELP_PARTITION_LATEST_OFFSET: &str = "Latest (high watermark) offset for a partition";
pub const HELP_PARTITION_EARLIEST_OFFSET: &str = "Earliest (low watermark) offset for a partition";
pub const HELP_GROUP_OFFSET: &str = "Last committed offset for a consumer group partition";
pub const HELP_GROUP_LAG: &str =
    "Offset lag (high_watermark - committed) for a consumer group partition";
pub const HELP_GROUP_LAG_SECONDS: &str = "Time lag in seconds for a consumer group partition";
pub const HELP_GROUP_MAX_LAG: &str =
    "Maximum offset lag across all partitions for a consumer group";
pub const HELP_GROUP_MAX_LAG_SECONDS: &str =
    "Maximum time lag in seconds across all partitions for a consumer group";
pub const HELP_GROUP_SUM_LAG: &str = "Sum of offset lag across all partitions for a consumer group";
pub const HELP_GROUP_TOPIC_SUM_LAG: &str = "Sum of offset lag per topic for a consumer group";
pub const HELP_POLL_TIME_MS: &str = "Time taken to poll all offsets in milliseconds";
pub const HELP_SCRAPE_DURATION_SECONDS: &str = "Duration of metrics collection in seconds";
pub const HELP_UP: &str = "1 if the exporter is healthy, 0 otherwise";
pub const HELP_LAST_UPDATE_TIMESTAMP: &str = "Unix timestamp of last successful metrics collection";
pub const HELP_COMPACTION_DETECTED: &str =
    "Number of partitions where log compaction was detected (time lag may be understated)";
pub const HELP_DATA_LOSS_PARTITIONS: &str =
    "Number of partitions where data loss occurred (committed offset < low watermark)";
pub const HELP_MESSAGES_LOST: &str =
    "Number of messages deleted by retention before consumer processed them";
pub const HELP_RETENTION_MARGIN: &str =
    "Offset distance between consumer position and deletion boundary (committed_offset - low_watermark, negative means data loss)";
pub const HELP_LAG_RETENTION_RATIO: &str =
    "Percentage of retention window occupied by consumer lag (0=caught up, 100=at boundary, >100=data loss)";
