use crate::error::{KlagError, Result};
use regex::Regex;
use serde::Deserialize;
use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub exporter: ExporterConfig,
    pub clusters: Vec<ClusterConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ExporterConfig {
    #[serde(with = "humantime_serde", default = "default_poll_interval")]
    pub poll_interval: Duration,
    #[serde(default = "default_http_port")]
    pub http_port: u16,
    #[serde(default = "default_http_host")]
    pub http_host: String,
    #[serde(default = "default_granularity")]
    pub granularity: Granularity,
    #[serde(default)]
    pub timestamp_sampling: TimestampSamplingConfig,
    #[serde(default)]
    pub otel: OtelConfig,
    #[serde(default)]
    pub leadership: LeadershipConfig,
    #[serde(default)]
    pub performance: PerformanceConfig,
    /// How long after the last successful collection a cluster's metrics
    /// are kept in `/metrics` output. Past this age the cluster's points
    /// are filtered out so Prometheus sees a gap instead of a frozen
    /// snapshot (e.g. when collection stalls on a broker issue).
    /// If unset, defaults to `poll_interval × 3`, which gives two full
    /// poll cycles of slack before metrics vanish.
    #[serde(default, with = "humantime_serde::option")]
    pub staleness_threshold: Option<Duration>,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Granularity {
    Topic,
    Partition,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TimestampSamplingMode {
    /// Read the actual message at the committed offset via a pooled
    /// `BaseConsumer` and use its produce timestamp. Exact, but the pool
    /// occupies meaningful resident memory and creates per-cycle FFI
    /// churn on large clusters.
    Message,
    /// Estimate time lag from the observed rate of change of high
    /// watermarks. Pure CPU, no consumer pool, no FFI. Accuracy depends
    /// on steady producer rate across the history window — good enough
    /// for lag alerting, not appropriate for audit-grade timestamp
    /// tracking.
    Rate,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TimestampSamplingConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// How the exporter derives the "seconds of lag" metric. Default is
    /// `rate` — much cheaper on large clusters and scales to thousands
    /// of partitions without a consumer pool. Set to `message` to restore
    /// the pre-Tier-3 behavior (read the actual message at the committed
    /// offset and subtract its produce timestamp from "now").
    #[serde(default = "default_timestamp_sampling_mode")]
    pub mode: TimestampSamplingMode,
    /// Cache TTL for the message-mode sampler. Ignored in `rate` mode.
    #[serde(with = "humantime_serde", default = "default_cache_ttl")]
    pub cache_ttl: Duration,
    /// Maximum concurrent message fetches in `message` mode. Ignored in
    /// `rate` mode (rate mode does no I/O).
    #[serde(default = "default_max_concurrent_fetches")]
    pub max_concurrent_fetches: usize,
    /// `message` mode: skip the per-message fetch for partitions whose
    /// committed offset is below the partition's low watermark. Retention has
    /// already deleted the committed message, so the fetch can only time out
    /// or read the wrong (earliest) message; the partition is still reported,
    /// with offset lag and time lag both 0 and `data_loss_detected` set (the
    /// unconsumable magnitude is carried by `messages_lost` / `retention_margin`).
    /// Off by default. Ignored in `rate` mode.
    #[serde(default = "default_skip_data_loss_partitions")]
    pub skip_data_loss_partitions: bool,
    /// `rate` mode: maximum number of (time, `high_watermark`) samples
    /// retained per partition. Larger = smoother rate estimate, more
    /// memory. Default 5.
    #[serde(default = "default_rate_history_samples")]
    pub rate_history_samples: usize,
    /// `rate` mode: samples older than this are evicted. Default 10
    /// minutes. On long poll intervals (> a few minutes) raise this so
    /// the ring buffer has enough points to compute a stable rate.
    #[serde(with = "humantime_serde", default = "default_rate_history_max_age")]
    pub rate_history_max_age: Duration,
    /// `rate` mode: producers observed to be running below this rate are
    /// treated as idle and time-lag is reported as missing rather than
    /// an unreliable division. Default 0.01 msg/sec.
    #[serde(default = "default_rate_min_msgs_per_sec")]
    pub rate_min_msgs_per_sec: f64,
}

/// Performance tuning configuration for large clusters.
/// These settings control parallelism and timeouts for Kafka operations.
#[derive(Debug, Deserialize, Clone)]
pub struct PerformanceConfig {
    /// Timeout for individual Kafka API operations (metadata, watermarks, etc.)
    #[serde(with = "humantime_serde", default = "default_kafka_timeout")]
    pub kafka_timeout: Duration,
    /// Timeout for fetching committed offsets per consumer group
    #[serde(with = "humantime_serde", default = "default_offset_fetch_timeout")]
    pub offset_fetch_timeout: Duration,
    /// Maximum number of consumer groups to fetch offsets for in parallel
    #[serde(default = "default_max_concurrent_groups")]
    pub max_concurrent_groups: usize,
    /// **Deprecated** since the batched Admin API replaced the per-partition
    /// watermark fan-out: watermarks are now fetched in two batched
    /// `ListOffsets` calls regardless of partition count, so a per-partition
    /// concurrency cap has no effect. Kept in the schema so existing configs
    /// continue to parse; emits a one-time startup INFO if set to a
    /// non-default value. Will be removed in a future release.
    #[serde(default = "default_max_concurrent_watermarks")]
    pub max_concurrent_watermarks: usize,
    /// Number of collection cycles between Kafka client recycling.
    /// Recycling destroys and recreates internal librdkafka clients to release
    /// accumulated metadata that librdkafka never frees on its own.
    /// Set to 0 to disable. Default: 50 (~25 min at 30s poll interval).
    #[serde(default = "default_client_recycle_interval")]
    pub client_recycle_interval: u64,
    /// Maximum size of the Tokio blocking-thread pool used for librdkafka FFI
    /// calls. The default Tokio limit is 512; each thread holds a native
    /// stack (2–8 MB depending on platform), so the worst-case virtual
    /// memory footprint is significant on large clusters. 64 is ample for
    /// this exporter: the hot path's concurrent blocking calls are bounded
    /// by `max_concurrent_groups` + a few for watermark / compacted-topic /
    /// timestamp-sampling FFI calls. Raise this if you set
    /// `max_concurrent_groups` above ~50.
    #[serde(default = "default_max_blocking_threads")]
    pub max_blocking_threads: usize,
    /// How long to cache each topic's `cleanup.policy` in memory.
    /// `cleanup.policy` rarely changes after topic creation, so caching it
    /// for a long time saves a per-cycle `DescribeConfigs` RPC over the
    /// monitored topic set. Only topics new to the cache (or whose entry
    /// has expired) get queried on a given cycle.
    #[serde(
        with = "humantime_serde",
        default = "default_compacted_topics_cache_ttl"
    )]
    pub compacted_topics_cache_ttl: Duration,
    /// How long to cache the derived "monitored partitions + monitored topics"
    /// set from cluster metadata. A cycle with a fresh cache entry skips the
    /// `fetch_metadata` call and the regex filtering pass over every topic
    /// name — significant savings on clusters with thousands of topics.
    /// New topics (or partition additions) become visible at most this long
    /// after they appear on the cluster. Set to 0 to disable the cache
    /// (refresh every cycle).
    #[serde(with = "humantime_serde", default = "default_metadata_cache_ttl")]
    pub metadata_cache_ttl: Duration,
    /// How long to cache the list of consumer groups on the cluster.
    /// `list_consumer_groups` is one RPC per collection cycle; caching
    /// it shaves off one round trip whenever the set of groups is stable
    /// (the common case). Newly-created consumer groups become visible
    /// at most this long after they appear. Set to 0 to disable.
    #[serde(
        with = "humantime_serde",
        default = "default_consumer_groups_cache_ttl"
    )]
    pub consumer_groups_cache_ttl: Duration,
}

#[derive(Debug, Deserialize, Clone)]
pub struct OtelConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_otel_endpoint")]
    pub endpoint: String,
    #[serde(with = "humantime_serde", default = "default_export_interval")]
    pub export_interval: Duration,
}

/// Configuration for leader election in high availability deployments.
#[derive(Debug, Deserialize, Clone)]
pub struct LeadershipConfig {
    /// Enable leader election. When disabled (default), runs in single-instance mode.
    #[serde(default)]
    pub enabled: bool,
    /// Leadership provider type. Currently only "kubernetes" is supported.
    #[serde(default = "default_leadership_provider")]
    pub provider: LeadershipProvider,
    /// Name of the Kubernetes Lease resource.
    #[serde(default = "default_lease_name")]
    pub lease_name: String,
    /// Namespace for the Lease resource. Supports env var substitution.
    #[serde(default = "default_lease_namespace")]
    pub lease_namespace: String,
    /// Identity of this instance. Defaults to HOSTNAME or `POD_NAME` env var.
    #[allow(dead_code)] // Used by kubernetes feature
    pub identity: Option<String>,
    /// Duration the lease is valid in seconds.
    #[serde(default = "default_lease_duration")]
    #[allow(dead_code)] // Used by kubernetes feature
    pub lease_duration_secs: u32,
    /// Grace period for lease renewal in seconds. Must be less than `lease_duration`.
    #[serde(default = "default_grace_period")]
    #[allow(dead_code)] // Used by kubernetes feature
    pub grace_period_secs: u32,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum LeadershipProvider {
    #[default]
    Kubernetes,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ClusterConfig {
    pub name: String,
    pub bootstrap_servers: String,
    #[serde(default = "default_whitelist")]
    pub group_whitelist: Vec<String>,
    #[serde(default)]
    pub group_blacklist: Vec<String>,
    #[serde(default = "default_whitelist")]
    pub topic_whitelist: Vec<String>,
    #[serde(default = "default_topic_blacklist")]
    pub topic_blacklist: Vec<String>,
    #[serde(default)]
    pub consumer_properties: HashMap<String, String>,
    #[serde(default)]
    pub labels: HashMap<String, String>,
}

const fn default_poll_interval() -> Duration {
    Duration::from_secs(30)
}

const fn default_http_port() -> u16 {
    8000
}

fn default_http_host() -> String {
    "0.0.0.0".to_string()
}

const fn default_granularity() -> Granularity {
    Granularity::Topic
}

const fn default_true() -> bool {
    true
}

const fn default_cache_ttl() -> Duration {
    Duration::from_mins(1)
}

const fn default_max_concurrent_fetches() -> usize {
    5
}

const fn default_skip_data_loss_partitions() -> bool {
    false
}

const fn default_timestamp_sampling_mode() -> TimestampSamplingMode {
    TimestampSamplingMode::Rate
}

const fn default_rate_history_samples() -> usize {
    5
}

const fn default_rate_history_max_age() -> Duration {
    Duration::from_mins(10)
}

const fn default_rate_min_msgs_per_sec() -> f64 {
    0.01
}

const fn default_kafka_timeout() -> Duration {
    Duration::from_secs(30)
}

const fn default_offset_fetch_timeout() -> Duration {
    Duration::from_secs(10)
}

const fn default_max_concurrent_groups() -> usize {
    10
}

const fn default_max_concurrent_watermarks() -> usize {
    50
}

const fn default_client_recycle_interval() -> u64 {
    50
}

const fn default_max_blocking_threads() -> usize {
    64
}

const fn default_compacted_topics_cache_ttl() -> Duration {
    Duration::from_hours(1)
}

const fn default_metadata_cache_ttl() -> Duration {
    Duration::from_mins(5)
}

const fn default_consumer_groups_cache_ttl() -> Duration {
    Duration::from_mins(1)
}

fn default_otel_endpoint() -> String {
    "http://localhost:4317".to_string()
}

const fn default_export_interval() -> Duration {
    Duration::from_mins(1)
}

const fn default_leadership_provider() -> LeadershipProvider {
    LeadershipProvider::Kubernetes
}

fn default_lease_name() -> String {
    "klag-exporter".to_string()
}

fn default_lease_namespace() -> String {
    "default".to_string()
}

const fn default_lease_duration() -> u32 {
    15
}

const fn default_grace_period() -> u32 {
    5
}

fn default_whitelist() -> Vec<String> {
    vec![".*".to_string()]
}

fn default_topic_blacklist() -> Vec<String> {
    vec!["__.*".to_string()]
}

impl Default for TimestampSamplingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            mode: default_timestamp_sampling_mode(),
            cache_ttl: default_cache_ttl(),
            max_concurrent_fetches: default_max_concurrent_fetches(),
            skip_data_loss_partitions: default_skip_data_loss_partitions(),
            rate_history_samples: default_rate_history_samples(),
            rate_history_max_age: default_rate_history_max_age(),
            rate_min_msgs_per_sec: default_rate_min_msgs_per_sec(),
        }
    }
}

impl Default for OtelConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint: default_otel_endpoint(),
            export_interval: default_export_interval(),
        }
    }
}

impl Default for LeadershipConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            provider: default_leadership_provider(),
            lease_name: default_lease_name(),
            lease_namespace: default_lease_namespace(),
            identity: None,
            lease_duration_secs: default_lease_duration(),
            grace_period_secs: default_grace_period(),
        }
    }
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            kafka_timeout: default_kafka_timeout(),
            offset_fetch_timeout: default_offset_fetch_timeout(),
            max_concurrent_groups: default_max_concurrent_groups(),
            max_concurrent_watermarks: default_max_concurrent_watermarks(),
            client_recycle_interval: default_client_recycle_interval(),
            max_blocking_threads: default_max_blocking_threads(),
            compacted_topics_cache_ttl: default_compacted_topics_cache_ttl(),
            metadata_cache_ttl: default_metadata_cache_ttl(),
            consumer_groups_cache_ttl: default_consumer_groups_cache_ttl(),
        }
    }
}

impl Config {
    pub fn load(path: Option<&str>) -> Result<Self> {
        let config_path = path.unwrap_or("config.toml");

        if !Path::new(config_path).exists() {
            return Err(KlagError::Config(format!(
                "Configuration file not found: {config_path}"
            )));
        }

        let content = std::fs::read_to_string(config_path)?;
        let content = Self::substitute_env_vars(&content);

        let config: Self = toml::from_str(&content)
            .map_err(|e| KlagError::Config(format!("TOML parse error: {e}")))?;

        config.validate()?;
        Ok(config)
    }

    fn substitute_env_vars(content: &str) -> String {
        // Supports:
        // - ${VAR} - replaced with env var value, empty string if not set
        // - ${VAR:-default} - replaced with env var value, or "default" if not set
        // - ${?VAR} - replaced with env var value if set, empty string if not set (same as ${VAR})
        let re = Regex::new(r"\$\{\??([^}:-]+)(?::-([^}]*))?\}").unwrap();
        re.replace_all(content, |caps: &regex::Captures| {
            let var_name = &caps[1];
            let default_value = caps.get(2).map_or("", |m| m.as_str());
            std::env::var(var_name).unwrap_or_else(|_| default_value.to_string())
        })
        .to_string()
    }

    pub fn validate(&self) -> Result<()> {
        if self.clusters.is_empty() {
            return Err(KlagError::Config(
                "At least one cluster must be configured".to_string(),
            ));
        }

        for cluster in &self.clusters {
            cluster.validate()?;
        }

        // A zero poll_interval would busy-loop the collector and — since the
        // default staleness threshold is derived from it — also make every
        // cluster's metrics disappear from /metrics instantly.
        if self.exporter.poll_interval.is_zero() {
            return Err(KlagError::Config(
                "poll_interval must be greater than 0".to_string(),
            ));
        }
        if let Some(threshold) = self.exporter.staleness_threshold {
            if threshold.is_zero() {
                return Err(KlagError::Config(
                    "staleness_threshold must be greater than 0 when set".to_string(),
                ));
            }
        }

        // Validate performance config
        if self.exporter.performance.max_concurrent_groups == 0 {
            return Err(KlagError::Config(
                "performance.max_concurrent_groups must be at least 1".to_string(),
            ));
        }
        if self.exporter.performance.max_concurrent_watermarks == 0 {
            return Err(KlagError::Config(
                "performance.max_concurrent_watermarks must be at least 1".to_string(),
            ));
        }

        // Timestamp-sampling validation. Only enforce the constraints that
        // actually matter for the configured mode so rate-mode users aren't
        // forced to tune message-mode knobs (and vice versa).
        let ts = &self.exporter.timestamp_sampling;
        if ts.enabled && ts.mode == TimestampSamplingMode::Message && ts.max_concurrent_fetches == 0
        {
            return Err(KlagError::Config(
                "timestamp_sampling.max_concurrent_fetches must be >= 1 when mode = 'message'"
                    .to_string(),
            ));
        }
        if ts.enabled && ts.mode == TimestampSamplingMode::Rate {
            if ts.rate_history_samples < 2 {
                return Err(KlagError::Config(
                    "timestamp_sampling.rate_history_samples must be >= 2 (need two samples \
                     to compute a rate)"
                        .to_string(),
                ));
            }
            if !ts.rate_min_msgs_per_sec.is_finite() || ts.rate_min_msgs_per_sec < 0.0 {
                return Err(KlagError::Config(format!(
                    "timestamp_sampling.rate_min_msgs_per_sec ({}) must be finite and >= 0",
                    ts.rate_min_msgs_per_sec
                )));
            }
        }
        // The blocking-thread pool must be able to hold every concurrent FFI
        // call our hot path spawns simultaneously. `max_concurrent_groups`
        // is the dominant consumer; the timestamp sampler adds up to
        // `max_concurrent_fetches` more — but only when sampling is enabled
        // AND in `message` mode. Rate mode does no I/O and consumes no
        // blocking threads. If the pool is too small, tasks queue behind
        // each other and we effectively serialize — silently undoing the
        // Tier 1 win.
        let sampler_uses_blocking_threads = self.exporter.timestamp_sampling.enabled
            && matches!(
                self.exporter.timestamp_sampling.mode,
                TimestampSamplingMode::Message
            );
        let sampler_contribution = if sampler_uses_blocking_threads {
            self.exporter.timestamp_sampling.max_concurrent_fetches
        } else {
            0
        };
        let min_needed = self.exporter.performance.max_concurrent_groups + sampler_contribution + 4; // small headroom for watermark / compacted-topic / metadata FFI
        if self.exporter.performance.max_blocking_threads < min_needed {
            let sampling_state = match (
                self.exporter.timestamp_sampling.enabled,
                self.exporter.timestamp_sampling.mode,
            ) {
                (false, _) => "disabled",
                (true, TimestampSamplingMode::Rate) => "rate (no FFI)",
                (true, TimestampSamplingMode::Message) => "message",
            };
            return Err(KlagError::Config(format!(
                "performance.max_blocking_threads ({}) must be >= max_concurrent_groups ({}) + \
                 timestamp_sampling.max_concurrent_fetches ({}, sampling {}) + 4 = {}",
                self.exporter.performance.max_blocking_threads,
                self.exporter.performance.max_concurrent_groups,
                sampler_contribution,
                sampling_state,
                min_needed,
            )));
        }

        Ok(())
    }
}

impl ClusterConfig {
    pub fn validate(&self) -> Result<()> {
        if self.name.is_empty() {
            return Err(KlagError::Config(
                "Cluster name cannot be empty".to_string(),
            ));
        }

        if self.bootstrap_servers.is_empty() {
            return Err(KlagError::Config(format!(
                "Cluster '{}': bootstrap_servers cannot be empty",
                self.name
            )));
        }

        self.compile_filters()?;
        Ok(())
    }

    pub fn compile_filters(&self) -> Result<CompiledFilters> {
        let group_whitelist = self
            .group_whitelist
            .iter()
            .map(|p| Regex::new(p))
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let group_blacklist = self
            .group_blacklist
            .iter()
            .map(|p| Regex::new(p))
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let topic_whitelist = self
            .topic_whitelist
            .iter()
            .map(|p| Regex::new(p))
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let topic_blacklist = self
            .topic_blacklist
            .iter()
            .map(|p| Regex::new(p))
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(CompiledFilters {
            group_whitelist,
            group_blacklist,
            topic_whitelist,
            topic_blacklist,
        })
    }
}

#[derive(Debug, Clone)]
pub struct CompiledFilters {
    pub group_whitelist: Vec<Regex>,
    pub group_blacklist: Vec<Regex>,
    pub topic_whitelist: Vec<Regex>,
    pub topic_blacklist: Vec<Regex>,
}

impl CompiledFilters {
    pub fn matches_group(&self, group: &str) -> bool {
        let matches_whitelist = self.group_whitelist.iter().any(|r| r.is_match(group));
        let matches_blacklist = self.group_blacklist.iter().any(|r| r.is_match(group));
        matches_whitelist && !matches_blacklist
    }

    pub fn matches_topic(&self, topic: &str) -> bool {
        let matches_whitelist = self.topic_whitelist.iter().any(|r| r.is_match(topic));
        let matches_blacklist = self.topic_blacklist.iter().any(|r| r.is_match(topic));
        matches_whitelist && !matches_blacklist
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_config_loads_from_file() {
        let config_content = r#"
[exporter]
poll_interval = "30s"
http_port = 8000

[[clusters]]
name = "test"
bootstrap_servers = "localhost:9092"
"#;

        let mut file = NamedTempFile::new().unwrap();
        file.write_all(config_content.as_bytes()).unwrap();

        let config = Config::load(Some(file.path().to_str().unwrap())).unwrap();
        assert_eq!(config.exporter.poll_interval, Duration::from_secs(30));
        assert_eq!(config.exporter.http_port, 8000);
        assert_eq!(config.clusters.len(), 1);
        assert_eq!(config.clusters[0].name, "test");
    }

    #[test]
    fn test_config_env_override() {
        std::env::set_var("TEST_KAFKA_USER", "myuser");

        let config_content = r#"
[exporter]
poll_interval = "30s"

[[clusters]]
name = "test"
bootstrap_servers = "localhost:9092"

[clusters.consumer_properties]
"sasl.username" = "${TEST_KAFKA_USER}"
"#;

        let mut file = NamedTempFile::new().unwrap();
        file.write_all(config_content.as_bytes()).unwrap();

        let config = Config::load(Some(file.path().to_str().unwrap())).unwrap();
        assert_eq!(
            config.clusters[0].consumer_properties.get("sasl.username"),
            Some(&"myuser".to_string())
        );

        std::env::remove_var("TEST_KAFKA_USER");
    }

    #[test]
    fn test_config_env_with_default() {
        // Ensure env var is NOT set
        std::env::remove_var("TEST_NONEXISTENT_VAR");

        let config_content = r#"
[exporter]
poll_interval = "30s"

[[clusters]]
name = "test"
bootstrap_servers = "${TEST_NONEXISTENT_VAR:-localhost:9092}"
"#;

        let mut file = NamedTempFile::new().unwrap();
        file.write_all(config_content.as_bytes()).unwrap();

        let config = Config::load(Some(file.path().to_str().unwrap())).unwrap();
        // Should use default value since env var is not set
        assert_eq!(config.clusters[0].bootstrap_servers, "localhost:9092");
    }

    #[test]
    fn test_config_env_override_default() {
        std::env::set_var("TEST_BOOTSTRAP", "kafka:29092");

        let config_content = r#"
[exporter]
poll_interval = "30s"

[[clusters]]
name = "test"
bootstrap_servers = "${TEST_BOOTSTRAP:-localhost:9092}"
"#;

        let mut file = NamedTempFile::new().unwrap();
        file.write_all(config_content.as_bytes()).unwrap();

        let config = Config::load(Some(file.path().to_str().unwrap())).unwrap();
        // Should use env var value instead of default
        assert_eq!(config.clusters[0].bootstrap_servers, "kafka:29092");

        std::env::remove_var("TEST_BOOTSTRAP");
    }

    #[test]
    fn test_config_validates_bootstrap_servers() {
        let config_content = r#"
[exporter]
poll_interval = "30s"

[[clusters]]
name = "test"
bootstrap_servers = ""
"#;

        let mut file = NamedTempFile::new().unwrap();
        file.write_all(config_content.as_bytes()).unwrap();

        let result = Config::load(Some(file.path().to_str().unwrap()));
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("bootstrap_servers cannot be empty"));
    }

    #[test]
    fn test_regex_filter_whitelist_match() {
        let filters = CompiledFilters {
            group_whitelist: vec![Regex::new("^my-group.*").unwrap()],
            group_blacklist: vec![],
            topic_whitelist: vec![Regex::new(".*").unwrap()],
            topic_blacklist: vec![],
        };

        assert!(filters.matches_group("my-group-1"));
        assert!(filters.matches_group("my-group-2"));
        assert!(!filters.matches_group("other-group"));
    }

    #[test]
    fn test_regex_filter_blacklist_reject() {
        let filters = CompiledFilters {
            group_whitelist: vec![Regex::new(".*").unwrap()],
            group_blacklist: vec![Regex::new("^internal-.*").unwrap()],
            topic_whitelist: vec![Regex::new(".*").unwrap()],
            topic_blacklist: vec![Regex::new("^__.*").unwrap()],
        };

        assert!(filters.matches_group("my-group"));
        assert!(!filters.matches_group("internal-group"));
        assert!(filters.matches_topic("my-topic"));
        assert!(!filters.matches_topic("__consumer_offsets"));
    }

    #[test]
    fn test_default_config_values() {
        let config_content = r#"
[exporter]

[[clusters]]
name = "test"
bootstrap_servers = "localhost:9092"
"#;

        let mut file = NamedTempFile::new().unwrap();
        file.write_all(config_content.as_bytes()).unwrap();

        let config = Config::load(Some(file.path().to_str().unwrap())).unwrap();
        assert_eq!(config.exporter.poll_interval, Duration::from_secs(30));
        assert_eq!(config.exporter.http_port, 8000);
        assert_eq!(config.exporter.http_host, "0.0.0.0");
        assert_eq!(config.exporter.granularity, Granularity::Topic);
        assert!(config.exporter.timestamp_sampling.enabled);
        assert_eq!(
            config.exporter.timestamp_sampling.mode,
            TimestampSamplingMode::Rate,
            "default mode should be rate (Tier 3)"
        );
        assert!(
            !config.exporter.timestamp_sampling.skip_data_loss_partitions,
            "skip_data_loss_partitions should default to false"
        );
        assert!(!config.exporter.otel.enabled);
        // Performance defaults
        assert_eq!(
            config.exporter.performance.kafka_timeout,
            Duration::from_secs(30)
        );
        assert_eq!(
            config.exporter.performance.offset_fetch_timeout,
            Duration::from_secs(10)
        );
        assert_eq!(config.exporter.performance.max_concurrent_groups, 10);
        assert_eq!(config.exporter.performance.max_concurrent_watermarks, 50);
        assert_eq!(config.exporter.performance.client_recycle_interval, 50);
        assert_eq!(config.exporter.performance.max_blocking_threads, 64);
        assert_eq!(
            config.exporter.performance.compacted_topics_cache_ttl,
            Duration::from_secs(3600)
        );
        assert_eq!(
            config.exporter.performance.metadata_cache_ttl,
            Duration::from_secs(300)
        );
        assert_eq!(
            config.exporter.performance.consumer_groups_cache_ttl,
            Duration::from_secs(60)
        );
        assert!(
            config.exporter.staleness_threshold.is_none(),
            "staleness_threshold should default to None (falls back to poll_interval * 3)"
        );
    }

    #[test]
    fn test_skip_data_loss_partitions_parses() {
        let config_content = r#"
[exporter]

[exporter.timestamp_sampling]
mode = "message"
skip_data_loss_partitions = true

[[clusters]]
name = "test"
bootstrap_servers = "localhost:9092"
"#;
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(config_content.as_bytes()).unwrap();
        let config = Config::load(Some(file.path().to_str().unwrap())).unwrap();
        assert!(config.exporter.timestamp_sampling.skip_data_loss_partitions);
    }

    #[test]
    fn test_zero_poll_interval_rejected() {
        let config_content = r#"
[exporter]
poll_interval = "0s"

[[clusters]]
name = "test"
bootstrap_servers = "localhost:9092"
"#;

        let mut file = NamedTempFile::new().unwrap();
        file.write_all(config_content.as_bytes()).unwrap();

        let err = Config::load(Some(file.path().to_str().unwrap()))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("poll_interval must be greater than 0"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_zero_staleness_threshold_rejected() {
        let config_content = r#"
[exporter]
poll_interval = "30s"
staleness_threshold = "0s"

[[clusters]]
name = "test"
bootstrap_servers = "localhost:9092"
"#;

        let mut file = NamedTempFile::new().unwrap();
        file.write_all(config_content.as_bytes()).unwrap();

        let err = Config::load(Some(file.path().to_str().unwrap()))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("staleness_threshold must be greater than 0"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_staleness_threshold_custom_value() {
        let config_content = r#"
[exporter]
poll_interval = "30s"
staleness_threshold = "10m"

[[clusters]]
name = "test"
bootstrap_servers = "localhost:9092"
"#;

        let mut file = NamedTempFile::new().unwrap();
        file.write_all(config_content.as_bytes()).unwrap();

        let config = Config::load(Some(file.path().to_str().unwrap())).unwrap();
        assert_eq!(
            config.exporter.staleness_threshold,
            Some(Duration::from_secs(600))
        );
    }

    #[test]
    fn test_performance_config_custom_values() {
        let config_content = r#"
[exporter]
poll_interval = "60s"

[exporter.performance]
kafka_timeout = "15s"
offset_fetch_timeout = "5s"
max_concurrent_groups = 20
max_concurrent_watermarks = 100
client_recycle_interval = 0

[[clusters]]
name = "test"
bootstrap_servers = "localhost:9092"
"#;

        let mut file = NamedTempFile::new().unwrap();
        file.write_all(config_content.as_bytes()).unwrap();

        let config = Config::load(Some(file.path().to_str().unwrap())).unwrap();
        assert_eq!(
            config.exporter.performance.kafka_timeout,
            Duration::from_secs(15)
        );
        assert_eq!(
            config.exporter.performance.offset_fetch_timeout,
            Duration::from_secs(5)
        );
        assert_eq!(config.exporter.performance.max_concurrent_groups, 20);
        assert_eq!(config.exporter.performance.max_concurrent_watermarks, 100);
        assert_eq!(config.exporter.performance.client_recycle_interval, 0);
    }

    #[test]
    fn test_max_blocking_threads_rejected_when_too_small() {
        // Message mode uses blocking threads. Default max_concurrent_groups=10,
        // max_concurrent_fetches=5, so minimum = 10 + 5 + 4 = 19. 16 must fail.
        let config_content = r#"
[exporter]

[exporter.timestamp_sampling]
mode = "message"

[exporter.performance]
max_blocking_threads = 16

[[clusters]]
name = "test"
bootstrap_servers = "localhost:9092"
"#;
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(config_content.as_bytes()).unwrap();

        let err = Config::load(Some(file.path().to_str().unwrap())).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("max_blocking_threads (16)"),
            "unexpected error: {msg}"
        );
        assert!(msg.contains("= 19"), "unexpected error: {msg}");
    }

    #[test]
    fn test_message_mode_rejects_zero_concurrent_fetches() {
        let config_content = r#"
[exporter]

[exporter.timestamp_sampling]
mode = "message"
max_concurrent_fetches = 0

[[clusters]]
name = "test"
bootstrap_servers = "localhost:9092"
"#;
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(config_content.as_bytes()).unwrap();
        let err = Config::load(Some(file.path().to_str().unwrap())).unwrap_err();
        assert!(
            err.to_string()
                .contains("max_concurrent_fetches must be >= 1"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_rate_mode_rejects_one_history_sample() {
        let config_content = r#"
[exporter]

[exporter.timestamp_sampling]
rate_history_samples = 1

[[clusters]]
name = "test"
bootstrap_servers = "localhost:9092"
"#;
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(config_content.as_bytes()).unwrap();
        let err = Config::load(Some(file.path().to_str().unwrap())).unwrap_err();
        assert!(
            err.to_string()
                .contains("rate_history_samples must be >= 2"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_rate_mode_rejects_negative_min_rate() {
        let config_content = r#"
[exporter]

[exporter.timestamp_sampling]
rate_min_msgs_per_sec = -1.0

[[clusters]]
name = "test"
bootstrap_servers = "localhost:9092"
"#;
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(config_content.as_bytes()).unwrap();
        let err = Config::load(Some(file.path().to_str().unwrap())).unwrap_err();
        assert!(
            err.to_string().contains("rate_min_msgs_per_sec")
                && err.to_string().contains("must be finite and >= 0"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_max_blocking_threads_sampler_excluded_when_disabled() {
        // Sampling disabled → sampler_contribution = 0. Required min becomes
        // max_concurrent_groups (10 default) + 4 = 14. 16 must be accepted
        // (would have been rejected with the old validation that always
        // counted max_concurrent_fetches = 5).
        let config_content = r#"
[exporter]

[exporter.timestamp_sampling]
enabled = false

[exporter.performance]
max_blocking_threads = 16

[[clusters]]
name = "test"
bootstrap_servers = "localhost:9092"
"#;
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(config_content.as_bytes()).unwrap();

        let config = Config::load(Some(file.path().to_str().unwrap()))
            .expect("should accept 16 when sampling is disabled");
        assert_eq!(config.exporter.performance.max_blocking_threads, 16);
    }

    #[test]
    fn test_max_blocking_threads_custom_value_accepted() {
        let config_content = r#"
[exporter]

[exporter.performance]
max_concurrent_groups = 30
max_blocking_threads = 128

[exporter.timestamp_sampling]
max_concurrent_fetches = 20

[[clusters]]
name = "test"
bootstrap_servers = "localhost:9092"
"#;
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(config_content.as_bytes()).unwrap();

        let config = Config::load(Some(file.path().to_str().unwrap())).unwrap();
        assert_eq!(config.exporter.performance.max_blocking_threads, 128);
    }

    #[test]
    fn test_performance_config_validates_zero_concurrency() {
        let config_content = r#"
[exporter]
poll_interval = "30s"

[exporter.performance]
max_concurrent_groups = 0

[[clusters]]
name = "test"
bootstrap_servers = "localhost:9092"
"#;

        let mut file = NamedTempFile::new().unwrap();
        file.write_all(config_content.as_bytes()).unwrap();

        let result = Config::load(Some(file.path().to_str().unwrap()));
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("max_concurrent_groups must be at least 1"));
    }

    #[test]
    fn test_performance_config_validates_zero_watermarks_concurrency() {
        let config_content = r#"
[exporter]
poll_interval = "30s"

[exporter.performance]
max_concurrent_watermarks = 0

[[clusters]]
name = "test"
bootstrap_servers = "localhost:9092"
"#;

        let mut file = NamedTempFile::new().unwrap();
        file.write_all(config_content.as_bytes()).unwrap();

        let result = Config::load(Some(file.path().to_str().unwrap()));
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("max_concurrent_watermarks must be at least 1"));
    }
}
