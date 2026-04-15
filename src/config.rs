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
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Granularity {
    Topic,
    Partition,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TimestampSamplingConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(with = "humantime_serde", default = "default_cache_ttl")]
    pub cache_ttl: Duration,
    #[serde(default = "default_max_concurrent_fetches")]
    pub max_concurrent_fetches: usize,
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
    /// Maximum number of partitions to fetch watermarks for in parallel
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
    /// Identity of this instance. Defaults to HOSTNAME or POD_NAME env var.
    #[allow(dead_code)] // Used by kubernetes feature
    pub identity: Option<String>,
    /// Duration the lease is valid in seconds.
    #[serde(default = "default_lease_duration")]
    #[allow(dead_code)] // Used by kubernetes feature
    pub lease_duration_secs: u32,
    /// Grace period for lease renewal in seconds. Must be less than lease_duration.
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

fn default_poll_interval() -> Duration {
    Duration::from_secs(30)
}

fn default_http_port() -> u16 {
    8000
}

fn default_http_host() -> String {
    "0.0.0.0".to_string()
}

fn default_granularity() -> Granularity {
    Granularity::Topic
}

fn default_true() -> bool {
    true
}

fn default_cache_ttl() -> Duration {
    Duration::from_secs(60)
}

fn default_max_concurrent_fetches() -> usize {
    5
}

fn default_kafka_timeout() -> Duration {
    Duration::from_secs(30)
}

fn default_offset_fetch_timeout() -> Duration {
    Duration::from_secs(10)
}

fn default_max_concurrent_groups() -> usize {
    10
}

fn default_max_concurrent_watermarks() -> usize {
    50
}

fn default_client_recycle_interval() -> u64 {
    50
}

fn default_max_blocking_threads() -> usize {
    64
}

fn default_compacted_topics_cache_ttl() -> Duration {
    Duration::from_secs(3600)
}

fn default_otel_endpoint() -> String {
    "http://localhost:4317".to_string()
}

fn default_export_interval() -> Duration {
    Duration::from_secs(60)
}

fn default_leadership_provider() -> LeadershipProvider {
    LeadershipProvider::Kubernetes
}

fn default_lease_name() -> String {
    "klag-exporter".to_string()
}

fn default_lease_namespace() -> String {
    "default".to_string()
}

fn default_lease_duration() -> u32 {
    15
}

fn default_grace_period() -> u32 {
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
            cache_ttl: default_cache_ttl(),
            max_concurrent_fetches: default_max_concurrent_fetches(),
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
        }
    }
}

impl Config {
    pub fn load(path: Option<&str>) -> Result<Config> {
        let config_path = path.unwrap_or("config.toml");

        if !Path::new(config_path).exists() {
            return Err(KlagError::Config(format!(
                "Configuration file not found: {}",
                config_path
            )));
        }

        let content = std::fs::read_to_string(config_path)?;
        let content = Self::substitute_env_vars(&content);

        let config: Config = toml::from_str(&content)
            .map_err(|e| KlagError::Config(format!("TOML parse error: {}", e)))?;

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
            let default_value = caps.get(2).map(|m| m.as_str()).unwrap_or("");
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
        // The blocking-thread pool must be able to hold every concurrent FFI
        // call our hot path spawns simultaneously. `max_concurrent_groups`
        // is the dominant consumer; the timestamp sampler adds up to
        // `max_concurrent_fetches` more. If the pool is too small, tasks
        // queue behind each other and we effectively serialize — silently
        // undoing the Tier 1 win.
        let min_needed = self.exporter.performance.max_concurrent_groups
            + self.exporter.timestamp_sampling.max_concurrent_fetches
            + 4; // small headroom for watermark / compacted-topic / metadata FFI
        if self.exporter.performance.max_blocking_threads < min_needed {
            return Err(KlagError::Config(format!(
                "performance.max_blocking_threads ({}) must be >= max_concurrent_groups ({}) + \
                 timestamp_sampling.max_concurrent_fetches ({}) + 4 = {}",
                self.exporter.performance.max_blocking_threads,
                self.exporter.performance.max_concurrent_groups,
                self.exporter.timestamp_sampling.max_concurrent_fetches,
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
        // default max_concurrent_groups=10, max_concurrent_fetches=5,
        // so minimum blocking threads = 10 + 5 + 4 = 19. 16 must fail.
        let config_content = r#"
[exporter]

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
