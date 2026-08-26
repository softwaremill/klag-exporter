<p align="center">
  <img src="https://img.shields.io/badge/rust-1.78%2B-orange?logo=rust" alt="Rust Version">
  <img src="https://img.shields.io/badge/license-MIT-blue" alt="License">
  <img src="https://img.shields.io/badge/prometheus-compatible-E6522C?logo=prometheus" alt="Prometheus">
  <img src="https://img.shields.io/badge/OpenTelemetry-0.27-blueviolet?logo=opentelemetry" alt="OpenTelemetry">
</p>

# klag-exporter

A high-performance Apache Kafka® consumer group lag exporter written in Rust. Calculates both **offset lag** and **time lag** (latency in seconds) with accurate timestamp-based measurements.

<p align="center">
  <img src="docs/images/grafana-dashboard.png" alt="Grafana Dashboard" width="800">
</p>

## Features

- **Accurate Time Lag Calculation** — Directly reads message timestamps from Kafka® partitions instead of interpolating from lookup tables
- **Compaction & Retention Detection** — Automatically detects when log compaction or retention deletion may affect time lag accuracy
- **Data Loss Detection** — Detects and quantifies message loss when consumers fall behind retention, with metrics for prevention alerts
- **Dual Export Support** — Native Prometheus HTTP endpoint (`/metrics`) and OpenTelemetry OTLP export
- **Non-blocking Scrapes** — Continuous background collection with instant metric reads (no Kafka® calls during Prometheus scrapes)
- **Multi-cluster Support** — Monitor multiple Kafka® clusters with independent collection loops and failure isolation
- **Flexible Filtering** — Regex-based whitelist/blacklist for consumer groups and topics
- **Configurable Granularity** — Topic-level (reduced cardinality) or partition-level metrics
- **Custom Labels** — Add environment, datacenter, or any custom labels to all metrics
- **Full Authentication Support** — SASL/PLAIN, SASL/SCRAM, SSL/TLS, and Kerberos via librdkafka
- **Production Ready** — Health (`/health`) and readiness (`/ready`) endpoints for Kubernetes deployments
- **High Availability** — Optional Kubernetes leader election for active-passive failover (see [HA Guide](docs/high-availability.md))
- **Resource Efficient** — Written in Rust with async/await, minimal memory footprint, and bounded concurrency

## Why klag-exporter?

### Comparison with Existing Solutions

| Feature                    | klag-exporter            | kafka-lag-exporter              | KMinion                       |
| -------------------------- | ------------------------ | ------------------------------- | ----------------------------- |
| **Language**               | Rust                     | Scala (JVM)                     | Go                            |
| **Time Lag**               | Direct timestamp reading | Interpolation from lookup table | Offset-only (requires PromQL) |
| **Idle Producer Handling** | Shows actual message age | Shows 0 (interpolation fails)   | N/A                           |
| **Memory Usage**           | ~20-50 MB                | ~200-500 MB (JVM)               | ~50-100 MB                    |
| **Startup Time**           | < 1 second               | 5-15 seconds (JVM warmup)       | < 1 second                    |
| **OpenTelemetry**          | Native OTLP              | No                              | No                            |
| **Blocking Scrapes**       | No                       | No                              | Yes                           |

### Time Lag Accuracy

**Problem with interpolation (kafka-lag-exporter):**

- Builds a lookup table of (offset, timestamp) pairs over time
- Interpolates to estimate lag — breaks when producers stop sending
- Shows 0 lag incorrectly for idle topics
- Requires many poll cycles to build accurate tables

**Our approach (two modes, default rate-based):**

klag-exporter supports two time-lag strategies, selectable via `timestamp_sampling.mode`:

#### `rate` mode (default)

- No consumer pool, no extra librdkafka clients — dramatically lower resident memory on large clusters
- Keeps a short ring buffer of `(observation_time, high_watermark)` per partition
- Rate = Δhigh_watermark / Δtime; `time_lag = (high_watermark − committed_offset) / rate`
- Accurate for lag alerting (magnitude right); accuracy depends on steady producer rate across the history window
- Returns no value (metric missing) on the first cycle after startup or topic creation (needs ≥ 2 samples) and on partitions producing below the configured floor

#### `message` mode

- Seeks directly to the consumer group's committed offset via a pooled BaseConsumer
- Reads the actual message timestamp — exact
- Handles idle producers correctly (shows true message age)
- TTL-cached to prevent excessive broker load
- Memory cost: each pooled BaseConsumer is a full librdkafka client (~5–15 MB) — keep `max_concurrent_fetches` small

**When to choose which:** Rate is the right default for almost everyone. Pick message mode only if you have a regulatory / audit requirement for exact timestamps and your cluster is small enough that the consumer pool's memory overhead is acceptable.

### Compaction and Retention Limitations

Both **log compaction** (`cleanup.policy=compact`) and **retention-based deletion** can affect time lag accuracy:

| Scenario       | Effect on Offset Lag                      | Effect on Time Lag                |
| -------------- | ----------------------------------------- | --------------------------------- |
| **Compaction** | Inflated (some offsets no longer exist)   | Understated (reads newer message) |
| **Retention**  | Inflated (deleted messages still counted) | Understated (reads newer message) |

**How it happens:** When a consumer's committed offset points to a deleted message, Kafka returns the next available message instead. This message has a later timestamp, making time lag appear smaller than reality.

**Detection:** klag-exporter automatically detects these conditions and exposes:

- `compaction_detected` and `data_loss_detected` labels on `kafka_consumergroup_group_lag_seconds`
- `kafka_lag_exporter_compaction_detected_total` and `kafka_lag_exporter_data_loss_partitions_total` counters

**Recommendations:**

- For affected partitions, rely more on offset lag than time lag
- Alert on `kafka_lag_exporter_compaction_detected_total > 0` or `kafka_lag_exporter_data_loss_partitions_total > 0`
- Investigate if detection counts are high — may indicate very lagging consumers or aggressive compaction/retention settings

See [docs/compaction-detection.md](docs/compaction-detection.md) for detailed technical explanation.

### Data Loss Detection

When a consumer falls too far behind, Kafka® retention policies may delete messages before they're processed. klag-exporter detects and quantifies this:

**How it works:** Data loss occurs when a consumer group's committed offset falls below the partition's low watermark (earliest available offset). This means messages were deleted by retention before the consumer could process them.

**Understanding `lag_retention_ratio`:**

This metric shows what percentage of the available retention window is occupied by consumer lag:

```
                    current_lag
lag_retention_ratio = ─────────────────── × 100
                    retention_window

where:
  retention_window = high_watermark - low_watermark  (total offsets in partition)
  current_lag      = high_watermark - committed_offset
```

| Ratio | Meaning                                                                         |
| ----- | ------------------------------------------------------------------------------- |
| 0%    | Consumer is caught up (no lag)                                                  |
| 50%   | Consumer lag equals half the retention window                                   |
| 100%  | Consumer is at the deletion boundary — next retention cycle may cause data loss |
| >100% | Data loss has already occurred                                                  |

Example: If a partition has offsets 1000-2000 (retention window = 1000) and consumer is at offset 1200:

- current_lag = 2000 - 1200 = 800
- lag_retention_ratio = (800 / 1000) × 100 = **80%** — consumer is 80% of the way to data loss

**Metrics provided:**

| Metric                                          | Description                                 | Example Use                |
| ----------------------------------------------- | ------------------------------------------- | -------------------------- |
| `kafka_consumergroup_group_messages_lost`       | Count of messages deleted before processing | Alert when > 0             |
| `kafka_consumergroup_group_retention_margin`    | Distance to deletion boundary               | Alert when approaching 0   |
| `kafka_consumergroup_group_lag_retention_ratio` | Lag as % of retention window                | Alert when > 80%           |
| `data_loss_detected` label                      | Boolean flag on lag metrics                 | Filter affected partitions |

**Prevention strategy:**

- Set alerts on `retention_margin` approaching zero (e.g., < 10% of typical lag)
- Monitor `lag_retention_ratio` — values approaching 100% indicate imminent data loss
- Use `messages_lost > 0` for post-incident detection
- Consider increasing topic retention or scaling consumers when ratios are high

**Example Prometheus alerts:**

```yaml
# Imminent data loss warning
- alert: KafkaConsumerNearDataLoss
  expr: kafka_consumergroup_group_lag_retention_ratio > 80
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "Consumer {{ $labels.group }} approaching retention boundary"

# Data loss occurred
- alert: KafkaConsumerDataLoss
  expr: kafka_consumergroup_group_messages_lost > 0
  labels:
    severity: critical
  annotations:
    summary: "Consumer {{ $labels.group }} lost {{ $value }} messages"
```

## Quick Start

### Using Helm (Kubernetes)

```bash
# Install from OCI registry
helm install klag-exporter oci://ghcr.io/softwaremill/helm/klag-exporter \
  --set config.clusters[0].bootstrap_servers="kafka:9092" \
  --set config.clusters[0].name="my-cluster" \
  -n kafka --create-namespace

# Or with custom values file
helm install klag-exporter oci://ghcr.io/softwaremill/helm/klag-exporter \
  -f values.yaml \
  -n kafka --create-namespace
```

See [`helm/klag-exporter/Readme.md`](helm/klag-exporter/Readme.md) for detailed Helm chart documentation.

### Using Docker

```bash
docker run -d \
  -p 8000:8000 \
  -v $(pwd)/config.toml:/etc/klag-exporter/config.toml \
  klag-exporter:latest \
  --config /etc/klag-exporter/config.toml
```

### Using Binary

```bash
# Build from source
cargo build --release

# Run with config file
./target/release/klag-exporter --config config.toml

# Run with debug logging
./target/release/klag-exporter -c config.toml -l debug
```

### Using Docker Compose (Demo Stack)

A complete demo environment with Kafka®, Prometheus, and Grafana is available in the `test-stack/` directory:

```bash
cd test-stack
docker-compose up -d --build

# Access points:
# - Grafana:       http://localhost:3000 (admin/admin)
# - Prometheus:    http://localhost:9090
# - klag-exporter: http://localhost:8000/metrics
# - Kafka UI:      http://localhost:8080
```

## Configuration

Create a `config.toml` file:

```toml
[exporter]
poll_interval = "30s"
http_port = 8000
http_host = "0.0.0.0"
granularity = "partition"  # "topic" or "partition"
# Optional. When unset, defaults to poll_interval * 3. Past this age a
# cluster's metrics are filtered out of /metrics so Prometheus sees a
# gap instead of a frozen snapshot. Raise it on large clusters where a
# single collection cycle can take several minutes.
# staleness_threshold = "3m"

[exporter.timestamp_sampling]
enabled = true
# "rate" (default, recommended for large clusters) or "message".
# See "Time Lag Modes" below for tradeoffs.
mode = "rate"

# Rate-mode tuning (only used when mode = "rate"):
rate_history_samples = 5           # ring buffer size per partition
rate_history_max_age = "10m"       # drop samples older than this
rate_min_msgs_per_sec = 0.01       # below this rate → time lag reported as missing

# Message-mode tuning (only used when mode = "message"):
cache_ttl = "60s"
max_concurrent_fetches = 10
fetch_timeout = "5s"
# Skip the per-message fetch for every partition whose committed offset has
# fallen below the low watermark, i.e. retention deleted the committed message.
# Such a fetch resets to the earliest surviving offset, and on a drained
# partition it blocks for the full fetch timeout waiting for a message that
# never arrives — enough of those stall the collection cycle on large clusters.
# The partition is still reported either way, with its offset lag and
# data_loss_detected; the trade-off is that time lag reports 0 rather than the
# age of the earliest surviving message. Default false.
skip_data_loss_partitions = false

[exporter.otel]
enabled = false
endpoint = "http://localhost:4317"
export_interval = "60s"

# Performance tuning for large clusters (optional)
# [exporter.performance]
# kafka_timeout = "30s"
# offset_fetch_timeout = "10s"
# group_fetch_retries = 0
# max_concurrent_groups = 10
# max_concurrent_watermarks = 50

[[clusters]]
name = "production"
bootstrap_servers = "kafka1:9092,kafka2:9092"
group_whitelist = [".*"]
group_blacklist = []
topic_whitelist = [".*"]
topic_blacklist = ["__.*"]  # Exclude internal topics

[clusters.consumer_properties]
"security.protocol" = "SASL_SSL"
"sasl.mechanism" = "PLAIN"
"sasl.username" = "${KAFKA_USER}"
"sasl.password" = "${KAFKA_PASSWORD}"

[clusters.labels]
environment = "production"
datacenter = "us-east-1"
```

### Environment Variable Substitution

Use `${VAR_NAME}` syntax in config values. The exporter will substitute with environment variable values at startup.

## Metrics

### Partition Offset Metrics

| Metric                            | Labels                         | Description           |
| --------------------------------- | ------------------------------ | --------------------- |
| `kafka_partition_latest_offset`   | cluster_name, topic, partition | High watermark offset |
| `kafka_partition_earliest_offset` | cluster_name, topic, partition | Low watermark offset  |

### Consumer Group Metrics (Partition Level)

| Metric                                  | Labels                                                                                                              | Description         |
| --------------------------------------- | ------------------------------------------------------------------------------------------------------------------- | ------------------- |
| `kafka_consumergroup_group_offset`      | cluster_name, group, topic, partition, member_host, consumer_id, client_id                                          | Committed offset    |
| `kafka_consumergroup_group_lag`         | cluster_name, group, topic, partition, member_host, consumer_id, client_id                                          | Offset lag          |
| `kafka_consumergroup_group_lag_seconds` | cluster_name, group, topic, partition, member_host, consumer_id, client_id, compaction_detected, data_loss_detected | Time lag in seconds |

### Data Loss Detection Metrics

| Metric                                          | Labels                                                                     | Description                                                       |
| ----------------------------------------------- | -------------------------------------------------------------------------- | ----------------------------------------------------------------- |
| `kafka_consumergroup_group_messages_lost`       | cluster_name, group, topic, partition, member_host, consumer_id, client_id | Messages deleted by retention before consumer processed them      |
| `kafka_consumergroup_group_retention_margin`    | cluster_name, group, topic, partition, member_host, consumer_id, client_id | Offset distance to deletion boundary (negative = data loss)       |
| `kafka_consumergroup_group_lag_retention_ratio` | cluster_name, group, topic, partition, member_host, consumer_id, client_id | Percentage of retention window occupied by lag (>100 = data loss) |

### Consumer Group Aggregate Metrics

| Metric                                      | Labels                     | Description                      |
| ------------------------------------------- | -------------------------- | -------------------------------- |
| `kafka_consumergroup_group_max_lag`         | cluster_name, group        | Max offset lag across partitions |
| `kafka_consumergroup_group_max_lag_seconds` | cluster_name, group        | Max time lag across partitions   |
| `kafka_consumergroup_group_sum_lag`         | cluster_name, group        | Sum of offset lag                |
| `kafka_consumergroup_group_topic_sum_lag`   | cluster_name, group, topic | Sum of offset lag per topic      |
| `kafka_consumergroup_group_state`          | cluster_name, group        | Consumer group state as integer (0=Unknown, 1=PreparingRebalance, 2=CompletingRebalance, 3=Stable, 4=Dead, 5=Empty, 6=Assigning, 7=Reconciling) |

### Operational Metrics

| Metric                                          | Labels       | Description                                                            |
| ----------------------------------------------- | ------------ | ---------------------------------------------------------------------- |
| `kafka_consumergroup_poll_time_ms`              | cluster_name | Time to poll all offsets                                               |
| `kafka_lag_exporter_scrape_duration_seconds`    | cluster_name | Collection cycle duration                                              |
| `kafka_lag_exporter_up`                         | —            | 1 if healthy, 0 otherwise                                              |
| `kafka_lag_exporter_compaction_detected_total`  | cluster_name | Partitions where log compaction was detected                           |
| `kafka_lag_exporter_data_loss_partitions_total` | cluster_name | Partitions where data loss occurred (committed offset < low watermark) |

## HTTP Endpoints

| Endpoint       | Description                                                             |
| -------------- | ----------------------------------------------------------------------- |
| `GET /metrics` | Prometheus metrics                                                      |
| `GET /health`  | Liveness probe (always 200 if running)                                  |
| `GET /ready`   | Readiness probe (200 when metrics available, 503 if standby in HA mode) |
| `GET /leader`  | Leadership status JSON (`{"is_leader": true/false}`)                    |
| `GET /`        | Basic info page                                                         |

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Main Application                                │
│  ┌──────────────┐  ┌──────────────────┐  ┌──────────────────────────────┐  │
│  │   Config     │  │   HTTP Server    │  │      Metrics Registry        │  │
│  │   Loader     │  │  (Prometheus +   │  │   (In-memory Gauge Store)    │  │
│  │              │  │   Health)        │  │                              │  │
│  └──────────────┘  └──────────────────┘  └──────────────────────────────┘  │
│                                                         ▲                   │
│  ┌──────────────────────────────────────────────────────┴────────────────┐  │
│  │                      Cluster Manager (per cluster)                    │  │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────┐   │  │
│  │  │  Offset         │  │  Timestamp      │  │  Metrics            │   │  │
│  │  │  Collector      │  │  Sampler        │  │  Calculator         │   │  │
│  │  │  (Admin API)    │  │  (Consumer)     │  │                     │   │  │
│  │  └─────────────────┘  └─────────────────┘  └─────────────────────┘   │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                      Export Layer                                     │  │
│  │  ┌─────────────────────────┐  ┌─────────────────────────────────┐    │  │
│  │  │  Prometheus Exporter    │  │  OpenTelemetry Exporter         │    │  │
│  │  │  (HTTP /metrics)        │  │  (OTLP gRPC)                    │    │  │
│  │  └─────────────────────────┘  └─────────────────────────────────┘    │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Building from Source

### Prerequisites

- Rust 1.78 or later
- CMake (for librdkafka)
- OpenSSL development libraries
- SASL development libraries (optional, for Kerberos)

### Build

```bash
# Debug build
cargo build

# Release build (optimized)
cargo build --release

# Release build with High Availability support
cargo build --release --features kubernetes

# Run tests
cargo test

# Run linter
cargo clippy
```

### Docker Build

```bash
docker build -t klag-exporter:latest .
```

## Kubernetes Deployment

For high availability with automatic failover, see the [HA Guide](docs/high-availability.md). Below is a basic single-instance deployment:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: klag-exporter
spec:
  replicas: 1
  selector:
    matchLabels:
      app: klag-exporter
  template:
    metadata:
      labels:
        app: klag-exporter
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "8000"
        prometheus.io/path: "/metrics"
    spec:
      containers:
        - name: klag-exporter
          image: klag-exporter:latest
          ports:
            - containerPort: 8000
          livenessProbe:
            httpGet:
              path: /health
              port: 8000
            initialDelaySeconds: 5
            periodSeconds: 10
          readinessProbe:
            httpGet:
              path: /ready
              port: 8000
            initialDelaySeconds: 5
            periodSeconds: 10
          volumeMounts:
            - name: config
              mountPath: /etc/klag-exporter
          env:
            - name: KAFKA_USER
              valueFrom:
                secretKeyRef:
                  name: kafka-credentials
                  key: username
            - name: KAFKA_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: kafka-credentials
                  key: password
      volumes:
        - name: config
          configMap:
            name: klag-exporter-config
---
apiVersion: v1
kind: Service
metadata:
  name: klag-exporter
  labels:
    app: klag-exporter
spec:
  ports:
    - port: 8000
      targetPort: 8000
      name: metrics
  selector:
    app: klag-exporter
```

## Grafana Dashboard

A pre-built Grafana dashboard is included in `test-stack/grafana/provisioning/dashboards/kafka-lag.json` with:

- Total consumer lag overview
- Max time lag per consumer group
- Per-partition lag breakdown
- Offset progression over time
- Message rate calculations
- Exporter health status

## Security Considerations

### Timestamp Sampling and Message Content

To calculate accurate time lag, klag-exporter fetches messages from Kafka® at the consumer group's committed offset to read the message timestamp. **Only the timestamp metadata is extracted** — the message payload (key and value) is never read, logged, or stored.

| Risk                  | Level                    | Notes                                                       |
| --------------------- | ------------------------ | ----------------------------------------------------------- |
| Data exposure in logs | **None**                 | Only topic/partition/offset/timestamp logged, never payload |
| Data in memory        | **Low**                  | Payload briefly in process memory (~ms), then dropped       |
| Data exfiltration     | **None**                 | Payload never sent, stored, or exposed via API              |
| Network exposure      | **Same as any consumer** | Use TLS (`security.protocol=SSL`) for encryption            |

**For sensitive environments:**

- Disable timestamp sampling with `timestamp_sampling.enabled = false` — you'll still get offset lag metrics
- Fetch size is limited to 256KB per partition to minimize data transfer
- Run klag-exporter with minimal privileges and restricted network access

## Large Cluster Configuration

klag-exporter uses librdkafka's **batched Admin API** for its collection hot path. Per collection cycle it makes approximately:

- **2** batched `ListOffsets` calls (one EARLIEST, one LATEST) regardless of partition count — librdkafka routes per leader broker internally, so on a 3-broker cluster this is ~6 broker RPCs even for 20,000 partitions
- **`ceil(groups / 100)`** batched `DescribeConsumerGroups` calls (currently a single chunked call up to 100 groups at a time)
- **one `ListConsumerGroupOffsets` call per group**, fanned out with `max_concurrent_groups` concurrency. Each call passes `NULL` partitions, so the broker returns only the committed partitions for that group — no client-side 19K-partition list is sent. *(librdkafka 2.12 enforces one group per call here; once that restriction is lifted, call count drops further.)*
- **one `DescribeConfigs`** restricted to the monitored topic set (topics surviving the whitelist/blacklist), for compacted-topic detection

Topic whitelist/blacklist is applied **before** any partition-touching call, so `__consumer_offsets` (50 partitions by default) and blacklisted topics are excluded from the hot path entirely.

### Symptoms of Scale Issues

- `Collection timed out` errors in logs
- Collection cycles consistently exceeding the poll interval
- RSS climbing cycle over cycle

### Performance Tuning Options

Add the `[exporter.performance]` section to tune parallelism and timeouts:

```toml
[exporter]
poll_interval = "60s"  # Increase for very large clusters if needed

[exporter.performance]
# Timeout for individual Kafka API operations (metadata, watermarks)
kafka_timeout = "30s"            # Default: 30s

# Timeout for each per-group committed-offsets fetch
offset_fetch_timeout = "10s"     # Default: 10s

# Additional attempts for retriable failures while fetching consumer-group
# descriptions and committed offsets (DescribeConsumerGroups and
# ListConsumerGroupOffsets). Set to 0 to disable retries.
group_fetch_retries = 2           # Default: 0

# Parallel in-flight ListConsumerGroupOffsets calls (one group per call).
# Increase on large clusters so a backlog of groups drains quickly.
max_concurrent_groups = 30       # Default: 10

# Legacy — no longer affects the hot path after the Tier 1 batched Admin
# API refactor (watermarks now come from one batched ListOffsets per
# broker), but still honored for compatibility.
max_concurrent_watermarks = 50   # Default: 50

# Client recycling interval — see Memory Management section below
client_recycle_interval = 50     # Default: 50 (set 0 to disable)
```

### Recommended Settings by Cluster Size

| Cluster Size | Groups | Partitions | poll_interval | max_concurrent_groups |
|--------------|--------|------------|---------------|------------------------|
| Small        | < 50   | < 500      | 30s           | 10 (default)           |
| Medium       | 50-200 | 500-2000   | 60s           | 20                     |
| Large        | 200–1000 | 2000–20000 | 60–120s | 30                     |
| Very large   | > 1000 | > 20000    | 120–180s      | 50                     |

When you raise `poll_interval`, the staleness threshold that controls how
long metrics linger in `/metrics` after a successful collection scales
with it automatically — the default is `poll_interval * 3`, so at
`poll_interval = 120s` metrics survive for 6 minutes before being
filtered out as stale. Set `exporter.staleness_threshold` explicitly if
you want to decouple it from the poll cadence (e.g. pin it to a fixed
duration for alerting consistency).

### Additional Recommendations for Large Clusters

1. **Use filters aggressively** — Narrow down to only the groups/topics you need:
   ```toml
   group_whitelist = ["^prod-.*"]
   group_blacklist = ["^test-.*", "^dev-.*"]
   topic_blacklist = ["__.*", ".*-dlq$"]
   ```

2. **Disable timestamp sampling if not needed** — Reduces broker load significantly:
   ```toml
   [exporter.timestamp_sampling]
   enabled = false
   ```

3. **Use topic-level granularity** — Reduces metric cardinality:
   ```toml
   [exporter]
   granularity = "topic"  # Instead of "partition"
   ```

4. **Consider running multiple instances** — Split monitoring across clusters or consumer group subsets using different whitelist patterns.

### Memory Management

#### The librdkafka metadata cache problem

librdkafka (the C library underlying the Rust Kafka client) maintains an internal hash table of topic handles. Every time the exporter touches a topic — via watermark fetches, offset lookups, or config queries — librdkafka creates or reuses a handle for that topic. These handles are **never freed** until the client is destroyed. There is no API to evict individual entries.

On large clusters with thousands of topics, the internal cache grows with every collection cycle. If topics are created and deleted over time (topic churn), the handle count only increases — deleted topics remain as stale entries.

#### Client recycling

To prevent unbounded memory growth, klag-exporter periodically destroys and recreates its internal Kafka clients, releasing all accumulated metadata. This is controlled by the `client_recycle_interval` setting:

```toml
[exporter.performance]
# Number of collection cycles between client recycling.
# Set to 0 to disable (recommended for small/stable clusters).
client_recycle_interval = 50   # Default: every 50 cycles (~25 min at 30s poll)
```

| Setting | When to use |
|---------|-------------|
| `0` (disabled) | Small clusters with few topics, or stable clusters with no topic churn |
| `50` (default) | Large clusters with many topics or moderate topic churn |
| `100+` | Large clusters where you want less frequent recycling overhead |

Recycling is safe — it only runs between collection cycles after all in-flight operations have completed. The trade-off is a brief memory spike (~2-10 MB) while new clients are created before old ones are fully torn down.

#### jemalloc

klag-exporter uses [jemalloc](https://jemalloc.net/) as the default memory allocator (enabled via the `jemalloc` feature flag). jemalloc provides significantly better memory return behavior than glibc malloc, which tends to hold onto freed pages indefinitely in long-running processes.

To disable jemalloc:

```bash
cargo build --release --no-default-features
```

#### Timestamp consumer pool sizing

Each entry in the timestamp consumer pool (`max_concurrent_fetches`) is a full librdkafka client with its own background threads and connection state, consuming ~5-15 MB of memory. Size the pool to match your actual concurrency needs, not your topic or partition count:

```toml
[exporter.timestamp_sampling]
max_concurrent_fetches = 5   # Default: 5. Each is a full Kafka client.
```

Each fetch polls for a message at the committed offset for up to `fetch_timeout` (default 5s) before giving up. On idle or largely idle clusters many committed offsets sit at the partition's high watermark with no message to read, so every one of those fetches blocks for the full timeout; on large clusters that serializes behind `max_concurrent_fetches` and can push a collection cycle past the poll interval. Lower it to bound the worst-case per-fetch wait:

```toml
[exporter.timestamp_sampling]
fetch_timeout = "5s"   # Default: 5s. Per-message poll timeout.
```

## Troubleshooting

### Time Lag Shows Gaps in Grafana

This is expected when:

- Consumer catches up completely (lag = 0)
- Timestamp cache expires and refetch is in progress
- Kafka® fetch times out

**Solutions:**

- Increase `cache_ttl` in config
- Use Grafana's "Connect null values" option
- For alerting, use `avg_over_time()` or `last_over_time()`

### High Memory Usage

- Reduce `max_concurrent_fetches` — each concurrent fetch is a full librdkafka client (~5-15 MB)
- Use `granularity = "topic"` instead of `"partition"`
- Add more restrictive `group_blacklist` / `topic_blacklist` patterns
- On large clusters with topic churn, ensure `client_recycle_interval` is enabled (see below)
- jemalloc is the default allocator and provides much better memory behavior than glibc malloc; disable with `--no-default-features` only if needed

### Connection Errors

- Verify `bootstrap_servers` are reachable
- Check authentication configuration in `consumer_properties`
- Ensure network policies allow connections to Kafka® brokers

## CI/CD Pipelines

This repository uses GitHub Actions for continuous integration and delivery.

- CI (ci.yml)
  - Triggers: push and pull_request to main and master
  - Jobs:
    - Format: cargo fmt --all --check
    - Clippy: cargo clippy --all-targets --all-features -- -D warnings
    - Test: cargo test --all-features
    - Build: cargo build --release
    - Lint Helm Chart: helm lint ./helm/klag-exporter and a template render check
  - Notes: Installs system packages (cmake, libssl-dev, libsasl2-dev, pkg-config), uses dtolnay/rust-toolchain and Swatinem/rust-cache.

- Release preparation (release-plz-pr.yml)
  - Triggers: push to main and manual dispatch; runs only for the softwaremill org and skips pushes where the head commit starts with "chore"
  - Actions:
    - Runs release-plz to open/update a “Release PR”
    - Updates helm/klag-exporter/values.yaml image tag and helm/klag-exporter/Chart.yaml version/appVersion on the release branch derived from release-plz output
  - Requires: RELEASE_PLZ_TOKEN with write permissions

- Release (release.yml)
  - Triggers: manual dispatch, or when a PR to main is closed and merged and has the label "release"
  - Actions: Runs release-plz release to create tags and publish artifacts (e.g., crates.io)
  - Requires: RELEASE_PLZ_TOKEN and CARGO_REGISTRY_TOKEN

- Post Release (post-release.yml)
  - Trigger: when a GitHub Release is created
  - Jobs:
    - Build binaries for linux x86_64 and aarch64 and upload them as artifacts
    - Upload binaries to the GitHub Release
    - Build and push multi-arch Docker images to ghcr.io using Dockerfile.release and the prebuilt binaries
      - Tags: full semver, major.minor, major, and latest (on default branch)
    - Package Helm chart and push to ghcr.io as an OCI artifact under OWNER/helm
  - Requires:
    - APP_ID and PRIVATE_KEY (GitHub App) for pushing with elevated permissions
    - GITHUB_TOKEN (provided by GitHub) for publishing images, charts, and release assets

Secrets and variables summary:

- RELEASE_PLZ_TOKEN: GitHub token with repo write permissions for release-plz
- CARGO_REGISTRY_TOKEN: crates.io publishing token
- APP_ID and PRIVATE_KEY: GitHub App credentials used during post-release Docker publishing
- GITHUB_TOKEN: auto-provided by GitHub Actions for the workflow run

## License

MIT License — see [LICENSE](LICENSE) for details.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.

---

## Trademark Notice

Apache®, Apache Kafka®, and Kafka® are either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries. This project is not affiliated with, endorsed by, or sponsored by the Apache Software Foundation. For more information about Apache trademarks, please see the [Apache Trademark Policy](https://www.apache.org/foundation/marks/).
