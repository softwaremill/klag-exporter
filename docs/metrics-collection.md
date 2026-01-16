# Metrics Collection in Klag Exporter

This document describes how metrics are collected, what data sources are used, and how configuration affects the collection process.

## Overview

Klag Exporter collects metrics from Kafka clusters using the Kafka protocol directly (via librdkafka). It does not require access to Kafka internals or JMX - only standard consumer group APIs.

## Data Sources

### 1. Consumer Group Offsets

**Source:** Kafka Consumer API (`committed_offsets`)

**What's collected:**
- Consumer group IDs
- Committed offsets per partition

**Used for metrics:**
- `kafka_consumergroup_group_offset` - committed offset per partition
- `kafka_consumergroup_group_lag` - offset lag (high_watermark - committed_offset)

### 2. Partition Watermarks

**Source:** Kafka Consumer API (`fetch_watermarks`)

**What's collected:**
- Low watermark (earliest available offset) per partition
- High watermark (latest offset) per partition

**Used for metrics:**
- `kafka_partition_earliest_offset` - low watermark
- `kafka_partition_latest_offset` - high watermark
- Lag calculation: `lag = high_watermark - committed_offset`
- Retention detection: `committed_offset < low_watermark`

### 3. Message Timestamps

**Source:** Kafka Consumer API (fetch message at committed offset)

**What's collected:**
- Timestamp of message at committed offset
- Whether compaction was detected (returned offset differs from requested)

**Used for metrics:**
- `kafka_consumergroup_group_lag_seconds` - time lag per partition
- `kafka_consumergroup_group_max_lag_seconds` - max time lag per group
- `compaction_detected` and `data_loss_detected` labels
- Data loss metrics: `messages_lost`, `retention_margin`, `lag_retention_ratio`

**How it works:**
1. For each partition with lag > 0, we seek to the committed offset
2. Fetch the message and read its timestamp
3. Calculate time lag: `(now - message_timestamp) / 1000`
4. If the returned offset differs from requested, compaction is detected

## Collection Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                     Collection Cycle                             │
│                  (runs every poll_interval)                      │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ 1. Collect Consumer Groups                                       │
│    - List all consumer groups                                    │
│    - Filter by whitelist/blacklist regex                         │
│    - Get committed offsets for each group                        │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ 2. Collect Partition Watermarks                                  │
│    - For each topic/partition in consumer group offsets          │
│    - Get low and high watermarks                                 │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ 3. Collect Timestamps (if enabled)                               │
│    - For each partition with lag > 0                             │
│    - Fetch message at committed offset                           │
│    - Extract timestamp, detect compaction                        │
│    - Cache results (TTL-based)                                   │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ 4. Calculate Lag Metrics                                         │
│    - Offset lag per partition                                    │
│    - Time lag per partition (if timestamp available)             │
│    - Aggregates: sum_lag, max_lag, max_lag_seconds per group     │
│    - Detect retention deletion (committed < low_watermark)       │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│ 5. Update Metrics Registry                                       │
│    - Convert to Prometheus format                                │
│    - Apply granularity settings                                  │
│    - Add custom labels                                           │
└─────────────────────────────────────────────────────────────────┘
```

## Configuration

Configuration is in TOML format. See the main README for a complete example.

### Exporter Settings

```toml
[exporter]
poll_interval = "30s"        # How often to collect metrics (default: 30s)
http_port = 8000             # Prometheus metrics port (default: 8000)
http_host = "0.0.0.0"        # Listen address (default: 0.0.0.0)
granularity = "partition"    # "topic" or "partition" (default: topic)

[exporter.timestamp_sampling]
enabled = true               # Enable time lag calculation (default: true)
cache_ttl = "60s"            # How long to cache timestamps (default: 60s)
max_concurrent_fetches = 10  # Parallel timestamp fetches (default: 10)

[exporter.otel]
enabled = false              # Enable OpenTelemetry export (default: false)
endpoint = "http://localhost:4317"
export_interval = "60s"
```

### Cluster Settings

```toml
[[clusters]]
name = "production"
bootstrap_servers = "kafka1:9092,kafka2:9092"

# Filtering (regex patterns, evaluated as arrays)
group_whitelist = [".*"]           # Only groups matching any pattern (default: [".*"])
group_blacklist = []               # Exclude groups matching any pattern
topic_whitelist = [".*"]           # Only topics matching any pattern (default: [".*"])
topic_blacklist = ["__.*"]         # Exclude internal topics (default: ["__.*"])

# Kafka client properties
[clusters.consumer_properties]
"security.protocol" = "SASL_SSL"
"sasl.mechanism" = "PLAIN"
"sasl.username" = "${KAFKA_USER}"
"sasl.password" = "${KAFKA_PASSWORD}"

# Custom labels added to all metrics from this cluster
[clusters.labels]
environment = "production"
datacenter = "us-east-1"
```

### Settings Impact

| Setting | Impact |
|---------|--------|
| `poll_interval` | How fresh the metrics are. Lower = more load on Kafka |
| `group_whitelist` | Only groups matching these regex patterns are monitored |
| `group_blacklist` | Groups matching these regex patterns are excluded |
| `topic_whitelist` | Only topics matching these regex patterns are included |
| `topic_blacklist` | Topics matching these regex patterns are excluded |
| `timestamp_sampling.enabled` | If false, no `lag_seconds` metrics are collected |
| `timestamp_sampling.cache_ttl` | How long to cache timestamps (reduces Kafka load) |
| `timestamp_sampling.max_concurrent_fetches` | Parallel timestamp fetches (higher = faster but more load) |
| `granularity` | Controls which metrics are emitted (see below) |

### Granularity Levels

**`partition`** - All metrics including per-partition:
- Per-partition: `group_lag`, `group_lag_seconds`, `group_offset`
- Per-topic: `group_topic_sum_lag`
- Per-group: `group_sum_lag`, `group_max_lag`, `group_max_lag_seconds`
- Per-cluster: partition offsets, detection totals

**`topic`** (default) - Skip partition-level consumer group metrics:
- Per-topic: `group_topic_sum_lag`
- Per-group: `group_sum_lag`, `group_max_lag`, `group_max_lag_seconds`
- Per-cluster: partition offsets, detection totals

## Metrics Reference

### Partition-Level Metrics

| Metric | Description | Labels |
|--------|-------------|--------|
| `kafka_consumergroup_group_lag` | Offset lag for partition | cluster_name, group, topic, partition, member_host, consumer_id, client_id |
| `kafka_consumergroup_group_lag_seconds` | Time lag in seconds | same + compaction_detected, data_loss_detected |
| `kafka_consumergroup_group_messages_lost` | Messages deleted before processing | same as group_lag |
| `kafka_consumergroup_group_retention_margin` | Offset distance to deletion boundary | same as group_lag |
| `kafka_consumergroup_group_lag_retention_ratio` | Lag as % of retention window | same as group_lag |
| `kafka_consumergroup_group_offset` | Committed offset | same as group_lag |

### Group-Level Metrics

| Metric | Description | Labels |
|--------|-------------|--------|
| `kafka_consumergroup_group_sum_lag` | Sum of lag across all partitions | cluster_name, group |
| `kafka_consumergroup_group_max_lag` | Max offset lag across partitions | cluster_name, group |
| `kafka_consumergroup_group_max_lag_seconds` | Max time lag across partitions | cluster_name, group |

### Topic-Level Metrics

| Metric | Description | Labels |
|--------|-------------|--------|
| `kafka_consumergroup_group_topic_sum_lag` | Sum of lag for topic | cluster_name, group, topic |

### Cluster-Level Metrics

| Metric | Description | Labels |
|--------|-------------|--------|
| `kafka_partition_earliest_offset` | Low watermark | cluster_name, topic, partition |
| `kafka_partition_latest_offset` | High watermark | cluster_name, topic, partition |
| `kafka_lag_exporter_compaction_detected_total` | Partitions with compaction | cluster_name |
| `kafka_lag_exporter_data_loss_partitions_total` | Partitions with data loss | cluster_name |

### Exporter Health Metrics

| Metric | Description | Labels |
|--------|-------------|--------|
| `kafka_lag_exporter_up` | 1 if exporter is healthy | — |
| `kafka_lag_exporter_last_update_timestamp_seconds` | Unix timestamp of last successful poll | cluster_name |
| `kafka_lag_exporter_scrape_duration_seconds` | Duration of last poll cycle | cluster_name |
| `kafka_consumergroup_poll_time_ms` | Time to poll all offsets | cluster_name |

## Timestamp Collection Details

### Why Timestamps?

Offset lag alone doesn't tell you how far behind a consumer is in real time. A lag of 1000 messages could be:
- 10 seconds behind (high throughput topic)
- 10 hours behind (low throughput topic)

Time lag (`lag_seconds`) gives the actual delay.

### How Timestamps Work

1. **Seek to committed offset**: When a partition has lag, we position a consumer at the committed offset
2. **Fetch one message**: Read the message to get its timestamp
3. **Calculate time lag**: `time_lag = now - message_timestamp`
4. **Cache the result**: Avoid fetching same offset repeatedly

### Timestamp Caching

Timestamps are cached to reduce Kafka load:

```
Cache Key: (cluster, group_id, topic, partition)
Cache Valid When:
  - TTL not expired (configurable, default 60s)
  - Offset hasn't changed (consumer hasn't moved)
```

If a consumer commits a new offset, the cache is invalidated and a fresh timestamp is fetched.

### Compaction Detection

When fetching a timestamp, if the returned message offset differs from the requested offset, it means the original message was compacted away. This is flagged in the `compaction_detected` label.

**Impact:** Time lag may be understated because we're measuring from a newer message, not the original one the consumer needs to process.

### Data Loss Detection

If `committed_offset < low_watermark`, the committed offset points to a message that has been deleted by retention policy. This is flagged in the `data_loss_detected` label.

**Impact:** Similar to compaction - time lag may be understated. Additionally, the following metrics quantify the data loss:
- `messages_lost`: Number of messages deleted before consumer could process them
- `retention_margin`: Distance to deletion boundary (negative when data loss occurred)
- `lag_retention_ratio`: Consumer lag as percentage of retention window (>100% indicates data loss)

See [compaction-detection.md](compaction-detection.md) for more details.

## Performance Considerations

### Kafka Load

Each collection cycle makes these Kafka API calls:
- 1 × ListGroups
- N × committed_offsets (N = number of groups)
- M × fetch_watermarks (M = unique partitions across all groups)
- P × FetchMessage (P = partitions with lag, if timestamp enabled)

### Reducing Load

1. **Increase `poll_interval`**: Poll less frequently
2. **Use `group_whitelist`**: Only monitor groups you care about
3. **Increase `timestamp_sampling.cache_ttl`**: Cache timestamps longer
4. **Reduce `timestamp_sampling.max_concurrent_fetches`**: Fewer parallel requests
5. **Use `granularity = "topic"`**: Skip partition-level metrics

### Timestamp Fetch Optimization

Timestamps are only fetched for partitions with `lag > 0`. Caught-up partitions don't need timestamp fetches.

The `max_concurrent_fetches` setting limits parallelism. With 10 partitions needing timestamps and `max_concurrent_fetches: 5`, fetches happen in 2 batches.

## Troubleshooting

### No metrics for a consumer group

1. Check if group is filtered by whitelist/blacklist
2. Verify group exists: `kafka-consumer-groups --list`
3. Check exporter logs for errors

### Time lag shows 0 but offset lag > 0

This can happen when:
- `timestamp_sampling.enabled = false`
- Timestamp fetch failed (check logs for errors)
- All partitions with lag failed timestamp fetch

### Compaction/retention warnings

These indicate the committed offset points to a deleted message. Time lag calculations may be inaccurate. Consider:
- Speeding up consumers to avoid falling behind
- Adjusting retention/compaction settings
- Accepting that time lag is a lower bound, not exact
