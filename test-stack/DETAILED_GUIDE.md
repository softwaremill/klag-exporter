# Kafka Lag Exporter Test Stack - Detailed Guide

This document provides an in-depth explanation of how the test stack works, what it demonstrates, and what capabilities of klag-exporter are not showcased.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Component Deep Dive](#component-deep-dive)
3. [Data Flow](#data-flow)
4. [Lag Creation Mechanism](#lag-creation-mechanism)
5. [Metrics Explained](#metrics-explained)
6. [Grafana Dashboard Breakdown](#grafana-dashboard-breakdown)
7. [Observable Behaviors](#observable-behaviors)
8. [What's NOT Demonstrated](#whats-not-demonstrated)
9. [Extending the Test Stack](#extending-the-test-stack)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           Docker Network: klag-network                   │
│                                                                          │
│  ┌──────────────┐                    ┌──────────────┐                   │
│  │   Producer   │───produces to────▶│    Kafka     │◀───consumes from───│
│  │   (bash)     │                    │  (KRaft)     │                   │
│  │              │                    │  Port: 9092  │     ┌──────────┐  │
│  │ Varies rate: │                    │              │     │ Consumer │  │
│  │ 10-100 msg/s │                    │  Topics:     │     │  (bash)  │  │
│  └──────────────┘                    │ - test-topic │     │          │  │
│                                      │   (6 parts)  │     │ Slower   │  │
│                                      │ - high-vol   │     │ than     │  │
│                                      │   (3 parts)  │     │ producer │  │
│                                      └──────────────┘     └──────────┘  │
│                                             │                            │
│                                             │ Admin API calls:           │
│                                             │ - list consumer groups     │
│                                             │ - describe groups          │
│                                             │ - fetch offsets            │
│                                             │ - fetch watermarks         │
│                                             ▼                            │
│                                      ┌──────────────┐                   │
│                                      │klag-exporter │                   │
│                                      │  Port: 8000  │                   │
│                                      │              │                   │
│                                      │ Endpoints:   │                   │
│                                      │ /metrics     │                   │
│                                      │ /health      │                   │
│                                      │ /ready       │                   │
│                                      └──────────────┘                   │
│                                             │                            │
│                                             │ Prometheus scrape          │
│                                             │ every 10 seconds           │
│                                             ▼                            │
│                                      ┌──────────────┐                   │
│                                      │  Prometheus  │                   │
│                                      │  Port: 9090  │                   │
│                                      │              │                   │
│                                      │ Stores 15d   │                   │
│                                      │ of metrics   │                   │
│                                      └──────────────┘                   │
│                                             │                            │
│                                             │ PromQL queries             │
│                                             ▼                            │
│                                      ┌──────────────┐                   │
│                                      │   Grafana    │                   │
│                                      │  Port: 3000  │                   │
│                                      │              │                   │
│                                      │ Dashboard    │                   │
│                                      │ with lag     │                   │
│                                      │ metrics      │                   │
│                                      └──────────────┘                   │
│                                                                          │
│  ┌──────────────┐                                                       │
│  │  Kafka UI    │  (Optional debugging - shows topics, messages, etc)   │
│  │  Port: 8080  │                                                       │
│  └──────────────┘                                                       │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Component Deep Dive

### 1. Kafka (KRaft Mode)

**Image:** `confluentinc/cp-kafka:7.5.0`

The Kafka broker runs in KRaft mode (Kafka Raft), meaning it doesn't require Zookeeper. This is the modern deployment model for Kafka 3.x+.

**Configuration highlights:**
- Single broker (node ID: 1)
- Acts as both broker and controller
- Auto-creates topics when producer writes to them
- Replication factor: 1 (single node)

**Topics created:**
| Topic | Partitions | Purpose |
|-------|------------|---------|
| `test-topic` | 6 | Main test topic with multiple partitions |
| `high-volume-topic` | 3 | Secondary topic for burst testing |

**Why 6 and 3 partitions?**
Multiple partitions allow you to observe:
- Per-partition lag distribution
- How lag can be uneven across partitions
- Partition-level metrics in Grafana

### 2. Producer (`scripts/producer.sh`)

The producer is a bash script using `kafka-console-producer`. It runs in phases to create interesting lag patterns:

```
┌─────────────────────────────────────────────────────────────────┐
│                     Producer Cycle (~3 minutes)                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Phase 1: Low Rate (30s)                                        │
│  ├── 10 messages/second to test-topic                           │
│  └── Creates baseline, consumer might keep up                   │
│                                                                  │
│  Phase 2: Burst (quick)                                         │
│  ├── 100 messages rapidly to test-topic                         │
│  └── Creates sudden lag spike                                   │
│                                                                  │
│  Phase 3: High-Volume Burst (quick)                             │
│  ├── 200 messages to high-volume-topic                          │
│  └── Tests second consumer group                                │
│                                                                  │
│  Phase 4: Medium Rate (30s)                                     │
│  ├── 20 messages/second to test-topic                           │
│  └── Moderate production, lag grows slowly                      │
│                                                                  │
│  Phase 5: Pause (20s)                                           │
│  ├── No production                                              │
│  └── Consumer can catch up, lag decreases                       │
│                                                                  │
│  Phase 6: Multi-Topic Burst (parallel)                          │
│  ├── 50 messages to test-topic                                  │
│  ├── 100 messages to high-volume-topic                          │
│  └── Simultaneous production to both topics                     │
│                                                                  │
│  [Repeat cycle]                                                  │
└─────────────────────────────────────────────────────────────────┘
```

**Message format:** `message-{counter}-{nanosecond_timestamp}`

This format helps with:
- Tracking message order
- Debugging
- Understanding when messages were produced

### 3. Consumer (`scripts/consumer.sh`)

The consumer intentionally runs **slower** than the producer to create observable lag:

```
┌─────────────────────────────────────────────────────────────────┐
│                     Consumer Cycle (~3-4 minutes)                │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Phase 1: Slow (deliberate lag buildup)                         │
│  ├── 5 messages with 0.5s delay each                            │
│  └── Very slow - lag grows significantly                        │
│                                                                  │
│  Phase 2: Medium                                                │
│  ├── 20 messages with 0.2s delay each                           │
│  └── Moderate consumption                                       │
│                                                                  │
│  Phase 3: Fast Catch-up                                         │
│  ├── 50 messages with 0.05s delay each                          │
│  └── Rapid consumption - lag decreases                          │
│                                                                  │
│  Phase 4: High-Volume Topic                                     │
│  ├── 30 messages from high-volume-topic                         │
│  ├── Uses different consumer group                              │
│  └── Shows multiple groups in metrics                           │
│                                                                  │
│  Phase 5: PAUSE (30s)                                           │
│  ├── Consumer does nothing                                      │
│  └── Lag grows rapidly (producer still running)                 │
│                                                                  │
│  Phase 6: Burst Consumption                                     │
│  ├── 100 messages with minimal delay                            │
│  └── Rapid catch-up                                             │
│                                                                  │
│  Phase 7: Both Topics (parallel)                                │
│  ├── 20 from test-topic                                         │
│  ├── 40 from high-volume-topic                                  │
│  └── Simultaneous consumption                                   │
│                                                                  │
│  [Repeat cycle]                                                  │
└─────────────────────────────────────────────────────────────────┘
```

**Consumer Groups:**
| Group Name | Topic | Purpose |
|------------|-------|---------|
| `test-consumer-group` | test-topic | Main consumer group |
| `high-volume-consumer` | high-volume-topic | Secondary group |

### 4. klag-exporter

**Configuration (`klag-config.toml`):**

```toml
[exporter]
poll_interval = "10s"        # How often to collect metrics
http_port = 8000
http_host = "0.0.0.0"
granularity = "partition"    # Show partition-level detail

[exporter.timestamp_sampling]
enabled = true               # Calculate time lag
cache_ttl = "30s"           # Cache timestamps for 30s
max_concurrent_fetches = 10  # Parallel timestamp fetches

[[clusters]]
name = "test-cluster"
bootstrap_servers = "kafka:29092"
group_whitelist = [".*"]     # Monitor all groups
topic_blacklist = ["^__.*"]  # Exclude internal topics

[clusters.labels]
environment = "test"         # Custom label added to all metrics
```

**What klag-exporter does every 10 seconds:**

1. **List Consumer Groups** - Discovers all consumer groups in Kafka
2. **Describe Groups** - Gets member assignments (which consumer owns which partition)
3. **Fetch Committed Offsets** - Gets current position for each group/partition
4. **Fetch Watermarks** - Gets earliest and latest offsets for all partitions
5. **Calculate Lag** - `lag = latest_offset - committed_offset`
6. **Fetch Timestamps** (if lag > 0) - Reads the message at committed offset to get its timestamp
7. **Calculate Time Lag** - `time_lag = now - message_timestamp`
8. **Update Registry** - Stores all metrics in memory
9. **Serve on /metrics** - Prometheus scrapes this endpoint

### 5. Prometheus

**Configuration (`prometheus/prometheus.yml`):**

```yaml
scrape_configs:
  - job_name: 'klag-exporter'
    static_configs:
      - targets: ['klag-exporter:8000']
    scrape_interval: 10s
```

Prometheus scrapes klag-exporter every 10 seconds and stores the time-series data.

### 6. Grafana

**Auto-provisioned with:**
- Prometheus datasource (pre-configured)
- Kafka Lag dashboard
- Variables for filtering (cluster, consumer group, topic)

---

## Data Flow

### How Lag Data Flows

```
1. Producer writes message to Kafka partition
   └── Kafka assigns offset 1000 to message

2. Kafka updates partition high watermark
   └── latest_offset = 1000

3. Consumer reads message (later)
   └── Consumer commits offset 1000

4. klag-exporter polls (every 10s)
   ├── Fetches latest_offset = 1050 (producer added more)
   ├── Fetches committed_offset = 1000
   ├── Calculates lag = 1050 - 1000 = 50 messages
   ├── Fetches timestamp of message at offset 1000
   └── Calculates time_lag = now - timestamp = 5.2 seconds

5. Prometheus scrapes /metrics
   └── Stores: kafka_consumergroup_group_lag{...} 50
              kafka_consumergroup_group_lag_seconds{...} 5.2

6. Grafana queries Prometheus
   └── Displays graphs showing lag over time
```

### Time Lag Calculation Detail

The **time lag** is the most valuable metric because it tells you "how far behind in real time is the consumer?"

```
                    Message Timeline
    ─────────────────────────────────────────────────▶ time

    │                    │                          │
    │                    │                          │
    10:00:00            10:00:05                   10:00:10
    Message produced    (waiting in Kafka)         Consumer reads
    offset=1000                                    commits offset=1000
    timestamp=10:00:00

    At 10:00:10, klag-exporter:
    - Fetches committed_offset = 1000
    - Reads message at offset 1000
    - Gets timestamp = 10:00:00
    - Calculates: time_lag = 10:00:10 - 10:00:00 = 10 seconds
```

---

## Lag Creation Mechanism

### Why Lag Happens in the Test Stack

```
Time    Producer Rate    Consumer Rate    Net Effect
────    ─────────────    ─────────────    ──────────
0-30s   10 msg/sec       ~2 msg/sec       Lag grows by ~240 messages
30-32s  100 msg burst    ~2 msg/sec       Lag spikes by ~96 messages
32-35s  200 msg burst    0 (different     Lag on high-volume grows
        (high-vol)       topic)
35-65s  20 msg/sec       ~5 msg/sec       Lag grows by ~450 messages
65-85s  0 (pause)        ~20 msg/sec      Lag decreases by ~400 messages
85-90s  150 msg burst    ~20 msg/sec      Lag spikes again
```

### Visual Representation of Expected Lag Pattern

```
Lag (messages)
    │
800 │                    ╱╲
    │                   ╱  ╲
600 │         ╱╲       ╱    ╲
    │        ╱  ╲     ╱      ╲
400 │       ╱    ╲   ╱        ╲
    │      ╱      ╲ ╱          ╲
200 │     ╱        ╳            ╲
    │    ╱                       ╲
  0 │───╱─────────────────────────╲────▶ time
    │  0    1    2    3    4    5    6  (minutes)
       │    │    │    │    │    │
       │    │    │    │    │    └── Catch-up
       │    │    │    │    └── Consumer pause (lag grows)
       │    │    └────┴── Steady growth
       │    └── Burst spike
       └── Initial buildup
```

---

## Metrics Explained

### Partition Offset Metrics

```prometheus
kafka_partition_latest_offset{cluster_name="test-cluster",topic="test-topic",partition="0"} 1523

kafka_partition_earliest_offset{cluster_name="test-cluster",topic="test-topic",partition="0"} 0
```

- **latest_offset**: The offset of the next message that will be written (high watermark)
- **earliest_offset**: The oldest available offset (messages before this are deleted)

### Consumer Group Partition Metrics

```prometheus
kafka_consumergroup_group_offset{
  cluster_name="test-cluster",
  group="test-consumer-group",
  topic="test-topic",
  partition="0",
  member_host="consumer-1",
  consumer_id="consumer-xyz",
  client_id="rdkafka"
} 1450
```

Shows the committed offset for each consumer group + topic + partition combination.

### Lag Metrics

```prometheus
# Offset lag (number of messages behind)
kafka_consumergroup_group_lag{...} 73

# Time lag (seconds behind)
kafka_consumergroup_group_lag_seconds{...} 4.523
```

### Aggregate Metrics

```prometheus
# Maximum lag across all partitions for a group
kafka_consumergroup_group_max_lag{cluster_name="test-cluster",group="test-consumer-group"} 150

# Sum of lag across all partitions for a group
kafka_consumergroup_group_sum_lag{cluster_name="test-cluster",group="test-consumer-group"} 523

# Maximum time lag across all partitions
kafka_consumergroup_group_max_lag_seconds{cluster_name="test-cluster",group="test-consumer-group"} 12.5

# Sum of lag per topic
kafka_consumergroup_group_topic_sum_lag{cluster_name="test-cluster",group="test-consumer-group",topic="test-topic"} 523
```

### Operational Metrics

```prometheus
# Exporter health (1 = healthy, 0 = unhealthy)
kafka_lag_exporter_up 1

# Time taken for last collection cycle
kafka_lag_exporter_scrape_duration_seconds 0.234

# Time taken to poll Kafka
kafka_consumergroup_poll_time_ms{cluster_name="test-cluster"} 187
```

---

## Grafana Dashboard Breakdown

### Row 1: Overview Stats

| Panel | Metric | What It Shows |
|-------|--------|---------------|
| Total Consumer Lag | `sum(kafka_consumergroup_group_sum_lag)` | Total messages behind across all groups |
| Max Time Lag | `max(kafka_consumergroup_group_max_lag_seconds)` | Worst-case time delay |
| Exporter Status | `kafka_lag_exporter_up` | UP/DOWN indicator |
| Scrape Duration | `kafka_lag_exporter_scrape_duration_seconds` | Collection performance |
| Consumer Groups | `count(kafka_consumergroup_group_sum_lag)` | Number of monitored groups |

### Row 2: Lag Over Time (Time Series)

| Panel | What It Shows |
|-------|---------------|
| Consumer Group Total Lag | Line graph of `kafka_consumergroup_group_sum_lag` over time |
| Consumer Group Time Lag | Line graph of `kafka_consumergroup_group_max_lag_seconds` over time |

These are the most important graphs - they show the lag trends.

### Row 3: Per Partition

| Panel | What It Shows |
|-------|---------------|
| Partition Lag | Individual partition lag breakdown |
| Partition Time Lag | Time lag per partition |

Shows if lag is evenly distributed or concentrated on specific partitions.

### Row 4: Throughput & Distribution

| Panel | What It Shows |
|-------|---------------|
| Partition Latest Offsets | Per-partition offset values (shows partition distribution) |
| Topic Message Rate | `rate()` of offset changes aggregated by topic (produce/consume throughput) |

---

## Observable Behaviors

### What You Should See

1. **Lag Buildup During Consumer Pause**
   - When consumer enters 30-second pause, watch lag climb steadily
   - Time lag increases proportionally

2. **Lag Spike on Bursts**
   - Producer burst → immediate spike in offset lag
   - Time lag follows shortly after

3. **Catch-up Recovery**
   - Fast consumption phases → lag decreases
   - Time lag drops to near-zero when caught up

4. **Multiple Consumer Groups**
   - `test-consumer-group` on test-topic
   - `high-volume-consumer` on high-volume-topic
   - Each tracked independently

5. **Partition Distribution**
   - 6 partitions on test-topic
   - Lag may be uneven (depends on partition assignment)

6. **Time Lag vs Offset Lag Correlation**
   - Time lag depends on produce rate
   - High produce rate → same offset lag = shorter time lag
   - Low produce rate → same offset lag = longer time lag

---

## What's NOT Demonstrated

The test stack doesn't showcase all klag-exporter capabilities:

### 1. Granularity Configuration

**What it is:** klag-exporter can output metrics at `topic` level (aggregated) or `partition` level (detailed).

**Current setting:** `granularity = "partition"` (full detail)

**Not shown:** How `granularity = "topic"` reduces metric cardinality by omitting partition-level consumer group metrics.

**To test:**
```toml
# Change in klag-config.toml
granularity = "topic"
```

With topic granularity, you'll see:
- Group aggregates (max_lag, sum_lag)
- Topic aggregates (topic_sum_lag)
- NO partition-level lag/offset metrics for consumer groups

### 2. Custom Cluster Labels

**What it is:** Add arbitrary labels to all metrics for a cluster.

**Current setting:** `environment = "test"`

**Not fully shown:** Multiple custom labels, using them for filtering.

**Potential use:**
```toml
[clusters.labels]
environment = "production"
datacenter = "us-west-2"
team = "platform"
cost_center = "12345"
```

These would appear on every metric, enabling queries like:
```promql
kafka_consumergroup_group_lag{datacenter="us-west-2"}
```

### 3. OpenTelemetry Export

**What it is:** Export metrics via OTLP protocol to observability backends like Jaeger, Honeycomb, Datadog.

**Current setting:** `enabled = false`

**Not shown:** How metrics flow to OTel collector.

**To enable:**
```toml
[exporter.otel]
enabled = true
endpoint = "http://otel-collector:4317"
export_interval = "60s"
```

Would require adding an OTel collector to docker-compose.

### 4. Multi-Cluster Monitoring

**What it is:** Monitor multiple Kafka clusters from one exporter instance.

**Current setting:** Single cluster (`test-cluster`)

**Not shown:** How metrics from multiple clusters appear together.

**Example:**
```toml
[[clusters]]
name = "production-us"
bootstrap_servers = "kafka-us:9092"

[[clusters]]
name = "production-eu"
bootstrap_servers = "kafka-eu:9092"
```

### 5. SASL/SSL Authentication

**What it is:** Secure connection to Kafka with authentication.

**Current setting:** Plaintext, no auth

**Not shown:** How to configure for secured Kafka.

**Example:**
```toml
[clusters.consumer_properties]
"security.protocol" = "SASL_SSL"
"sasl.mechanism" = "PLAIN"
"sasl.username" = "${KAFKA_USER}"
"sasl.password" = "${KAFKA_PASSWORD}"
```

### 6. Consumer Group Filtering

**What it is:** Whitelist/blacklist consumer groups by regex.

**Current setting:** `group_whitelist = [".*"]` (all groups)

**Not shown:** Filtering specific groups.

**Example:**
```toml
group_whitelist = ["^prod-.*", "^staging-.*"]
group_blacklist = ["^test-.*", "^__.*"]
```

### 7. Topic Filtering

**What it is:** Include/exclude specific topics.

**Current setting:** All topics except internal (`^__.*`)

**Not shown:** More complex filtering.

**Example:**
```toml
topic_whitelist = ["^orders-.*", "^payments-.*"]
topic_blacklist = [".*-dlq$", ".*-retry$"]
```

### 8. Timestamp Caching Behavior

**What it is:** Timestamps are cached to reduce Kafka load.

**Current setting:** `cache_ttl = 30s`

**Not shown:**
- Cache hit/miss behavior
- How changing cache TTL affects accuracy vs performance
- Cache cleanup (runs at 2x TTL interval)

### 9. Concurrent Timestamp Fetching

**What it is:** Parallel timestamp fetches for multiple partitions.

**Current setting:** `max_concurrent_fetches = 10`

**Not shown:** Performance difference with different concurrency levels.

### 10. Backoff Behavior

**What it is:** Exponential backoff when Kafka is unavailable.

**Not shown:** How exporter behaves when Kafka goes down.

**To test:** Stop Kafka container and watch logs:
```bash
docker-compose stop kafka
docker-compose logs -f klag-exporter
```

### 11. Member Assignment Details

**What it is:** Metrics include which consumer instance owns each partition.

**Labels available but not highlighted:**
- `member_host` - hostname of consumer
- `consumer_id` - unique consumer identifier
- `client_id` - client library identifier

These are visible in partition-level metrics but the test stack uses generic consumers without distinct IDs.

### 12. Health & Readiness Endpoints

**What it is:** Kubernetes-style health endpoints.

**Endpoints:**
- `/health` - Is the exporter process healthy?
- `/ready` - Is at least one cluster being monitored?

**Not shown in dashboard:** Could add panels monitoring these.

---

## Extending the Test Stack

### Add OTel Collector

```yaml
# Add to docker-compose.yml
otel-collector:
  image: otel/opentelemetry-collector:latest
  ports:
    - "4317:4317"
  volumes:
    - ./otel-config.yaml:/etc/otel/config.yaml
  command: ["--config", "/etc/otel/config.yaml"]
```

### Add More Consumer Groups

```bash
# Create additional consumers with different group IDs
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic test-topic \
  --group analytics-consumer \
  --from-beginning
```

### Simulate Kafka Failure

```bash
# Stop Kafka
docker-compose stop kafka

# Watch exporter backoff behavior
docker-compose logs -f klag-exporter

# Restart Kafka
docker-compose start kafka
```

### Test with More Partitions

```bash
# Create topic with many partitions
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create --topic high-partition-topic \
  --partitions 50 \
  --replication-factor 1
```

### Stress Test

```bash
# High-volume producer (no delays)
docker exec -it kafka bash -c '
  for i in $(seq 1 100000); do
    echo "message-$i"
  done | kafka-console-producer \
    --bootstrap-server localhost:9092 \
    --topic test-topic
'
```

---

## Summary

The test stack demonstrates:

| Feature | Demonstrated | Quality |
|---------|--------------|---------|
| Basic lag monitoring | ✅ Yes | Full |
| Time lag calculation | ✅ Yes | Full |
| Multiple topics | ✅ Yes | Good |
| Multiple consumer groups | ✅ Yes | Good |
| Partition-level metrics | ✅ Yes | Full |
| Prometheus integration | ✅ Yes | Full |
| Grafana dashboard | ✅ Yes | Full |
| Custom labels | ⚠️ Partial | Basic |
| Granularity settings | ⚠️ Partial | Only partition mode |
| Topic/group filtering | ⚠️ Partial | Default filters |
| SASL/SSL auth | ❌ No | Not configured |
| Multi-cluster | ❌ No | Single cluster |
| OpenTelemetry | ❌ No | Disabled |
| Failure scenarios | ❌ No | Not automated |

The stack provides a solid foundation for understanding klag-exporter's core functionality. For production use cases, you'll want to explore the additional configuration options documented above.
