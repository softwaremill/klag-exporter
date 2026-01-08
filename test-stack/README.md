# Kafka Lag Exporter Test Stack

A complete Docker Compose setup to test and demonstrate the klag-exporter with Kafka, Prometheus, and Grafana.

## Components

- **Kafka** (Confluent 7.5, KRaft mode - no Zookeeper)
- **Kafka UI** - Web interface for Kafka debugging (optional)
- **klag-exporter** - The Kafka consumer lag exporter
- **Prometheus** - Metrics storage and querying
- **Grafana** - Pre-configured dashboard for lag visualization
- **OpenTelemetry Collector** - Receives OTLP metrics and exports to various backends
- **Producer** - Generates messages at varying rates
- **Consumer** - Consumes messages slower than producer (creates lag)

## Quick Start

```bash
# Build and start the stack
docker-compose up --build -d

# View logs
docker-compose logs -f

# Stop the stack
docker-compose down

# Stop and remove volumes
docker-compose down -v
```

## Access Points

| Service | URL | Credentials |
|---------|-----|-------------|
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | - |
| klag-exporter | http://localhost:8000/metrics | - |
| Kafka UI | http://localhost:8080 | - |
| OTel Collector metrics | http://localhost:8889/metrics | - |
| OTel Collector health | http://localhost:8888/metrics | - |

## What to Observe

### 1. Grafana Dashboard

Open http://localhost:3000 and navigate to **Kafka Consumer Lag** dashboard.

The dashboard shows:
- **Total Consumer Lag** - Sum of all lag across groups
- **Max Time Lag** - Maximum lag in seconds (how far behind in time)
- **Exporter Status** - UP/DOWN indicator
- **Scrape Duration** - How long each collection cycle takes
- **Consumer Groups** - Count of monitored groups

Time-series graphs:
- Consumer Group Total Lag over time
- Time Lag in seconds over time
- Per-partition lag breakdown
- Partition latest offsets (distribution across partitions)
- Topic message rates (produce vs consume throughput)

### 2. Lag Patterns

The producer and consumer scripts create observable patterns:

**Producer phases:**
1. Low rate (10 msg/sec) - steady baseline
2. Burst (100 messages quickly) - creates sudden lag spike
3. High-volume topic burst (200 messages)
4. Medium rate (20 msg/sec) - moderate production
5. Pause - allows consumer to catch up
6. Multi-topic burst - simultaneous production

**Consumer phases:**
1. Slow consumption (0.5s delay) - lag builds up
2. Medium consumption (0.2s delay)
3. Fast catch-up (0.05s delay) - lag decreases
4. High-volume topic consumption
5. Pause (30s) - lag grows significantly
6. Burst consumption - rapid catch-up

### 3. Metrics Endpoint

View raw metrics:
```bash
curl http://localhost:8000/metrics
```

Key metrics:
- `kafka_consumergroup_group_lag` - Offset lag per partition
- `kafka_consumergroup_group_lag_seconds` - Time lag per partition
- `kafka_consumergroup_group_sum_lag` - Total lag per group
- `kafka_consumergroup_group_max_lag_seconds` - Max time lag per group
- `kafka_partition_latest_offset` - Latest offset per partition
- `kafka_lag_exporter_scrape_duration_seconds` - Collection time

### 4. OpenTelemetry Metrics

The klag-exporter sends OTLP metrics to the OpenTelemetry Collector every 15 seconds.

**View OTel Collector received metrics:**
```bash
# Metrics exported by the collector (from klag-exporter)
curl http://localhost:8889/metrics | grep klag

# Collector's own internal metrics
curl http://localhost:8888/metrics
```

**View OTel Collector logs (shows received metrics):**
```bash
docker-compose logs -f otel-collector
```

The collector exports metrics in two ways:
1. **Debug exporter** - Logs metrics to stdout (visible in `docker-compose logs otel-collector`)
2. **Prometheus exporter** - Makes metrics scrapeable at http://localhost:8889/metrics

**Verify OTel data flow:**
```bash
# Check if klag-exporter is sending metrics
docker-compose logs klag-exporter | grep -i otel

# Check collector is receiving
docker-compose logs otel-collector | grep -i "metrics"
```

## Manual Testing

### Produce messages manually

```bash
# Connect to Kafka container
docker exec -it kafka bash

# Produce messages
kafka-console-producer --bootstrap-server localhost:9092 --topic test-topic
# Type messages and press Enter

# Or produce from host
docker exec -i kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic test-topic <<< "test message"
```

### Consume messages manually

```bash
# Consume with a specific group
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic test-topic \
  --group manual-consumer \
  --from-beginning
```

### Check consumer groups

```bash
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --all-groups
```

### Create topics

```bash
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create --topic my-topic \
  --partitions 3 \
  --replication-factor 1
```

## Customization

### Modify producer/consumer behavior

Edit `scripts/producer.sh` and `scripts/consumer.sh` to change:
- Message rates
- Phase durations
- Topics
- Burst sizes

Restart the services:
```bash
docker-compose restart producer consumer
```

### Modify klag-exporter config

Edit `klag-config.toml` to change:
- Poll interval
- Timestamp sampling settings
- Group/topic filters
- Granularity (topic vs partition)

Restart the exporter:
```bash
docker-compose restart klag-exporter
```

### Scale consumers

Create multiple consumer instances:
```bash
docker-compose up -d --scale consumer=3
```

## Troubleshooting

### Exporter not showing metrics

1. Check if Kafka is healthy:
   ```bash
   docker-compose logs kafka
   ```

2. Check exporter logs:
   ```bash
   docker-compose logs klag-exporter
   ```

3. Verify connectivity:
   ```bash
   docker exec klag-exporter wget -qO- http://localhost:8000/health
   ```

### No lag visible

- Ensure producer is running: `docker-compose logs producer`
- Ensure consumer is running: `docker-compose logs consumer`
- Check Kafka UI at http://localhost:8080 to see message counts

### Grafana not showing data

1. Check Prometheus targets: http://localhost:9090/targets
2. Verify klag-exporter endpoint shows data
3. Check Grafana datasource connection

## Architecture

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   Producer   │────▶│    Kafka     │◀────│   Consumer   │
└──────────────┘     └──────────────┘     └──────────────┘
                            │
                            │ (Admin API)
                            ▼
                     ┌──────────────┐
                     │klag-exporter │
                     │   :8000      │
                     └──────────────┘
                       │         │
          /metrics     │         │ OTLP (gRPC :4317)
                       │         │
                       ▼         ▼
              ┌──────────────┐  ┌──────────────┐
              │  Prometheus  │  │OTel Collector│
              │   :9090      │  │ :8889/:8888  │
              └──────────────┘  └──────────────┘
                       │
                       │ PromQL
                       ▼
              ┌──────────────┐
              │   Grafana    │
              │   :3000      │
              └──────────────┘
```
