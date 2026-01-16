# Offset Deletion Detection in Kafka Lag Exporter

## The Problem

Both **log compaction** and **retention-based deletion** can cause inaccurate time lag measurements.

### What is Log Compaction?

Kafka topics can be configured with `cleanup.policy=compact`. Instead of deleting old messages based on time/size, Kafka retains only the **latest message per key**, removing older duplicates.

```
Before compaction:
  offset 0: key=A, value=1, timestamp=10:00
  offset 1: key=B, value=2, timestamp=10:01
  offset 2: key=A, value=3, timestamp=10:02  <- newer value for key A
  offset 3: key=C, value=4, timestamp=10:03

After compaction:
  offset 1: key=B, value=2, timestamp=10:01
  offset 2: key=A, value=3, timestamp=10:02
  offset 3: key=C, value=4, timestamp=10:03

  (offset 0 is gone - compacted away)
```

### What is Retention-Based Deletion?

Kafka topics with `cleanup.policy=delete` (the default) remove old messages based on time (`retention.ms`) or size (`retention.bytes`). Unlike compaction, retention deletes messages from the **beginning** of the log only.

```
Before retention:
  offset 0: key=A, timestamp=day1
  offset 1: key=B, timestamp=day1
  offset 2: key=C, timestamp=day2
  offset 3: key=D, timestamp=day3

After retention (7 days, day1 messages expired):
  offset 2: key=C, timestamp=day2  <- now the earliest
  offset 3: key=D, timestamp=day3

  low_watermark moved from 0 to 2
```

### How This Affects Lag Measurement

**Offset lag** = `latest_offset - committed_offset`

If a consumer's committed offset points to a deleted message (compaction or retention):
- The offset count suggests N messages behind
- But some of those offsets no longer exist
- **Result: Offset lag is inflated** (shows more lag than actual messages to process)

**Time lag** = `now - timestamp_of_committed_offset_message`

To calculate time lag, we seek to the committed offset and read the message timestamp. But if that offset was deleted:
- Kafka returns the **next available message** instead
- That message has a **later timestamp** than the original
- **Result: Time lag is understated** (shows less lag than reality)

**Both compaction and retention cause the same effect on measurements.**

### Example

```
Consumer committed offset: 100
Actual situation after compaction:
  - Offsets 100-149 were compacted away
  - Next available message is at offset 150

When we measure:
  - We seek to offset 100
  - Kafka returns message at offset 150
  - We use offset 150's timestamp (which is much later)
  - Time lag appears smaller than it really is
```

## Detection Mechanisms

### Compaction Detection

The exporter detects compaction by comparing offsets after polling:
- **Requested offset**: The committed offset we seek to
- **Actual offset**: The offset of the message Kafka returns (`msg.offset()`)

```rust
if actual_offset > requested_offset {
    // Compaction detected - Kafka skipped to next available message
    compaction_detected = true;
}
```

**Note:** This detection happens when we poll for the message. If the committed offset was in a "gap" in the log, Kafka returns the next available message.

### Retention Detection

The exporter detects retention deletion by comparing committed offset to low watermark:
- **Committed offset**: The consumer group's last committed position
- **Low watermark**: The earliest available offset in the partition

```rust
if committed_offset < low_watermark {
    // Data loss detected - committed offset fell off the log
    data_loss_detected = true;
}
```

**Note:** This detection is cheaper - we already fetch watermarks, no extra Kafka call needed.

## Metrics Exposed

### Per-partition labels on lag_seconds
```
kafka_consumergroup_group_lag_seconds{
  cluster_name="...",
  group="...",
  topic="...",
  partition="...",
  compaction_detected="true",   # or "false"
  data_loss_detected="false"    # or "true"
} 5.2
```

Use these labels to identify exactly which partitions are affected and why.

### Per-partition data loss metrics
```
kafka_consumergroup_group_messages_lost{...} 50        # Messages deleted before processing
kafka_consumergroup_group_retention_margin{...} -50    # Negative = data loss occurred
kafka_consumergroup_group_lag_retention_ratio{...} 120 # >100 = data loss
```

These metrics help detect and prevent data loss:
- `messages_lost`: Count of messages deleted by retention before consumer processed them
- `retention_margin`: Distance to deletion boundary (`committed_offset - low_watermark`). Negative means data loss.
- `lag_retention_ratio`: Consumer lag as percentage of retention window. >100% indicates data loss.

### Cluster-wide counters
```
kafka_lag_exporter_compaction_detected_total{cluster_name="..."} 3
kafka_lag_exporter_data_loss_partitions_total{cluster_name="..."} 1
```

Total count of partitions affected by each type of deletion in the last collection cycle.

## Log Messages

When compaction is detected:

```
WARN Compaction detected: requested offset was compacted, got next available message.
     Time lag may be understated.
     topic=my-topic partition=0 requested_offset=100 actual_offset=150 offset_gap=50
```

Batch summary:
```
INFO Compaction detected in some partitions - time lag may be understated for these
     cluster=my-cluster compaction_affected_count=3 total_timestamps=24
```

## Grafana Dashboard

The default dashboard in `test-stack/` focuses on core lag metrics. To monitor compaction and retention detection, you can add custom panels using these queries:

```promql
# Count of partitions with compaction detected
kafka_lag_exporter_compaction_detected_total{cluster_name="$cluster"}

# Count of partitions with data loss detected
kafka_lag_exporter_data_loss_partitions_total{cluster_name="$cluster"}

# Time lag for compacted partitions only
kafka_consumergroup_group_lag_seconds{compaction_detected="true"}

# Time lag for data-loss-affected partitions only
kafka_consumergroup_group_lag_seconds{data_loss_detected="true"}

# Consumers approaching data loss (lag > 80% of retention window)
kafka_consumergroup_group_lag_retention_ratio > 80

# Messages already lost
kafka_consumergroup_group_messages_lost > 0
```

## Recommendations

1. **For affected topics**: Rely more on offset lag than time lag
2. **Alert on deletion events**: Set up alerts on:
   - `kafka_lag_exporter_compaction_detected_total > 0`
   - `kafka_lag_exporter_data_loss_partitions_total > 0`
3. **Prevent data loss**: Monitor `lag_retention_ratio` approaching 100%
4. **Investigate root cause**: High detection counts may indicate:
   - Consumer is very far behind
   - Aggressive compaction settings (`min.compaction.lag.ms` too low)
   - Short retention period (`retention.ms` too low)
   - Consumer offset pointing to ancient data
   - Dead consumer group with stale offsets

## Configuration

No configuration needed - both compaction and retention detection are automatic and always enabled.


## How Values Are Obtained

| Value | Function | File | Kafka API |
|-------|----------|------|-----------|
| Latest offset (high watermark) | `fetch_all_watermarks()` | `src/kafka/client.rs` | `fetch_watermarks()` |
| Committed offset | `fetch_group_offsets()` | `src/collector/offset_collector.rs` | `committed_offsets()` |
| Message timestamp | `fetch_timestamp()` | `src/kafka/consumer.rs` | `assign()` + `poll()` + `msg.timestamp()` |