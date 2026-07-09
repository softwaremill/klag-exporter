#!/bin/bash

# Consumer script that consumes messages at varying rates
# Intentionally slower than producer to create observable lag

BOOTSTRAP_SERVER="kafka:29092"
TOPIC1="test-topic"
TOPIC2="high-volume-topic"
GROUP_ID="test-consumer-group"
GRAFANA_URL="http://grafana:3000"

# Message counter
MSG_COUNT=0

# Function to create Grafana annotation for phase visibility
annotate() {
    local text="$1"
    local tags="$2"
    curl -s -X POST "$GRAFANA_URL/api/annotations" \
        -H "Content-Type: application/json" \
        -d "{\"text\": \"$text\", \"tags\": [$tags]}" \
        2>/dev/null || true  # Don't fail if Grafana isn't ready
}

echo "Starting consumer with consumer group: $GROUP_ID"
echo "This consumer runs slower than producer to create observable lag"

# Function to consume with rate limiting
consume_with_rate() {
    local topic=$1
    local group=$2
    local max_messages=$3
    local delay=$4

    echo "[$(date)] Consuming up to $max_messages messages from $topic (delay: ${delay}s between messages)"

    # Use timeout to limit consumption time, with rate limiting via processing delay
    timeout 60 kafka-console-consumer \
        --bootstrap-server $BOOTSTRAP_SERVER \
        --topic $topic \
        --group $group \
        --max-messages $max_messages \
        2>/dev/null | while read -r line; do
            MSG_COUNT=$((MSG_COUNT + 1))
            if [ "$delay" != "0" ]; then
                sleep $delay
            fi
        done

    echo "[$(date)] Consumed batch from $topic"
}

while true; do
    # Phase 1: Slow consumption (creates lag buildup)
    annotate "Consumer Phase 1: Slow consumption (lag building)" "\"consumer\", \"phase1\", \"slow\""
    echo "[$(date)] Phase 1: Slow consumption - 5 msg with 0.5s delay each"
    consume_with_rate $TOPIC1 $GROUP_ID 5 0.5

    # Phase 2: Medium consumption
    annotate "Consumer Phase 2: Medium consumption" "\"consumer\", \"phase2\""
    echo "[$(date)] Phase 2: Medium consumption - 20 msg with 0.2s delay each"
    consume_with_rate $TOPIC1 $GROUP_ID 20 0.2

    # Phase 3: Fast catch-up
    annotate "Consumer Phase 3: Fast catch-up" "\"consumer\", \"phase3\", \"catchup\""
    echo "[$(date)] Phase 3: Fast catch-up - 50 msg with 0.05s delay each"
    consume_with_rate $TOPIC1 $GROUP_ID 50 0.05

    # Phase 4: Consume from high-volume topic (different consumer group)
    annotate "Consumer Phase 4: High-volume topic consumption" "\"consumer\", \"phase4\""
    echo "[$(date)] Phase 4: High-volume topic consumption"
    consume_with_rate $TOPIC2 "high-volume-consumer" 30 0.1

    # Phase 5: Pause (lag builds up)
    annotate "Consumer Phase 5: Pause for 30s (lag building)" "\"consumer\", \"phase5\", \"pause\""
    echo "[$(date)] Phase 5: Consumer pause for 30s (lag building)"
    sleep 30

    # Phase 6: Burst consumption
    annotate "Consumer Phase 6: Burst consumption" "\"consumer\", \"phase6\", \"burst\""
    echo "[$(date)] Phase 6: Burst consumption - 100 msg with minimal delay"
    consume_with_rate $TOPIC1 $GROUP_ID 100 0.01

    # Phase 7: Both topics
    annotate "Consumer Phase 7: Both topics consumption" "\"consumer\", \"phase7\""
    echo "[$(date)] Phase 7: Both topics consumption"
    consume_with_rate $TOPIC1 $GROUP_ID 20 0.1 &
    consume_with_rate $TOPIC2 "high-volume-consumer" 40 0.05 &
    wait

    echo "[$(date)] Consumer cycle complete"
    echo "---"
done
