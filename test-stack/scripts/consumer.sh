#!/bin/bash

# Consumer script that consumes messages at varying rates
# Intentionally slower than producer to create observable lag

BOOTSTRAP_SERVER="kafka:29092"
TOPIC1="test-topic"
TOPIC2="high-volume-topic"
GROUP_ID="test-consumer-group"

# Message counter
MSG_COUNT=0

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
    echo "[$(date)] Phase 1: Slow consumption - 5 msg with 0.5s delay each"
    consume_with_rate $TOPIC1 $GROUP_ID 5 0.5

    # Phase 2: Medium consumption
    echo "[$(date)] Phase 2: Medium consumption - 20 msg with 0.2s delay each"
    consume_with_rate $TOPIC1 $GROUP_ID 20 0.2

    # Phase 3: Fast catch-up
    echo "[$(date)] Phase 3: Fast catch-up - 50 msg with 0.05s delay each"
    consume_with_rate $TOPIC1 $GROUP_ID 50 0.05

    # Phase 4: Consume from high-volume topic (different consumer group)
    echo "[$(date)] Phase 4: High-volume topic consumption"
    consume_with_rate $TOPIC2 "high-volume-consumer" 30 0.1

    # Phase 5: Pause (lag builds up)
    echo "[$(date)] Phase 5: Consumer pause for 30s (lag building)"
    sleep 30

    # Phase 6: Burst consumption
    echo "[$(date)] Phase 6: Burst consumption - 100 msg with minimal delay"
    consume_with_rate $TOPIC1 $GROUP_ID 100 0.01

    # Phase 7: Both topics
    echo "[$(date)] Phase 7: Both topics consumption"
    consume_with_rate $TOPIC1 $GROUP_ID 20 0.1 &
    consume_with_rate $TOPIC2 "high-volume-consumer" 40 0.05 &
    wait

    echo "[$(date)] Consumer cycle complete"
    echo "---"
done
