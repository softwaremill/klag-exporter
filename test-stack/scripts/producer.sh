#!/bin/bash

# Producer script that generates messages at varying rates
# This creates lag patterns that can be observed in Grafana

BOOTSTRAP_SERVER="kafka:29092"
TOPIC1="test-topic"
TOPIC2="high-volume-topic"

# Message counter
MSG_COUNT=0

# Function to produce messages
produce_messages() {
    local topic=$1
    local count=$2
    local delay=$3

    for ((i=1; i<=count; i++)); do
        MSG_COUNT=$((MSG_COUNT + 1))
        echo "message-$MSG_COUNT-$(date +%s%N)" | kafka-console-producer \
            --bootstrap-server $BOOTSTRAP_SERVER \
            --topic $topic \
            2>/dev/null

        if [ "$delay" != "0" ]; then
            sleep $delay
        fi
    done
}

echo "Starting producer with varying message rates..."
echo "This will create lag patterns observable in Grafana"

while true; do
    # Phase 1: Steady low rate (10 msg/sec for 30 seconds)
    echo "[$(date)] Phase 1: Low rate - 10 msg/sec for 30s"
    for ((j=1; j<=30; j++)); do
        produce_messages $TOPIC1 10 0.1
    done

    # Phase 2: Burst - high volume (100 messages quickly)
    echo "[$(date)] Phase 2: Burst - 100 messages to $TOPIC1"
    produce_messages $TOPIC1 100 0.01

    # Phase 3: High volume topic burst
    echo "[$(date)] Phase 3: High volume burst - 200 messages to $TOPIC2"
    produce_messages $TOPIC2 200 0.005

    # Phase 4: Steady medium rate (20 msg/sec for 30 seconds)
    echo "[$(date)] Phase 4: Medium rate - 20 msg/sec for 30s"
    for ((j=1; j<=30; j++)); do
        produce_messages $TOPIC1 20 0.05
    done

    # Phase 5: Pause (let consumer catch up)
    echo "[$(date)] Phase 5: Pause for 20s (consumer catch-up)"
    sleep 20

    # Phase 6: Multi-topic burst
    echo "[$(date)] Phase 6: Multi-topic burst"
    produce_messages $TOPIC1 50 0.02 &
    produce_messages $TOPIC2 100 0.01 &
    wait

    echo "[$(date)] Cycle complete. Total messages produced: $MSG_COUNT"
    echo "---"
done
