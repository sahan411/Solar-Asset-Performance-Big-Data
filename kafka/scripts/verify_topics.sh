#!/usr/bin/env bash
# Inspect the SolarIQ topics, and optionally read a few live telemetry events.
#
# The first thing to reach for when the dashboard is empty: it answers "is the
# producer actually publishing?" without involving Spark, Postgres or the API,
# which narrows the fault to one side of Kafka in a few seconds.
#
# Usage:
#   kafka/scripts/verify_topics.sh              # describe topics
#   kafka/scripts/verify_topics.sh --consume    # also read 5 telemetry events
#   kafka/scripts/verify_topics.sh --consume 20 # read 20

set -euo pipefail

# See create_topics.sh: Git Bash rewrites container-internal paths unless this
# is set, and the exec fails naming a Windows path nobody wrote.
export MSYS_NO_PATHCONV=1

CONTAINER="${KAFKA_CONTAINER:-solariq-kafka}"
BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"
TELEMETRY_TOPIC="${KAFKA_TELEMETRY_TOPIC:-solar.telemetry.raw}"
INVALID_TOPIC="${KAFKA_INVALID_TOPIC:-solar.telemetry.invalid}"
ALERT_TOPIC="${KAFKA_ALERT_TOPIC:-solar.alerts}"

CONSUME=false
MESSAGES=5
if [[ "${1:-}" == "--consume" ]]; then
  CONSUME=true
  MESSAGES="${2:-5}"
fi

if ! docker ps --format '{{.Names}}' | grep -qx "${CONTAINER}"; then
  echo "Kafka container '${CONTAINER}' is not running." >&2
  echo "Start it first:  docker compose up -d kafka" >&2
  exit 1
fi

echo "=== topics on ${BOOTSTRAP} ==="
docker exec "${CONTAINER}" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "${BOOTSTRAP}" --describe \
  | sed 's/^/  /'

echo
echo "=== message counts (sum of partition end offsets) ==="
for topic in "${TELEMETRY_TOPIC}" "${INVALID_TOPIC}" "${ALERT_TOPIC}"; do
  # -1 asks for the end offset of each partition. Summing them gives the total
  # messages ever written, which is what "is anything flowing?" really asks.
  #
  # org.apache.kafka.tools, not the old kafka.tools: the shell tools moved
  # package in Kafka 3.x. The old path fails with ClassNotFoundException, which
  # this pipeline would otherwise turn into a silent blank.
  total=$(docker exec "${CONTAINER}" /opt/kafka/bin/kafka-run-class.sh \
    org.apache.kafka.tools.GetOffsetShell \
    --bootstrap-server "${BOOTSTRAP}" --topic "${topic}" --time -1 2>/dev/null \
    | awk -F: '{sum += $3} END {print sum + 0}') || total="query failed"
  printf '  %-28s %s\n' "${topic}" "${total}"
done

if [[ "${CONSUME}" == true ]]; then
  echo
  echo "=== ${MESSAGES} events from ${TELEMETRY_TOPIC} (newest tail) ==="
  # Keys are printed because "is the key plant_id:inverter_id?" is the other
  # question worth answering here — a wrong key breaks per-inverter ordering.
  docker exec "${CONTAINER}" /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server "${BOOTSTRAP}" \
    --topic "${TELEMETRY_TOPIC}" \
    --max-messages "${MESSAGES}" \
    --property print.key=true \
    --property key.separator=' | ' \
    --timeout-ms 15000 2>/dev/null \
    | sed 's/^/  /' \
    || echo "  (no messages within 15s — is the simulator running?)"
fi
