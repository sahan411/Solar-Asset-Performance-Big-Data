#!/usr/bin/env bash
# Create the SolarIQ Kafka topics. Safe to run repeatedly.
#
# Idempotent on purpose: bootstrap.sh calls it on every start, and a demo reset
# calls it again. Creating a topic that already exists must be a no-op, not an
# error that aborts the bootstrap.
#
# Auto-creation is disabled on the broker, so this script is the ONLY thing that
# defines partition counts. Without it a consumer connecting first would get
# nothing at all.
#
# Usage:
#   kafka/scripts/create_topics.sh                 # inside Docker network
#   KAFKA_BOOTSTRAP_SERVERS=localhost:29092 \
#     kafka/scripts/create_topics.sh               # from the host

set -euo pipefail

# Git Bash / MSYS rewrites anything that looks like a Unix path before the
# command is executed, so `/opt/kafka/bin/kafka-topics.sh` reaches Docker as
# `C:/Program Files/Git/opt/kafka/bin/...` and the exec fails with a confusing
# "no such file or directory" naming a path nobody wrote. The paths here are
# INSIDE the Linux container and must be passed through untouched. Harmless on
# Linux and macOS, where the variable is simply ignored.
export MSYS_NO_PATHCONV=1

CONTAINER="${KAFKA_CONTAINER:-solariq-kafka}"
BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"
TELEMETRY_TOPIC="${KAFKA_TELEMETRY_TOPIC:-solar.telemetry.raw}"
INVALID_TOPIC="${KAFKA_INVALID_TOPIC:-solar.telemetry.invalid}"
ALERT_TOPIC="${KAFKA_ALERT_TOPIC:-solar.alerts}"

# Telemetry carries 35 inverters keyed by asset, so three partitions give the
# stream real parallelism while keeping each inverter's events ordered within
# one partition. The quarantine and alert topics are low volume: one partition
# keeps their ordering trivially total and costs nothing.
TELEMETRY_PARTITIONS="${KAFKA_TELEMETRY_PARTITIONS:-3}"
SIDE_TOPIC_PARTITIONS=1

# Single broker: nothing can be replicated. Correct for a local educational
# environment and wrong for anything else, which is why it is named here.
REPLICATION_FACTOR=1

# One simulated day is 300 real seconds, so a few hours of retention covers many
# full demo runs while keeping the volume small.
RETENTION_MS="${KAFKA_RETENTION_MS:-10800000}"

kafka_topics() {
  docker exec "${CONTAINER}" /opt/kafka/bin/kafka-topics.sh "$@"
}

ensure_topic() {
  local topic="$1" partitions="$2"

  if kafka_topics --bootstrap-server "${BOOTSTRAP}" --list 2>/dev/null | grep -qx "${topic}"; then
    echo "  = ${topic} already exists"
    return 0
  fi

  kafka_topics \
    --bootstrap-server "${BOOTSTRAP}" \
    --create \
    --topic "${topic}" \
    --partitions "${partitions}" \
    --replication-factor "${REPLICATION_FACTOR}" \
    --config "retention.ms=${RETENTION_MS}" \
    >/dev/null
  echo "  + ${topic} created (${partitions} partition(s))"
}

if ! docker ps --format '{{.Names}}' | grep -qx "${CONTAINER}"; then
  echo "Kafka container '${CONTAINER}' is not running." >&2
  echo "Start it first:  docker compose up -d kafka" >&2
  exit 1
fi

# Prove the broker answers before creating anything. Without this probe the
# per-topic existence check below swallows stderr, so a broker that is up but
# not yet serving produces a silent no-op that looks like success.
if ! kafka_topics --bootstrap-server "${BOOTSTRAP}" --list >/dev/null; then
  echo "Kafka is running but not answering on ${BOOTSTRAP}." >&2
  echo "It may still be starting; wait for the healthcheck:" >&2
  echo "  docker compose ps" >&2
  exit 1
fi

echo "Creating SolarIQ topics on ${BOOTSTRAP}"
ensure_topic "${TELEMETRY_TOPIC}" "${TELEMETRY_PARTITIONS}"
ensure_topic "${INVALID_TOPIC}" "${SIDE_TOPIC_PARTITIONS}"
ensure_topic "${ALERT_TOPIC}" "${SIDE_TOPIC_PARTITIONS}"

echo
echo "Topics now present:"
kafka_topics --bootstrap-server "${BOOTSTRAP}" --list | sed 's/^/  /'
