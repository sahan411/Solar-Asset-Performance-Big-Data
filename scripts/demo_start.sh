#!/usr/bin/env bash
# Run the scripted SolarIQ assessment demo.
#
# Prints the timeline first, so the examiner knows what is coming and can watch
# it happen rather than being told afterwards that it did.
#
#   scripts/demo_start.sh            one simulated day, then stop
#   scripts/demo_start.sh --forever  run until Ctrl-C
#   scripts/demo_start.sh --days 2   two simulated days

set -euo pipefail
export MSYS_NO_PATHCONV=1

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"

DAYS="1.05"
case "${1:-}" in
  --forever) DAYS="" ;;
  --days) DAYS="${2:?--days needs a number}" ;;
esac

# The simulator runs on the HOST, so the broker's external listener is the right
# address. Inside Compose it would be kafka:9092; getting this backwards gives a
# connection that succeeds and then hangs.
export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:29092}"
export SIMULATION_OUTPUT_DIR="${SIMULATION_OUTPUT_DIR:-./data/daily}"
export PROMETHEUS_PORT="${PROMETHEUS_PORT:-9101}"

PYTHON="${PYTHON:-}"
if [[ -z "${PYTHON}" ]]; then
  if [[ -x .venv/Scripts/python.exe ]]; then PYTHON=".venv/Scripts/python.exe"
  elif [[ -x .venv/bin/python ]]; then PYTHON=".venv/bin/python"
  else PYTHON="$(command -v python3 || command -v python)"; fi
fi

if ! docker ps --format '{{.Names}}' | grep -qx solariq-kafka; then
  echo "Kafka is not running. Start the infrastructure first:" >&2
  echo "  scripts/bootstrap.sh" >&2
  exit 1
fi

cat <<BANNER

======================================================================
  SolarIQ  —  scripted assessment demo
======================================================================

  Portfolio     5 plants, 35 inverters, 21 MW
  Clock         1 simulated day = 300 real seconds (288x compression)
  Seed          8203  — the run is identical every time

  Every event carries SIMULATED time. A simulated day spans a full
  24 hours of event time, which is what lets the stream's event-time
  windows fill inside a five-minute demo.

----------------------------------------------------------------------
  Timeline (repeats each simulated day)
----------------------------------------------------------------------

     0- 90s   NORMAL                 baseline generation, no alerts
    90-150s   UNDERPERFORMANCE       PLANT_03/INV_02 at 45% power,
                                     irradiance normal  -> the fault is
                                     detectable only by comparing the two
   150-190s   RECOVERY               PLANT_03/INV_02 back to normal
   190-235s   INVERTER OFFLINE       PLANT_04/INV_01 reports a zero
   235-260s   TELEMETRY GAP          PLANT_05 goes silent entirely
                                     -> silence, not a zero: a pipeline
                                     alert, not an operational one
   260-300s   RECOVERY               PLANT_05 resumes

----------------------------------------------------------------------
  Watch it
----------------------------------------------------------------------

  Prometheus alerts   http://localhost:9090/alerts
  Live events         kafka/scripts/verify_topics.sh --consume 5
  Raw metrics         http://localhost:${PROMETHEUS_PORT}/metrics
  Daily feed          ./data/daily/

  Ctrl-C stops cleanly and flushes anything still buffered.

======================================================================

BANNER

if [[ -n "${DAYS}" ]]; then
  echo "Running ${DAYS} simulated day(s). Ctrl-C to stop early."
  exec "${PYTHON}" -m simulators.streaming.simulator --days "${DAYS}"
else
  echo "Running until interrupted. Ctrl-C to stop."
  exec "${PYTHON}" -m simulators.streaming.simulator
fi
