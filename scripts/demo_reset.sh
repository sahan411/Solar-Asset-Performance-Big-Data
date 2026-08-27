#!/usr/bin/env bash
# Return SolarIQ to a clean pre-demo state.
#
# THIS SCRIPT DELETES DATA. It is written to delete only data the demo itself
# generated, and to be obvious about what it is about to remove.
#
# Deliberately NOT used here, and worth stating because they are the usual
# reach for this job:
#
#   docker compose down -v   removes every volume including ones other members
#                            own, without naming them
#   docker system prune -a   removes images and volumes across ALL projects on
#                            the machine, not just this one
#   rm -rf "$SOME_VAR"/...   deletes the filesystem root if the variable is
#                            empty, which is exactly when things go wrong
#
# Each removal below is targeted, guarded, and printed before it happens.
#
#   scripts/demo_reset.sh          reset ingestion (Kafka topics, daily feed)
#   scripts/demo_reset.sh --all    also clear PostgreSQL and MinIO
#   scripts/demo_reset.sh --yes    skip the confirmation prompt

set -euo pipefail
export MSYS_NO_PATHCONV=1

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"

RESET_ALL=false
ASSUME_YES=false
for arg in "$@"; do
  case "${arg}" in
    --all) RESET_ALL=true ;;
    --yes|-y) ASSUME_YES=true ;;
    *) echo "Unknown option: ${arg}" >&2; exit 2 ;;
  esac
done

DAILY_DIR="${SIMULATION_OUTPUT_DIR:-./data/daily}"

# Guard: refuse to touch anything outside the repository. A mistyped or empty
# SIMULATION_OUTPUT_DIR must abort, never widen the blast radius.
DAILY_ABS="$(cd "$(dirname "${DAILY_DIR}")" 2>/dev/null && pwd)/$(basename "${DAILY_DIR}")" || DAILY_ABS=""
if [[ -z "${DAILY_ABS}" || "${DAILY_ABS}" != "${REPO_ROOT}"/* ]]; then
  echo "Refusing to clear '${DAILY_DIR}': it is outside the repository." >&2
  echo "demo_reset.sh only removes files it can prove this project generated." >&2
  exit 1
fi

daily_files=()
if [[ -d "${DAILY_ABS}" ]]; then
  while IFS= read -r -d '' f; do daily_files+=("$f"); done \
    < <(find "${DAILY_ABS}" -maxdepth 1 -name 'daily_reference_*.csv' -print0 2>/dev/null)
fi

echo "About to reset the SolarIQ demo:"
echo
echo "  - delete and recreate the three Kafka topics (all published events lost)"
echo "  - delete ${#daily_files[@]} daily reference file(s) from ${DAILY_ABS}"
if [[ "${RESET_ALL}" == true ]]; then
  echo "  - TRUNCATE every table in the 'solariq' PostgreSQL database"
  echo "  - empty the MinIO raw archive bucket"
fi
echo
echo "  Source code, configuration and .env are NOT touched."
echo

if [[ "${ASSUME_YES}" != true ]]; then
  read -r -p "Continue? [y/N] " reply
  [[ "${reply}" =~ ^[Yy]$ ]] || { echo "Aborted."; exit 0; }
fi

if ! docker ps --format '{{.Names}}' | grep -qx solariq-kafka; then
  echo "Kafka is not running; start it first with scripts/bootstrap.sh" >&2
  exit 1
fi

# ---------------------------------------------------------------------------
echo
echo "[1] Kafka topics"
# ---------------------------------------------------------------------------
# Deleting and recreating is cleaner than consuming the backlog away: it resets
# offsets too, so a restarted consumer group does not skip the new run's events.
for topic in solar.telemetry.raw solar.telemetry.invalid solar.alerts; do
  if docker exec solariq-kafka /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server kafka:9092 --list 2>/dev/null | grep -qx "${topic}"; then
    docker exec solariq-kafka /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server kafka:9092 --delete --topic "${topic}" >/dev/null
    echo "  - deleted ${topic}"
  fi
done

# Deletion is asynchronous: recreating too soon fails with TOPIC_ALREADY_EXISTS
# because the old topic is still being removed.
echo "  waiting for deletion to settle"
sleep 5
bash kafka/scripts/create_topics.sh | sed 's/^/  /'

# ---------------------------------------------------------------------------
echo
echo "[2] Daily reference feed"
# ---------------------------------------------------------------------------
if (( ${#daily_files[@]} == 0 )); then
  echo "  nothing to remove"
else
  for f in "${daily_files[@]}"; do
    rm -f -- "$f"
    echo "  - removed $(basename "$f")"
  done
fi
mkdir -p "${DAILY_ABS}"

# ---------------------------------------------------------------------------
if [[ "${RESET_ALL}" == true ]]; then
  echo
  echo "[3] PostgreSQL"
  # TRUNCATE rather than dropping the database: the schema is Member 2's
  # migrations, and dropping it would force him to re-run them before the next
  # demo. Emptying the tables leaves the structure intact.
  if docker ps --format '{{.Names}}' | grep -qx solariq-postgres; then
    docker exec solariq-postgres psql -U solariq -d solariq -qtc "
      DO \$\$
      DECLARE stmt text;
      BEGIN
        SELECT string_agg(format('TRUNCATE TABLE %I.%I CASCADE', schemaname, tablename), '; ')
          INTO stmt FROM pg_tables WHERE schemaname = 'public';
        IF stmt IS NOT NULL THEN EXECUTE stmt; END IF;
      END \$\$;" >/dev/null 2>&1 \
      && echo "  - truncated all public tables" \
      || echo "  ! could not truncate (no schema yet?) - skipped"
  else
    echo "  postgres is not running - skipped"
  fi

  echo
  echo "[4] MinIO raw archive"
  if docker ps --format '{{.Names}}' | grep -qx solariq-minio; then
    bucket="${MINIO_RAW_BUCKET:-solariq-raw}"
    docker exec solariq-minio sh -c "
      mc alias set local http://localhost:9000 \"\$MINIO_ROOT_USER\" \"\$MINIO_ROOT_PASSWORD\" >/dev/null 2>&1 &&
      mc rm --recursive --force \"local/${bucket}\" >/dev/null 2>&1" \
      && echo "  - emptied bucket ${bucket}" \
      || echo "  ! could not empty ${bucket} (may not exist yet) - skipped"
  else
    echo "  minio is not running - skipped"
  fi
fi

cat <<'NEXT'

Reset complete. The next run starts from a clean state and, because the
simulator is seeded, will reproduce exactly the same events as the last one.

  scripts/demo_start.sh
NEXT
