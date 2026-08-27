#!/usr/bin/env bash
# Bring SolarIQ's local infrastructure up from nothing.
#
# Safe to run repeatedly: it creates only what is missing and waits for what is
# already there. Nothing here deletes data — that is demo_reset.sh's job, kept
# separate on purpose so a bootstrap can never destroy a run.
#
#   scripts/bootstrap.sh

set -euo pipefail

# Git Bash rewrites container-internal paths without this; see
# kafka/scripts/create_topics.sh for the full explanation.
export MSYS_NO_PATHCONV=1

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"

step() { printf '\n[%s] %s\n' "$1" "$2"; }
fail() { printf '\nERROR: %s\n' "$1" >&2; exit 1; }

# ---------------------------------------------------------------------------
step 1 "Checking Docker"
# ---------------------------------------------------------------------------
command -v docker >/dev/null 2>&1 || fail "docker is not on PATH. Install Docker Desktop and restart your shell."
docker info >/dev/null 2>&1 || fail "Docker is installed but the engine is not running. Start Docker Desktop and wait for 'Engine running'."
echo "  Docker is running: $(docker --version)"

# ---------------------------------------------------------------------------
step 2 "Preparing local configuration"
# ---------------------------------------------------------------------------
# Credentials are generated rather than shipped. .env.example documents the
# variable NAMES with empty values; a working password committed to the
# repository would still be a committed credential.
if [[ -f .env ]]; then
  echo "  .env already exists, leaving it untouched"
else
  [[ -f .env.example ]] || fail ".env.example is missing; cannot generate .env."

  generate() { LC_ALL=C tr -dc 'A-Za-z0-9' </dev/urandom | head -c 32; }
  pg_password="$(generate)"

  cp .env.example .env
  # The trailing '' is required by BSD sed (macOS) and accepted nowhere else,
  # so write to a temp file instead and keep one code path on every platform.
  python_bin="$(command -v python3 || command -v python || true)"
  if [[ -n "${python_bin}" ]]; then
    "${python_bin}" - "$pg_password" <<'PY'
import pathlib, re, secrets, sys
password = sys.argv[1]
access = "solariq-local-" + secrets.token_hex(4)
secret = secrets.token_urlsafe(24)
path = pathlib.Path(".env")
text = path.read_text(encoding="utf-8")
replacements = {
    "POSTGRES_PASSWORD": password,
    "MINIO_ACCESS_KEY": access,
    "MINIO_SECRET_KEY": secret,
    "DATABASE_URL": f"postgresql://solariq:{password}@postgres:5432/solariq",
}
for key, value in replacements.items():
    text = re.sub(rf"(?m)^{key}=.*$", f"{key}={value}", text)
path.write_text(text, encoding="utf-8")
PY
    echo "  Generated .env with fresh local credentials (gitignored)"
  else
    fail "Python is required to generate .env. Install Python 3, or copy .env.example to .env and fill in the blanks by hand."
  fi
fi

mkdir -p data/daily
echo "  data/daily ready (daily reference feed lands here)"

# ---------------------------------------------------------------------------
step 3 "Starting infrastructure"
# ---------------------------------------------------------------------------
docker compose up -d
echo "  Containers requested"

# ---------------------------------------------------------------------------
step 4 "Waiting for health checks"
# ---------------------------------------------------------------------------
# Polling the healthchecks rather than sleeping a fixed time: Kafka needs ~15s
# on a warm start and considerably longer on a cold one, and a fixed sleep is
# either too short (flaky) or too long (annoying) at any value.
wait_healthy() {
  local service="$1" container="$2" attempts=40
  for ((i = 1; i <= attempts; i++)); do
    local status
    status="$(docker inspect --format '{{.State.Health.Status}}' "${container}" 2>/dev/null || echo missing)"
    case "${status}" in
      healthy) echo "  ${service} is healthy"; return 0 ;;
      unhealthy) fail "${service} reported unhealthy. Inspect with: docker compose logs ${service}" ;;
    esac
    sleep 3
  done
  fail "${service} did not become healthy within 2 minutes. Inspect with: docker compose logs ${service}"
}

wait_healthy kafka solariq-kafka
wait_healthy postgres solariq-postgres
wait_healthy minio solariq-minio
wait_healthy prometheus solariq-prometheus

# ---------------------------------------------------------------------------
step 5 "Creating Kafka topics"
# ---------------------------------------------------------------------------
# Idempotent, and required: auto-creation is disabled on the broker, so without
# this the topics simply would not exist.
bash kafka/scripts/create_topics.sh

# ---------------------------------------------------------------------------
cat <<'NEXT'

SolarIQ infrastructure is up.

  Kafka broker      localhost:29092   (kafka:9092 inside Compose)
  PostgreSQL        localhost:5432    database 'solariq', user 'solariq'
  MinIO API         localhost:9000    console at localhost:9001
  Prometheus        localhost:9090    alerts at /alerts

Next:

  scripts/demo_start.sh          run the scripted assessment demo
  kafka/scripts/verify_topics.sh --consume 5
                                 confirm telemetry is flowing
  docker compose ps              check container health
NEXT
