# SolarIQ — Real-Time Solar Asset Performance & Revenue Intelligence

Applied Big Data Engineering mini-project implementing a Lambda architecture for
simulated commercial solar portfolio monitoring (Kafka + Spark Structured Streaming
for the speed layer, Airflow + Parquet/MinIO for the batch layer, PostgreSQL serving
store, FastAPI + React dashboard).

## Project docs

- [`SolarIQ_Master_Project_Specification.md`](SolarIQ_Master_Project_Specification.md) — architecture, contracts, scope, delivery plan. Source of truth for the whole team.
- [`SolarIQ_Member_1_Claude_Code_Playbook.md`](SolarIQ_Member_1_Claude_Code_Playbook.md) — Platform, Simulation, Kafka & Observability Foundation.
- [`SolarIQ_Member_2_Claude_Code_Playbook.md`](SolarIQ_Member_2_Claude_Code_Playbook.md) — Spark Streaming, Batch Layer, Airflow & Storage.
- [`SolarIQ_Member_3_Claude_Code_Playbook.md`](SolarIQ_Member_3_Claude_Code_Playbook.md) — API, Dashboard, Reporting & Demo Experience.
- [`docs/architecture.md`](docs/architecture.md) — why the speed and batch layers compute different (both correct) numbers.
- [`docs/data-contracts.md`](docs/data-contracts.md) — the frozen interfaces between subsystems.
- [`docs/member-2-handoff.md`](docs/member-2-handoff.md) — everything the API reads from PostgreSQL, with units and nullability.
- [`docs/demo-runbook.md`](docs/demo-runbook.md) — the exact, deterministic assessment demo script.

## Architecture

```text
Python Simulator -> Kafka -> Spark Structured Streaming -> PostgreSQL (live)
                                                                |
                                                                v
                                                         FastAPI -> React Dashboard

Spark (normalized raw events) -> MinIO/Parquet -> Airflow daily batch
                                                       -> PostgreSQL (daily) -> FastAPI -> Daily Report UI
```

The speed layer answers "what's happening right now?" from whatever telemetry
has arrived; the batch layer answers "what actually happened yesterday?" from
the complete, immutable day. They legitimately disagree — see
[`docs/architecture.md`](docs/architecture.md).

## Prerequisites

- Docker Desktop (Compose v2)
- Python 3.11 (3.10–3.12 also work; avoid pre-release interpreters — some
  dependencies below ship no prebuilt wheel for them)
- Node.js 20+ and npm, only if developing the dashboard outside Docker
- ~4 GB free RAM for the full stack (Kafka + Spark + Airflow + Postgres + MinIO)

## Environment setup

```bash
cp .env.example .env
```

`.env` is gitignored; `scripts/bootstrap.sh` generates local credentials into
it automatically if you skip this step (see below). Never commit `.env`.

## How to start

```bash
./scripts/bootstrap.sh
```

Starts every Compose service — Kafka, PostgreSQL, MinIO, Prometheus, Airflow,
the serving `api`, and the `dashboard` — and waits for health checks.
Airflow's own startup applies the PostgreSQL migrations and seeds the plant
registry, so the serving store is ready by the time this finishes.

The Spark streaming job and the telemetry simulator both run on the **host**
(not as Compose services), so their logs are visible directly:

```bash
# terminal 1 — speed layer
KAFKA_BOOTSTRAP_SERVERS=localhost:29092 \
DATABASE_URL=postgresql://solariq:<password from .env>@localhost:5432/solariq \
MINIO_ENDPOINT=http://localhost:9000 \
SPARK_CHECKPOINT_DIR=./data/spark-checkpoints \
python -m processing.streaming.job --master 'local[2]'

# terminal 2 — simulator
./scripts/demo_start.sh
```

Full walkthrough, including the daily reconciliation trigger and the scripted
anomaly timeline: [`docs/demo-runbook.md`](docs/demo-runbook.md).

### URLs

| Service | URL |
|---|---|
| Dashboard | http://localhost:5173 |
| API (OpenAPI docs) | http://localhost:8000/docs |
| API health / ready / metrics | http://localhost:8000/health, `/ready`, `/metrics` |
| Prometheus | http://localhost:9090 |
| Airflow | http://localhost:8080 (`admin` / `admin`) |
| MinIO console | http://localhost:9001 |

The dashboard's JavaScript runs in your browser, not inside Docker — it calls
the API at `http://localhost:8000`, never the Compose service name `api`.

## How to reset the demo

```bash
./scripts/demo_reset.sh --all --yes
```

Deletes and recreates the Kafka topics, clears the daily reference feed, and
(`--all`) truncates PostgreSQL and empties the MinIO archive — never touches
source code, configuration, or `.env`. Because the simulator is seeded
(`SIMULATION_SEED=8203`), a reset-and-rerun reproduces the identical sequence
of events. Stop and restart the Spark job after a reset (it deletes/recreates
the Kafka topics your running consumer refers to) — see
[`docs/demo-runbook.md`](docs/demo-runbook.md) section 5.

## How to run tests

```bash
# Python (processing, storage, simulators, api) — from the repository root
python -m pytest

# API-only
python -m pytest api/tests

# Integration tests need a throwaway PostgreSQL (never point at real data):
docker run --rm -d --name solariq-test-pg -p 55432:5432 \
  -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=solariq_test postgres:16-alpine
SOLARIQ_TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:55432/solariq_test \
  python -m pytest -m integration

# Dashboard
cd dashboard && npm run test && npm run lint && npm run build

# Full-stack smoke test, against a running deployment
pip install -r tests/e2e/requirements.txt
python tests/e2e/smoke_test.py
```

## How simulated time works

```text
SIMULATION_DAY_SECONDS=300      1 simulated day = 5 real minutes (288x)
TELEMETRY_INTERVAL_SECONDS=3    one tick per inverter every 3 real seconds
SIMULATION_SEED=8203            fixed seed — every run is identical
SIMULATION_START_DATE=2026-08-21
```

Every timestamp inside the pipeline is **simulated event time**, not wall
clock — a simulated day still spans a full 24 hours of event time, which is
what lets hour-scale windows (like alert sustain duration) mean something
inside a five-minute demo. All four variables are environment-configurable;
nothing downstream hard-codes them.

## How to trigger/observe anomalies

The anomaly schedule is deterministic and repeats every simulated day — see
[`docs/data-contracts.md`](docs/data-contracts.md) section 7 for the exact
windows. Nothing needs to be manually triggered; the simulator injects them
on schedule. Watch them land:

- **Business alerts:** `/alerts` on the dashboard, or `GET
  /api/v1/alerts?status=active`.
- **Pipeline health:** `SELECT * FROM pipeline_health;`, or the Prometheus
  alert rules at http://localhost:9090/alerts.

`INVERTER_OFFLINE` (a reported zero) and `TELEMETRY_GAP` (silence — no events
at all) are deliberately different faults with different owners: the first is
an operational alert, the second an engineering one.

## How to run daily reconciliation

The Airflow DAG's schedule is intentionally `None` — a wall-clock cron
schedule means nothing under a 288x-compressed clock, so it takes
`simulation_date` as a run parameter instead:

```bash
docker compose exec airflow-webserver airflow dags trigger \
  solariq_daily_reconciliation \
  --conf '{"simulation_date": "2026-08-21"}'
```

Or trigger it from the Airflow UI ("Trigger DAG w/ config"). Once it
completes, `GET /api/v1/reports/daily?date=2026-08-21` and the dashboard's
`/reports/daily` page both render the reconciled report.

## Project structure

```text
simulators/       Member 1 — Python telemetry + daily-reference generators
kafka/             Member 1 — topic setup, verification scripts
observability/     Member 1 — Prometheus config and alert rules

processing/        Member 2 — Spark Structured Streaming + Airflow batch tasks
orchestration/      Member 2 — Airflow DAGs, Docker image
storage/            Member 2 — SQL migrations, portfolio seeding

api/               Member 3 — FastAPI serving layer (typed models, repositories, routers)
dashboard/          Member 3 — React + TypeScript operations dashboard

tests/             batch/, processing/, simulators/, storage/ — per-owner unit +
                   integration tests; integration/ and e2e/ — Member 3, cross-subsystem
docs/              shared reference documentation
```

## Team contributions

- **Member 1 — Platform, Simulation, Kafka & Observability:** portfolio
  configuration, the streaming telemetry and daily-reference generators,
  Kafka topic management, Prometheus metrics/alert rules, `bootstrap.sh` /
  `demo_start.sh` / `demo_reset.sh`.
- **Member 2 — Processing, Orchestration & Storage:** Spark Structured
  Streaming (validation, dedup, watermarking, live metrics, alert detection),
  the Parquet raw archive, the Airflow daily-reconciliation DAG, and every
  PostgreSQL migration.
- **Member 3 — Serving API, Dashboard & Demo Experience:** the FastAPI
  serving layer (health/readiness/metrics, portfolio/plant/alert/report
  endpoints, a PostgreSQL repository layer), the React operations dashboard
  (live portfolio/plant views, alerts, daily reporting with CSV export),
  Docker integration for both, the end-to-end smoke test, and this demo
  runbook.

## Known limitations

- **Phase 1 uses simulated data throughout.** No real inverter, SCADA, meter,
  weather, authentication, multi-tenancy, or AI/ML integration — by design,
  not an oversight.
- **The expected-power proxy is not a bankable PV model** (no temperature,
  soiling, shading, or clipping terms) — see
  [`docs/architecture.md`](docs/architecture.md) section 7.
- **Money uses a fictional, simulated PPA rate**, stored as `DOUBLE
  PRECISION`; a real settlement system would use `NUMERIC`.
- **Live availability is sampled** at each microbatch, not a continuously
  measured uptime.
- **No per-inverter live metrics table** — the inverters endpoint serves
  configuration plus the plant's aggregate online/offline counts.
- **No authentication.** Explicitly out of scope for this phase.
