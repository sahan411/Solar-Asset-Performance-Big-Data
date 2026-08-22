# SolarIQ — Claude Code Execution Playbook

> **Important:** This file is an execution prompt. Open the SolarIQ repository in Claude Code, then paste/provide this document as the implementation instruction for the assigned member.
>
> Claude Code must treat `SolarIQ_Master_Project_Specification.md` as the source of truth for architecture, contracts, scope, names, and Definition of Done. If this playbook and the master specification appear to conflict, **stop and report the conflict before changing a shared contract**.

## Non-Negotiable Project Context

SolarIQ is a two-week Applied Big Data Engineering mini-project implementing a **Lambda Architecture** for simulated commercial solar portfolio monitoring.

The system must demonstrate:

- a continuous Python streaming source,
- a daily Python batch source,
- Apache Kafka,
- Apache Spark Structured Streaming,
- Apache Airflow,
- a queryable storage layer,
- meaningful transformations including validation, joins, aggregation/windowing and reconciliation,
- a consolidated dashboard/report,
- structured logging,
- at least one health-check/alert,
- reproducible local execution through Docker Compose,
- tests and documentation,
- a deterministic 5–10 minute assessment demo.

The Phase 1 implementation uses **simulated data intentionally**. Do not add real inverter, SCADA, meter, weather, authentication, multi-tenancy, AI/ML, work-order, or cloud-production features unless all required work is complete and the team explicitly approves the scope change.

## Required Engineering Behaviour for Claude Code

Claude Code must follow this workflow for **every milestone**:

1. Inspect the current repository state before editing anything.
2. Read:
   - `SolarIQ_Master_Project_Specification.md`
   - `README.md`
   - `docs/architecture.md` if present
   - `docs/data-contracts.md` if present
   - `contracts/*`
   - any files in the subsystem being modified.
3. Run `git status`, inspect the current branch, and identify uncommitted work.
4. Never discard or overwrite another member's uncommitted work.
5. State a short implementation plan for the current milestone before writing code.
6. Implement **only that milestone**.
7. Add/update automated tests for the milestone.
8. Run the smallest relevant tests first, then broader smoke tests if available.
9. Inspect the diff.
10. Update documentation/configuration examples when behavior changes.
11. Commit the completed logical step using a clear Conventional Commit message.
12. Only then proceed to the next milestone.

### Stop Conditions

Claude Code must **stop and report a blocker instead of guessing** if any of the following occurs:

- shared field names differ from `contracts/`,
- another member has changed a shared schema incompatibly,
- required infrastructure is missing and ownership is unclear,
- a test failure appears to originate in another member's subsystem,
- a requested change would require changing another member's public contract,
- a secret/credential would need to be hard-coded,
- Docker/service versions are incompatible and a coordinated change is required,
- a requirement cannot be implemented without violating the master specification.

### Git Rules

- Work on the assigned branch only unless the human explicitly tells you otherwise.
- Commit after each completed logical step.
- Never use vague commit messages.
- Never combine unrelated refactors and features.
- Do not use force push.
- Do not rewrite another member's history.
- Do not commit `.env`, secrets, local DB files, Docker volumes, generated dependency folders, build output, or large logs.
- Before each commit:
  - run relevant tests,
  - run formatter/linter where configured,
  - check `git diff --check`,
  - review `git diff`,
  - check `git status`.

### Coding Standards

- Prefer simple, explicit, understandable code.
- Avoid unnecessary abstractions.
- Use UTC internally.
- Use environment variables for all external service addresses/credentials.
- Use typed Python where practical.
- Validate data at system boundaries.
- Never silently swallow exceptions.
- Emit structured logs with stable event names.
- Make retry behavior bounded and explicit.
- Make side-effecting operations idempotent where practical.
- Use deterministic seeded data in tests.
- Never hard-code final demo metrics in the API or dashboard.
- All final displayed values must come from the running pipeline.
- Every important calculation must be explainable in a viva.

## Frozen Shared Contracts

Unless the team explicitly versions a change, assume the following are fixed:

### Kafka topics

```text
solar.telemetry.raw
solar.telemetry.invalid
solar.alerts
```

### Kafka key

```text
plant_id:inverter_id
```

### Streaming telemetry event

```json
{
  "event_id": "uuid-string",
  "plant_id": "PLANT_01",
  "inverter_id": "INV_01",
  "active_power_kw": 422.7,
  "energy_today_kwh": 2150.2,
  "irradiance_wm2": 782.4,
  "module_temp_c": 47.3,
  "inverter_temp_c": 51.0,
  "status": "ONLINE",
  "availability": 1.0,
  "timestamp": "2026-08-21T05:00:00Z",
  "simulator_scenario": null
}
```

Allowed `status` values:

```text
ONLINE
OFFLINE
WARNING
```

### Daily reference feed

```text
simulation_date
plant_id
plant_capacity_kw
expected_generation_kwh
expected_peak_power_kw
forecast_irradiance_kwh_m2
ppa_rate_per_kwh
maintenance_flag
source_version
```

### Core PostgreSQL tables

```text
plants
inverters
live_plant_metrics
live_portfolio_metrics
alerts
daily_reference
daily_plant_summary
pipeline_health
```

### Core API paths

```text
GET /health
GET /ready
GET /metrics

GET /api/v1/portfolio/live
GET /api/v1/portfolio/daily?date=

GET /api/v1/plants
GET /api/v1/plants/{plant_id}/live
GET /api/v1/plants/{plant_id}/history?from=&to=
GET /api/v1/plants/{plant_id}/inverters

GET /api/v1/alerts
GET /api/v1/alerts?status=active

GET /api/v1/reports/daily?date=
```

## Shared Demo Clock

Default:

```text
1 simulated day = 5 real minutes
stream interval = 3 seconds
```

Both must be configurable via environment variables.

## Shared Definition of Success

At final integration, the following path must work using real runtime data:

```text
Python Simulator
    -> Kafka
    -> Spark Structured Streaming
    -> PostgreSQL live metrics
    -> FastAPI
    -> React Dashboard
```

and:

```text
Spark normalized raw events
    -> MinIO/Parquet
    -> Airflow daily batch
    -> PostgreSQL daily summary
    -> FastAPI
    -> Daily Report UI
```

The demo must also show:

- an injected solar underperformance event,
- an inverter offline or telemetry-loss event,
- an operational/business alert,
- an engineering/pipeline-health alert,
- a completed simulated-day batch reconciliation,
- expected vs actual generation,
- lost energy,
- estimated lost revenue,
- logs/metrics/health.


# Member 2 Execution Prompt
## Spark Streaming, Batch Layer, Airflow & Storage

**Assigned branch:** `member-2/data-processing`

## 1. Mission

You own the **core data engineering implementation**.

Your subsystem must convert incoming telemetry into useful real-time metrics, preserve a replayable raw history, and produce authoritative daily reconciliation using Airflow.

You are responsible for making the project genuinely demonstrate a Lambda architecture rather than merely drawing one in a diagram.

Your output contracts must be stable enough for Member 3's API/dashboard.

You do **not** own the telemetry simulator, Kafka producer, user interface, or business API presentation layer.

---

# 2. Files/Areas You Own

Primary ownership:

```text
processing/
  streaming/
  batch/
  common/

orchestration/
  dags/

storage/
  migrations/
  sql/
  seeds/

tests/
  processing/
  batch/
  storage/
```

Shared with care:

```text
contracts/
docker-compose.yml
.env.example
docs/data-contracts.md
docs/architecture.md
```

Do not redesign:

```text
simulators/
kafka/
dashboard/
```

Do not change API contracts without coordination with Member 3.

---

# 3. Milestone 0 — Inspect Repository & Verify Input Contract

Before coding:

```bash
git status
git branch --show-current
git log --oneline -10
```

Read:

```text
SolarIQ_Master_Project_Specification.md
contracts/*
simulators/*
kafka/*
docker-compose.yml
.env.example
```

Verify that a sample telemetry event matches the frozen contract.

If Member 1's implementation differs, **stop and report it** rather than making Spark silently compensate for undocumented schema drift.

## Commit

No commit unless fixes are needed.

---

# 4. Milestone 1 — PostgreSQL Schema & Migrations

## Goal

Create the serving schema before stream/batch writers.

Use a migration mechanism appropriate to the project's simplicity. Alembic is acceptable, or ordered SQL migration files if the team prefers lower complexity.

Do not let Spark/FastAPI auto-create production tables.

## Required Tables

### `plants`

Recommended SQL shape:

```sql
CREATE TABLE plants (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    capacity_kw DOUBLE PRECISION NOT NULL CHECK (capacity_kw > 0),
    timezone TEXT NOT NULL DEFAULT 'UTC',
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

### `inverters`

```sql
CREATE TABLE inverters (
    id TEXT NOT NULL,
    plant_id TEXT NOT NULL REFERENCES plants(id),
    name TEXT NOT NULL,
    rated_power_kw DOUBLE PRECISION NOT NULL CHECK (rated_power_kw > 0),
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (plant_id, id)
);
```

### `live_plant_metrics`

Use `(plant_id, window_start, window_end)` or another documented unique key.

Include:

```text
plant_id
window_start
window_end
current_power_kw
avg_power_kw
availability_pct
performance_pct
estimated_loss_kw
updated_at
```

### `live_portfolio_metrics`

Include:

```text
window_start
window_end
current_power_kw
avg_power_kw
online_inverters
offline_inverters
availability_pct
performance_pct
updated_at
```

### `alerts`

Use a UUID/string primary key.

Include:

```text
id
plant_id
inverter_id nullable
alert_type
severity
message
started_at
ended_at nullable
status
estimated_loss_kwh nullable
estimated_revenue_loss nullable
created_at
```

### `daily_reference`

Use composite key:

```text
(simulation_date, plant_id)
```

### `daily_plant_summary`

Use composite key:

```text
(simulation_date, plant_id)
```

### `pipeline_health`

Use component primary key or another explicit unique key.

## Indexes

Add indexes for:

```text
alerts(status, started_at)
daily_plant_summary(simulation_date)
live_plant_metrics(plant_id, window_end DESC)
```

Do not add many speculative indexes.

## Seed Data

Seed plants/inverters from the shared portfolio config in an idempotent manner.

## Tests

- migration on empty DB,
- unique constraints,
- required FK relationships,
- idempotent seed behavior.

## Commit

```text
feat(storage): add SolarIQ serving schema and migrations
```

---

# 5. Milestone 2 — Spark Structured Streaming Skeleton

## Goal

Prove reliable Kafka -> Spark parsing before business logic.

Suggested file layout:

```text
processing/streaming/job.py
processing/streaming/schema.py
processing/streaming/config.py
processing/streaming/sinks.py
processing/streaming/transforms.py
```

## Spark Kafka Read

Reference pattern:

```python
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
    .option("subscribe", settings.telemetry_topic)
    .option("startingOffsets", settings.starting_offsets)
    .load()
)
```

Do not hard-code `latest`/`earliest`; make it configured, with a documented default suitable for demo.

Parse:

```python
from_json(col("value").cast("string"), telemetry_schema)
```

Keep Kafka metadata such as:

```text
topic
partition
offset
kafka_timestamp
```

if useful for debugging.

## Checkpoint Directory

Use a persistent configurable checkpoint path.

Never omit Structured Streaming checkpoints for production-like behavior.

Example:

```text
/spark-checkpoints/telemetry-main
```

## Commit

```text
feat(stream): consume and parse Kafka solar telemetry
```

---

# 6. Milestone 3 — Validation, Quarantine, Deduplication & Watermark

## Goal

Make stream processing robust.

## Validation

Reject/quarantine records when:

- JSON cannot parse,
- required IDs missing,
- active power < 0,
- irradiance < 0,
- availability outside `[0, 1]`,
- unsupported status,
- timestamp invalid.

Do not silently drop without a metric/log.

If writing invalid records back to Kafka is operationally difficult in Spark for this mini-project, a clearly documented invalid Parquet/DB sink is acceptable **only if it remains consistent with the team contract**. Prefer the existing `solar.telemetry.invalid` topic if practical.

## Deduplication

Use `event_id`.

Reference pattern:

```python
events = (
    valid_events
    .withWatermark("event_time", "2 minutes")
    .dropDuplicates(["event_id"])
)
```

The exact watermark can be configured.

## Event Time

Convert `timestamp` into Spark `TimestampType` and use it for windows.

Do not use processing time for all business windows.

## Metrics

Expose counters/metrics for:

```text
processed events
invalid events
latest processed event timestamp
```

Coordinate metric naming with Member 1.

## Tests

Use Spark local mode and small DataFrames to verify:

- invalid rows excluded,
- duplicates removed,
- timestamp parsing,
- late data behavior at a basic level.

## Commit

```text
feat(stream): validate deduplicate and watermark telemetry
```

---

# 7. Milestone 4 — Raw Normalized Parquet/MinIO Sink

## Goal

Implement the replayable historical/raw layer required for a defensible Lambda architecture.

Write **normalized valid telemetry** to Parquet.

Partition by:

```text
simulation_date
plant_id
```

Preferred logical path:

```text
s3a://solariq-raw/telemetry/
```

Spark must be configured for MinIO's S3-compatible endpoint.

Required environment/config:

```text
MINIO_ENDPOINT=http://minio:9000
MINIO_ACCESS_KEY=...
MINIO_SECRET_KEY=...
MINIO_RAW_BUCKET=solariq-raw
```

Credentials may have safe **local demo defaults** in Docker Compose only if clearly non-production and documented; `.env.example` should show configuration, and no real secret is committed.

## Important MinIO/S3A Considerations

Claude Code must ensure compatible Hadoop AWS dependencies exist for the chosen Spark image/version.

Do not guess random jar versions.

Inspect the Spark/Hadoop version first and select compatible artifacts. If dependency compatibility is unclear, stop and report it.

Typical Spark config pattern:

```python
hconf = spark.sparkContext._jsc.hadoopConfiguration()
hconf.set("fs.s3a.endpoint", settings.minio_endpoint)
hconf.set("fs.s3a.access.key", settings.minio_access_key)
hconf.set("fs.s3a.secret.key", settings.minio_secret_key)
hconf.set("fs.s3a.path.style.access", "true")
hconf.set("fs.s3a.connection.ssl.enabled", "false")
```

Write:

```python
(
    events
    .withColumn("simulation_date", to_date(col("event_time")))
    .writeStream
    .format("parquet")
    .option("path", raw_path)
    .option("checkpointLocation", raw_checkpoint)
    .partitionBy("simulation_date", "plant_id")
    .outputMode("append")
    .start()
)
```

Use a **separate checkpoint** from other sinks.

## Tests

At least an integration/diagnostic test proving Parquet appears in MinIO/local S3-compatible storage.

## Commit

```text
feat(lambda): archive normalized telemetry to Parquet
```

---

# 8. Milestone 5 — Real-Time Plant Window Metrics

## Goal

Calculate useful live metrics using event-time windows.

Recommended window:

```text
1 minute
```

with configurable slide:

```text
15 seconds
```

For demo speed, these can be shorter if documented.

## Plant-Level Aggregation

Group by:

```text
window(event_time, ...)
plant_id
```

Calculate:

```text
current_power_kw       # use max_by/last semantics carefully; or most recent value via foreachBatch
avg_power_kw
avg_irradiance_wm2
availability_pct
online_inverter count
offline inverter count
```

### Important: `current_power_kw`

A window `sum(active_power_kw)` across multiple events is **not current power**; it would overcount repeated samples.

Correct approaches include:

1. calculate latest event per inverter inside each microbatch, then sum latest inverter powers, or
2. use a narrowly defined most-recent aggregation and document it.

For simplicity and correctness in a two-week project, use `foreachBatch`:

- within each microbatch,
- select latest row per `(plant_id, inverter_id)` using timestamp,
- sum current inverter power by plant,
- combine with window-based averages as needed.

Do not represent sample sums as instantaneous power.

## Availability

A simple live availability measure can be:

```text
online inverter observations / total inverter observations * 100
```

or preferably:

```text
currently online configured inverters / configured inverters * 100
```

Document exactly which definition is used.

## Expected Power Proxy

For Phase 1, use an explainable irradiance-normalized expected power approximation:

```python
expected_power_kw =
    plant_capacity_kw
    * min(max(irradiance_wm2 / 1000.0, 0.0), 1.0)
    * temperature_factor
```

A simpler acceptable version:

```python
expected_power_kw =
    plant_capacity_kw * min(irradiance_wm2 / 1000.0, 1.0)
```

Clearly label it a simplified expected-power proxy, not a bankable PV model.

Then:

```python
performance_pct = (
    actual_power_kw / expected_power_kw * 100
    if expected_power_kw >= minimum_expected_power_threshold
    else None
)
```

Cap display values carefully if needed, but do not hide legitimate >100% simulation noise unless documented.

## Estimated Loss kW

```python
estimated_loss_kw = max(expected_power_kw - actual_power_kw, 0.0)
```

## Commit

```text
feat(stream): calculate live plant performance metrics
```

---

# 9. Milestone 6 — Portfolio Metrics

Aggregate current plant metrics into:

```text
current_power_kw
avg_power_kw
online_inverters
offline_inverters
availability_pct
performance_pct
```

Do not average plant performance percentages naively if plant sizes differ.

Prefer weighted portfolio performance:

```text
sum(actual_power_kw) / sum(expected_power_kw) * 100
```

when expected power is valid.

Persist into `live_portfolio_metrics`.

## Commit

```text
feat(stream): aggregate live portfolio metrics
```

---

# 10. Milestone 7 — Robust PostgreSQL Sink

## Goal

Write stream results idempotently.

Avoid opening a new DB connection per row.

Recommended pattern:

```text
writeStream.foreachBatch(...)
```

Inside each microbatch:

1. convert only aggregated rows to manageable Python/DB batch,
2. use batch insert/upsert,
3. write inside transaction,
4. close connection cleanly.

Because this demo has small aggregate volume, `foreachBatch` + psycopg/SQLAlchemy is acceptable.

## Upsert Pattern

For `live_plant_metrics`:

```sql
INSERT INTO live_plant_metrics (...)
VALUES (...)
ON CONFLICT (plant_id, window_start, window_end)
DO UPDATE SET
    current_power_kw = EXCLUDED.current_power_kw,
    ...
    updated_at = NOW();
```

Do not use string-formatted SQL with untrusted values.

## Tests

- repeat same microbatch -> no duplicate logical rows,
- DB failure produces clear error,
- transaction rolls back on failure.

## Commit

```text
feat(stream): persist idempotent live metrics to PostgreSQL
```

---

# 11. Milestone 8 — Business Underperformance Alert

## Goal

Generate an explainable solar operational alert.

Rule:

```text
IF expected_power_kw >= minimum_expected_power_kw
AND performance_pct < 80
FOR a sustained period
THEN create an UNDERPERFORMANCE alert
```

For compressed demo time, use a shorter sustained period such as:

```text
30–45 real seconds
```

but explain that production thresholds would be much longer.

## Avoid Alert Spam

Do not insert a new alert every microbatch.

Maintain/open a single active alert for the same:

```text
plant_id + inverter_id/plant + alert_type
```

Then close it when recovered.

Status values:

```text
ACTIVE
RESOLVED
```

Severity:

```text
WARNING
CRITICAL
```

## Offline Alert

Also create a simple inverter/plant offline alert if `status=OFFLINE` persists or appropriate current state indicates it.

## Commit

```text
feat(alerts): detect sustained solar underperformance
```

---

# 12. Milestone 9 — Pipeline Health State

Update `pipeline_health` with at least:

```text
component = spark-stream
status
last_event_at
last_success_at
message
updated_at
```

Expose a Prometheus-compatible metric if possible:

```text
solariq_stream_last_processed_timestamp_seconds
solariq_events_processed_total
solariq_events_invalid_total
```

Coordinate with Member 1 so Prometheus can alert on **processed** telemetry staleness.

This is stronger than only checking producer output.

## Commit

```text
feat(observability): expose stream-processing health
```

---

# 13. Milestone 10 — Daily Reference Ingestion

## Goal

Build batch code that validates and inserts the daily reference feed idempotently.

Suggested:

```text
processing/batch/reference.py
```

Validation:

- required columns exact,
- one row per configured plant,
- duplicate `(simulation_date, plant_id)` rejected,
- expected generation > 0,
- peak power > 0 and reasonable vs plant capacity,
- rate >= 0,
- valid maintenance flag.

Write to `daily_reference` using upsert or reject duplicate version clearly.

Use `source_version`.

## Commit

```text
feat(batch): validate and load daily reference data
```

---

# 14. Milestone 11 — Daily Actual Aggregation from Raw Parquet

## Goal

Read the full simulated day's normalized raw history from MinIO/Parquet.

Do **not** derive the authoritative daily report solely from current/live tables.

For each plant calculate:

### Actual Generation

Preferred:

```text
max(energy_today_kwh) - min(energy_today_kwh)
```

for each inverter, then sum, if the cumulative meter begins near zero.

However, if the simulated day starts exactly at reset, simply using max cumulative energy per inverter can be valid.

To protect against resets or gaps, document the chosen method and test it.

Alternative:

integrate average power over event intervals, but that is more complex.

Given our simulator owns a monotonic `energy_today_kwh`, the recommended academic solution is:

```text
daily inverter energy = max(energy_today_kwh)
daily plant actual = sum(daily inverter energy)
```

provided the simulator resets at day boundary and the report states this assumption.

### Availability

Calculate based on observations/status:

```text
available observations / total expected/received observations
```

or another documented method.

A more robust demo measure:

```text
ONLINE observations / all received observations * 100
```

This does not capture missing telemetry as downtime unless separately incorporated. Document limitation.

### Downtime

Count or estimate intervals where inverter status is OFFLINE.

Use event interval assumptions from the simulator.

## Commit

```text
feat(batch): aggregate authoritative daily solar actuals
```

---

# 15. Milestone 12 — Expected vs Actual Reconciliation

Join daily actuals with `daily_reference` by:

```text
simulation_date
plant_id
```

Calculate:

```python
performance_pct = actual_generation_kwh / expected_generation_kwh * 100
estimated_lost_energy_kwh = max(
    expected_generation_kwh - actual_generation_kwh,
    0
)
estimated_actual_revenue = actual_generation_kwh * ppa_rate_per_kwh
estimated_lost_revenue = estimated_lost_energy_kwh * ppa_rate_per_kwh
```

### Maintenance Flag

If `maintenance_flag=true`, do not silently exclude the plant.

Keep it in the report and optionally mark performance as maintenance-affected.

## Data Quality Checks

Fail or flag when:

- actual generation < 0,
- expected generation <= 0,
- duplicate summary key,
- reference row missing,
- unknown plant,
- unreasonable extreme values.

## Persist

Upsert `daily_plant_summary`.

## Tests

Use a tiny deterministic fixture with hand-calculated expected answers.

Example:

```text
expected = 1000 kWh
actual = 800 kWh
rate = 0.15

performance = 80%
lost energy = 200 kWh
actual revenue = 120
lost revenue = 30
```

Use Decimal for money if the project adopts precise monetary representation; otherwise document float usage as a demo simplification.

## Commit

```text
feat(batch): reconcile daily performance and revenue impact
```

---

# 16. Milestone 13 — Airflow DAG

## Goal

Orchestrate the daily batch pipeline.

Recommended DAG ID:

```text
solariq_daily_reconciliation
```

Task graph:

```text
wait_for_reference_file
        |
validate_reference_feed
        |
check_raw_daily_data
        |
load_reference
        |
compute_daily_actuals
        |
reconcile_expected_actual
        |
write_daily_summary
        |
run_data_quality_checks
```

If report file generation is owned by Member 3, stop at summary/data-quality and expose a completion dependency or task hook.

## Airflow Principles

- DAG parse must not perform business processing.
- Business functions belong in `processing/batch/`.
- Airflow tasks call tested Python functions.
- Avoid giant inline Python callables inside the DAG.
- Retries should be small and explicit.
- Use a clearly configured schedule or manual trigger in demo mode.
- Demo should support running the DAG immediately when the daily file appears.

### File Sensor

If using `FileSensor`, ensure the path exists inside the Airflow container and is a shared mounted volume.

Do not reference a host-only path.

### Idempotency

Rerunning the DAG for the same date must not create duplicate summaries.

## Commit

```text
feat(airflow): orchestrate daily solar reconciliation
```

---

# 17. Milestone 14 — Batch/Streaming Consistency Documentation

Create/update:

```text
docs/architecture.md
```

Clearly state:

### Speed layer
Near-real-time, potentially approximate/current.

### Batch layer
End-of-day, full-day authoritative reconciliation.

Explain that the same business ideas may appear in both layers but calculations differ:

- live expected-power proxy,
- final daily expected-generation reference.

This prevents the viva question:

> “Why are the live and daily performance numbers calculated differently?”

Answer:

- live path optimizes latency with currently available telemetry,
- batch path uses complete daily actuals and authoritative daily reference data.

## Commit

```text
docs(lambda): explain speed and batch reconciliation semantics
```

---

# 18. Milestone 15 — Member 2 Tests

Required unit tests:

```text
stream schema parsing
invalid-event handling
deduplication
watermark/event time basics
current-power logic
plant aggregation
weighted portfolio performance
underperformance rule
alert dedup/resolution
daily reference validation
daily actual aggregation
expected-actual join
lost energy
revenue
data quality checks
```

Integration tests:

```text
Kafka -> Spark
Spark -> PostgreSQL
Spark -> MinIO/Parquet
Airflow batch -> daily_plant_summary
```

Not every integration test must run on every unit-test invocation. Mark/containerize slow tests clearly.

## Final Verification

From a clean or controlled environment:

1. migrations run,
2. seed data loads,
3. Kafka receives Member 1 events,
4. Spark creates live rows,
5. raw Parquet appears,
6. alert is created on deterministic anomaly,
7. daily reference arrives,
8. Airflow completes,
9. `daily_plant_summary` contains expected values.

## Final Commits

```text
test(processing): cover stream and batch business logic
```

and:

```text
docs(processing): document Lambda calculations and data quality
```

---

# 19. Reference SQL Queries for Manual Verification

Latest live plant metrics:

```sql
SELECT DISTINCT ON (plant_id)
    plant_id,
    window_end,
    current_power_kw,
    availability_pct,
    performance_pct,
    estimated_loss_kw
FROM live_plant_metrics
ORDER BY plant_id, window_end DESC;
```

Active alerts:

```sql
SELECT
    id,
    plant_id,
    inverter_id,
    alert_type,
    severity,
    started_at,
    status
FROM alerts
WHERE status = 'ACTIVE'
ORDER BY started_at DESC;
```

Daily summary:

```sql
SELECT
    simulation_date,
    plant_id,
    actual_generation_kwh,
    expected_generation_kwh,
    performance_pct,
    estimated_lost_energy_kwh,
    estimated_actual_revenue,
    estimated_lost_revenue
FROM daily_plant_summary
ORDER BY simulation_date DESC, plant_id;
```

---

# 20. Member 2 Handoff Contract

Provide Member 3 with:

- exact DB connection environment variables,
- migrations command,
- table definitions,
- which columns are nullable,
- latest-live query semantics,
- alert statuses/severities,
- daily summary semantics,
- unit of every numerical field,
- sample SQL queries,
- sample rows,
- known limitations.

Do not force Member 3 to reverse-engineer the database.

---

# 21. Member 2 Definition of Done

- [ ] migrations create all required tables,
- [ ] seeds are idempotent,
- [ ] Spark consumes Kafka,
- [ ] event schema is validated,
- [ ] invalid events are observable/quarantined,
- [ ] duplicates are handled,
- [ ] event-time watermark is used,
- [ ] normalized raw data is archived to Parquet/MinIO,
- [ ] live current power is not incorrectly calculated by summing repeated samples,
- [ ] live plant metrics are calculated,
- [ ] live portfolio metrics are calculated,
- [ ] performance formula is documented,
- [ ] underperformance alert is sustained and deduplicated,
- [ ] alert recovery works,
- [ ] stream results write idempotently to PostgreSQL,
- [ ] stream health timestamp/metric exists,
- [ ] daily reference is validated,
- [ ] daily actuals come from raw historical data,
- [ ] expected vs actual join works,
- [ ] lost energy is calculated,
- [ ] actual and lost revenue are calculated,
- [ ] daily summary is persisted idempotently,
- [ ] Airflow DAG executes end-to-end,
- [ ] data-quality checks exist,
- [ ] unit tests pass,
- [ ] integration path is verified,
- [ ] Member 3 has documented serving-table semantics,
- [ ] every core calculation can be explained in a viva.

**Once complete, prioritize integration reliability over extra analytics.**
