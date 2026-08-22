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


# Member 1 Execution Prompt
## Platform, Simulation, Kafka & Observability Foundation

**Assigned branch:** `member-1/platform-ingestion`

## 1. Mission

You own the reliable **entry point and platform foundation** for SolarIQ.

Your work must guarantee that the rest of the team receives:

1. deterministic, realistic solar telemetry,
2. a deterministic once-per-simulated-day reference feed,
3. correctly configured Kafka topics,
4. a reliable Kafka producer,
5. stable shared configuration,
6. Prometheus-compatible platform health signals,
7. a deterministic anomaly schedule suitable for the evaluation demo,
8. reproducible bootstrap/reset/demo scripts.

You do **not** own Spark transformation logic, Airflow reconciliation logic, FastAPI business endpoints, or the React dashboard.

Your job is complete when another developer can start the dependencies, run the simulators, consume valid events from Kafka, receive the daily reference feed, and reproduce the planned demo anomalies.

---

# 2. Files/Areas You Own

Primary ownership:

```text
simulators/
  streaming/
  batch/

kafka/
  config/
  scripts/

observability/
  prometheus/
  alert-rules/

scripts/
  bootstrap.sh
  demo_start.sh
  demo_reset.sh

contracts/
  # shared; edit only during approved contract setup/change

docker-compose.yml
  # platform-related services only; coordinate if other members also edit it

.env.example
  # shared; append documented variables without renaming others
```

Do not modify these without coordination except for integration fixes explicitly requested:

```text
processing/
orchestration/
storage/
api/
dashboard/
reports/
```

---

# 3. Milestone 0 — Repository Inspection & Contract Freeze

## Goal

Confirm that the repo is safe to work on and that shared contracts match the master specification.

## Actions

1. Run:

```bash
git status
git branch --show-current
git log --oneline -10
```

2. Confirm branch:

```text
member-1/platform-ingestion
```

3. Read all shared contracts.
4. Check whether `docker-compose.yml` already contains Kafka/Postgres/MinIO/etc.
5. Check whether another member has already committed files that must be preserved.
6. Create or complete `docs/data-contracts.md` only if the team has not already done so.
7. Ensure `.gitignore` includes at least:

```text
.env
.venv/
__pycache__/
.pytest_cache/
.mypy_cache/
.ruff_cache/
node_modules/
dist/
build/
*.log
.DS_Store
airflow/logs/
data/
tmp/
```

Do not ignore source fixtures required by tests.

## Acceptance Criteria

- branch is correct,
- no existing work is destroyed,
- contracts match the master specification,
- repo structure is clear,
- no secrets are committed.

## Commit

If changes were necessary:

```text
chore(repo): align shared contracts and ingestion structure
```

---

# 4. Milestone 1 — Shared Python Configuration & Logging

## Goal

Create small shared utilities for simulator/config/logging behavior without creating an over-engineered framework.

Recommended files:

```text
simulators/common/config.py
simulators/common/logging.py
simulators/common/time.py
```

## Required Environment Variables

Document sensible defaults in `.env.example`:

```text
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_TELEMETRY_TOPIC=solar.telemetry.raw
KAFKA_INVALID_TOPIC=solar.telemetry.invalid
KAFKA_ALERT_TOPIC=solar.alerts

SIMULATION_DAY_SECONDS=300
TELEMETRY_INTERVAL_SECONDS=3
SIMULATION_SEED=8203
SIMULATION_START_DATE=2026-08-21
SIMULATION_OUTPUT_DIR=/data/daily

PROMETHEUS_PORT=9101
NO_TELEMETRY_ALERT_SECONDS=60
```

If local host execution uses different addresses, support an override rather than duplicating code.

## Structured Logging

Use standard Python `logging`. JSON output can be implemented using a tiny formatter rather than pulling in an unnecessary framework.

Every log should support:

```text
timestamp
level
service
event
message
```

and optional:

```text
plant_id
inverter_id
event_id
error
```

Example logical log event:

```json
{
  "timestamp": "...",
  "level": "INFO",
  "service": "streaming-simulator",
  "event": "telemetry_published",
  "plant_id": "PLANT_01",
  "inverter_id": "INV_03",
  "message": "Published solar telemetry event"
}
```

## Acceptance Criteria

- env variables load safely,
- no secrets are hard-coded,
- structured logs are emitted,
- timestamps are UTC,
- unit tests cover config parsing and invalid values.

## Suggested Commit

```text
feat(platform): add simulator configuration and structured logging
```

---

# 5. Milestone 2 — Deterministic Portfolio Model

## Goal

Represent exactly 5 simulated solar plants and 5–10 inverters per plant.

Create an explicit fixture/config rather than scattering values throughout code.

Suggested:

```text
simulators/config/portfolio.yaml
```

Example shape:

```yaml
plants:
  - id: PLANT_01
    name: North Solar
    capacity_kw: 5000
    inverters:
      - id: INV_01
        rated_power_kw: 1000
      - id: INV_02
        rated_power_kw: 1000
```

Use fictional plant names. Do not imply real customer data.

## Required Validation

- exactly or at least 5 configured plants for default demo,
- every plant has unique ID,
- every inverter has unique `(plant_id, inverter_id)`,
- sum of inverter ratings should be reasonably consistent with plant capacity,
- capacities > 0.

## Tests

Test:

- portfolio loads,
- duplicate IDs fail,
- invalid capacities fail.

## Commit

```text
feat(simulator): add validated solar portfolio configuration
```

---

# 6. Milestone 3 — Realistic Solar-Day Curve

## Goal

Generate physically plausible-looking power instead of independent random values.

Use a simple explainable deterministic formula.

## Recommended Reference Formula

Let `progress` be a value from 0.0 to 1.0 across the simulated day.

Use:

```python
solar_shape = max(0.0, math.sin(math.pi * progress))
```

A slightly sharper midday profile may use:

```python
solar_shape = max(0.0, math.sin(math.pi * progress)) ** 1.5
```

Then:

```python
irradiance_wm2 = clear_sky_peak_wm2 * solar_shape
```

Bound typical simulated irradiance to approximately:

```text
0–1000 W/m²
```

For inverter power:

```python
temperature_factor = max(0.80, 1.0 - max(module_temp_c - 25.0, 0.0) * 0.004)
base_power_kw = inverter_rated_kw * solar_shape * temperature_factor
```

Apply small deterministic noise:

```python
noise = rng.uniform(0.97, 1.03)
active_power_kw = max(0.0, min(inverter_rated_kw, base_power_kw * noise))
```

This is a **demo approximation**, not a production PV model. Document that limitation.

## Energy Today

Do not generate cumulative energy randomly.

Integrate power over elapsed simulated time:

```python
energy_increment_kwh = active_power_kw * simulated_elapsed_hours
energy_today_kwh += energy_increment_kwh
```

Reset `energy_today_kwh` at simulated-day boundary.

## Temperatures

Example safe demo approximation:

```python
ambient_c = 26 + 5 * solar_shape
module_temp_c = ambient_c + 20 * solar_shape + rng.uniform(-1.5, 1.5)
inverter_temp_c = ambient_c + 14 * solar_shape + rng.uniform(-1.0, 1.0)
```

Keep bounds reasonable.

## Tests

Use a fixed seed and test:

- zero/near-zero at day beginning/end,
- maximum around midday,
- no negative power,
- no power above inverter rating,
- cumulative energy never decreases during the day,
- same seed produces same sequence.

## Commit

```text
feat(simulator): model deterministic solar generation curve
```

---

# 7. Milestone 4 — Telemetry Event Builder & Schema Validation

## Goal

Build valid events matching the frozen contract.

Use a Pydantic model or JSON Schema validation.

If `contracts/telemetry.schema.json` exists, use it as the canonical schema.

### Event Builder Requirements

- `event_id` UUID,
- `timestamp` ISO 8601 UTC,
- status enum,
- availability `0.0` or `1.0` in default cases,
- consistent plant/inverter identity,
- scenario label when anomaly is active.

### Invalid Event Support

The simulator should be able to intentionally create a small invalid-event scenario for testing, but this must be disabled by default in normal demo mode unless explicitly configured.

Possible invalid event:

```text
negative active_power_kw
```

The producer must refuse to publish invalid normal events to `solar.telemetry.raw`.

If a deliberate invalid-event test is enabled, publish the rejected record to:

```text
solar.telemetry.invalid
```

with error metadata, or log it clearly if architecture chooses source-side quarantine.

## Commit

```text
feat(simulator): validate telemetry event contract
```

---

# 8. Milestone 5 — Kafka Infrastructure

## Goal

Make Kafka reproducibly available through Docker Compose.

Prefer the simplest stable Kafka mode supported by the chosen container image. Avoid adding ZooKeeper unless the selected image/version requires it.

## Docker Requirements

- predictable internal bootstrap address,
- health check,
- persistent data only if useful for local development,
- no exposed credentials,
- topic bootstrap script.

## Topic Creation Script

Create:

```text
kafka/scripts/create_topics.sh
```

It must create:

```text
solar.telemetry.raw
solar.telemetry.invalid
solar.alerts
```

Use a small partition count such as:

```text
3
```

for telemetry.

Use replication factor `1` for the local educational Docker environment.

The script should be idempotent.

## Verification Script

Add a command/script that can:

- list topics,
- optionally consume a few telemetry events.

## Commit

```text
feat(kafka): configure local topics and bootstrap scripts
```

---

# 9. Milestone 6 — Kafka Producer

## Goal

Publish validated telemetry reliably.

Recommended library:

```text
confluent-kafka
```

or the team-approved Kafka Python client.

## Required Producer Behavior

Configuration should include:

- bootstrap servers from env,
- acknowledgements suitable for reliability,
- bounded retries,
- JSON encoding,
- key = `plant_id:inverter_id`,
- delivery callback,
- graceful flush on shutdown.

Do not create a new producer per event.

### Reference Pattern

```python
producer.produce(
    topic=topic,
    key=f"{event['plant_id']}:{event['inverter_id']}".encode(),
    value=json.dumps(event).encode(),
    on_delivery=delivery_callback,
)
producer.poll(0)
```

Flush only on shutdown or controlled intervals, not after every single record unless required for a tiny diagnostic tool.

## Retry/Failure Rule

If Kafka is temporarily unavailable:

- log a structured error,
- retry in a bounded way,
- do not lose the process silently,
- expose producer failure metrics.

## Tests

Mock the producer and verify:

- correct topic,
- correct key,
- valid JSON body,
- delivery failure logging.

## Commit

```text
feat(kafka): publish keyed validated solar telemetry
```

---

# 10. Milestone 7 — Deterministic Anomaly Engine

## Goal

Create anomalies that the evaluation can reproduce every time.

Do not rely on random anomalies.

Implement named scenarios.

Suggested scenario schedule for a 300-second simulated day:

```text
0–90 sec      NORMAL
90–150 sec    INV_UNDERPERFORMANCE on PLANT_03 / INV_02
150–190 sec   NORMAL/RECOVERY
190–235 sec   INV_OFFLINE on PLANT_04 / INV_01
235–260 sec   TELEMETRY_GAP for PLANT_05 or selected inverter
260–300 sec   RECOVERY
```

Make the schedule configurable in a YAML/JSON file.

## Scenario Behavior

### INV_UNDERPERFORMANCE

Scale active power:

```python
active_power_kw *= 0.45
status = "WARNING"
```

but leave irradiance near normal.

This is important because Member 2's detection rule should distinguish low generation under good resource conditions.

### INV_OFFLINE

```text
active_power_kw = 0
availability = 0
status = OFFLINE
```

### TELEMETRY_GAP

Do not publish events for the target asset during the configured interval.

This is different from OFFLINE and enables pipeline/missing-data logic.

### RECOVERY

Return to the normal model.

## Tests

For a fixed seed and schedule, assert exact scenario transitions.

## Commit

```text
feat(simulator): add deterministic assessment anomaly scenarios
```

---

# 11. Milestone 8 — Daily Reference Generator

## Goal

Produce exactly one reference file per simulated day.

Location should be visible to Airflow through a shared Docker volume or agreed path.

Suggested:

```text
/data/daily/daily_reference_2026-08-21.csv
```

## Content

One row per plant.

Calculate/reference values consistently with portfolio configuration.

For example:

```text
expected_generation_kwh ≈ plant_capacity_kw * expected_equivalent_sun_hours
```

Use a simple fixed/configurable assumption, e.g. 4.0–5.5 equivalent sun hours depending on plant.

`expected_peak_power_kw` should not exceed plant capacity.

`ppa_rate_per_kwh` should be a fictional configurable commercial value. Avoid presenting it as an actual current tariff unless externally verified.

## Timing

At the simulated-day boundary:

1. finalize/generate the day's file,
2. use atomic write:
   - write temporary file,
   - rename to final filename,
3. log `daily_reference_ready`.

Airflow should not read a half-written file.

## Idempotency

If the same simulation day is regenerated in reset/demo mode, behavior must be explicit:

- overwrite deterministically in demo reset, or
- refuse unless reset mode enabled.

## Tests

- schema columns exact,
- one row per plant,
- unique plant IDs,
- valid positive expectations,
- deterministic output under fixed seed.

## Commit

```text
feat(batch-source): generate atomic daily solar reference feed
```

---

# 12. Milestone 9 — Prometheus Metrics

## Goal

Expose platform-level metrics from simulator/producer components.

Recommended metrics:

```text
solariq_events_produced_total
solariq_events_invalid_total
solariq_producer_failures_total
solariq_last_event_timestamp_seconds
solariq_simulation_day
solariq_active_simulation_scenario
```

Use labels sparingly. Do **not** create high-cardinality labels using `event_id`.

Reasonable labels:

```text
plant_id
scenario
```

only where needed.

### Reference Pattern

```python
from prometheus_client import Counter, Gauge, start_http_server

EVENTS_PRODUCED = Counter(
    "solariq_events_produced_total",
    "Total telemetry events published",
    ["plant_id"],
)

LAST_EVENT = Gauge(
    "solariq_last_event_timestamp_seconds",
    "Unix timestamp of latest published telemetry event",
)
```

## Commit

```text
feat(observability): expose ingestion and simulation metrics
```

---

# 13. Milestone 10 — Prometheus Configuration & No-Telemetry Rule

## Goal

Configure Prometheus to scrape relevant services and define at least one pipeline-health alert.

Do not confuse this with the business underperformance alert.

Suggested rule:

```yaml
groups:
  - name: solariq-pipeline
    rules:
      - alert: SolarIQNoTelemetryProduced
        expr: time() - solariq_last_event_timestamp_seconds > 60
        for: 15s
        labels:
          severity: critical
        annotations:
          summary: "SolarIQ telemetry producer is stale"
          description: "No telemetry has been produced within the configured threshold."
```

If the team uses a processed-event timestamp from Spark for final observability, coordinate with Member 2. Prefer ultimately alerting on **processed** telemetry because that checks more of the pipeline, but this member can establish the ingestion rule first.

Prometheus config must use Docker service names.

## Commit

```text
feat(observability): add Prometheus scrape config and telemetry alert
```

---

# 14. Milestone 11 — Bootstrap, Reset & Demo Scripts

## Goal

Make the project reproducible for assessment.

Create small shell scripts with `set -euo pipefail`.

### `scripts/bootstrap.sh`

Should:

1. verify Docker availability,
2. create required directories,
3. start dependencies,
4. wait on health checks,
5. create Kafka topics,
6. initialize shared infrastructure if not handled by service init containers,
7. print next steps.

### `scripts/demo_reset.sh`

Should:

- stop simulator/application components as appropriate,
- clear only **demo-generated** data,
- preserve source code/config,
- reset deterministic simulation state,
- restart required components or explain next command.

Be extremely careful with destructive commands.

Never run broad commands such as:

```bash
rm -rf /
docker system prune -a
```

### `scripts/demo_start.sh`

Should:

- start deterministic demo mode,
- print scenario timeline,
- print important URLs/ports,
- make it obvious when the simulated day starts.

## Commit

```text
feat(demo): add reproducible bootstrap reset and demo scripts
```

---

# 15. Milestone 12 — Member 1 Test Suite

At minimum, include:

```text
tests for:
- portfolio config validation
- solar curve
- energy integration
- schema validation
- deterministic seed
- anomaly schedule
- daily reference generation
- Kafka message serialization/key
```

Use unit tests that do not require Kafka where possible.

Add one integration test or diagnostic script that confirms a real Kafka container receives events.

## Final Member 1 Verification

Before declaring completion:

```bash
git status
pytest <member-1 tests>
docker compose config
docker compose up -d kafka
# create topics
# run simulator
# consume a few events
```

Confirm:

- valid event shape,
- key correctness,
- scenario transitions,
- daily file generation,
- Prometheus endpoint.

## Final Commit

```text
test(ingestion): verify deterministic simulator and Kafka pipeline
```

Then, if documentation finalization is needed:

```text
docs(ingestion): document simulator Kafka and demo controls
```

---

# 16. Member 1 Handoff Contract

Provide the team with:

```text
Kafka bootstrap address:
Kafka telemetry topic:
Kafka invalid topic:
Kafka alert topic:
Kafka key rule:

Daily reference directory:
Daily filename format:

Prometheus scrape endpoint:
Relevant metric names:

Demo commands:
Reset command:
Scenario schedule:
```

Also provide a sample valid event and a sample daily reference file.

Do not hand off undocumented assumptions.

---

# 17. Member 1 Definition of Done

You are done only when:

- [ ] default portfolio has 5 plants,
- [ ] every plant/inverter identity is stable,
- [ ] simulated solar curve is realistic enough for demonstration,
- [ ] cumulative energy is calculated rather than random,
- [ ] telemetry schema is validated,
- [ ] Kafka topics are created idempotently,
- [ ] producer uses stable keyed messages,
- [ ] anomaly scenarios are deterministic,
- [ ] telemetry gap is distinct from inverter offline,
- [ ] daily reference file is produced atomically,
- [ ] daily feed matches the frozen schema,
- [ ] structured logs are emitted,
- [ ] Prometheus metrics are exposed,
- [ ] at least one no-telemetry health rule exists,
- [ ] bootstrap/reset/demo scripts are safe and documented,
- [ ] tests pass,
- [ ] Docker configuration validates,
- [ ] no hard-coded secrets exist,
- [ ] Git history contains logical tested commits,
- [ ] Member 2 can consume telemetry without changing your contract,
- [ ] the team can reproduce the anomaly timeline during the assessment.

**Do not add optional features after reaching this point. Help integration instead.**
