# SolarIQ — Real-Time Solar Asset Performance & Revenue Intelligence
## Master Project Specification, Team Division & Delivery Plan

**Module:** Applied Big Data Engineering — Data Engineering Mini-Project  
**Project Type:** Group Project (3 members)  
**Implementation Window:** 2 weeks  
**Architecture:** Lambda Architecture  
**Primary Use Case:** Adaptation of Use Case 3 — Smart Grid Energy Monitoring & Billing  
**Working Product Name:** SolarIQ  
**Status:** Proposed implementation specification for team review before member-specific execution prompts are created.

---

# 1. Executive Summary

SolarIQ is a real-time solar asset performance and revenue intelligence platform designed to monitor a simulated portfolio of commercial solar plants.

The academic implementation will simulate live inverter/site telemetry and a daily reference feed containing expected generation, weather/irradiance expectations, tariff or PPA rate, and plant metadata. The platform will process the live stream for immediate operational insights while a scheduled batch pipeline performs daily reconciliation and produces authoritative historical performance and revenue reports.

The project is intentionally designed to satisfy the module requirements while remaining a credible foundation for a future commercial product.

The central business question is:

> **Which solar assets are underperforming right now, how much energy are they losing, and what is the estimated financial impact?**

The project must remain small enough to complete professionally in two weeks. Real inverter, SCADA, meter, or weather integrations are explicitly out of scope for the university implementation. Simulated data is the official Phase 1 data source. Production connectors can replace the simulators in a future commercial phase without redesigning the core pipeline.

---

# 2. Academic Requirement Alignment

The implementation must satisfy the following module expectations:

- End-to-end Lambda or Kappa architecture.
- A continuous streaming source simulated using Python.
- A once-per-simulated-day batch source simulated using Python.
- Apache Kafka for ingestion.
- Apache Spark Structured Streaming or Apache Storm for stream processing.
- Apache Airflow for orchestration.
- A suitable queryable storage layer.
- Meaningful transformations, not pass-through processing.
- Consolidated dashboard and/or scheduled report.
- Structured logging.
- At least one health-check or alert rule.
- Architecture choice and stack must be explicitly justified.
- Reproducible local execution, preferably through Docker Compose.
- Git repository with code, setup instructions, comments, and relevant tests.
- 5–10 minute end-to-end demo or equivalent live demonstration.
- Technical report with architecture, reasoning, results, observability, trade-offs, and limitations.

## Requirement Traceability

| Requirement | SolarIQ Implementation |
|---|---|
| Streaming source | Python solar telemetry simulator |
| Daily batch source | Python daily reference-data generator |
| Kafka | Telemetry ingestion using Kafka topics and partitions |
| Spark | Structured Streaming for cleansing, enrichment, windows and real-time KPIs |
| Airflow | Daily reference ingestion, reconciliation, reporting and quality checks |
| Storage | PostgreSQL serving store + MinIO/S3-compatible raw Parquet archive |
| Meaningful processing | Validation, enrichment, joins, windowing, aggregation, expected-vs-actual analysis, loss calculations |
| Dashboard | Portfolio, plant detail, alerts, daily report |
| Observability | Structured logs, Prometheus metrics, health checks and alert rule |
| Architecture reasoning | Lambda architecture with speed and batch paths |
| Reproducibility | Docker Compose + environment template + seeded simulators |
| Demo | Compressed simulated day and deterministic anomaly scenarios |

---

# 3. Academic Use-Case Positioning

SolarIQ is an adaptation of the module's **Smart Grid Energy Monitoring & Billing** use case.

The module scenario includes:

- live smart-meter/solar measurements,
- daily tariff/billing and/or weather information,
- real-time renewable contribution,
- alerts,
- daily consolidated reports.

SolarIQ narrows the scenario from household/grid monitoring to **commercial solar portfolio monitoring**.

This keeps the project academically defensible while giving it greater commercial and CV value.

The report must explicitly explain that the fields and business question were adapted while preserving the required streaming + daily-batch structure of the approved use case.

---

# 4. Project Objectives

## 4.1 Academic Objective

Design and implement a reproducible Lambda-architecture data platform that:

1. ingests continuous solar telemetry,
2. processes it in near-real-time,
3. preserves raw events for replay/historical computation,
4. ingests a daily reference feed,
5. reconciles real data against daily expectations,
6. exposes processed results through an API,
7. displays operational and financial KPIs in a dashboard,
8. generates daily reports,
9. exposes pipeline health and observability information.

## 4.2 Business Objective

Demonstrate how raw solar telemetry can be converted into business decisions such as:

- which plant is underperforming,
- which inverter requires investigation,
- estimated lost energy,
- estimated lost revenue,
- plant availability,
- actual versus expected generation,
- portfolio ranking by performance.

---

# 5. Scope

## 5.1 In Scope — Phase 1 University Project

### Simulated portfolio
- 5 solar plants.
- 5–10 inverters per plant.
- Configurable plant capacity.
- Configurable normal output profile.
- Deterministic anomaly injection.

### Streaming telemetry
- Generated every 3–5 seconds.
- Published to Kafka.
- Cleaned and validated using Spark.
- Aggregated in short time windows.
- Used for live performance calculations.

### Daily batch feed
- One file per simulated day.
- Includes expected generation, forecast/reference irradiance, commercial rate and plant metadata.
- Orchestrated by Airflow.
- Used for end-of-day reconciliation.

### Real-time intelligence
- current portfolio power,
- current plant power,
- inverter status,
- rolling average power,
- availability,
- expected vs actual performance proxy,
- underperformance detection,
- missing-telemetry detection,
- estimated energy loss,
- estimated revenue impact.

### Historical intelligence
- daily actual generation,
- daily expected generation,
- daily performance percentage,
- daily availability,
- estimated lost energy,
- estimated lost revenue,
- plant rankings,
- anomaly count.

### Serving
- REST API.
- Web dashboard.
- Daily report.

### Engineering quality
- Docker Compose.
- configuration through environment variables,
- structured logging,
- metrics,
- tests,
- data contracts,
- migration/schema setup,
- deterministic demo mode,
- clean Git history.

## 5.2 Explicitly Out of Scope for Phase 1

Do **not** add these unless the mandatory project is already complete and stable:

- real Huawei FusionSolar integration,
- real Sungrow/SMA/SolarEdge integration,
- physical weather stations,
- real SCADA,
- real smart meters,
- user authentication,
- multi-tenant SaaS,
- billing/payment features,
- technician mobile app,
- AI/LLM agents,
- ML anomaly detection,
- predictive maintenance,
- work-order management,
- GIS/map features,
- complex permissions,
- production cloud deployment,
- Kubernetes.

These are future commercial phases.

---

# 6. Architecture Decision

## 6.1 Selected Architecture: Lambda

SolarIQ will implement a practical Lambda architecture.

### Speed Layer

```text
Python Telemetry Simulator
          |
          v
        Kafka
          |
          v
Spark Structured Streaming
    |              |
    |              +--> Raw normalized events -> MinIO/Parquet
    |
    +--> live metrics / alerts -> PostgreSQL
```

The speed layer provides near-real-time operational visibility.

### Batch Layer

```text
Daily Reference Generator
          |
          v
     Daily CSV/JSON
          |
          v
       Airflow
          |
          +--> validate reference feed
          |
          +--> read raw daily telemetry from MinIO/Parquet
          |
          +--> daily reconciliation job
          |
          v
      PostgreSQL
```

The batch layer produces authoritative daily/historical values.

### Serving Layer

```text
PostgreSQL
     |
     v
  FastAPI
     |
     +--> React Dashboard
     |
     +--> Daily Report
```

## 6.2 Why Lambda

Lambda is the preferred choice because the scenario contains two genuinely different workloads:

- **live telemetry** needs low-latency processing;
- **daily reconciliation** prioritizes completeness and consistency.

The batch layer also allows raw data to be recomputed if processing logic changes.

This provides a strong academic discussion around:

- latency,
- replay,
- correctness,
- operational complexity,
- cost,
- eventual reconciliation,
- duplicate logic between speed and batch paths.

## 6.3 Rejected Alternative: Kappa

Kappa could simplify the architecture by treating all events as one replayable stream, including reference updates.

However, for this project:

- a daily reference file is explicitly required,
- Airflow orchestration is explicitly expected,
- authoritative end-of-day reconciliation is naturally batch-oriented,
- Lambda makes the distinction between immediate and historical processing easier to demonstrate.

The report must still acknowledge Lambda's extra operational complexity and duplication.

---

# 7. Technology Stack

| Layer | Technology | Reason |
|---|---|---|
| Simulation | Python | Required/easy deterministic source generation |
| Streaming transport | Apache Kafka | Required; partitioned event ingestion |
| Stream processing | Apache Spark Structured Streaming | Required/preferred; windows, aggregations, stateful processing |
| Batch orchestration | Apache Airflow | Required/preferred; scheduled file/reconciliation pipeline |
| Raw archive | MinIO (S3-compatible) + Parquet | Immutable/replayable raw history for Lambda batch path |
| Serving DB | PostgreSQL | Queryable relational serving layer, simple local operation |
| API | FastAPI | Lightweight Python serving layer |
| Dashboard | React + a restrained charting library | Professional portfolio dashboard |
| Metrics | Prometheus | Pipeline/service metrics and alert expressions |
| Containers | Docker Compose | Reproducible assessment/demo environment |
| Testing | pytest + focused frontend tests | Unit/integration verification |
| Version control | Git | Professional step-by-step history |

Avoid adding technologies without a clear academic or operational reason.

---

# 8. Repository Structure

Use a monorepo with explicit ownership boundaries.

```text
solariq/
├── README.md
├── .env.example
├── .gitignore
├── docker-compose.yml
├── Makefile
├── docs/
│   ├── architecture.md
│   ├── data-contracts.md
│   ├── demo-runbook.md
│   └── contribution-notes.md
│
├── contracts/
│   ├── telemetry.schema.json
│   ├── daily_reference.schema.json
│   └── shared_constants.py
│
├── simulators/
│   ├── streaming/
│   └── batch/
│
├── kafka/
│   ├── config/
│   └── scripts/
│
├── processing/
│   ├── streaming/
│   ├── batch/
│   └── common/
│
├── orchestration/
│   └── dags/
│
├── storage/
│   ├── migrations/
│   ├── sql/
│   └── seeds/
│
├── api/
│   ├── app/
│   └── tests/
│
├── dashboard/
│   ├── src/
│   └── tests/
│
├── observability/
│   ├── prometheus/
│   ├── alert-rules/
│   └── dashboards/
│
├── reports/
│   └── templates/
│
├── tests/
│   ├── integration/
│   └── e2e/
│
└── scripts/
    ├── bootstrap.sh
    ├── demo_start.sh
    ├── demo_reset.sh
    └── smoke_test.sh
```

Do not allow each member to invent a separate folder structure.

---

# 9. Data Contracts

All members must treat the contracts as shared interfaces. Contracts are frozen after the initial architecture checkpoint unless all three members agree to a versioned change.

## 9.1 Streaming Telemetry Event

Topic:

```text
solar.telemetry.raw
```

Recommended Kafka key:

```text
plant_id:inverter_id
```

Suggested fields:

| Field | Type | Description |
|---|---|---|
| event_id | UUID/string | Unique event identifier |
| plant_id | string | Solar plant ID |
| inverter_id | string | Inverter ID |
| active_power_kw | float | Current output |
| energy_today_kwh | float | Cumulative daily generation |
| irradiance_wm2 | float | Simulated measured irradiance |
| module_temp_c | float | Module temperature |
| inverter_temp_c | float | Inverter temperature |
| status | enum | ONLINE / OFFLINE / WARNING |
| availability | float | 0 or 1 at event level |
| timestamp | UTC timestamp | Event time |
| simulator_scenario | string/null | Demo/anomaly label |

### Event Rules

- `event_id` must be unique.
- timestamps must be UTC internally.
- numerical values must have defensible physical ranges.
- invalid events should be rejected/quarantined rather than silently accepted.
- simulator must be seedable for deterministic demos.
- duplicate events must be possible in test mode so idempotency can be tested.

## 9.2 Daily Reference Feed

Suggested file:

```text
daily_reference_YYYY-MM-DD.csv
```

Fields:

| Field | Type | Description |
|---|---|---|
| simulation_date | date | Simulated day |
| plant_id | string | Plant ID |
| plant_capacity_kw | float | Installed plant capacity |
| expected_generation_kwh | float | Expected daily generation |
| expected_peak_power_kw | float | Expected peak |
| forecast_irradiance_kwh_m2 | float | Daily expected irradiation |
| ppa_rate_per_kwh | float | Commercial value per kWh |
| maintenance_flag | boolean | Whether planned maintenance exists |
| source_version | string | Feed version |

---

# 10. Simulated Time

Default demo configuration:

```text
1 simulated day = 5 real minutes
telemetry interval = 3 seconds
```

This must be configurable.

The simulator should model a solar day curve rather than random independent values.

Recommended simplified profile:

- dawn -> near zero,
- morning ramp,
- midday peak,
- afternoon decline,
- sunset -> zero.

Use bounded noise so the output looks realistic.

A deterministic demo schedule should inject anomalies such as:

1. inverter power degradation,
2. complete inverter offline event,
3. no-telemetry event,
4. plant-wide underperformance,
5. recovery.

---

# 11. Kafka Design

## Topics

Minimum:

```text
solar.telemetry.raw
solar.telemetry.invalid
solar.alerts
```

Optional only if useful:

```text
solar.telemetry.cleaned
```

## Partitioning

Partition by `plant_id` or stable `plant_id:inverter_id` key.

For the demo, use a small fixed partition count such as 3.

The report should justify partitioning based on:

- preservation of order for an asset key,
- horizontal scalability,
- portfolio distribution.

## Producer Requirements

- JSON serialization.
- schema validation before publish.
- delivery acknowledgements.
- retry policy.
- clear error logging.
- configurable bootstrap server.
- graceful shutdown.
- seed/demo mode.

---

# 12. Streaming Processing Requirements

Spark Structured Streaming must perform meaningful processing.

## Mandatory Stream Steps

1. Read Kafka events.
2. Parse JSON.
3. Validate schema.
4. Normalize types and timestamps.
5. Drop or quarantine invalid data.
6. Deduplicate using `event_id`.
7. Apply event-time watermark.
8. Calculate rolling/windowed metrics.
9. Detect operational anomalies.
10. Write live metrics to PostgreSQL.
11. Write normalized raw events to Parquet/MinIO.
12. Publish or store generated alerts.

## Mandatory Live KPIs

At portfolio level:

- current power,
- online inverter count,
- offline inverter count,
- active plant count,
- rolling portfolio power,
- estimated performance percentage.

At plant level:

- current power,
- rolling 5-minute average,
- availability,
- expected-vs-actual power proxy,
- underperformance percentage,
- approximate current loss rate.

## Underperformance Rule

Use a simple, explainable rule rather than ML.

Example:

```text
IF irradiance is above the minimum operating threshold
AND actual rolling power < expected rolling power * 0.80
FOR a sustained window
THEN create UNDERPERFORMANCE alert
```

The exact formula must be documented and tested.

---

# 13. Batch Pipeline Requirements

Airflow owns the daily workflow.

Recommended DAG:

```text
wait_for_reference_file
        |
validate_reference_feed
        |
check_raw_daily_data
        |
compute_daily_actuals
        |
join_expected_and_actual
        |
calculate_losses_and_revenue
        |
write_daily_summary
        |
generate_report
        |
run_data_quality_checks
```

## Daily Metrics

Per plant:

- actual generation,
- expected generation,
- performance percentage,
- average availability,
- downtime minutes,
- lost energy estimate,
- PPA/tariff rate,
- estimated actual revenue,
- estimated lost revenue,
- alert count.

Portfolio:

- total expected generation,
- total actual generation,
- total lost generation,
- portfolio performance,
- portfolio availability,
- estimated revenue,
- estimated lost revenue,
- best/worst performing sites.

---

# 14. Storage Design

## 14.1 Raw Layer

MinIO/S3-compatible bucket.

Suggested layout:

```text
s3://solariq-raw/telemetry/
  simulation_date=2026-08-21/
    plant_id=PLANT_01/
      part-*.parquet
```

This gives the batch layer replayable immutable history.

## 14.2 PostgreSQL Serving Tables

Minimum tables:

### `plants`

```text
id
name
capacity_kw
timezone
active
created_at
```

### `inverters`

```text
id
plant_id
name
rated_power_kw
active
created_at
```

### `live_plant_metrics`

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

### `daily_plant_summary`

```text
simulation_date
plant_id
actual_generation_kwh
expected_generation_kwh
performance_pct
availability_pct
downtime_minutes
estimated_lost_energy_kwh
estimated_actual_revenue
estimated_lost_revenue
alert_count
computed_at
```

### `pipeline_health`

```text
component
status
last_event_at
last_success_at
message
updated_at
```

Use migrations. Do not create production tables ad hoc from application code.

---

# 15. API Scope

FastAPI endpoints should remain focused.

## Health

```text
GET /health
GET /ready
GET /metrics
```

## Portfolio

```text
GET /api/v1/portfolio/live
GET /api/v1/portfolio/daily?date=
```

## Plants

```text
GET /api/v1/plants
GET /api/v1/plants/{plant_id}/live
GET /api/v1/plants/{plant_id}/history?from=&to=
GET /api/v1/plants/{plant_id}/inverters
```

## Alerts

```text
GET /api/v1/alerts
GET /api/v1/alerts?status=active
```

## Reports

```text
GET /api/v1/reports/daily?date=
```

No authentication is required for Phase 1.

Use versioned routes and documented response models.

---

# 16. Dashboard Scope

Build a restrained, professional dashboard rather than a visually complex application.

## Screen 1 — Portfolio Overview

Cards:

- Installed Capacity
- Current Generation
- Energy Today
- Expected Energy
- Portfolio Performance
- Availability
- Estimated Energy Loss
- Estimated Revenue Loss

Visuals:

- actual vs expected portfolio power chart,
- plant performance ranking,
- active alerts,
- plant status table.

## Screen 2 — Plant Detail

Show:

- plant current power,
- expected power,
- performance,
- availability,
- energy today,
- estimated loss,
- inverter status,
- power trend,
- recent alerts.

## Screen 3 — Alerts

Table:

- severity,
- timestamp,
- plant,
- inverter,
- type,
- duration,
- estimated impact,
- state.

## Screen 4 — Daily Report

Show:

- date,
- portfolio totals,
- per-plant summary,
- best and worst performers,
- lost energy,
- estimated revenue impact.

Keep the dashboard operational and readable. Avoid unnecessary animations and visual effects.

---

# 17. Observability

Observability is a mandatory project feature, not optional polish.

## Structured Logging

Every major component should emit JSON-structured logs with fields such as:

```text
timestamp
level
service
event
plant_id optional
inverter_id optional
correlation_id optional
message
error optional
```

Services:

- streaming simulator,
- batch simulator,
- Spark stream job,
- Airflow tasks,
- API,
- report generator.

## Metrics

Expose useful metrics such as:

```text
solariq_events_produced_total
solariq_events_invalid_total
solariq_events_processed_total
solariq_stream_last_event_timestamp
solariq_active_alerts
solariq_batch_runs_total
solariq_batch_failures_total
solariq_api_requests_total
```

## Minimum Alert Rule

Mandatory:

> Trigger a pipeline health alert when no streaming telemetry has been processed for more than a configured threshold.

Recommended threshold in demo mode:

```text
30–60 seconds
```

Also include at least one business alert:

> sustained solar underperformance.

These are two different concepts and should not be confused:

- **pipeline alert** = engineering/observability problem,
- **business alert** = solar operational problem.

---

# 18. Testing Strategy

Tests are part of the implementation, not postponed until the end.

## Unit Tests

### Simulator
- deterministic seed,
- correct solar curve bounds,
- valid schemas,
- anomaly injection.

### Stream processing
- invalid-event rejection,
- duplicate handling,
- rolling aggregation,
- availability calculation,
- underperformance rule.

### Batch processing
- expected/actual join,
- daily aggregation,
- lost-energy calculation,
- revenue calculation,
- missing reference detection.

### API
- response status,
- schema validation,
- empty states,
- known seeded values.

## Integration Tests

At minimum:

1. producer -> Kafka,
2. Spark -> PostgreSQL,
3. Spark -> Parquet/MinIO,
4. Airflow batch -> daily summary,
5. API -> PostgreSQL.

## End-to-End Smoke Test

A script should:

1. ensure services are healthy,
2. start deterministic simulation,
3. confirm Kafka receives events,
4. confirm Spark processes events,
5. confirm DB rows appear,
6. confirm API returns metrics,
7. confirm alert appears after injected anomaly,
8. confirm daily summary appears after simulated day.

---

# 19. Docker Compose Services

Expected services:

```text
kafka
spark-master / spark-worker (or project-appropriate Spark container layout)
postgres
minio
airflow-webserver
airflow-scheduler
airflow-init
prometheus
api
dashboard
streaming-simulator
```

Keep compose architecture as small as practical.

Use health checks and service dependencies carefully.

Do not rely on arbitrary long sleep commands where health-based readiness can be used.

---

# 20. Demo Design

The demo must be deterministic.

## Demo Story

### Minute 0–1
Start the platform and show healthy services.

### Minute 1–2
Show telemetry flowing:

```text
Simulator -> Kafka -> Spark -> PostgreSQL -> API -> Dashboard
```

### Minute 2–4
Portfolio runs normally.

Show:

- live generation,
- availability,
- plant comparison.

### Minute 4–5
Inject an inverter underperformance event.

Show:

- stream detects deviation,
- plant performance falls,
- alert appears,
- estimated loss increases.

### Minute 5–6
Inject no-telemetry or inverter-offline event.

Show engineering/business alert distinction.

### Minute 6–8
Complete the compressed simulated day.

Airflow processes the daily reference feed and raw telemetry.

Show:

- daily summary,
- expected vs actual,
- lost energy,
- estimated lost revenue.

### Minute 8–10
Show:

- logs,
- metrics/health,
- architecture diagram,
- ability to reproduce through Docker Compose.

Do not depend on spontaneous random anomalies during the evaluation.

---

# 21. Git & Engineering Standards

This repository must be treated like a real professional project.

## Mandatory Rules for All Members and Claude Code

1. **Never implement large unrelated changes in one commit.**
2. **Commit after every completed logical implementation step.**
3. **Run relevant tests before each commit.**
4. **Do not commit known-broken code.**
5. **Do not commit secrets, credentials, generated binaries, database volumes or environment files.**
6. **Use `.env.example` for documented configuration.**
7. **Follow existing project structure and contracts exactly.**
8. **Do not silently rename shared fields or API contracts.**
9. **Do not modify another member's owned subsystem without coordination.**
10. **Prefer simple, explicit code over premature abstraction.**
11. **Use typed models where practical.**
12. **Handle errors explicitly.**
13. **Use UTC internally.**
14. **Use reproducible deterministic data for tests.**
15. **Add or update documentation whenever behavior/configuration changes.**
16. **No TODO placeholders may remain in required functionality at final integration.**
17. **No mocked API/dashboard values in the final demonstration.**
18. **Final dashboard data must come from the running pipeline.**
19. **All core calculations must be explainable during viva.**
20. **Claude Code must inspect current repository state before changing files.**
21. **Claude Code must not rewrite working subsystems merely for style.**
22. **Claude Code must state the intended step, implement only that step, test it, and commit it before moving on.**
23. **Every commit should leave the repository in a coherent state.**

## Branch Strategy

Recommended:

```text
main
develop
member-1/platform-ingestion
member-2/data-processing
member-3/serving-ui
```

Merge through reviewed pull requests or coordinated merge checkpoints.

## Commit Convention

Use Conventional Commit-style messages:

```text
chore(repo): initialize project structure
feat(simulator): add deterministic solar telemetry generator
feat(kafka): publish validated telemetry events
feat(stream): add Spark event-time window aggregation
feat(storage): add serving schema migrations
feat(batch): add daily reconciliation DAG
feat(api): expose portfolio live metrics
feat(ui): add portfolio overview
feat(observability): add no-telemetry alert rule
test(batch): verify revenue reconciliation
docs(demo): add deterministic demo runbook
fix(stream): make duplicate handling idempotent
```

Avoid:

```text
update
changes
final
working now
fix stuff
```

---

# 22. Team Division

The project is divided into three ownership domains.

A shared contract-first phase happens before parallel implementation.

---

# 23. Member 1 — Platform, Simulation, Kafka & Observability Foundation

## Primary Ownership

Member 1 owns the platform entry point and infrastructure needed by all other members.

### Responsibilities

- monorepo/bootstrap support,
- Docker Compose base services,
- environment/configuration convention,
- streaming telemetry simulator,
- daily reference generator,
- Kafka topic/bootstrap setup,
- Kafka producer,
- schema validation at source,
- deterministic anomaly scenarios,
- MinIO bootstrap coordination,
- Prometheus configuration,
- shared structured logging utilities,
- pipeline health metric foundation,
- no-telemetry observability rule,
- smoke/bootstrap scripts.

### Member 1 Deliverables

```text
simulators/
kafka/
observability/
scripts/bootstrap.sh
scripts/demo_start.sh
scripts/demo_reset.sh
docker-compose.yml portions owned by platform
```

### Expected Integration Contract

Member 1 guarantees that Member 2 can reliably consume:

```text
solar.telemetry.raw
```

and access a deterministic daily reference file.

Member 1 must publish exact schemas in `contracts/`.

---

# 24. Member 2 — Spark, Lambda Batch Layer, Airflow & Storage

## Primary Ownership

Member 2 owns the core data engineering logic.

### Responsibilities

- PostgreSQL schema and migrations,
- Spark Structured Streaming job,
- parsing and validation,
- deduplication,
- watermarking,
- windowed aggregations,
- performance logic,
- live metrics persistence,
- raw normalized Parquet archive,
- MinIO storage path convention,
- daily batch transformations,
- Airflow DAG,
- expected vs actual reconciliation,
- loss calculations,
- revenue calculations,
- data-quality checks,
- core processing tests.

### Member 2 Deliverables

```text
processing/
orchestration/
storage/
tests relating to transformation logic
```

### Expected Integration Contract

Member 2 guarantees stable PostgreSQL serving tables and documented calculations that Member 3 can consume.

---

# 25. Member 3 — API, Dashboard, Reporting, Integration QA & Demo Experience

## Primary Ownership

Member 3 owns the user-facing serving layer and final integrated experience.

### Responsibilities

- FastAPI application,
- typed API response models,
- health/readiness endpoints,
- portfolio endpoints,
- plant endpoints,
- alerts endpoints,
- daily report endpoint,
- React dashboard,
- portfolio overview,
- plant detail,
- alerts screen,
- daily report screen,
- report rendering/export if included,
- API/frontend tests,
- integration smoke tests,
- end-to-end demo runbook,
- final UX polish,
- final integrated demo verification.

### Member 3 Deliverables

```text
api/
dashboard/
reports/
tests/integration/
tests/e2e/
docs/demo-runbook.md
```

### Expected Integration Contract

Member 3 must consume actual PostgreSQL/API data.

The dashboard must not depend on hard-coded final-demo values.

---

# 26. Shared Responsibilities

The following are **not owned by one member alone**:

- architecture decisions,
- shared data contracts,
- Lambda vs Kappa justification,
- final report,
- final architecture diagram,
- demo rehearsal,
- integration review,
- contribution statement,
- final README accuracy.

Each member must understand the full pipeline sufficiently to explain their own component and the end-to-end data flow.

---

# 27. Workload Balance

Approximate effort distribution:

| Area | Member 1 | Member 2 | Member 3 |
|---|---:|---:|---:|
| Architecture/contracts | Shared | Shared | Shared |
| Infra/Docker | High | Medium | Medium |
| Simulators | High | Low | Low |
| Kafka | High | Medium | Low |
| Spark | Low | High | Low |
| Airflow | Low | High | Low |
| PostgreSQL design | Low | High | Medium |
| API | Low | Low | High |
| Dashboard | Low | Low | High |
| Observability | High | Medium | Medium |
| Tests | Medium | High | High |
| Demo | Medium | Medium | High |
| Report | Shared | Shared | Shared |

No member should disappear into an isolated subsystem without participating in integration.

---

# 28. Shared Phase 0 — Must Happen Before Parallel Coding

Before giving each member an independent Claude Code prompt, the team must finalize these contracts:

1. repository structure,
2. telemetry schema,
3. daily reference schema,
4. Kafka topic names,
5. Kafka key strategy,
6. PostgreSQL table names,
7. API response expectations,
8. simulator clock,
9. anomaly scenarios,
10. environment variable names,
11. branch naming,
12. coding/testing standards.

Do not allow three Claude Code sessions to make independent assumptions about these.

This master document is intended to freeze those assumptions.

---

# 29. Two-Week Delivery Plan

Assume 10 primary working days with parallel work.

## Day 1 — Architecture & Bootstrap

**All members**

- review module brief,
- confirm master specification,
- initialize repository,
- create branches,
- freeze contracts,
- create Docker Compose skeleton,
- create README skeleton,
- verify Git workflow.

**Checkpoint:** all members can clone, start base dependencies, and see the same contracts.

---

## Day 2 — First Vertical Foundations

### Member 1
- implement deterministic telemetry simulator,
- create Kafka topics,
- publish valid events.

### Member 2
- create PostgreSQL migrations,
- create Spark job skeleton,
- prove Kafka consumption.

### Member 3
- create FastAPI skeleton,
- create dashboard skeleton,
- define typed API models against agreed schema.

**Checkpoint:** producer -> Kafka -> Spark connectivity works.

---

## Day 3 — Real-Time Processing

### Member 1
- anomaly injection,
- daily reference generator,
- logging.

### Member 2
- validation,
- normalization,
- deduplication,
- event-time windows,
- live plant metrics.

### Member 3
- plant/portfolio endpoints,
- initial dashboard live cards.

**Checkpoint:** simulator -> Kafka -> Spark -> Postgres -> API -> UI first vertical slice.

---

## Day 4 — Lambda Raw Layer & Live Alerts

### Member 1
- MinIO configuration,
- Prometheus configuration,
- health metrics foundation.

### Member 2
- normalized Parquet raw sink,
- underperformance rule,
- alerts persistence.

### Member 3
- alerts API,
- alerts UI,
- empty/loading/error states.

**Checkpoint:** live anomaly appears in the dashboard.

---

## Day 5 — Batch Pipeline

### Member 1
- daily-file production timing,
- deterministic simulated-day trigger.

### Member 2
- Airflow DAG,
- raw daily read,
- reference validation,
- daily actual aggregation.

### Member 3
- daily report API contract and UI skeleton.

**Checkpoint:** Airflow can process one complete simulated day.

---

## Day 6 — Reconciliation & Financial Intelligence

### Member 1
- edge-case simulator scenarios,
- observability improvements.

### Member 2
- expected vs actual join,
- loss calculations,
- revenue calculations,
- daily summary persistence.

### Member 3
- daily report screen,
- plant historical charts,
- portfolio ranking.

**Checkpoint:** full academic business question is answered.

---

## Day 7 — Testing & Observability

### Member 1
- producer tests,
- health alert,
- Prometheus alert rule.

### Member 2
- transformation unit tests,
- Airflow/batch tests,
- data-quality checks.

### Member 3
- API tests,
- frontend tests,
- integration smoke test.

**Checkpoint:** critical tests pass and pipeline health is visible.

---

## Day 8 — Integration Hardening

**All members**

- merge integration branch,
- resolve interface mismatches,
- remove hard-coded demo values,
- validate clean-start workflow,
- validate reset workflow,
- confirm all required metrics,
- confirm all alert paths,
- validate Docker Compose from a clean environment.

**Checkpoint:** one command/setup sequence can reproduce the project.

---

## Day 9 — Demo & Documentation

**All members**

- freeze required features,
- no new scope,
- create architecture diagram,
- finish README,
- prepare report screenshots,
- write limitations/trade-offs,
- write Lambda vs Kappa argument,
- prepare demo script,
- record contribution notes.

**Checkpoint:** complete 10-minute rehearsal.

---

## Day 10 — Final QA

**All members**

- fresh clone test,
- clean Docker startup,
- full end-to-end demo,
- run tests,
- inspect logs,
- inspect metrics,
- validate Airflow DAG,
- validate dashboard,
- validate report,
- fix only blocking defects,
- tag release candidate.

Suggested tag:

```text
v1.0.0-assessment
```

---

# 30. Integration Checkpoints

Do not wait until the end to merge.

Mandatory checkpoints:

## Checkpoint A — Contracts
End of Day 1.

## Checkpoint B — First Vertical Slice
End of Day 3:

```text
Simulator -> Kafka -> Spark -> PostgreSQL -> API -> Dashboard
```

## Checkpoint C — Full Lambda
End of Day 6:

```text
Streaming + Raw Archive + Airflow Batch + Daily Reconciliation
```

## Checkpoint D — Assessment Ready
End of Day 9.

---

# 31. Definition of Done

The project is **not done** because each member's code works independently.

It is done only when all conditions below are true.

## Functional

- telemetry simulator works,
- daily feed generator works,
- Kafka receives events,
- Spark processes them,
- invalid data is handled,
- live metrics are persisted,
- raw normalized telemetry is archived,
- Airflow runs daily reconciliation,
- expected vs actual metrics are calculated,
- revenue impact is calculated,
- alerts are generated,
- API returns live and historical data,
- dashboard uses actual API data,
- daily report is produced.

## Observability

- structured logs exist,
- health endpoint works,
- metrics endpoint works,
- no-telemetry alert can be demonstrated.

## Quality

- project starts from documented instructions,
- migrations work from empty DB,
- key tests pass,
- no secrets are committed,
- environment template is complete,
- no required TODOs remain,
- fresh-clone smoke test succeeds.

## Academic

- Lambda architecture is clearly visible,
- rejected Kappa alternative is justified,
- stack decisions are justified,
- transformations are meaningful,
- both sources are demonstrated,
- observability is demonstrated,
- limitations/trade-offs are documented,
- contribution statement is ready.

---

# 32. Report Planning

Recommended report structure:

1. Executive Summary
2. Business Scenario
3. Requirements Interpretation
4. Architecture Decision
   - Lambda
   - Kappa alternative
   - trade-offs
5. System Architecture
6. Data Sources & Simulation
7. Kafka Ingestion Design
8. Spark Stream Processing
9. Batch Layer & Airflow
10. Storage & Serving
11. Dashboard & Business Outputs
12. Observability
13. Testing
14. Results
15. Limitations
16. Production-Scale Evolution
17. Team Contributions
18. Conclusion

Avoid marketing language in the academic core. Clearly distinguish what was actually implemented from future commercial ideas.

---

# 33. Production-Scale Evolution — Future, Not Phase 1

After the assessment, the simulators can be replaced by:

```text
Huawei / Sungrow / SMA / SolarEdge / SCADA / Meter / Weather APIs
                            |
                            v
                          Kafka
```

Possible later features:

- real performance ratio,
- specific yield,
- inverter/string benchmarking,
- advanced loss attribution,
- forecast comparison,
- automatic daily/monthly reports,
- technician workflows,
- work orders,
- SLA tracking,
- multi-tenant organizations,
- portfolio benchmarking,
- BESS/wind support,
- AI-assisted diagnosis.

These should be mentioned as future work, not built during the two-week mini-project unless the required scope is already complete.

---

# 34. Commercial Direction

The commercial positioning should eventually be:

> **Solar Portfolio Performance & Revenue Intelligence**

rather than a generic inverter monitoring dashboard.

The long-term value proposition is:

```text
Raw Telemetry
     |
     v
Operational Performance
     |
     v
Energy Loss
     |
     v
Financial Impact
     |
     v
Prioritized Action
```

This business direction is useful for the CV and future commercialization, but the university implementation should remain focused on the required data-engineering outcomes.

---

# 35. Rules for Future Member-Specific Claude Code Prompts

After this master specification is approved, create **three separate Markdown execution prompts**, one for each member.

Each member prompt must:

1. identify that member's exact ownership,
2. include all shared contracts they must obey,
3. forbid scope changes,
4. give an ordered implementation sequence,
5. require repository inspection before changes,
6. require a plan before coding each milestone,
7. require tests at each milestone,
8. require a Git commit after each logical step,
9. provide expected commit-message examples,
10. include acceptance criteria for every milestone,
11. list files/directories the member owns,
12. identify files/interfaces they may consume but should not change,
13. define integration checkpoints,
14. explain how to communicate any required contract change,
15. prohibit hard-coded final-demo data,
16. require production-quality error handling,
17. require documentation updates,
18. require clean lint/test state,
19. instruct Claude Code to stop and report blockers rather than making incompatible assumptions,
20. finish with a subsystem-specific Definition of Done.

The prompts should be detailed enough that each member can open their own Claude Code session and implement their work independently while remaining compatible with the common architecture.

---

# 36. Final Team Principle

The priority order is:

```text
1. Fulfil every module requirement.
2. Make the end-to-end system reliable.
3. Make architecture and calculations explainable.
4. Make the demo deterministic.
5. Keep engineering quality professional.
6. Polish the dashboard.
7. Only then consider optional features.
```

Do not sacrifice completion or correctness for extra features.

**A small, complete, explainable and reproducible SolarIQ system is better than a larger half-finished platform.**
