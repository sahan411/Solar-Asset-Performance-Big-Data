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


# Member 3 Execution Prompt
## FastAPI, Dashboard, Daily Reporting, Integration QA & Demo Experience

**Assigned branch:** `member-3/serving-ui`

## 1. Mission

You own the **serving layer and assessment-facing experience**.

Your work must prove that the data-engineering platform produces useful business outcomes.

The dashboard is not a mockup. It must consume real FastAPI responses backed by PostgreSQL populated by the live/batch pipelines.

Your scope includes:

- API,
- typed response models,
- health/readiness,
- portfolio/plant/alert/report endpoints,
- React dashboard,
- loading/error/empty states,
- daily report view,
- integration smoke tests,
- deterministic demo runbook,
- final assessment experience.

You do not own Kafka, Spark, Airflow, or the simulator implementation.

---

# 2. Files/Areas You Own

Primary ownership:

```text
api/
dashboard/
reports/
tests/integration/
tests/e2e/
docs/demo-runbook.md
```

Shared with care:

```text
docker-compose.yml
.env.example
README.md
docs/architecture.md
docs/data-contracts.md
```

Do not change:

```text
simulators/
kafka/
processing/
orchestration/
storage/migrations/
```

unless the team explicitly requests an integration fix.

---

# 3. Milestone 0 — Inspect Repo & Serving Contract

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
storage/migrations/*
processing/*
```

Inspect actual DB column names rather than guessing.

If storage tables are not committed yet, create API interfaces/models from the master contract but do not invent conflicting schema.

## Commit

No commit unless structural fixes are needed.

---

# 4. Milestone 1 — FastAPI Application Skeleton

## Goal

Create a clean small serving application.

Suggested structure:

```text
api/
├── pyproject.toml or requirements.txt
├── Dockerfile
└── app/
    ├── main.py
    ├── config.py
    ├── db.py
    ├── logging.py
    ├── dependencies.py
    ├── models/
    │   ├── portfolio.py
    │   ├── plant.py
    │   ├── alert.py
    │   └── report.py
    ├── repositories/
    │   ├── portfolio.py
    │   ├── plants.py
    │   ├── alerts.py
    │   └── reports.py
    └── routers/
        ├── portfolio.py
        ├── plants.py
        ├── alerts.py
        └── reports.py
```

Keep business SQL in repositories, not route handlers.

## Configuration

Environment variables:

```text
DATABASE_URL=postgresql://...
API_HOST=0.0.0.0
API_PORT=8000
LOG_LEVEL=INFO
CORS_ORIGINS=http://localhost:5173
STALE_DATA_SECONDS=60
```

No auth for Phase 1.

## Commit

```text
feat(api): initialize typed SolarIQ FastAPI service
```

---

# 5. Milestone 2 — Database Access

## Goal

Use a safe, simple PostgreSQL connection layer.

Either:

- SQLAlchemy 2.x, or
- asyncpg/psycopg with a small repository layer.

Do not introduce a second ORM model schema that attempts to own migrations.

Member 2's migrations are authoritative.

### SQLAlchemy Recommendation

Use Core/text queries or lightweight models bound to existing tables.

Configure connection pooling reasonably.

Handle DB unavailable errors explicitly.

## Repository Rules

- parameterized SQL only,
- no SQL string interpolation with user input,
- date/time values handled as typed values,
- return `None`/empty lists intentionally,
- do not fabricate fallback metrics.

## Commit

```text
feat(api): add PostgreSQL repository layer
```

---

# 6. Milestone 3 — Health, Readiness & Metrics

## `/health`

Meaning:

> API process is alive.

Return:

```json
{
  "status": "ok",
  "service": "solariq-api"
}
```

This should not fail merely because PostgreSQL is unavailable.

## `/ready`

Meaning:

> API is ready to serve real data.

Check:

- database connectivity,
- key table query succeeds.

Possible response:

```json
{
  "status": "ready",
  "database": "ok"
}
```

Return non-2xx if not ready.

## `/metrics`

Expose Prometheus-compatible application metrics.

Recommended:

```text
solariq_api_requests_total
solariq_api_request_duration_seconds
solariq_api_errors_total
```

Avoid high-cardinality path labels. Use normalized route template if possible.

## Structured Logs

Use the project's shared log format:

```text
timestamp
level
service=api
event
message
```

## Commit

```text
feat(api): add health readiness and metrics endpoints
```

---

# 7. Milestone 4 — Portfolio Live Endpoint

Endpoint:

```text
GET /api/v1/portfolio/live
```

## Required Behavior

Query latest `live_portfolio_metrics`.

Also provide useful metadata from plant config/table:

- installed capacity,
- latest metric timestamp,
- data freshness/stale flag.

Suggested response:

```json
{
  "timestamp": "2026-08-21T05:14:45Z",
  "installed_capacity_kw": 25000.0,
  "current_power_kw": 18700.0,
  "avg_power_kw": 18120.0,
  "availability_pct": 98.1,
  "performance_pct": 92.6,
  "online_inverters": 28,
  "offline_inverters": 2,
  "estimated_loss_kw": 1510.0,
  "data_status": "LIVE"
}
```

If `estimated_loss_kw` is not present at portfolio table level, derive it from latest plant metrics only if this is an agreed serving calculation. Prefer Member 2 to expose the required field.

Do not calculate daily revenue loss from instantaneous loss unless clearly labeled.

## Freshness

If latest timestamp older than configured threshold:

```text
data_status = STALE
```

Do not return fake current values.

## Commit

```text
feat(api): expose live portfolio metrics
```

---

# 8. Milestone 5 — Plant Endpoints

## `GET /api/v1/plants`

Return:

```text
id
name
capacity_kw
active
latest performance/status summary if efficiently available
```

## `GET /api/v1/plants/{plant_id}/live`

Return latest plant metric plus:

- plant metadata,
- freshness status.

## `GET /api/v1/plants/{plant_id}/history?from=&to=`

Return time-ordered live metric windows.

Validate:

- ISO timestamps,
- `from <= to`,
- sensible maximum range if needed.

## `GET /api/v1/plants/{plant_id}/inverters`

Return configured inverters and latest status if available.

If Member 2 does not persist per-inverter live state, do not invent it. Either:

- return configuration only, or
- coordinate a minimal serving table/view.

## Error Behavior

Unknown plant:

```text
404
```

Invalid time range:

```text
422 or 400
```

## Commit

```text
feat(api): add plant live history and inverter endpoints
```

---

# 9. Milestone 6 — Alerts API

Endpoint:

```text
GET /api/v1/alerts
```

Supported filter:

```text
status=active
```

Optionally:

```text
plant_id
severity
limit
```

Keep optional filters small.

Response fields:

```text
id
plant_id
inverter_id
alert_type
severity
message
started_at
ended_at
status
estimated_loss_kwh
estimated_revenue_loss
```

Use DB status semantics exactly.

Sort newest/most relevant first.

## Commit

```text
feat(api): expose solar operational alerts
```

---

# 10. Milestone 7 — Daily Portfolio/Report API

## `GET /api/v1/portfolio/daily?date=YYYY-MM-DD`

Read `daily_plant_summary` and aggregate portfolio totals.

### Correct Portfolio Aggregation

```python
total_actual = sum(actual_generation_kwh)
total_expected = sum(expected_generation_kwh)
portfolio_performance_pct = (
    total_actual / total_expected * 100
    if total_expected > 0
    else None
)
total_lost_energy = sum(estimated_lost_energy_kwh)
total_actual_revenue = sum(estimated_actual_revenue)
total_lost_revenue = sum(estimated_lost_revenue)
```

Do not average plant percentages.

Return plant rows too if useful.

## `GET /api/v1/reports/daily?date=...`

Return a structured report payload suitable for the UI.

Suggested:

```json
{
  "simulation_date": "2026-08-21",
  "portfolio": {
    "actual_generation_kwh": 101200,
    "expected_generation_kwh": 110000,
    "performance_pct": 92.0,
    "lost_energy_kwh": 8800,
    "actual_revenue": 15180,
    "lost_revenue": 1320
  },
  "plants": [...],
  "best_performer": {...},
  "worst_performer": {...},
  "generated_at": "..."
}
```

If no report exists yet:

```text
404
```

or a clear empty response, consistently documented.

## Commit

```text
feat(api): expose daily reconciliation report
```

---

# 11. Milestone 8 — API Tests

Use FastAPI TestClient or async client.

Required tests:

```text
/health
/ready success/failure
portfolio live with seeded DB
portfolio stale state
unknown plant
plant history validation
alerts active filter
daily portfolio weighted calculation
daily report no-data behavior
```

Use a test database or transaction fixture.

Do not make unit tests depend on the full Docker stack.

## Commit

```text
test(api): cover SolarIQ serving endpoints
```

---

# 12. Milestone 9 — React Dashboard Foundation

## Goal

Build a restrained professional operations dashboard.

Suggested stack:

```text
React
TypeScript
Vite
TanStack Query or simple fetch hooks
React Router
Recharts or another small chart library
```

Do not add a giant component framework unless the repository already uses one.

Do not spend time on complex design systems.

## Suggested Structure

```text
dashboard/src/
├── api/
│   ├── client.ts
│   └── types.ts
├── components/
│   ├── KpiCard.tsx
│   ├── StatusBadge.tsx
│   ├── DataState.tsx
│   └── charts/
├── pages/
│   ├── PortfolioPage.tsx
│   ├── PlantPage.tsx
│   ├── AlertsPage.tsx
│   └── DailyReportPage.tsx
├── hooks/
└── routes/
```

API base URL from environment:

```text
VITE_API_BASE_URL=http://localhost:8000
```

## UI Principles

- no dramatic gradients,
- no excessive rounded cards,
- no meaningless decorative graphics,
- clear information hierarchy,
- consistent numeric units,
- proper loading/error/empty states,
- responsive enough for laptop evaluation,
- timestamps visible,
- stale state clearly indicated.

## Commit

```text
feat(ui): initialize SolarIQ operations dashboard
```

---

# 13. Milestone 10 — Portfolio Overview Page

Must show:

### KPI Cards

- Installed Capacity
- Current Generation
- Energy Today or latest daily actual where appropriate
- Expected Energy or latest daily expected where appropriate
- Portfolio Performance
- Availability
- Estimated Energy Loss
- Estimated Revenue Loss

Be precise about **live vs daily** values.

Do not display a daily lost-revenue number as if it were live unless the API provides a current accumulated value.

A safe approach:

Top row live:

```text
Installed Capacity
Current Generation
Live Performance
Availability
```

Second row daily reconciliation (after batch exists):

```text
Energy Today/Actual
Expected Energy
Lost Energy
Lost Revenue
```

### Chart

Actual/live portfolio power over time.

If no expected power series exists from API, do not fabricate it.

If expected series is available, show actual vs expected.

### Plant Ranking

Rank by latest or daily performance and label which one.

### Active Alerts

Show recent active alerts.

### Status Table

Per plant:

```text
Plant
Current Power
Performance
Availability
Status
Last Update
```

## Auto Refresh

Use a moderate polling interval such as:

```text
5 seconds
```

Do not poll every few hundred milliseconds.

## Commit

```text
feat(ui): build live portfolio overview
```

---

# 14. Milestone 11 — Plant Detail Page

Route:

```text
/plants/:plantId
```

Show:

- plant name/capacity,
- current power,
- performance,
- availability,
- estimated loss,
- last updated,
- power history chart,
- inverters/config,
- recent alerts.

### Stale Data

If API returns stale:

- show visible `STALE` badge,
- keep last value visible,
- do not pretend it is current.

## Commit

```text
feat(ui): add plant performance detail page
```

---

# 15. Milestone 12 — Alerts Page

Show table:

```text
Severity
Started
Plant
Inverter
Type
Message
Estimated Impact
Status
```

Filters:

```text
All / Active
```

Optional severity filter.

Severity should have clear visual emphasis but remain accessible.

Do not rely only on color; include text/icon.

## Commit

```text
feat(ui): add operational alerts view
```

---

# 16. Milestone 13 — Daily Report Page

Route:

```text
/reports/daily
```

Provide date selection.

Show:

### Portfolio Summary

```text
Actual Generation
Expected Generation
Performance
Availability if available
Lost Energy
Actual Revenue
Lost Revenue
```

### Plant Table

```text
Plant
Actual
Expected
Performance
Availability
Lost Energy
Lost Revenue
Alerts
```

### Ranking

Best/worst performers.

### Report Export

A downloadable CSV is acceptable if simple.

A generated PDF is **not required** by the mini-project unless the team specifically wants it. Do not waste time building complex PDF generation before everything else works.

If export is implemented:

- export current API response,
- include simulation date,
- use stable column names,
- do not introduce a second calculation path.

## Commit

```text
feat(ui): add daily reconciliation report
```

---

# 17. Milestone 14 — Dashboard Reliability States

Every data panel must intentionally handle:

### Loading

Show skeleton/spinner with accessible text.

### Empty

Example:

```text
No daily report is available for this simulated date yet.
```

### Error

Show:

```text
Unable to load portfolio metrics.
```

and retry action if useful.

### Stale

Show:

```text
Data stale — last update 72 seconds ago.
```

### Offline API

Do not crash the entire React app.

## Commit

```text
fix(ui): handle loading stale empty and error states
```

---

# 18. Milestone 15 — Docker Integration

Add/coordinate services:

```text
api
dashboard
```

### API

Health check:

```text
GET /health
```

### Dashboard

Ensure the browser can reach the API using the host-exposed address.

Be careful:

Inside Docker:

```text
api:8000
```

Browser JavaScript cannot normally resolve Docker service name `api` from the host.

Use:

```text
http://localhost:8000
```

for host demo unless routed through a reverse proxy.

Document this clearly.

## Commit

```text
chore(docker): integrate API and dashboard services
```

---

# 19. Milestone 16 — Integration Smoke Test

Create:

```text
scripts/smoke_test.sh
```

if not already owned/created by Member 1, or place the test under `tests/e2e/` and coordinate.

It should verify:

1. API health returns 200.
2. API ready returns 200 after DB available.
3. portfolio endpoint returns non-empty live metrics.
4. at least 5 plants returned.
5. after anomaly window, active alert exists.
6. daily summary exists after Airflow reconciliation.

Use `curl`, `jq`, and SQL checks only if dependencies are documented.

A Python smoke test may be more portable.

### Reference Python Skeleton

```python
import requests
import time

BASE = "http://localhost:8000"

def wait_until(predicate, timeout=120, interval=2):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    raise TimeoutError("condition not reached")
```

Do not use fixed sleeps as the only readiness mechanism.

## Commit

```text
test(e2e): verify live and daily serving paths
```

---

# 20. Milestone 17 — Deterministic Demo Runbook

Create:

```text
docs/demo-runbook.md
```

This is critical.

The runbook must contain exact commands and expected outputs.

## Suggested Structure

### Before Evaluation

```bash
git checkout <integration/final branch>
cp .env.example .env
./scripts/bootstrap.sh
./scripts/demo_reset.sh
```

### Start Demo

```bash
./scripts/demo_start.sh
```

### URLs

Document actual ports, e.g.:

```text
Dashboard:    http://localhost:5173
API Docs:     http://localhost:8000/docs
Prometheus:   http://localhost:9090
Airflow:      http://localhost:8080
MinIO:        http://localhost:9001
```

Only include services actually configured.

### Demo Timeline

Use the shared deterministic schedule.

For each phase specify what to show.

Example:

```text
0:00–1:00
- architecture slide
- docker services healthy

1:00–2:30
- dashboard current generation changes
- Kafka/Spark logs briefly visible

2:30–4:00
- PLANT_03 / INV_02 underperformance begins
- plant performance drops
- alert becomes active

4:00–5:30
- offline or telemetry-gap scenario
- explain difference between business and pipeline alert

5:30–7:30
- simulated day completes
- trigger/show Airflow DAG
- daily report appears

7:30–9:00
- expected vs actual
- lost energy
- lost revenue

9:00–10:00
- Prometheus/logs
- limitations and production evolution
```

Do not make the team improvise the assessment demo.

## Commit

```text
docs(demo): add end-to-end assessment runbook
```

---

# 21. Milestone 18 — README Integration Section

Coordinate with all members.

README should explain:

```text
What SolarIQ is
Architecture summary
Prerequisites
Environment setup
How to start
How to reset demo
How to run tests
URLs
How simulated time works
How to trigger/observe anomalies
How to run daily reconciliation
Project structure
Team contributions
Known limitations
```

Do not claim features not implemented.

## Commit

```text
docs(readme): document integrated SolarIQ workflow
```

---

# 22. Milestone 19 — Frontend/API Tests

Frontend tests should focus on valuable behavior rather than snapshots of everything.

Test:

- KPI formatting,
- stale badge,
- API error state,
- daily report table,
- alert severity/status rendering.

Use mocked API **in tests only**.

Final runtime must use real API.

API tests already described should remain green.

## Commit

```text
test(ui): cover dashboard data states and reporting
```

---

# 23. Milestone 20 — Final Integration QA

Run from a fresh/clean environment if possible.

## Mandatory Checklist

### Startup

- Docker Compose validates.
- All required services start.
- API reaches ready state.
- dashboard loads.

### Live Path

- simulator emits.
- Kafka receives.
- Spark processes.
- Postgres live rows update.
- API returns them.
- dashboard updates.

### Alert Path

- deterministic underperformance appears.
- alert is visible.
- recovery resolves alert if implemented.

### Health Path

- no-telemetry condition can be demonstrated or clearly simulated.
- Prometheus/health state shows it.

### Batch Path

- daily file generated.
- Airflow DAG succeeds.
- daily summary exists.
- report API returns.
- dashboard report renders.

### Quality

- browser console has no major errors,
- API logs have no repeated unexplained exceptions,
- no broken links,
- all displayed units correct,
- timestamps sensible,
- no hard-coded demo values.

## Commit

Only for fixes made during QA:

```text
fix(integration): harden assessment demo workflow
```

---

# 24. UI Formatting Rules

Use consistent units.

Examples:

```text
< 1000 kW          -> display kW
>= 1000 kW         -> optionally display MW

< 1000 kWh         -> display kWh
>= 1000 kWh        -> optionally display MWh
```

Do not change raw API units. Format only for presentation.

Percent:

```text
92.6%
```

Money:

Use a neutral label if the simulated PPA currency is fictional or configurable:

```text
Revenue
Estimated Revenue Loss
```

If the team sets a currency, display it consistently and state it is simulated.

Time:

Display local-friendly UI time but preserve UTC in API.

---

# 25. API Response Design Principles

- stable versioned JSON,
- snake_case,
- ISO 8601 timestamps,
- units included in field names where ambiguity exists,
- nullable values represented as null rather than magic numbers,
- no NaN/Infinity in JSON,
- clear 404 for missing entities/reports,
- no SQL/internal stack traces in responses.

---

# 26. Reference Repository Queries

Latest portfolio:

```sql
SELECT *
FROM live_portfolio_metrics
ORDER BY window_end DESC
LIMIT 1;
```

Latest each plant:

```sql
SELECT DISTINCT ON (plant_id) *
FROM live_plant_metrics
ORDER BY plant_id, window_end DESC;
```

Active alerts:

```sql
SELECT *
FROM alerts
WHERE status = 'ACTIVE'
ORDER BY started_at DESC;
```

Daily rows:

```sql
SELECT *
FROM daily_plant_summary
WHERE simulation_date = $1
ORDER BY performance_pct ASC;
```

Use parameterized equivalents in code.

---

# 27. Demo-Specific Failure Prevention

Before assessment:

1. Do not update dependencies on the final day.
2. Do not upgrade Docker images without a reason.
3. Do not change schemas after the final integration checkpoint.
4. Pre-pull Docker images if internet reliability is uncertain.
5. Verify the demo can run without external APIs.
6. Verify all data sources are local/simulated.
7. Keep a screen-recorded backup demo if allowed by assessment rules.
8. Keep commands in the runbook copy-pasteable.
9. Reset and rehearse the exact deterministic scenario.
10. Ensure Airflow credentials/local access are known.
11. Ensure the browser does not cache an obsolete API base URL.
12. Ensure CORS is configured correctly.
13. Ensure system clock/timezone differences do not break date queries.
14. Ensure daily report date is the **simulated date**, not blindly the host date.

---

# 28. Member 3 Handoff/Contribution Notes

At final integration, document:

- API endpoints implemented,
- dashboard pages implemented,
- report behavior,
- test coverage,
- demo runbook ownership,
- any serving limitations.

This helps the required group contribution statement.

---

# 29. Member 3 Definition of Done

- [ ] FastAPI service starts cleanly,
- [ ] DB access is parameterized and robust,
- [ ] `/health` works,
- [ ] `/ready` checks DB,
- [ ] `/metrics` exposes useful API metrics,
- [ ] portfolio live endpoint returns real DB data,
- [ ] plant endpoints return real DB data,
- [ ] alerts endpoint returns real alert data,
- [ ] daily report API uses `daily_plant_summary`,
- [ ] portfolio percentage is weighted correctly,
- [ ] API handles missing/stale data,
- [ ] React dashboard has four required screens,
- [ ] dashboard never uses hard-coded final metrics,
- [ ] live dashboard refresh works,
- [ ] stale/error/empty/loading states work,
- [ ] daily report renders after Airflow reconciliation,
- [ ] units/timestamps are consistent,
- [ ] API tests pass,
- [ ] meaningful frontend tests pass,
- [ ] integration smoke test passes,
- [ ] Docker API/dashboard integration works,
- [ ] deterministic demo runbook is complete,
- [ ] a fresh demo rehearsal completes inside 10 minutes,
- [ ] no major browser console/API errors remain,
- [ ] README integration instructions are accurate,
- [ ] all commits are logical and tested.

**After this is complete, do not add decorative UI features. Help the team rehearse and stabilize the final system.**
