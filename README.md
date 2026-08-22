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

## Branches

```text
main                          protected integration branch
member-1/platform-ingestion   simulators/, kafka/, observability/, bootstrap+demo scripts
member-2/data-processing      processing/, orchestration/, storage/
member-3/serving-ui           api/, dashboard/, reports/, tests/integration, tests/e2e
```

Each member works on their assigned branch and opens a PR into `main` at the
integration checkpoints defined in the master specification (end of Day 3, Day 6,
Day 9).

## Status

Repository initialized. Contracts frozen per the master specification. Implementation
not yet started — see each member's playbook for the milestone sequence.
