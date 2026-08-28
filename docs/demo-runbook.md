# SolarIQ — Assessment Demo Runbook

> Owner: Member 3 (serving/UI). This is the deterministic script for the
> 5–10 minute assessment demo. Follow it verbatim — do not improvise on the
> day.

The run is seeded (`SIMULATION_SEED=8203`): every rehearsal produces the same
events at the same simulated-time offsets. Rehearse this exact sequence at
least once before the real assessment.

---

## 1. Before evaluation

From a clean checkout of the integration branch:

```bash
git checkout main
cp .env.example .env
./scripts/bootstrap.sh
```

`bootstrap.sh` generates local credentials into `.env` (gitignored), starts
every Docker Compose service — Kafka, PostgreSQL, MinIO, Prometheus, Airflow,
**and now the `api` and `dashboard` services** — and waits for the
infrastructure health checks. Airflow's own startup (`airflow-init`) applies
the database migrations and seeds the plant/inverter registry, so PostgreSQL
already has the asset registry by the time `bootstrap.sh` finishes.

Then reset to a clean state:

```bash
./scripts/demo_reset.sh --all --yes
```

Expected output ends with:

```text
Reset complete. The next run starts from a clean state and, because the
simulator is seeded, will reproduce exactly the same events as the last one.
```

### Verify before you walk away from setup

```bash
docker compose ps
curl -sf http://localhost:8000/health   && echo " api: healthy"
curl -sf http://localhost:8000/ready    && echo " api: ready"
curl -sf http://localhost:5173/         > /dev/null && echo "dashboard: serving"
```

If `api` is not healthy, check `docker compose logs api` — the most common
cause is `POSTGRES_PASSWORD` not yet propagated; `docker compose restart api`
after Postgres reports healthy resolves it.

**Pre-pull images** the night before if venue internet is uncertain:

```bash
docker compose build
docker compose pull kafka postgres minio prometheus
```

---

## 2. Start the demo

The Spark Structured Streaming job is **not** a Compose service — like the
simulator, it runs on the host so its logs are visible directly. Start it
first, in its own terminal, with host-reachable addresses (the "Host
execution" overrides from `.env.example`):

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:29092 \
DATABASE_URL=postgresql://solariq:<password from .env>@localhost:5432/solariq \
MINIO_ENDPOINT=http://localhost:9000 \
SPARK_CHECKPOINT_DIR=./data/spark-checkpoints \
python -m processing.streaming.job --master 'local[2]'
```

(`SPARK_CHECKPOINT_DIR` defaults to the container path `/spark-checkpoints`,
which does not exist on the host — override it to a local writable directory,
the same way `SIMULATION_OUTPUT_DIR` is overridden for host execution.)

Wait for its structured logs to show `stream_started` (a few seconds) before
starting the simulator, in a second terminal:

```bash
./scripts/demo_start.sh
```

This runs the Python simulator **on the host** (not in Docker) for 1.05
simulated days, then stops. Use `--forever` to run until you press Ctrl-C
instead, if you want buffer time before the scripted anomaly window.

While it runs, in a third terminal, optionally confirm telemetry is flowing:

```bash
kafka/scripts/verify_topics.sh --consume 5
```

### URLs

| Service | URL |
|---|---|
| Dashboard | http://localhost:5173 |
| API docs (OpenAPI) | http://localhost:8000/docs |
| API health / ready / metrics | http://localhost:8000/health, /ready, /metrics |
| Prometheus | http://localhost:9090 |
| Prometheus alerts | http://localhost:9090/alerts |
| Airflow | http://localhost:8080 (user `admin`, password `admin`) |
| MinIO console | http://localhost:9001 |

**Browser gotcha:** the dashboard's JavaScript runs in your browser, not
inside Docker, so it must call the API at `http://localhost:8000` — never the
Compose service name `api`. This is already the default
(`dashboard/.env.example`); only worry about it if you changed
`VITE_API_BASE_URL`. If the dashboard was rebuilt with a different
`VITE_API_BASE_URL`, the value is baked into the JS bundle at build time —
changing `.env` afterwards does nothing until you rebuild the `dashboard`
image.

**Cache gotcha:** if you rebuilt the dashboard image with a different API
URL, hard-refresh the browser (Ctrl+Shift+R) — a cached bundle will keep
calling the old address.

---

## 3. Demo timeline

The simulated clock: **1 simulated day = 300 real seconds** (288× real time),
telemetry every 3 real seconds. The anomaly schedule below repeats every
simulated day and is defined in `docs/data-contracts.md` section 7 — this is
what makes the demo identical on every rehearsal.

```text
0:00–1:00   Setup
  - Show the architecture slide / docs/architecture.md's Lambda diagram.
  - `docker compose ps` — every service healthy.
  - Open the dashboard at http://localhost:5173 — Portfolio Overview.

1:00–2:30   Live path
  - Point at the "Current Generation" KPI changing on its own (5s poll).
  - Briefly show Kafka receiving events: kafka/scripts/verify_topics.sh --consume 5
  - Switch to the Spark job's terminal (started in section 2) — its structured
    JSON logs show microbatch_processed events as they land.

2:30–4:00   Business alert — underperformance
  - Simulated-day window 90–150s: PLANT_03 / INV_02 drops to 45% power,
    status=WARNING, irradiance stays normal — the fault is visible only by
    comparing power against irradiance, not from a raw power threshold.
  - After ALERT_SUSTAIN_SECONDS (~12 real seconds under the demo clock), the
    alert opens. Show it on /alerts and in the Portfolio page's "Active
    Alerts" panel. Point out the severity badge and the estimated-loss figure
    growing on each poll while the fault is open.

4:00–5:30   Operational vs. pipeline alert
  - Window 190–235s: PLANT_04 / INV_01 goes OFFLINE (reported zero,
    availability=0) — an operational alert.
  - Window 235–260s: PLANT_05 goes silent entirely (TELEMETRY_GAP — no
    events at all, not a zero) — a pipeline-health condition, not a business
    one. Show docs/architecture.md section 5's distinction: OFFLINE is
    "the asset is broken" (alerts table); TELEMETRY_GAP is "we lost sight of
    the asset" (pipeline_health / Prometheus). Show Prometheus alerts
    (http://localhost:9090/alerts) alongside the dashboard's alert to make
    the contrast concrete.

5:30–7:30   Batch reconciliation
  - The DAG's schedule is deliberately `None` (a wall-clock cron schedule
    means nothing under a 288x-compressed clock) — trigger it once the
    simulated day's reference CSV has landed in `./data/daily/`:

    ```bash
    docker compose exec airflow-webserver airflow dags trigger \
      solariq_daily_reconciliation \
      --conf '{"simulation_date": "2026-08-21"}'
    ```

    Replace the date with `SIMULATION_START_DATE` from your `.env` if you
    changed it from the default. You can trigger the same run from the
    Airflow UI (Trigger DAG w/ config) instead if you prefer showing the UI.
  - Show it in the Airflow UI (http://localhost:8080) — the DAG graph and its
    green run. It takes well under a minute for one simulated day.
  - Refresh the dashboard's /reports/daily — the report now renders instead
    of "No daily report is available for this simulated date yet."

7:30–9:00   Expected vs. actual, lost energy, lost revenue
  - Walk the Portfolio Summary KPIs: Actual Generation, Expected Generation,
    Lost Energy, Lost Revenue.
  - Point out the portfolio performance figure is energy-weighted (sum kWh,
    then divide) — never an average of the five plants' own percentages;
    docs/member-2-handoff.md explains why that matters at portfolio scale.
  - Show the Best/Worst Performer cards and the per-plant table.
  - Optionally: click "Export CSV" and open the downloaded file — it is the
    exact numbers already on screen, not a second calculation.

9:00–10:00  Observability and limitations
  - http://localhost:8000/metrics — solariq_api_requests_total and friends.
  - Structured JSON logs: docker compose logs api --tail 20 (or the
    stream/batch services' logs for the pipeline side).
  - State plainly: Phase 1 uses simulated data throughout (no real inverter/
    SCADA/meter/weather integration); the expected-power proxy is
    intentionally simplified (docs/architecture.md section 7); money uses a
    fictional simulated PPA rate.
```

Adjust the clock allocation to fit your actual assessment slot; the anomaly
windows themselves are fixed by `docs/data-contracts.md` and do not move.

---

## 4. If something looks wrong mid-demo

```bash
# Is the pipeline alive?
curl -s http://localhost:8000/ready

# Is data actually arriving?
curl -s http://localhost:8000/api/v1/portfolio/live

# What's currently wrong?
curl -s "http://localhost:8000/api/v1/alerts?status=active"

# Pipeline-level health (engineering, not solar)
docker compose logs api --tail 50
```

The full smoke test (`tests/e2e/smoke_test.py`) automates exactly these
checks with bounded polling instead of guesswork:

```bash
pip install -r tests/e2e/requirements.txt
python tests/e2e/smoke_test.py            # health/ready/live/plants/alert
python tests/e2e/smoke_test.py --full     # also waits for the daily report
```

If the dashboard shows a red error panel instead of data: check the browser
console first (an offline API surfaces there), then `curl
http://localhost:8000/health` from the same machine the browser is on — a
firewall or an unrebuilt `VITE_API_BASE_URL` are the two most common causes.

---

## 5. Reset and rehearse again

`demo_reset.sh` deletes and recreates the Kafka topics, which invalidates a
running stream job's consumer/checkpoint state — stop it (Ctrl-C in its
terminal) before resetting, and delete its checkpoint directory so it starts
clean rather than trying to resume against topics that no longer exist:

```bash
# stop the streaming job (Ctrl-C in its terminal), then:
rm -rf ./data/spark-checkpoints
./scripts/demo_reset.sh --all --yes
# restart the streaming job (section 2's command), then:
./scripts/demo_start.sh
```

Because the simulator is seeded, a reset-and-rerun reproduces the identical
sequence of events at the identical simulated-time offsets — rehearse until
the timeline above is comfortable, then stop touching anything.

---

## 6. Known limitations to state during the demo

- Simulated data throughout — no real inverter, SCADA, meter, or weather
  integration (Phase 1 scope, master specification).
- The expected-power proxy is a simplified irradiance ratio, not a bankable
  PV model (docs/architecture.md, section 7).
- Money figures use a fictional, simulated PPA rate stored as
  `DOUBLE PRECISION` — a real settlement system would use `NUMERIC`.
- Live availability is sampled at each microbatch, not continuously measured
  uptime.
- No authentication in Phase 1 — out of scope by design, not an oversight.
