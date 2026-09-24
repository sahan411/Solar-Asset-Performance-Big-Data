# Run Book: Smart Grid Energy Monitoring & Billing

This is a step-by-step guide to setting up, running, checking, demonstrating
and resetting the project. Follow the steps **in order**.

For how the project works and why it is built this way, see [README.md](README.md).

---

## Contents

1. [What you need](#1-what-you-need)
2. [First-time setup](#2-first-time-setup)
3. [Start the system](#3-start-the-system)
4. [Check that everything works](#4-check-that-everything-works)
5. [The user interfaces (UIs)](#5-the-user-interfaces-uis)
6. [What happens over time](#6-what-happens-over-time)
7. [Demo script](#7-demo-script-about-10-minutes)
8. [Everyday commands](#8-everyday-commands)
9. [Stop, restart and reset](#9-stop-restart-and-reset)
10. [Run the tests](#10-run-the-tests)
11. [Troubleshooting](#11-troubleshooting)
12. [Quick reference](#12-quick-reference)

---

## 1. What you need

| Requirement | Why | Check with |
|---|---|---|
| **Docker Desktop** (with Compose v2) | runs all 9 services | `docker --version` and `docker compose version` |
| **4 GB of free memory** | Kafka, Spark and Airflow are the heaviest | Docker Desktop → Settings → Resources |
| **About 5 GB of free disk space** | Docker images (about 3 GB) + data | – |
| **Internet** (first time only) | to download the Docker images | – |
| **Git** | to get the code | `git --version` |
| *Optional:* Python 3.11 and Java 17+ | only needed to run the unit tests | `python --version`, `java -version` |

**These ports must be free:**

| Port | Used by |
|---|---|
| 3000 | Grafana |
| 8000 | API |
| 8080 | Airflow |
| 5432 | PostgreSQL |
| 29092 | Kafka (access from your computer) |

> **Windows:** make sure Docker Desktop is **running** (whale icon in the
> taskbar) before you run any `docker` command.

---

## 2. First-time setup

### Step 2.1: Get the code

```bash
git clone <repository-url>
cd Solar-Asset-Performance-Big-Data
git checkout simplified/smart-grid
```

### Step 2.2: Download and build the images

```bash
docker compose build
docker compose pull
```

- This downloads about 3 GB, the biggest images being Airflow, Spark and Grafana.
- On a slow connection it can take 30–60 minutes. **You only do this once.**
- This step is optional, because `docker compose up` does it automatically,
  but doing it first means the demo starts quickly.

No `.env` file or other configuration is needed. All settings are in
`docker-compose.yml`.

---

## 3. Start the system

### Step 3.1: Start everything

```bash
docker compose up -d --build
```

This starts 9 services, in this order (Docker waits for each dependency):

| Order | Service | What it does |
|---|---|---|
| 1 | `postgres` | the database; creates all tables on first start |
| 1 | `kafka` | the message queue |
| 1 | `tariff-simulator` | drops one tariff CSV file per simulated day |
| 2 | `kafka-init` | creates the `meter-readings` topic (3 partitions), then exits |
| 3 | `meter-simulator` | sends meter readings to Kafka every ~3 seconds |
| 3 | `spark-streaming` | cleans readings, calculates hourly totals, raises alerts |
| 3 | `airflow` | batch layer: reads Kafka every minute into `data/raw/`, then calculates the daily bills |
| 3 | `api` | the REST API |
| 3 | `grafana` | the dashboard and alert rules |

### Step 3.2: Wait about 1 minute, then check the status

```bash
docker compose ps
```

Expected result:

```text
SERVICE            STATUS
airflow            Up
api                Up
grafana            Up
kafka              Up (healthy)
kafka-init         Exited (0)      <- normal: it created the topic and finished
meter-simulator    Up
postgres           Up (healthy)
spark-streaming    Up
tariff-simulator   Up
```

> If a service shows `Restarting` or `Exited (1)`, see [Troubleshooting](#11-troubleshooting).

---

## 4. Check that everything works

Run these checks in order, about 1–2 minutes after starting.

### Step 4.1: Data is arriving

```bash
curl localhost:8000/health
```

Expected: `"status":"ok"`.

`"no_data"` in the first minute is normal; wait and try again.

### Step 4.2: Live grid numbers exist

```bash
curl localhost:8000/api/grid/current
```

Expected: 3 areas (`central`, `north`, `south`), each with `grid_load_kwh` and `renewable_pct`.

### Step 4.3: The meter simulator is sending

```bash
docker compose logs --tail 3 meter-simulator
```

Expected: `"event": "readings_sent"` lines, with `"sent": 30`.

### Step 4.4: Spark is processing

```bash
docker compose logs --tail 5 spark-streaming
```

Expected: `"event": "readings_stored"` and `"event": "zone_load_updated"` lines.

### Step 4.5: Airflow is reading Kafka into the raw copy (batch layer)

```bash
ls data/raw
```

Expected (after about 1–2 minutes): `date=2026-01-01` and `date=unparsed`
folders and a `_latest_event_time` file. They grow every minute, each time
Airflow's `ingest_from_kafka` task runs.

### Step 4.6: Airflow has loaded the billing job

```bash
docker compose exec airflow airflow dags list
```

Expected: `smart_grid_daily_billing`, with `is_paused` set to `False`.

### Step 4.7: The first bill is ready (about 6 minutes after start)

```bash
curl localhost:8000/api/bills/latest
```

Expected: a summary for `2026-01-01` with `"households_billed": 30`.

The CSV report is in `data/reports/`.

✅ If all seven checks pass, the whole pipeline is working.

---

## 5. The user interfaces (UIs)

| UI | Address | Login | Use it for |
|---|---|---|---|
| **Grafana** (dashboard) | http://localhost:3000 | none to view; `admin` / `admin` to edit | the main screen: live grid, bills, pipeline health, alerts |
| **API docs** (FastAPI) | http://localhost:8000/docs | – | trying every API endpoint in the browser |
| **Airflow** | http://localhost:8080 | `admin` / `admin` | billing job runs, task logs, retries |
| **PostgreSQL** | `localhost:5432`, database `smartgrid` | `grid` / `grid` | querying tables directly (DBeaver, pgAdmin or `psql`) |
| **Kafka** | `localhost:29092` (from your computer), `kafka:9092` (inside Docker) | – | reading raw messages (see [section 8](#8-everyday-commands)) |

### 5.1 Grafana (http://localhost:3000)

It opens straight onto the **Smart Grid Monitoring & Billing** dashboard, which
refreshes every 5 seconds.

| Section | Panels | What to look for |
|---|---|---|
| **Real-time grid** (speed layer) | latest simulated hour, total load, renewable %, alert count, days billed; a table per area; alerts table; 24-hour bar charts | the renewable % cell turns **red** below 20% during a storm |
| **Daily billing report** (batch layer) | report date, homes billed, total billed, unbilled homes, raw readings rejected by the batch layer; a bill table for every home | appears after the first day is billed (about 6 minutes) |
| **Pipeline health** (observability) | seconds since last reading, seconds since last raw-archive write, readings stored, invalid %, duplicates, end-to-end delay; rejected readings by reason; latest batches; latest rejected readings with trace IDs | freshness should stay under 30 s; invalid % around 2% |

**Alert rules:** menu (☰) → **Alerting** → **Alert rules** → folder *Smart Grid*.

| Rule | Fires when |
|---|---|
| No meter data received for 60 seconds | nothing has reached the database for 60 s (health check) |
| Invalid reading rate above 5% | too many bad readings in 5 minutes (error rate) |
| Batch ingestion has not run for 3 minutes | Airflow stopped reading Kafka, so the batch layer gets no new data (health check) |
| Low renewable contribution in a grid zone | Spark raised a low-renewable alert in the last 2 minutes |

Every alert is also **sent to the API** (`POST /api/alert-webhook`), which logs
it. See the delivered alerts with:

```bash
docker compose logs api | grep grafana_alert
```

Each rule is *Normal*, *Pending* (condition true, waiting) or *Firing*.

### 5.2 API docs (http://localhost:8000/docs)

Click an endpoint → **Try it out** → **Execute**.

| Endpoint | Returns |
|---|---|
| `GET /health` | `ok` / `stale` / `no_data` / `down` |
| `GET /api/grid/current` | live load, solar and renewable % per area |
| `GET /api/grid/history?hours=24` | hourly totals per area |
| `GET /api/alerts` | the latest low-renewable alerts |
| `GET /api/bills/latest` | the latest daily billing report |
| `GET /api/bills/{date}` | one day's report, e.g. `2026-01-01` |
| `GET /api/metrics` | readings in, rejected, duplicates, delay, totals |
| `GET /api/trace/{trace_id}` | the full journey of one reading |

### 5.3 Airflow (http://localhost:8080)

1. Log in with `admin` / `admin`.
2. Click **smart_grid_daily_billing**.
3. **Grid** view: one column per run.
   - `ingest_from_kafka` is **green in every run**: it reads the new Kafka messages each minute.
   - The billing tasks are **pink (skipped)** when no finished day is ready yet. This is normal.
   - They are **green (success)** when a day was billed (about every 5 minutes).
4. The graph has 6 tasks: `ingest_from_kafka` → `pick_day` → (`load_tariffs` and
   `process_raw_readings` in parallel) → `compute_bills` → `export_report`.
5. Click a green box → **Logs** to see its JSON log lines, e.g.
   `kafka_messages_ingested` (how many messages were read) or
   `tariff_row_rejected` (on day 3).

### 5.4 PostgreSQL

```bash
docker compose exec postgres psql -U grid -d smartgrid
```

Useful queries:

```sql
SELECT * FROM billing_runs;                                        -- billed days
SELECT * FROM daily_bills WHERE bill_date = '2026-01-01' LIMIT 5;  -- bills
SELECT * FROM zone_load_hourly ORDER BY window_start DESC LIMIT 6; -- live totals
SELECT * FROM alerts ORDER BY created_at DESC;                     -- alerts
SELECT reason, COUNT(*) FROM rejected_readings GROUP BY reason;    -- bad data
SELECT * FROM pipeline_metrics ORDER BY recorded_at DESC LIMIT 5;  -- metrics
```

Type `\q` to exit.

---

## 6. What happens over time

**1 simulated day = 5 real minutes.** Everything starts at 2026-01-01 00:00.

| Real time after start | Simulated time | What you will see |
|---|---|---|
| 0–1 min | day 1, 00:00–05:00 | readings start; the dashboard fills in; `/health` becomes `ok` |
| about 2–3 min | day 1, 10:00–14:00 | **storm over `north`**: renewable % drops to ~3%, and 4 alerts appear |
| about 6 min | day 2, 01:00 | Airflow bills **2026-01-01**; the report appears in Grafana and `data/reports/` |
| about 7–8 min | day 2, 10:00–14:00 | storm over `central` |
| about 11 min | day 3, 01:00 | **2026-01-02** billed |
| about 12–13 min | day 3, 10:00–14:00 | storm over `south` |
| about 16 min | day 4, 01:00 | **2026-01-03** billed: 1 broken tariff row rejected, 29 billed + **1 unbilled** |
| then | every 5 min | a new day is billed; the storm keeps rotating north → central → south |

Because the data uses a fixed seed, **every fresh run produces exactly the same
numbers**. For example, day 1 always totals **98.62**.

---

## 7. Demo script (about 10 minutes)

**Before the demo:** reset to a clean start (see [section 9.3](#93-reset-everything-start-again-from-day-1)),
then start the system about 3 minutes before you begin presenting.

| # | Show | How | Point to make |
|---|---|---|---|
| 1 | All services running | `docker compose ps` | one command starts the whole system |
| 2 | The live source | `docker compose logs --tail 5 meter-simulator` | 30 readings every ~3 s, with the assignment's fields |
| 3 | The live dashboard | Grafana → *Real-time grid* | speed layer: live load and renewable % per area |
| 4 | The storm alert | Grafana alerts table, `GET /api/alerts` | threshold alert: renewable % < 20% |
| 5 | Spark cleaning the data | Grafana → *Rejected readings by reason* | bad records rejected with a reason; duplicates dropped |
| 6 | Daily billing | Airflow UI → green run → `compute_bills` log | batch layer: tariffs joined with readings |
| 7 | The daily report | Grafana *Daily billing report*, and the CSV in `data/reports/` | the consolidated daily report per home |
| 8 | The failure alert | see below | observability: health check and alert rule |
| 9 | Tracing | see below | follow one reading through every stage |
| 10 | Bad batch data (after day 3) | Airflow `load_tariffs` log, *Unbilled households* panel | batch validation |
| 11 | Layers are independent | see below | the batch layer keeps billing with Spark stopped |

**Step 8: make an alert fire, then recover**

```bash
docker compose stop meter-simulator
# wait about 2 minutes:
#   Grafana → Alerting → "No meter data received for 60 seconds" shows Firing
#   curl localhost:8000/health  shows "stale"
docker compose start meter-simulator
# recovers within seconds; the missed readings are re-sent automatically
```

**Step 9: follow one reading (tracing)**

```bash
# 1. pick a rejected reading's trace ID (also shown in the dashboard's last panel)
docker compose exec postgres psql -U grid -d smartgrid -c "SELECT trace_id, reason FROM rejected_readings ORDER BY rejected_at DESC LIMIT 1;"

# 2. find it in the logs of each stage
docker compose logs meter-simulator spark-streaming | grep <trace_id>

# 3. see its full journey
curl localhost:8000/api/trace/<trace_id>
```

**Step 11: prove the two layers are independent (Lambda)**

```bash
docker compose stop spark-streaming
# the live dashboard stops updating and "No meter data" fires,
# but Airflow keeps reading Kafka (ls data/raw) and keeps billing new days
docker compose start spark-streaming
```

---

## 8. Everyday commands

### Logs

```bash
docker compose logs -f spark-streaming               # follow one service (Ctrl+C to stop)
docker compose logs --tail 20 meter-simulator        # last 20 lines
docker compose logs spark-streaming | grep rejected  # search the logs
```

Service names: `meter-simulator`, `tariff-simulator`, `spark-streaming`,
`airflow`, `api`, `grafana`, `kafka`, `postgres`.

### Files produced

```bash
ls data/incoming        # daily tariff files   (tariffs_2026-01-01.csv, ...)
ls data/reports         # daily billing reports (daily_billing_report_2026-01-01.csv, ...)
ls data/raw             # Airflow's raw copy of Kafka: one folder per day (date=2026-01-01, ...)
head -c 400 data/raw/date=2026-01-01/part-0.jsonl   # one raw message, exactly as received
```

### API from the command line

```bash
curl localhost:8000/health
curl localhost:8000/api/grid/current
curl localhost:8000/api/alerts
curl localhost:8000/api/bills/latest
curl localhost:8000/api/bills/2026-01-01
curl localhost:8000/api/metrics
```

### Kafka

```bash
# read 5 raw messages
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic meter-readings --from-beginning --max-messages 5

# topic details (3 partitions)
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic meter-readings
```

### Airflow

```bash
docker compose exec airflow airflow dags list
docker compose exec airflow airflow dags list-runs -d smart_grid_daily_billing
```

---

## 9. Stop, restart and reset

### 9.1 Stop (keeps all data)

```bash
docker compose down
```

Start again later with `docker compose up -d`. The simulated clock keeps
running while stopped, so after a restart the meters re-send up to one
simulated day of missed readings.

### 9.2 Restart one service

```bash
docker compose restart spark-streaming
```

### 9.3 Reset everything (start again from day 1)

Do this **before every demo** so it starts clean from 2026-01-01.

```bash
docker compose down -v      # stop and delete the database, Kafka data and Spark checkpoints
rm -rf data                 # delete the raw copy, tariff files, reports and the simulated clock
docker compose up -d        # start fresh
```

On **Windows PowerShell**, use this instead of `rm -rf data`:

```powershell
Remove-Item -Recurse -Force data
```

> `down -v` deletes all stored data. Source code and configuration are never touched.
>
> **Do both steps.** `down -v` clears the database but not the `data` folder,
> which holds the simulated clock. If you skip `rm -rf data`, the clock
> continues from the old run (e.g. from 2026-01-28 instead of 2026-01-01).

---

## 10. Run the tests

The tests run on your computer, not in Docker.

```bash
python -m venv .venv
# activate it:
#   Windows:     .venv\Scripts\activate
#   Mac/Linux:   source .venv/bin/activate
pip install -r requirements-dev.txt
python -m pytest
```

Expected: `38 passed`.

| Test file | Tests |
|---|---|
| `tests/test_simulators.py` | the data sources (fields, same seed → same data, storm, bad records, tariff files) |
| `tests/test_stream_validation.py` | the Spark cleaning and hourly totals (needs Java 17+) |
| `tests/test_batch_readings.py` | Airflow's Kafka ingestion and the batch layer's own cleaning and daily totals |
| `tests/test_billing.py` | tariff checks and the bill formula |

---

## 11. Troubleshooting

| Problem | Cause | Fix |
|---|---|---|
| `error during connect ... dockerDesktopLinuxEngine` | Docker Desktop is not running | start Docker Desktop and wait until it says *running* |
| `port is already allocated` | another program uses 3000, 8000, 8080, 5432 or 29092 | stop that program (e.g. a local PostgreSQL), or change the left-hand port in `docker-compose.yml` |
| First start takes very long | images are still downloading (about 3 GB) | wait; check progress with `docker compose pull` |
| `/health` shows `no_data` | Spark is still starting (about 30–60 s) | wait a minute; check `docker compose logs spark-streaming` |
| `/health` shows `stale` | the meter simulator or Spark has stopped | `docker compose ps`, then `docker compose start meter-simulator` or `docker compose restart spark-streaming` |
| http://localhost:3000 shows `{"error":"not found"}` | another program is using port 3000 on `127.0.0.1`, e.g. **VS Code port forwarding** | in VS Code open the **Ports** tab and stop forwarding port 3000 (or close that program), then reload |
| No new bills, and the "Batch ingestion" alert fires | Airflow is stopped or its tasks are failing | `docker compose ps` / Airflow UI; `docker compose start airflow`. Kafka kept the messages, so the next run reads them all; nothing is lost |
| Spark log shows `TimeoutException ... Kafka` | usually after the PC slept or Kafka restarted | nothing to do: it restarts itself (`restart: unless-stopped`); or run `docker compose restart spark-streaming` |
| No bills after 6+ minutes | Airflow still starting, or no day finished yet | check `docker compose logs airflow`; in the Airflow UI, runs should be pink (skipped) or green |
| Airflow UI won't load | the webserver takes 1–2 minutes to start | wait, then refresh http://localhost:8080 |
| Grafana panels show *No data* | no data yet, or the billing panels before the first bill | wait; billing panels fill in after about 6 minutes |
| Dates look strange (e.g. 2026-04-xx) | the system ran for a long time (1 real hour = 12 simulated days) | reset ([9.3](#93-reset-everything-start-again-from-day-1)) |
| Simulation doesn't restart at 2026-01-01, or old days show 0 homes billed | a **half reset**: `docker compose down -v` was run but the `data` folder was kept, so the old clock and tariff files survived while the database was wiped | always do **both** `docker compose down -v` **and** `rm -rf data` ([9.3](#93-reset-everything-start-again-from-day-1)) |
| Git Bash: `docker compose exec kafka /opt/kafka/...` says *no such file* | Git Bash rewrites `/opt/...` paths | put `MSYS_NO_PATHCONV=1` in front of the command, or use PowerShell |
| The computer is slow | the stack uses about 4 GB of memory | give Docker more memory, or close other programs |

**Where to look first:**

```bash
docker compose ps                          # which service is not "Up"?
docker compose logs --tail 50 <service>    # what did it log?
curl localhost:8000/api/metrics            # is data flowing? what is the invalid %?
```

---

## 12. Quick reference

```text
START        docker compose up -d --build
STATUS       docker compose ps
HEALTH       curl localhost:8000/health
LOGS         docker compose logs -f <service>
STOP         docker compose down
RESET        docker compose down -v  &&  rm -rf data  &&  docker compose up -d
TESTS        python -m pytest

GRAFANA      http://localhost:3000        (view: no login | edit: admin / admin)
API DOCS     http://localhost:8000/docs
AIRFLOW      http://localhost:8080        (admin / admin)
POSTGRES     localhost:5432  db=smartgrid (grid / grid)
KAFKA        localhost:29092              topic=meter-readings

1 simulated day = 5 real minutes  |  first bill ≈ 6 min  |  day-3 bad tariff row ≈ 16 min
```
