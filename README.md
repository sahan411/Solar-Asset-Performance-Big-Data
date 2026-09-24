# Smart Grid Energy Monitoring & Billing

Applied Big Data Engineering mini-project: **Use Case 3, Smart Grid Energy Monitoring & Billing**.

---

## 1. The problem

A power company has **30 homes** in **3 areas** (`north`, `central`, `south`).
Every home has a smart meter, and some homes also have solar panels.

The company wants answers to two questions:

1. **Right now:** how much electricity is each area using, and how much of it
   comes from solar?
2. **Every day:** what is each home's bill, once that day's electricity price
   (tariff) is applied?

This is the business question from the assignment:
*"What is the current grid load and renewable contribution by zone, and what
will each household's bill look like once daily tariff data is applied to their
consumption?"*

**What the assignment asks for, and where to find it:**

| Assignment asks for | Where it is |
|---|---|
| An API showing live grid load and renewable % by area | `GET /api/grid/current` |
| An alert when renewable % is very low | `LOW_RENEWABLE` alerts (Spark), plus a Grafana alert rule |
| A daily billing and solar report per home | Airflow writes it to the database and to `data/reports/*.csv`; also shown in Grafana |

---

## 2. The big picture

```text
 meter_stream.py ──► Kafka ──► Spark Streaming ──► PostgreSQL ──► FastAPI + Grafana
 (a reading every ~3 s)       (clean, hourly totals,   │   ▲
                               alerts)                 │   │
 tariff_batch.py ──► CSV file ──► Airflow ─────────────┘───┘
 (one file per day)               (daily bills + CSV report)
```

Two data sources feed two processing paths, which meet in one database, which
one API and one dashboard read from.

| Part | Tool | Code |
|---|---|---|
| Live data source | Python script | `simulators/meter_stream.py` |
| Daily data source | Python script | `simulators/tariff_batch.py` |
| Message queue | Apache Kafka | `docker-compose.yml` |
| Live processing (speed layer) | Spark Structured Streaming | `spark/stream_job.py` |
| Daily processing (batch layer) | Apache Airflow | `airflow/dags/daily_billing_dag.py` |
| Database | PostgreSQL | `sql/init.sql` |
| API | FastAPI | `api/main.py` |
| Dashboard and alert rules | Grafana | `grafana/` |
| Shared code (clock, homes, billing rules, logging) | Python | `common/` |
| Tests | pytest | `tests/` |

---

## 3. Simulated time

Nobody can wait a real day to see a bill, so time runs faster:

```text
1 simulated day      = 5 real minutes   (288 times faster)
1 meter reading      = every 15 simulated minutes = about every 3 real seconds
first simulated day  = 2026-01-01
random seed          = 42  → every run produces exactly the same data
```

All processing uses the **time written inside each reading** (called *event
time*), not the computer's clock. So "1 hour" always means one simulated hour.

The values are set in `docker-compose.yml` (`SIM_DAY_SECONDS`, `SIM_START_DATE`, `SIM_SEED`).

---

## 4. How it works, step by step

### Step 1: The two data sources (Python)

**`meter_stream.py`: the live source.** Every 15 simulated minutes, each of
the 30 meters sends one message with exactly the assignment's fields:

```json
{"meter_id": "M001", "household_id": "H001", "grid_zone": "north",
 "power_consumption_kwh": 0.21, "solar_generation_kwh": 0.64,
 "timestamp": "2026-01-01T10:15:00Z"}
```

To make it realistic and to test the pipeline, the simulator also:

- follows a daily pattern: homes use more electricity in the morning and
  evening, and solar only produces between 06:00 and 18:00;
- creates a **storm over one area from 10:00 to 14:00 every day** (`north` on
  day 1, `central` on day 2, `south` on day 3, …). Solar drops almost to zero,
  so the low-renewable alert fires;
- sends about **2% bad records** (negative values, missing fields, an unknown
  area, broken JSON) and about **1% duplicates**, so the cleaning step has
  something to catch;
- after a restart, first **re-sends the readings it missed** (up to one day),
  like a real meter uploading its memory. Readings already stored are ignored.

**`tariff_batch.py`: the daily source.** At the start of every simulated day it
drops one file, `data/incoming/tariffs_<date>.csv`:

```csv
household_id,tariff_rate,billing_tier,subsidy_flag
H001,0.1623,A,true
```

- The price per kWh depends on the home's tier (A 0.16, B 0.19, C 0.23) and
  changes a little every day.
- **Every third day, one row is deliberately broken** (a blank or negative
  price), to show that bad batch data is caught.

### Step 2: Kafka (the message queue)

Meter readings go to the Kafka **topic** `meter-readings`:

- It has **3 partitions**.
- Each message is **keyed by `meter_id`**, so all readings from one meter go to
  the same partition and stay in order.
- Kafka acts as a buffer: if Spark stops, readings wait in Kafka and nothing is
  lost.

### Step 3: Spark (the speed layer: "what is happening right now?")

Spark reads Kafka in **small batches every 3 seconds** and does two jobs.

**Job 1: clean the data and store it.**

1. Parse each message and check it: all fields present, a known area, no
   negative numbers.
2. Bad records go to the `rejected_readings` table **with the reason**, for
   example `unknown_grid_zone`.
3. Duplicates are removed, in two places:
   - inside each batch, with `dropDuplicates`;
   - across batches, because the table's primary key is `(meter_id, event_time)`.
4. Clean readings are saved in `meter_readings`. This is the **full, permanent
   history** (the "master dataset") that the daily bills are calculated from.

**Job 2: live totals per area.**

1. Group readings into **1-hour windows per area**.
2. For each window, calculate:
   - total consumption;
   - total solar;
   - net grid load (consumption minus solar);
   - **renewable %**, the share of consumption covered by solar.
3. Readings up to 1 simulated hour late are still counted. This allowance is
   called the **watermark**.
4. Results are saved in `zone_load_hourly`.

**Alert rule:** between 09:00 and 15:00, if an area's renewable % drops below
**20%**, Spark saves a `LOW_RENEWABLE` alert. It only judges an hour once at
least 75% of that hour's readings have arrived.

### Step 4: Airflow (the batch layer: "what does each home owe?")

The Airflow job (a *DAG*) runs every minute. When a simulated day is finished,
it bills that day in 4 steps:

```text
pick_day → load_tariffs → compute_bills → export_report
```

1. **pick_day:** finds the first day that is finished (plus 1 simulated hour for
   late readings), has a tariff file, and hasn't been billed yet. If there
   isn't one, the run is skipped.
2. **load_tariffs:** checks every row of the tariff CSV. Bad rows are rejected
   and written to the log. Good rows are saved to `tariffs`.
3. **compute_bills:** SQL adds up each home's day and **joins it with that
   day's tariff**. Then `common/billing.py` calculates the bill:

   ```text
   energy charge  = electricity bought from the grid × price
   solar credit   = solar electricity sent back to the grid × price × 50%
   service charge = fixed daily charge by tier (A 0.50 / B 0.75 / C 1.00)
   subsidy        = 20% off the energy charge, if the home is eligible
   total bill     = energy charge − solar credit + service charge − subsidy
   ```

   A home with readings but no valid tariff row is counted as **unbilled**.
4. **export_report:** writes `data/reports/daily_billing_report_<date>.csv` and
   marks the day as done.

Every step is **safe to run again**. Re-running a day gives exactly the same
bills, because existing rows are updated, never duplicated.

### Step 5: PostgreSQL (the database)

| Table | Filled by | What it holds |
|---|---|---|
| `meter_readings` | Spark | every clean reading (the full history) |
| `rejected_readings` | Spark | bad readings and why they were rejected |
| `zone_load_hourly` | Spark | live hourly totals per area |
| `alerts` | Spark | low-renewable alerts |
| `tariffs` | Airflow | checked daily prices |
| `daily_bills` | Airflow | one bill per home per day |
| `billing_runs` | Airflow | one summary row per billed day |
| `pipeline_metrics` | Spark and Airflow | row counts, timings and delays (for monitoring) |

### Step 6: The API and the dashboard

**API** (FastAPI). Interactive docs: http://localhost:8000/docs

| Endpoint | What it returns |
|---|---|
| `GET /api/grid/current` | live load, solar and renewable % for each area |
| `GET /api/grid/history?hours=24` | hourly totals for the last N hours |
| `GET /api/alerts` | the latest low-renewable alerts |
| `GET /api/bills/latest` | the latest daily billing report |
| `GET /api/bills/2026-01-01` | the billing report for one day |
| `GET /api/metrics` | pipeline numbers: readings in, rejected, duplicates, delay |
| `GET /api/trace/{trace_id}` | the full journey of one reading |
| `GET /health` | `ok`, or `stale` if no data has arrived for 60 seconds |

**Dashboard** (Grafana). Open http://localhost:3000; it goes straight to the
*Smart Grid Monitoring & Billing* dashboard, which has 3 sections:

1. **Live grid:** load and renewable % per area, alerts, and 24-hour bar charts.
2. **Daily billing report:** the bill for every home, with totals.
3. **Pipeline health:** how fresh the data is, invalid %, duplicates, delay,
   and the latest rejected readings with their trace IDs.

---

## 5. Why a Lambda architecture (and not Kappa)

The two questions need different things:

| | "What's happening now?" | "What does each home owe?" |
|---|---|---|
| Needs | an answer within seconds | an exact, repeatable number |
| Can be slightly off? | yes | no, it's money |
| Handled by | **speed layer**: Spark | **batch layer**: Airflow |

A Lambda architecture has one layer for each need, so it fits.

**Why we rejected Kappa** (where everything is processed as a single stream):

1. **Bills must be exact and repeatable.** In a stream, results keep changing as
   late or duplicate readings arrive. A bill should be calculated once, after
   the day is complete.
2. **Recalculating is expensive.** Kappa recalculates by replaying Kafka, so
   Kafka would have to keep all history forever. We keep the history in
   `meter_readings`, and Kafka is only a short buffer.
3. **The tariff comes as a daily file, not a stream.** A scheduled daily job
   fits it naturally.

**The price we pay:** there are two processing paths to maintain, and the live
numbers can differ slightly from the final bill. For example, a duplicate
reading is counted in the live hourly total but not in the bill. That is the
normal Lambda trade-off: fast first, exact later.

---

## 6. Why these tools

| Tool | Why it fits this project |
|---|---|
| **Kafka** | Meters send a never-ending stream of readings. Kafka stores them safely, so nothing is lost if Spark restarts. Partitions keyed by `meter_id` keep each meter's readings in order. |
| **Spark Structured Streaming** | Has built-in time windows and late-data handling (watermarks), which we need because every reading has its own timestamp. Batches every 3 seconds are fast enough for live monitoring. Checkpoints let it restart where it stopped. |
| **Airflow** | Billing is a daily job with ordered steps, retries and a history of runs, which is exactly what Airflow is for. Every run is visible in its web UI. |
| **PostgreSQL** | The data is small and structured, and the queries need SQL joins (readings + tariffs). Primary keys make re-runs safe (no duplicates). `NUMERIC` stores money exactly. For 30 meters, Cassandra or HDFS would only add complexity. |
| **FastAPI** | Small and simple, with automatic API documentation at `/docs`. |
| **Grafana** | Builds the dashboard straight from PostgreSQL, so there's no dashboard code to write. It also runs the alert rules. Everything is set up from files, so it's the same on every machine. |
| **Docker Compose** | One command starts all 9 services with the same versions on any computer. |

---

## 7. Monitoring (observability): how we know it's working

| What | How | Why |
|---|---|---|
| **Logs** | Every part writes one JSON line per event, e.g. `{"service": "spark-streaming", "event": "readings_stored", "rows_in": 30, "rejected": 1}` | Easy to search and filter, and shows which part and which batch a problem came from. |
| **Metrics** | `pipeline_metrics` stores, for every batch: rows in, valid, invalid, duplicate, processing time and **end-to-end delay** (about 3 seconds). Shown in Grafana and at `/api/metrics`. | Shows speed, data quality and delay over time. |
| **Health check** | `/health` returns `ok`, or `stale` if no reading arrived in 60 seconds. | Shows at a glance whether data is still flowing. |
| **Alert rules** (Grafana) | 1. No meter data for 60 seconds. 2. More than 5% invalid readings. 3. Low renewable % in an area. | Rules 1–2 catch pipeline failures. Rule 3 is the business alert from the assignment. |
| **Tracing** | Every reading has a `trace_id` in its Kafka message header. It appears in the simulator log, the Spark log and the database. `/api/trace/<id>` shows its whole journey. | To follow one reading: was it sent, was it rejected and why, which hourly total and which bill did it go into? |

---

## 8. How to run it

> For the full step-by-step guide (setup, checks, every UI, demo script and
> troubleshooting), see **[run_book.md](run_book.md)**.

**You need:** Docker Desktop and about 4 GB of free memory.

```bash
docker compose up -d --build    # the first time downloads about 3 GB of images
docker compose ps               # everything "Up"; kafka-init "Exited (0)" is normal
```

| Open | Address | Login |
|---|---|---|
| Dashboard (Grafana) | http://localhost:3000 | none needed to view; `admin` / `admin` to edit |
| API docs | http://localhost:8000/docs | – |
| Airflow | http://localhost:8080 | `admin` / `admin` |
| Database | `localhost:5432`, database `smartgrid` | `grid` / `grid` |

### What you will see (timeline)

| Real time after start | What happens |
|---|---|
| about 1 minute | Live data appears on the dashboard; `/health` shows `ok` |
| about 2–3 minutes | Storm over `north`: 4 low-renewable alerts appear |
| about 6 minutes | Airflow bills **2026-01-01**: first daily report and CSV file |
| every 5 minutes after | Another day is billed, and the storm moves to the next area |
| about 16 minutes | Day 2026-01-03: one broken tariff row is rejected, so 29 homes are billed and 1 is *unbilled* |

About 2% of readings are rejected on purpose, so a day has slightly fewer than
30 × 96 = 2,880 readings.

### Demo script (about 10 minutes)

1. Start the system and open the Grafana dashboard.
2. Show the live area table and the storm alerts.
3. When day 1 is billed, show the Airflow UI, the bill table and the CSV in
   `data/reports/`.
4. **Show an alert firing:** run `docker compose stop meter-simulator` and wait
   about 2 minutes. "No meter data" fires in Grafana → Alerting, and `/health`
   shows `stale`. Run `docker compose start meter-simulator` and it recovers.
5. **Follow one reading (tracing):**
   ```bash
   # pick a rejected reading (also shown on the dashboard)
   docker compose exec postgres psql -U grid -d smartgrid -c \
     "SELECT trace_id, reason FROM rejected_readings ORDER BY rejected_at DESC LIMIT 1;"
   # find it in the logs
   docker compose logs meter-simulator spark-streaming | grep <trace_id>
   # see its full journey
   curl localhost:8000/api/trace/<trace_id>
   ```
6. After day 3, show the rejected tariff row and the unbilled home.

### Useful commands

```bash
docker compose logs -f spark-streaming      # live JSON logs (any service name works)
ls data/incoming data/reports               # daily tariff files and billing reports
curl localhost:8000/api/grid/current
curl localhost:8000/api/bills/latest
curl localhost:8000/api/metrics

# look at raw Kafka messages
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic meter-readings --max-messages 5

# query the database
docker compose exec postgres psql -U grid -d smartgrid -c "SELECT * FROM billing_runs;"
```

### Stop, or start again from day 1

```bash
docker compose down         # stop (keeps the data)

docker compose down -v      # stop and delete all data (database, Kafka, Spark checkpoints)
rm -rf data                 # delete tariff files, reports and the simulated clock
docker compose up -d        # start fresh from 2026-01-01
```

### Run the tests

```bash
pip install -r requirements-dev.txt   # the Spark tests also need Java 17 or newer
python -m pytest                      # 26 tests, no Docker needed
```

| Test file | What it checks |
|---|---|
| `tests/test_simulators.py` | readings have exactly the assignment's fields; the same seed gives the same data; no solar at night; the storm pushes renewable % below 20%; bad records really are bad; tariff files are correct (with one bad row every third day) |
| `tests/test_stream_validation.py` | the real Spark code: each kind of bad record gets the right reason; trace IDs are read; hourly totals and renewable % are correct |
| `tests/test_billing.py` | tariff row checks (blank, negative, wrong tier, …); the bill formula with and without solar and subsidy |

---

## 9. Limitations, and what we would change in a real system

- **Simulated data.** There are no real meters or billing system. The prices,
  charges, subsidy and solar credit are simple made-up rules.
- **Small scale.** 30 meters, 1 Kafka broker, Spark on one machine, one
  database. Spark brings each small batch into memory before saving it, which
  is fine for 30 rows but not for millions. A real system would use a Kafka
  cluster, Spark on a cluster, and store the raw history as Parquet files on
  S3 or HDFS instead of in PostgreSQL.
- **Live numbers are approximate.** The live hourly totals count duplicate
  readings (about 1%) and skip readings more than 1 hour late. The daily
  bills are exact.
- **Airflow timing.** Airflow checks every real minute because of the fast
  clock. In a real system it would run once a day (e.g. at 01:00) and wait for
  the tariff file.
- **Security.** Demo passwords are written in `docker-compose.yml`, and the API
  has no login. A real system would use a secret manager and authentication.
- **Monitoring.** Metrics are kept in PostgreSQL and alerts only show in
  Grafana. A real system would use Prometheus for metrics, a log system such as
  Loki or ELK, OpenTelemetry for tracing, and would send alerts by email or
  Slack.

---

## 10. Team contributions

*(fill in: who did what)*

- Member 1 –
- Member 2 –
- Member 3 –
