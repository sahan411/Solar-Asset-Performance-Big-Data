# Member 2 Handoff — Processing, Orchestration & Storage

**For Member 3 (serving/UI).** Everything the API needs to read, with units,
nullability and the queries that answer each dashboard question.

Nothing here is guesswork: every table below is created by a migration in
`storage/migrations/`, and every semantic is covered by a test.

---

## 1. Start the serving store

```bash
docker compose up -d postgres          # or scripts/bootstrap.sh for everything
python -m storage.migrate              # create/upgrade the schema
python -m storage.seed_portfolio        # load plants + inverters
```

Both commands are idempotent — safe to re-run, including after a demo reset.
`storage/migrate.py --status` lists what is applied.

```bash
DATABASE_URL=postgresql://solariq:<password>@localhost:5432/solariq
```

Inside Compose the host is `postgres`; from your machine it is `localhost`. The
password is whatever `bootstrap.sh` generated into your `.env`.

---

## 2. THE THING MOST LIKELY TO CATCH YOU OUT: NULL is not zero

Several numeric columns are **NULL by design**, and rendering them as `0` will
show a healthy plant as catastrophically broken.

At night, or under heavy cloud below `MIN_IRRADIANCE_WM2` (150 W/m²), there is no
meaningful expectation to divide by. Expected power approaches zero and the ratio
becomes meaningless or explosive. So:

| Column | NULL when | Render as |
|---|---|---|
| `performance_pct` | irradiance below threshold | `—` or "Not available (low light)" |
| `expected_power_kw` | same | `—` |
| `estimated_loss_kw` | same | `—` |
| `availability_pct` | no configured inverters | `—` |

`current_power_kw` is **never** NULL — zero output at night is a fact, and it is
reported as `0.0`.

The same applies to `alerts.estimated_revenue_loss`: it is NULL until the day's
reference feed has loaded, because the energy lost is known before its price is.
`estimated_loss_kwh` is populated either way.

**Second gotcha:** `performance_pct` can legitimately exceed 100. The expected-power
model is deliberately conservative, and clamping it would hide model error. Show
values above 100 as they are.

---

## 3. Live tables (speed layer)

Written by the Spark streaming job, one row per plant per microbatch. The window
is the event-time range of the telemetry that batch received, so window widths
vary — do not assume fixed intervals.

### `live_plant_metrics`

Key: `(plant_id, window_start, window_end)`

| Column | Type | Unit | Null? | Meaning |
|---|---|---|---|---|
| `plant_id` | text | | no | FK to `plants.id` |
| `window_start` / `window_end` | timestamptz | UTC | no | Event-time span of the batch |
| `current_power_kw` | float | kW | no | Sum of each inverter's **latest** reading |
| `avg_power_kw` | float | kW | no | Mean plant power across the window's instants |
| `expected_power_kw` | float | kW | **yes** | Irradiance-derived proxy |
| `avg_irradiance_wm2` | float | W/m² | yes | Mean of latest per-inverter readings |
| `availability_pct` | float | 0–100 | yes | Online ÷ **configured** inverters |
| `performance_pct` | float | 0–100+ | **yes** | `current ÷ expected × 100` |
| `estimated_loss_kw` | float | kW | **yes** | `max(expected − current, 0)` |
| `online_inverters` | int | count | yes | |
| `offline_inverters` | int | count | yes | Includes inverters that stopped reporting |
| `updated_at` | timestamptz | UTC | no | |

`current_power_kw` is **not** a sum of samples. With telemetry every 3 seconds a
60-second window holds ~20 readings per inverter; summing them would report ~20×
the real output. It is the sum of one latest reading per asset.

### `live_portfolio_metrics`

Key: `(window_start, window_end)`. Same columns minus `plant_id` and
`avg_irradiance_wm2`.

`performance_pct` here is **capacity-weighted** — `Σ actual ÷ Σ expected × 100`.
If you ever need to recompute a portfolio figure yourself, weight it the same
way. Averaging the plants' percentages lets a 500 kW site distort a 21 MW
portfolio.

### Reading "now"

Latest row per plant:

```sql
SELECT DISTINCT ON (plant_id) *
  FROM live_plant_metrics
 ORDER BY plant_id, window_end DESC;
```

Latest portfolio row:

```sql
SELECT * FROM live_portfolio_metrics ORDER BY window_end DESC LIMIT 1;
```

Both access paths are indexed. For a plant's history chart, filter on
`window_end BETWEEN %s AND %s` and order ascending.

### Freshness

There is no `is_stale` column — staleness depends on your threshold, so compute
it in the API:

```sql
SELECT NOW() - MAX(window_end) AS age FROM live_portfolio_metrics;
```

Compare against `STALE_DATA_SECONDS`. Under the demo clock the stream writes
every few seconds; anything older than ~60s means the pipeline stopped.

---

## 4. `alerts` — business alerts only

Solar operational problems. Pipeline problems live in `pipeline_health`; keeping
them apart is a project requirement and a demo talking point.

| Column | Values / unit | Null? |
|---|---|---|
| `id` | text UUID | no |
| `plant_id` | FK | no |
| `inverter_id` | text | **yes** — NULL means plant-wide |
| `alert_type` | `UNDERPERFORMANCE` \| `INVERTER_OFFLINE` \| `TELEMETRY_GAP` | no |
| `severity` | `WARNING` \| `CRITICAL` | no |
| `message` | human-readable, safe to display | no |
| `started_at` | timestamptz — when the fault **began** | no |
| `ended_at` | timestamptz | yes — NULL while active |
| `status` | `ACTIVE` \| `RESOLVED` | no |
| `estimated_loss_kwh` | kWh, cumulative | yes |
| `estimated_revenue_loss` | currency | yes — NULL before the reference feed loads |

Use these strings exactly; they are enforced by CHECK constraints.

**At most one ACTIVE alert exists per (plant, inverter, type)** — guaranteed by a
partial unique index, not by convention. You never need to de-duplicate.

`started_at` is when the fault began, not when it was detected. Alerts open only
after a fault is sustained, so expect `started_at` to be earlier than
`created_at` — display `started_at`, and derive duration from
`COALESCE(ended_at, NOW()) - started_at`.

Impact figures **grow while an alert is open**, so a value can change between
polls. That is intended: it is the cost of leaving the fault unfixed.

```sql
SELECT * FROM alerts WHERE status = 'ACTIVE' ORDER BY started_at DESC;
```

**Do not read `alert_conditions`.** It holds unconfirmed observations the
pipeline has not decided about yet — internal to the stream job.

---

## 5. Daily tables (batch layer)

### `daily_plant_summary`

Key: `(simulation_date, plant_id)`. Written by the Airflow DAG once a simulated
day completes. **This is the authoritative record** — it is recomputed from the
full Parquet archive, not from the live tables.

| Column | Unit | Null? | Notes |
|---|---|---|---|
| `actual_generation_kwh` | kWh | no | |
| `expected_generation_kwh` | kWh | no | From the reference feed |
| `performance_pct` | 0–100+ | yes | |
| `availability_pct` | 0–100 | yes | Share of observations reporting ONLINE |
| `downtime_minutes` | minutes | yes | Sampled estimate |
| `estimated_lost_energy_kwh` | kWh | yes | `max(expected − actual, 0)` |
| `ppa_rate_per_kwh` | currency/kWh | yes | **The rate actually used for this row** |
| `estimated_actual_revenue` | currency | yes | |
| `estimated_lost_revenue` | currency | yes | |
| `alert_count` | count | no | Alerts that *started* that day |
| `maintenance_flag` | bool | no | Plant kept in the report, flagged |
| `computed_at` | timestamptz | no | |

Portfolio totals: **sum the energy columns, then divide** — do not average
`performance_pct`.

```sql
SELECT SUM(actual_generation_kwh)                                   AS actual_kwh,
       SUM(expected_generation_kwh)                                 AS expected_kwh,
       SUM(actual_generation_kwh) / NULLIF(SUM(expected_generation_kwh), 0) * 100
                                                                    AS performance_pct,
       SUM(estimated_lost_energy_kwh)                               AS lost_kwh,
       SUM(estimated_actual_revenue)                                AS revenue,
       SUM(estimated_lost_revenue)                                  AS lost_revenue
  FROM daily_plant_summary
 WHERE simulation_date = %s;
```

Best/worst performer:

```sql
SELECT plant_id, performance_pct
  FROM daily_plant_summary
 WHERE simulation_date = %s AND performance_pct IS NOT NULL
 ORDER BY performance_pct DESC;
```

**No rows for a date is normal** — it means the DAG has not run for that
simulated day yet. Return 404 or a clear empty state; do not fabricate zeros.

### `daily_reference`

The day's expectations as loaded from Member 1's feed. Mostly of interest for
showing the PPA rate and `maintenance_flag`; the summary already carries what
the report needs.

---

## 6. `pipeline_health` — engineering health

One row per component. Read this for `/ready` and any "is the pipeline alive"
indicator.

| Column | Meaning |
|---|---|
| `component` | `spark-stream`, `airflow-daily-reconciliation` |
| `status` | `HEALTHY` \| `DEGRADED` \| `STALE` \| `FAILED` |
| `last_event_at` | Event time of the newest telemetry processed (high-water mark) |
| `last_success_at` | Wall clock of the last successful unit of work |
| `message` | Human-readable |

`DEGRADED` means alive but idle — a microbatch with no telemetry. It is not an
error. The job never writes `STALE` itself: **staleness is yours to compute**
from `last_success_at` against now, because only the reader knows the threshold.

---

## 7. Live vs daily — the question you will be asked

The same plant can show 92% live and 89% daily on the same day, and both are
correct. They answer different questions from different inputs:

- **Live** compares actual power against what *measured irradiance* implies. It
  says whether the asset is healthy given the sun it actually got.
- **Daily** compares actual energy against the *forecast* in the reference feed.
  It says whether the day earned what it was supposed to.

A cloudier-than-forecast day scores well live and poorly daily. Label your KPI
cards explicitly — "Live" versus "Today (reconciled)" — and never mix a live
instantaneous figure with a daily cumulative one in the same row.

Full reasoning: [`docs/architecture.md`](architecture.md), sections 2 and 6.

---

## 8. Units and formatting

- `*_kw` kilowatts, `*_kwh` kilowatt-hours, `*_pct` percent (0–100, not 0–1),
  `*_wm2` W/m², `*_minutes` minutes.
- **All timestamps are UTC** (`timestamptz`). Format for display, keep UTC in the
  API.
- Money is `DOUBLE PRECISION` against a fictional PPA rate — a Phase 1
  simplification. Use a neutral label ("Estimated Revenue Loss"), and state
  somewhere that the currency is simulated.
- Never emit `NaN` or `Infinity` in JSON. The pipeline does not produce them, but
  guard any division you do yourself.

---

## 9. Known limitations

Worth knowing before you build a UI that implies more precision than exists.

1. **The expected-power proxy is not a PV model.** No temperature, soiling,
   shading or clipping terms. Live percentages are indicative.
2. **Live availability is sampled**, not time-weighted uptime.
3. **`downtime_minutes` has one-telemetry-interval resolution**; an outage
   shorter than one interval is invisible.
4. **Daily availability only counts telemetry that arrived** — a silent inverter
   neither raises nor lowers it. Telemetry gaps surface as `TELEMETRY_GAP` alerts
   and in `offline_inverters`, not in that percentage.
5. **Window widths vary** with microbatch timing; they are not fixed intervals.
6. **The asset registry is read once at stream start** — a plant added mid-run
   appears after a restart.
7. **No per-inverter live metrics table.** `GET /plants/{id}/inverters` can serve
   configuration from `inverters` plus the plant's `online_inverters` /
   `offline_inverters` counts. If you need per-inverter live state, ask — it is a
   schema change, not something to infer.

---

## 10. If something looks wrong

```sql
-- Is the stream running?
SELECT * FROM pipeline_health;

-- Is live data arriving?
SELECT MAX(window_end), NOW() - MAX(window_end) AS age FROM live_portfolio_metrics;

-- Did the batch run for this day?
SELECT COUNT(*) FROM daily_plant_summary WHERE simulation_date = '2026-08-21';

-- What is currently wrong with the portfolio?
SELECT plant_id, inverter_id, alert_type, severity, started_at
  FROM alerts WHERE status = 'ACTIVE' ORDER BY started_at DESC;
```

Empty live tables with a `HEALTHY` row in `pipeline_health` means the simulator
is not publishing — Member 1's side. Empty tables with no `pipeline_health` row
at all means the Spark job never started.
