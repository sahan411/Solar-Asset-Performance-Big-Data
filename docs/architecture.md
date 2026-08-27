# SolarIQ — Speed and Batch Layer Semantics

> Owner: Member 2 (processing, orchestration, storage).
> Companion to `SolarIQ_Master_Project_Specification.md`, which remains the
> authority on scope and shared contracts.

This document explains how the two halves of the Lambda architecture compute
their numbers, why those numbers legitimately differ, and what each simplifying
assumption costs. It exists to answer one question directly:

> "Why does the live dashboard say 92% while the daily report says 89%?"

---

## 1. The two layers at a glance

```text
                          ┌──────────────────────────────┐
   Kafka                  │ SPEED LAYER (seconds)        │
   solar.telemetry.raw ──▶│ Spark Structured Streaming   │──▶ live_plant_metrics
                          │ current state, approximate   │    live_portfolio_metrics
                          └──────────────────────────────┘    alerts

                          ┌──────────────────────────────┐
   MinIO/Parquet          │ BATCH LAYER (once per day)   │
   normalized telemetry ─▶│ Airflow + Spark              │──▶ daily_plant_summary
   + daily_reference      │ complete day, authoritative  │
                          └──────────────────────────────┘
```

The speed layer answers **"what is happening right now?"** using whatever
telemetry has arrived. The batch layer answers **"what actually happened
yesterday?"** using the complete, immutable day plus the authoritative
expectation feed.

When the two disagree, **the batch layer is correct**. The speed layer trades
completeness for latency, by design.

---

## 2. Why the numbers differ

The layers do not run the same calculation faster or slower — they answer
different questions from different inputs.

| | Speed layer | Batch layer |
|---|---|---|
| Input | Telemetry received so far | Full day's Parquet archive |
| Expectation | Irradiance-derived proxy, per instant | `daily_reference.expected_generation_kwh` |
| Quantity | Power (kW), instantaneous | Energy (kWh), integrated over the day |
| Late data | May be missed after the watermark | Always included — the day is re-read whole |
| Purpose | Operational reaction | Reporting and revenue |

Three concrete causes of divergence:

1. **Different expectation sources.** Live performance compares actual power
   against what *measured irradiance* implies. Daily performance compares actual
   energy against the *forecast* in the reference feed. A day that was cloudier
   than forecast shows good live performance (the plant did well given the sun it
   got) and poor daily performance (it missed the forecast). Both are true and
   useful: the first says the asset is healthy, the second says revenue is down.

2. **Power versus energy.** A live percentage is an instantaneous ratio. A daily
   percentage is a ratio of integrated totals, so it weights midday — when most
   generation happens — far more heavily than a simple average of live readings
   would.

3. **Late and missing data.** The speed layer drops events arriving after the
   watermark. The batch layer reads the whole day's archive after it is complete,
   so those events are counted.

---

## 3. Speed layer calculations

### 3.1 Current power — why it is not a windowed sum

The natural-looking implementation is wrong:

```python
# WRONG — inflates output by the number of samples in the window
.groupBy(window("event_time", "60 seconds"), "plant_id")
.agg(sum("active_power_kw"))
```

With telemetry every 3 seconds, a 60-second window holds roughly 20 readings per
inverter. Summing them adds up **repeated observations of the same physical
quantity**, reporting about 20× the plant's real output.

Current power is instantaneous: the sum of each inverter's *most recent* reading,
one value per asset.

```text
current_power_kw = Σ over inverters ( latest active_power_kw )
```

That requires latest-per-inverter followed by a per-plant sum — two chained
aggregations. Structured Streaming forbids two aggregations on a streaming
DataFrame but permits them on the batch DataFrame inside `foreachBatch`. **This
constraint is the reason the job is built around `foreachBatch` rather than a
sliding window.**

Event-time watermarking is still used, in de-duplication.

Ties on event time are broken by Kafka offset, so replaying a batch cannot select
a different "latest" reading and produce a different answer for identical input.

### 3.2 Average power

```text
plant_power(t)  = Σ over inverters ( active_power_kw at t )
avg_power_kw    = mean over observed instants t of plant_power(t)
```

Averaging raw samples instead would return mean *inverter* power, which is a
different quantity by a factor of the inverter count.

### 3.3 Expected power — a simplified proxy

```text
expected_power_kw = capacity_kw × min( irradiance_wm2 / 1000, 1 )
```

1000 W/m² is irradiance at Standard Test Conditions, the level at which a panel
is rated, so the ratio is the fraction of nameplate capacity the available
sunlight can support. It is capped at 1 because brief cloud-edge enhancement can
exceed 1000 W/m² while the plant still cannot exceed its inverter rating.

**Deliberately excluded:** temperature derate, soiling, shading, angle of
incidence, cable and inverter losses, and inverter clipping. A bankable
performance-ratio calculation includes all of them. This is a demonstration
proxy, not a PV model, and it is consistently conservative — which is why
performance above 100% is reported rather than clamped. Hiding overshoot would
hide model error.

### 3.4 Performance and loss

```text
performance_pct   = current_power_kw / expected_power_kw × 100
estimated_loss_kw = max( expected_power_kw − current_power_kw, 0 )
```

Both are **NULL** when average irradiance is below `MIN_IRRADIANCE_WM2`
(default 150 W/m²). After sunset, expected power approaches zero and the ratio
becomes meaningless or explosive. Reporting NULL is honest; reporting 0% at
night would look like a total outage every evening.

Loss is floored at zero: a plant beating a conservative proxy is not producing
negative loss.

### 3.5 Availability — measured against the configured fleet

```text
availability_pct = online_inverters / configured_inverters × 100
```

The denominator is the **configured** inverter count from the asset registry,
not the number that happened to report. This matters: dividing by the reporting
count would let a silent inverter quietly shrink the denominator and flatter the
plant to 100% while an asset is missing entirely. Using the configured count
turns a telemetry gap into visible lost availability.

An inverter counts as online only if its status is not `OFFLINE` **and** it
reports itself available; the two disagree during a fault the firmware has
already noticed. `WARNING` still counts as online — a degraded inverter is
generating, which is an alert, not downtime.

### 3.6 Portfolio roll-up — capacity weighted

```text
portfolio_performance_pct = Σ actual_power_kw / Σ expected_power_kw × 100
```

Never the mean of the plants' percentages. A 5 MW plant at 95% alongside a
500 kW plant at 50% is a portfolio at ~91%, not 72.5% — an unweighted mean lets
the smallest site distort the figure the business acts on.

Plants whose expected power is NULL (below the irradiance threshold) are excluded
from **both** sides of the ratio. Counting their zero output in the numerator
while omitting their unknown expectation from the denominator would drag
portfolio performance toward zero at dusk, turning every sunset into a fake
outage.

---

## 4. Alerting

### 4.1 Detection is per-inverter

A plant-level threshold cannot see the fault it most needs to catch. Degrading
one inverter to 45% on a five-inverter plant moves plant output by roughly 11%,
so an 80% plant-level rule stays silent while the asset loses money all day.
Each inverter is therefore judged against its own nameplate rating.

### 4.2 Conditions are mutually exclusive

Priority order, one condition per inverter:

```text
TELEMETRY_GAP  >  INVERTER_OFFLINE  >  UNDERPERFORMANCE
```

An offline inverter produces zero power, which also reads as severe
underperformance; raising both would double-report one fault. A gap outranks
everything because once an asset stops reporting, every other judgement about it
is guesswork.

`TELEMETRY_GAP` is detected by driving the evaluation from the **asset registry**
rather than from arriving telemetry — a silent inverter cannot be found by
looking at data it did not send. This keeps "the inverter is broken" distinct
from "we have lost sight of the inverter", which are different problems with
different owners.

### 4.3 Sustained, in event time

An alert opens only after a condition has persisted for `ALERT_SUSTAIN_SECONDS`.
A passing cloud drops output for seconds; a failing inverter stays down.

Durations are measured in **event time**, not wall clock. Under the compressed
demo clock one simulated day passes in five real minutes, so wall time says
nothing about how long a plant has actually underperformed. Event time also makes
alerting **replay-safe**: reprocessing history produces the same alerts it
produced live.

The default is one simulated hour, about 12 real seconds under the default demo
clock. Production would use hours.

In-progress observations live in `alert_conditions`, not `alerts`. A row in
`alerts` is a statement to an operator; keeping unconfirmed observations out of it
is what stops the dashboard flickering.

### 4.4 One alert per fault

`alerts` carries a partial unique index permitting at most one `ACTIVE` row per
`(plant_id, inverter_id, alert_type)`. De-duplication is a **database
guarantee**, not stream-job discipline. When the condition clears, the alert is
resolved and its tracking row removed, so a recurrence opens a new incident
rather than reopening the old one.

### 4.5 Financial impact

```text
estimated_loss_kwh     = mean shortfall (kW) × duration (hours)
estimated_revenue_loss = estimated_loss_kwh × ppa_rate_per_kwh
```

The mean is taken over the condition's life rather than using the latest
instantaneous reading, which could be a single cloudy outlier. Figures are
refreshed while the fault persists, so an operator sees the cost of leaving it
unfixed rather than a number frozen at detection time.

The rate comes from `daily_reference`. Before that day's feed arrives, energy
loss is still recorded and revenue loss is NULL — the physical loss is known even
when its price is not.

---

## 5. Business alerts versus pipeline health

Two different failures that look identical on a dashboard showing zero
generation, and need opposite responses:

| | Business alert | Pipeline health |
|---|---|---|
| Table | `alerts` | `pipeline_health` |
| Means | A solar asset is underperforming | Our data platform is broken |
| Audience | O&M technician | Data engineer |
| Example | `INV_02` at 45% of expectation | No telemetry processed for 60s |

The streaming job reports what it directly knows — `HEALTHY` when a batch
processed events, `DEGRADED` when the job is alive but the batch was empty,
`FAILED` when a batch raised. **STALE is computed by consumers** (the Prometheus
rule, the API's readiness check) by comparing `last_success_at` against wall
clock, because only they can know how long is too long.

`last_event_at` is kept as a high-water mark, so an empty batch cannot erase the
last event time genuinely seen, and a failed batch does not advance
`last_success_at` — otherwise a crash-looping job would look permanently healthy.

**Alert on processed telemetry, not produced telemetry.** The producer can be
publishing happily while Kafka, Spark, or the database write is broken; only the
processed timestamp notices.

---

## 6. Batch layer calculations

### 6.1 Why the archive exists

The batch layer reads the immutable Parquet archive, **not** the live tables.
That is what makes it a genuine batch layer rather than a nightly re-read of
approximations: a change to the performance model can be replayed over past days,
and a late-arriving event lands in the day it was measured.

Partitioned by `(simulation_date, plant_id)` to match how the daily job
queries — one simulated day, grouped per plant — so Spark prunes to a single date
directory rather than scanning the archive.

`simulation_date` derives from **event time**, never processing time. Under the
compressed clock, wall-clock time says nothing about which simulated day a
reading belongs to.

---

## 7. Known limitations

Honest statements of what this implementation does not do.

1. **The expected-power proxy is not a PV model.** No temperature, soiling,
   shading, incidence-angle or clipping terms. Live performance percentages are
   indicative, not bankable.
2. **Money uses `DOUBLE PRECISION`.** Acceptable for a simulated fictional PPA
   rate; a system settling real invoices would use `NUMERIC(14,4)` to avoid
   binary floating-point rounding.
3. **Availability is sampled, not continuous.** It reflects inverter state at the
   observed instants in each microbatch, not a true time-weighted uptime.
4. **The speed layer can miss very late events.** Anything arriving after the
   watermark is dropped from live metrics. The batch layer catches them; this is
   the intended division of labour.
5. **The asset registry is loaded once at job start.** A plant added mid-run is
   picked up on restart — acceptable for a fleet that changes on the scale of
   months.
6. **Window bounds follow microbatch boundaries.** Each live metric row covers
   the event-time range of the telemetry that batch received, so window sizes vary
   with trigger timing rather than being fixed clock intervals.
7. **Simulated data throughout.** No real inverter, SCADA, meter or weather
   integration exists in Phase 1, by design.

---

## 8. Configuration reference (Member 2)

| Variable | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | — (required) | PostgreSQL serving store |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka brokers |
| `KAFKA_TELEMETRY_TOPIC` | `solar.telemetry.raw` | Source topic |
| `KAFKA_INVALID_TOPIC` | `solar.telemetry.invalid` | Quarantine topic |
| `STREAM_STARTING_OFFSETS` | `earliest` | Kafka start position |
| `STREAM_WATERMARK` | `2 minutes` | Event-time watermark for de-duplication |
| `SPARK_CHECKPOINT_DIR` | `/spark-checkpoints` | Parent of per-sink checkpoints |
| `MINIO_ENDPOINT` | `http://minio:9000` | S3-compatible endpoint |
| `MINIO_ACCESS_KEY` | — (required) | No credential is committed to the repo |
| `MINIO_SECRET_KEY` | — (required) | No credential is committed to the repo |
| `MINIO_RAW_BUCKET` | `solariq-raw` | Raw archive bucket |
| `MIN_IRRADIANCE_WM2` | `150` | Below this, performance is NULL |
| `REFERENCE_IRRADIANCE_WM2` | `1000` | Standard Test Conditions irradiance |
| `UNDERPERFORMANCE_THRESHOLD_PCT` | `80` | Performance below this is a fault |
| `ALERT_SUSTAIN_SECONDS` | `3600` | **Event-time** seconds before an alert opens |
| `SIMULATION_OUTPUT_DIR` | `/data/daily` | Where the daily reference feed lands |
| `PORTFOLIO_CONFIG_PATH` | `simulators/config/portfolio.yaml` | Shared asset definition |
| `STREAM_METRICS_PORT` | `9102` | Prometheus scrape port for the stream |
