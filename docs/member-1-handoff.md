# Member 1 Handoff — Platform, Simulation, Kafka & Observability

**Status: complete and verified against running infrastructure.**

Everything below has been observed working, not just written. A full simulated
day published 3,770 events with zero delivery failures.

For Member 2 (processing) and Member 3 (serving/UI): this is everything you need
from the ingestion layer. Nothing here is an assumption — where a value came from
your code, that is noted.

---

## 1. Start everything

```bash
scripts/bootstrap.sh          # brings up infrastructure, creates topics
scripts/demo_start.sh         # runs one scripted simulated day
```

`bootstrap.sh` is safe to re-run: it creates only what is missing. It generates a
local `.env` with fresh credentials on first run — `.env` is gitignored, so **you
each generate your own**, and `.env.example` documents every variable name.

Requires Docker Desktop running. On Windows, run these from **Git Bash or WSL**,
not PowerShell.

---

## 2. Kafka

| | |
|---|---|
| From inside Compose | `kafka:9092` |
| From your host | `localhost:29092` |
| Telemetry topic | `solar.telemetry.raw` — 3 partitions |
| Quarantine topic | `solar.telemetry.invalid` — 1 partition |
| Alert topic | `solar.alerts` — 1 partition |
| Replication factor | 1 (single-node local cluster) |
| Retention | 3 hours |
| Message key | `plant_id:inverter_id`, UTF-8 |
| Message value | UTF-8 JSON, one inverter per message |

**Two listeners, and getting them backwards is the usual mistake.** A client
bootstraps once, then reconnects to whatever address the broker advertises. Use
`kafka:9092` from a container and `localhost:29092` from your host — the wrong one
connects successfully and then hangs.

**Topic auto-creation is disabled on the broker.** If a topic is missing, it will
never appear on its own; run `kafka/scripts/create_topics.sh` (idempotent).

Keying by asset is what keeps one inverter's events ordered within a partition.
`energy_today_kwh` is a cumulative meter reading, so out-of-order delivery would
look like energy running backwards.

### Sample event, taken off the topic

```json
{"event_id":"f8ce537b-8c5b-582f-966c-6aafd83a0387","plant_id":"PLANT_01",
 "inverter_id":"INV_01","active_power_kw":0.0,"energy_today_kwh":0.0,
 "irradiance_wm2":0.0,"module_temp_c":24.62,"inverter_temp_c":25.07,
 "status":"ONLINE","availability":1.0,"timestamp":"2026-08-21T00:00:00Z",
 "simulator_scenario":null}
```

Canonical schema: [`contracts/telemetry.schema.json`](../contracts/telemetry.schema.json).
Field-by-field rules: [`docs/data-contracts.md`](data-contracts.md) section 2.

### Guarantees

- Every event is validated against the schema **before** publication. Nothing
  malformed reaches `solar.telemetry.raw`.
- Rejected records go to `solar.telemetry.invalid` with a machine-readable
  `rejection_reason`, using **the same reason strings as
  `processing/streaming/validation.py`** — so grouping the quarantine topic by
  reason works regardless of which side rejected the record.
- `event_id` is a deterministic UUID5 over `(seed, plant, inverter, tick)`, unique
  within a run and identical across runs. 3,500 events per simulated day, zero
  collisions, asserted by test. Member 2's de-duplication can rely on it.
- Delivery is idempotent (`enable.idempotence`, `acks=all`) with bounded retries.

---

## 3. THE THING MOST LIKELY TO CATCH YOU OUT: event time

**Every event's `timestamp` is SIMULATED time, not wall-clock time.**

One simulated day is a full 24 hours of event time compressed into 300 real
seconds — a **288× factor**.

```text
real  0s  ->  2026-08-21T00:00:00Z
real 150s ->  2026-08-21T12:00:00Z
real 300s ->  2026-08-22T00:00:00Z
```

This is deliberate and it is what makes the demo work. Member 2's job windows on
event time and sustains alerts for `ALERT_SUSTAIN_SECONDS = 3600` — one *simulated*
hour, which is **12.5 real seconds** under this clock. Had the producer published
wall-clock timestamps, a simulated day would span five minutes of event time, no
window would ever fill, and no alert would ever fire.

If you are computing a duration, be explicit about which clock you are in.

---

## 4. Daily reference feed

| | |
|---|---|
| Directory | `SIMULATION_OUTPUT_DIR`, default `/data/daily` (`./data/daily` on host) |
| Filename | `daily_reference_YYYY-MM-DD.csv` |
| Written | at each simulated-day boundary, for the day that just **ended** |
| Log event | `daily_reference_ready` |

Writes are **atomic** — a temp file in the destination directory, fsynced, then
renamed. Airflow can never read a half-written file. Regenerating an existing day
is refused unless overwrite is passed explicitly (`demo_reset.sh` does).

```text
simulation_date,plant_id,plant_capacity_kw,expected_generation_kwh,expected_peak_power_kw,forecast_irradiance_kwh_m2,ppa_rate_per_kwh,maintenance_flag,source_version
2026-08-21,PLANT_01,6000.0,73691.723,5376.0,13.354,0.081,false,v1
2026-08-21,PLANT_02,4000.0,49127.815,3584.0,13.354,0.094,false,v1
2026-08-21,PLANT_03,4000.0,49127.815,3584.0,13.354,0.088,false,v1
2026-08-21,PLANT_04,4500.0,55268.792,4032.0,13.354,0.079,false,v1
2026-08-21,PLANT_05,2500.0,30704.885,2240.0,13.354,0.112,false,v1
```

Verified accepted by `processing/batch/reference.py`: five rows, zero warnings.

**`expected_generation_kwh` is derived by running the same generation model the
simulator runs, with noise switched off** — not from an independent sun-hours
assumption. That is deliberate: a baseline that does not come from the same source
as the measurement is not a baseline. A healthy day reconciles to 97–103% of its
own forecast, so any shortfall the reconciliation reports is genuinely caused by
the scripted anomalies.

Consequence worth knowing: these plants yield ~12.3 equivalent sun hours, roughly
double a real site, because the demo clock keeps the sun up for the whole
simulated day. Your `MAX_EQUIVALENT_SUN_HOURS = 14.0` accommodates it, and a test
asserts we stay under it.

---

## 5. Portfolio

`simulators/config/portfolio.yaml` — the single definition of which assets exist.
Member 2's `storage/seed_portfolio.py` already parses it; both implementations were
run against it and agree on all 5 plant rows and all 35 inverter rows.

| Plant | Name | Capacity | Inverters | PPA |
|---|---|---|---|---|
| PLANT_01 | North Ridge Solar | 6000 kW | 6 × 1000 | 0.081 |
| PLANT_02 | East Field Solar | 4000 kW | 5 × 800 | 0.094 |
| PLANT_03 | Harbour Flats Solar | 4000 kW | 8 × 500 | 0.088 |
| PLANT_04 | Windmere Plains Solar | 4500 kW | 6 × 750 | 0.079 |
| PLANT_05 | Southgate Solar | 2500 kW | 10 × 250 | 0.112 |

35 inverters, 21 MW. Rates differ per plant so lost revenue is not simply
proportional to lost energy.

---

## 6. Observability

| | |
|---|---|
| Simulator metrics | `http://localhost:9101/metrics` |
| Prometheus | `http://localhost:9090` — alerts at `/alerts` |
| Scrape interval | 15s |

```text
solariq_events_produced_total{plant_id}
solariq_events_invalid_total{reason}
solariq_producer_failures_total
solariq_last_event_timestamp_seconds
solariq_simulation_day
solariq_active_simulation_scenario{scenario}
solariq_telemetry_suppressed_total{plant_id}
solariq_daily_reference_written_total
```

`solariq_last_event_timestamp_seconds` carries **real** unix time, because the
staleness alert computes `time() - metric`.

`solariq_active_simulation_scenario` is 0 during normal operation, so
`> 0` means a scripted fault is running.

**Member 2:** Prometheus already has a `solariq-processing` job pointed at
`host.docker.internal:9102`. It shows DOWN until you expose an endpoint there —
declared in advance so a missing exporter is visible rather than invisible.

Please keep label cardinality low. Anything labelled by `event_id` creates one
time series per event — 3,500 a simulated day — and will eventually take
Prometheus down.

### Alert rules — `observability/alert-rules/pipeline.yml`

| Alert | Fires when |
|---|---|
| `SolarIQNoTelemetryProduced` | no telemetry for 60s |
| `SolarIQSimulatorDown` | metrics endpoint unreachable for 30s |
| `SolarIQProducerDeliveryFailures` | Kafka rejecting events |
| `SolarIQInvalidEventsProduced` | source-side quarantine active |

These are **engineering** alerts — the platform is broken. Keep them separate from
your operational alerts (an inverter underperforming), because an outage in the
monitoring must not look like an outage in the plant.

---

## 7. Demo timeline

Fires at these points of **every** simulated day, identically every run
(`SIMULATION_SEED=8203`):

| Window | Scenario | Target | Effect |
|---|---|---|---|
| 0–90s | normal | — | baseline |
| 90–150s | `INV_UNDERPERFORMANCE` | PLANT_03/INV_02 | power × 0.45, `WARNING`, **irradiance normal** |
| 150–190s | `RECOVERY` | PLANT_03/INV_02 | back to normal |
| 190–235s | `INV_OFFLINE` | PLANT_04/INV_01 | power 0, availability 0, `OFFLINE` |
| 235–260s | `TELEMETRY_GAP` | PLANT_05 (all 10) | **nothing published at all** |
| 260–300s | `RECOVERY` | PLANT_05 | resumes |

Schedule: `simulators/config/scenarios.yaml`. Windows scale automatically if
`SIMULATION_DAY_SECONDS` changes.

### Two things to build against

**`INV_OFFLINE` and `TELEMETRY_GAP` are different faults.** Offline is a *reported*
zero — the asset publishes and says it is down. A gap is *silence* — nothing
arrives, so you cannot distinguish "generating fine" from "on fire". Operational
alert versus pipeline-health alert. The assessment wants both demonstrated.

**Do not read `simulator_scenario` in detection logic.** It is demo metadata. If a
rule keys off the label, the pipeline is echoing the simulator's own answer back
and proves nothing. Underperformance must be inferred from power against
irradiance.

To help: irradiance stays normal during underperformance, and healthy inverters
never fall below a 0.80 performance ratio (asserted across all 35 inverters for a
full day), so your 80% threshold will not false-alarm.

---

## 8. Commands

```bash
scripts/bootstrap.sh                        # start infrastructure + topics
scripts/demo_start.sh                       # run the scripted demo day
scripts/demo_start.sh --forever             # run until Ctrl-C
scripts/demo_reset.sh                       # reset ingestion state
scripts/demo_reset.sh --all                 # also clear Postgres + MinIO
kafka/scripts/create_topics.sh              # idempotent topic creation
kafka/scripts/verify_topics.sh --consume 5  # is telemetry flowing?
```

`verify_topics.sh` is the first thing to reach for when the dashboard is empty —
it answers "is the producer publishing?" without involving Spark, Postgres or the
API, narrowing the fault to one side of Kafka in seconds.

Ctrl-C on the simulator stops cleanly and flushes buffered events.

---

## 9. Gotchas that cost me time

- **Run shell scripts from Git Bash or WSL, not PowerShell.** They are bash.
- **Git Bash rewrites container paths.** `docker exec ... /opt/kafka/...` becomes
  `C:/Program Files/Git/opt/kafka/...`. The scripts set `MSYS_NO_PATHCONV=1`; if
  you write your own `docker exec` calls, you will need it too.
- **`.gitattributes` pins `.sh` and `.yml` to LF.** Without it Windows checkouts
  get CRLF and containers fail with `/bin/bash^M: bad interpreter`.
- **Kafka tools moved package in 3.x** — `org.apache.kafka.tools.GetOffsetShell`,
  not `kafka.tools.GetOffsetShell`.
- **Docker's disk image can be relocated** (Settings → Resources → Advanced) if
  your C: drive is tight.

---

## 10. What I own

```text
simulators/     kafka/     observability/     scripts/
docker-compose.yml     contracts/telemetry.schema.json
```

Raise a change to `contracts/`, the Kafka topics or key rule, or the daily feed
schema with the team before building against a different shape — those are frozen
shared contracts.

`docker-compose.yml` also defines `postgres`, `minio` and `prometheus`. Service
names, ports and the database name were taken from `processing/common/config.py`
so Member 2's defaults resolve unchanged — **Member 2, please review them**, they
are your subsystem's infrastructure and I set them up only to unblock you.
