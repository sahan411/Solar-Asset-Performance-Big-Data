# SolarIQ — Shared Data Contracts

**Status:** frozen at the architecture checkpoint.
**Source of truth:** [`SolarIQ_Master_Project_Specification.md`](../SolarIQ_Master_Project_Specification.md) sections 8–10.

This file restates the interfaces the three subsystems share, in one place, so no
member has to read another member's code to integrate. It describes contracts
only — never implementation.

Changing anything here requires all three members to agree. A member who needs a
different shape must raise it before writing code against the new shape, not
after.

| Contract | Produced by | Consumed by |
|---|---|---|
| Portfolio config | Member 1 | Member 2 (DB seed), Member 1 (simulation) |
| Telemetry event | Member 1 | Member 2 (Spark streaming) |
| Daily reference feed | Member 1 | Member 2 (Airflow batch) |
| PostgreSQL tables | Member 2 | Member 3 (API) |
| REST API | Member 3 | Member 3 (dashboard) |

---

## 1. Portfolio configuration

**File:** `simulators/config/portfolio.yaml` — owned by Member 1.
**Override:** `PORTFOLIO_CONFIG_PATH`.

The single definition of which plants and inverters exist. Member 1's simulator
generates telemetry from it; Member 2's `storage/seed_portfolio.py` seeds the
`plants` and `inverters` tables from the same file. Because both sides read one
file, asset identity cannot drift between the stream and the database.

```yaml
plants:
  - id: PLANT_01              # required, unique, uppercase PLANT_NN
    name: North Ridge Solar   # required, display name (fictional)
    capacity_kw: 6000         # required, > 0
    timezone: UTC             # optional, defaults to UTC
    inverters:                # required, 5-10 per plant
      - id: INV_01            # required, unique within its plant
        rated_power_kw: 1000  # required, > 0
        name: ...             # optional, defaults to the inverter id
```

Rules:

- exactly 5 plants in the default demo portfolio,
- plant IDs unique across the portfolio; inverter IDs unique within a plant,
- `(plant_id, inverter_id)` is the global asset identity,
- 5–10 inverters per plant (master specification section 5.1),
- all capacities strictly positive,
- the sum of a plant's inverter ratings must be within 25% of its
  `capacity_kw` — outside that, the file is treated as a typo.

## 2. Streaming telemetry event

**Kafka topics**

```text
solar.telemetry.raw       valid telemetry, 3 partitions
solar.telemetry.invalid   quarantined records with error metadata
solar.alerts              alerts raised by the processing layer
```

Replication factor is 1 — a single-broker local Docker environment.

**Kafka key** — UTF-8 encoded, and the reason per-inverter events stay ordered
within a partition:

```text
plant_id:inverter_id
```

**Value** — UTF-8 JSON, one event per inverter per tick:

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

| Field | Type | Rules |
|---|---|---|
| `event_id` | string | UUID, unique per event |
| `plant_id` | string | must exist in the portfolio config |
| `inverter_id` | string | must exist under that plant |
| `active_power_kw` | float | `0 <= value <= inverter rated_power_kw` |
| `energy_today_kwh` | float | `>= 0`, non-decreasing within a simulated day, resets at the day boundary |
| `irradiance_wm2` | float | `0 <= value <= ~1000` |
| `module_temp_c` | float | plausible ambient-to-module range |
| `inverter_temp_c` | float | plausible ambient-to-inverter range |
| `status` | enum | `ONLINE` \| `OFFLINE` \| `WARNING` |
| `availability` | float | `0.0` or `1.0` |
| `timestamp` | string | ISO 8601, UTC, `Z` suffix |
| `simulator_scenario` | string \| null | anomaly label when a scenario is active, else `null` |

Producer guarantees:

- only events passing validation reach `solar.telemetry.raw`,
- a record that fails validation goes to `solar.telemetry.invalid` with error
  metadata, never to the raw topic,
- `simulator_scenario` is demo metadata. **No detection logic may depend on it** —
  Member 2's rules must infer underperformance from the physical fields alone,
  otherwise the pipeline proves nothing.

## 3. Daily reference feed

**Directory:** `SIMULATION_OUTPUT_DIR`, default `/data/daily`.
**Filename:** `daily_reference_YYYY-MM-DD.csv` — the date is the *simulated* day.

One CSV per simulated day, one row per plant, header row required. Columns in
exactly this order:

```text
simulation_date,plant_id,plant_capacity_kw,expected_generation_kwh,expected_peak_power_kw,forecast_irradiance_kwh_m2,ppa_rate_per_kwh,maintenance_flag,source_version
```

| Column | Type | Rules |
|---|---|---|
| `simulation_date` | date | `YYYY-MM-DD`, identical on every row, matches the filename |
| `plant_id` | string | must exist in the portfolio config, unique within the file |
| `plant_capacity_kw` | float | `> 0`, matches the portfolio config |
| `expected_generation_kwh` | float | `> 0` |
| `expected_peak_power_kw` | float | `> 0`, `<= plant_capacity_kw` |
| `forecast_irradiance_kwh_m2` | float | `> 0` |
| `ppa_rate_per_kwh` | float | `> 0`, fictional commercial rate |
| `maintenance_flag` | boolean | `true`/`false` |
| `source_version` | string | feed schema version, e.g. `v1` |

Write guarantees:

- the file is written to a temporary name and then renamed into place, so Airflow
  can never read a partially written file,
- the generator logs `daily_reference_ready` once the rename completes,
- regenerating the same simulated day is only permitted in explicit reset/demo
  mode.

## 4. Simulated clock

```text
SIMULATION_DAY_SECONDS=300      1 simulated day = 5 real minutes
TELEMETRY_INTERVAL_SECONDS=3    one tick per inverter every 3 real seconds
SIMULATION_SEED=8203            fixed seed; same seed produces the same run
SIMULATION_START_DATE=2026-08-21
```

All internal timestamps are UTC. Every value is environment-configurable; nothing
downstream may hard-code these numbers.

## 5. PostgreSQL serving tables

Owned by Member 2 (`storage/migrations/`), read by Member 3.

```text
plants                  asset master, seeded from portfolio.yaml
inverters               asset master, seeded from portfolio.yaml
live_plant_metrics      windowed per-plant metrics from the speed layer
live_portfolio_metrics  windowed portfolio rollup
alerts                  operational and pipeline alerts
daily_reference         loaded daily reference feed
daily_plant_summary     reconciled actual vs expected per plant per day
pipeline_health         processing-layer health signals
```

## 6. REST API

Owned by Member 3.

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

## 7. Deterministic anomaly timeline

Member 1 injects these on a fixed schedule so every demo run is identical.
Member 2's detection rules must find them without reading `simulator_scenario`.

| Simulated-day window | Scenario | Effect |
|---|---|---|
| 0–90 s | `NORMAL` | baseline generation |
| 90–150 s | `INV_UNDERPERFORMANCE` | target inverter power × 0.45, `status=WARNING`, **irradiance stays normal** |
| 150–190 s | `RECOVERY` | back to baseline |
| 190–235 s | `INV_OFFLINE` | power 0, `availability=0`, `status=OFFLINE` |
| 235–260 s | `TELEMETRY_GAP` | target publishes **no events at all** |
| 260–300 s | `RECOVERY` | back to baseline |

`INV_OFFLINE` and `TELEMETRY_GAP` are deliberately different: offline is a
reported zero, a gap is silence. The first is an operational alert, the second a
pipeline-health alert.

Exact windows and targets are configurable; the schedule file is the contract for
what the assessment demo will show.
