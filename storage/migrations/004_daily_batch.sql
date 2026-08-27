-- SolarIQ serving schema — 004: batch-layer (daily) tables.
--
-- daily_reference is the ingested expectation feed (Member 1's generator).
-- daily_plant_summary is the authoritative end-of-day reconciliation computed by
-- Airflow from the raw Parquet archive, NOT from the live tables. Both are keyed
-- by (simulation_date, plant_id) so re-running the DAG for a date is idempotent.
--
-- MONEY REPRESENTATION: revenue columns use DOUBLE PRECISION. This is a
-- deliberate Phase 1 simplification for a simulated fictional PPA rate; a
-- commercial deployment settling real invoices would use NUMERIC(14,4) to avoid
-- binary floating-point rounding. Documented in docs/architecture.md.

CREATE TABLE IF NOT EXISTS daily_reference (
    simulation_date            DATE NOT NULL,
    plant_id                   TEXT NOT NULL REFERENCES plants(id) ON DELETE CASCADE,

    plant_capacity_kw          DOUBLE PRECISION NOT NULL CHECK (plant_capacity_kw > 0),
    expected_generation_kwh    DOUBLE PRECISION NOT NULL CHECK (expected_generation_kwh > 0),
    expected_peak_power_kw     DOUBLE PRECISION NOT NULL CHECK (expected_peak_power_kw > 0),
    forecast_irradiance_kwh_m2 DOUBLE PRECISION NOT NULL CHECK (forecast_irradiance_kwh_m2 >= 0),
    -- Fictional simulated commercial rate, per kWh, in the project's notional currency.
    ppa_rate_per_kwh           DOUBLE PRECISION NOT NULL CHECK (ppa_rate_per_kwh >= 0),
    maintenance_flag           BOOLEAN NOT NULL DEFAULT FALSE,
    source_version             TEXT NOT NULL,

    loaded_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (simulation_date, plant_id)
);

COMMENT ON TABLE daily_reference IS 'Daily expectation feed: what each plant SHOULD have generated, and what that energy is worth.';


CREATE TABLE IF NOT EXISTS daily_plant_summary (
    simulation_date           DATE NOT NULL,
    plant_id                  TEXT NOT NULL REFERENCES plants(id) ON DELETE CASCADE,

    actual_generation_kwh     DOUBLE PRECISION NOT NULL CHECK (actual_generation_kwh >= 0),
    expected_generation_kwh   DOUBLE PRECISION NOT NULL,

    performance_pct           DOUBLE PRECISION,
    availability_pct          DOUBLE PRECISION,
    downtime_minutes          DOUBLE PRECISION,

    estimated_lost_energy_kwh DOUBLE PRECISION,
    -- Copied from daily_reference at compute time so the report is reproducible
    -- even if the rate is later revised: the summary records the rate it used.
    ppa_rate_per_kwh          DOUBLE PRECISION,
    estimated_actual_revenue  DOUBLE PRECISION,
    estimated_lost_revenue    DOUBLE PRECISION,

    alert_count               INTEGER NOT NULL DEFAULT 0,
    -- Carried through rather than used to exclude the plant: a plant under
    -- planned maintenance still appears in the report, flagged.
    maintenance_flag          BOOLEAN NOT NULL DEFAULT FALSE,

    computed_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (simulation_date, plant_id)
);

COMMENT ON TABLE daily_plant_summary IS 'Authoritative end-of-day reconciliation from the raw Parquet archive.';
COMMENT ON COLUMN daily_plant_summary.ppa_rate_per_kwh IS 'Rate actually used for this row''s revenue figures.';

-- Serving pattern: fetch a whole simulated day for the report screen.
CREATE INDEX IF NOT EXISTS daily_plant_summary_date_idx ON daily_plant_summary (simulation_date);
