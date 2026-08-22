-- SolarIQ serving schema — 002: speed-layer (live) metric tables.
--
-- Written by the Spark Structured Streaming job, one row per event-time window.
-- The primary keys are the window identity, which is what makes the stream sink
-- idempotent: replaying a microbatch after a failure upserts the same row
-- instead of appending a duplicate.
--
-- UNITS: *_kw = kilowatts (instantaneous power), *_pct = percent (0-100),
--        *_wm2 = watts per square metre. All timestamps are UTC.

CREATE TABLE IF NOT EXISTS live_plant_metrics (
    plant_id           TEXT NOT NULL REFERENCES plants(id) ON DELETE CASCADE,
    window_start       TIMESTAMPTZ NOT NULL,
    window_end         TIMESTAMPTZ NOT NULL,

    -- Sum of the latest reading per inverter in this window, NOT a sum of all
    -- samples (that would multiply-count repeated 3-second readings).
    current_power_kw   DOUBLE PRECISION NOT NULL,
    avg_power_kw       DOUBLE PRECISION NOT NULL,

    -- Simplified irradiance-normalised expected-power proxy. Persisted so the
    -- dashboard can plot actual vs expected without recomputing the model.
    expected_power_kw  DOUBLE PRECISION,
    avg_irradiance_wm2 DOUBLE PRECISION,

    availability_pct   DOUBLE PRECISION,
    -- NULL when expected power is below the night/low-light threshold: dividing
    -- by a near-zero expectation produces meaningless percentages after dark.
    performance_pct    DOUBLE PRECISION,
    estimated_loss_kw  DOUBLE PRECISION,

    online_inverters   INTEGER,
    offline_inverters  INTEGER,

    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (plant_id, window_start, window_end)
);

COMMENT ON COLUMN live_plant_metrics.current_power_kw IS 'Sum of latest active_power_kw per inverter within the window.';
COMMENT ON COLUMN live_plant_metrics.performance_pct  IS 'actual/expected*100; NULL below the minimum expected-power threshold.';

-- Serving pattern is "latest window per plant" (SELECT DISTINCT ON ... ORDER BY
-- plant_id, window_end DESC), so index the plant/recency access path.
CREATE INDEX IF NOT EXISTS live_plant_metrics_plant_recent_idx
    ON live_plant_metrics (plant_id, window_end DESC);


CREATE TABLE IF NOT EXISTS live_portfolio_metrics (
    window_start      TIMESTAMPTZ NOT NULL,
    window_end        TIMESTAMPTZ NOT NULL,

    current_power_kw  DOUBLE PRECISION NOT NULL,
    avg_power_kw      DOUBLE PRECISION NOT NULL,
    expected_power_kw DOUBLE PRECISION,

    online_inverters  INTEGER NOT NULL,
    offline_inverters INTEGER NOT NULL,

    availability_pct  DOUBLE PRECISION,
    -- Capacity-weighted: sum(actual)/sum(expected)*100 across plants, never a
    -- naive mean of per-plant percentages (plants differ in size).
    performance_pct   DOUBLE PRECISION,
    estimated_loss_kw DOUBLE PRECISION,

    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (window_start, window_end)
);

COMMENT ON COLUMN live_portfolio_metrics.performance_pct IS 'Weighted: sum(actual_power_kw)/sum(expected_power_kw)*100.';

CREATE INDEX IF NOT EXISTS live_portfolio_metrics_recent_idx
    ON live_portfolio_metrics (window_end DESC);
