-- SolarIQ serving schema — 006: sustained-condition tracking for alerts.
--
-- An alert must not fire on a single bad reading. A passing cloud drops output
-- for seconds; a failing inverter stays down. This table records how long a
-- fault condition has persisted so the alert engine can require it to be
-- SUSTAINED before opening an alert, and can close the alert once it clears.
--
-- It is deliberately separate from `alerts`: a condition is an observation the
-- pipeline is still making up its mind about, whereas a row in `alerts` is a
-- statement to an operator that something is wrong. Keeping unconfirmed
-- observations out of `alerts` is what stops the dashboard flickering.
--
-- Durations are measured in EVENT time, not wall-clock time. Under the
-- compressed demo clock one simulated day passes in five real minutes, so wall
-- time says nothing about how long a plant has actually been underperforming.
-- Event time also makes alerting replay-safe: reprocessing history produces the
-- same alerts it produced live.

CREATE TABLE IF NOT EXISTS alert_conditions (
    plant_id           TEXT NOT NULL REFERENCES plants(id) ON DELETE CASCADE,
    -- Empty string rather than NULL for plant-wide conditions, so the composite
    -- primary key compares correctly (NULLs are never equal to each other).
    inverter_id        TEXT NOT NULL DEFAULT '',
    alert_type         TEXT NOT NULL
        CHECK (alert_type IN ('UNDERPERFORMANCE', 'INVERTER_OFFLINE', 'TELEMETRY_GAP')),

    severity           TEXT NOT NULL CHECK (severity IN ('WARNING', 'CRITICAL')),
    message            TEXT NOT NULL,

    -- Event time the condition was first and most recently seen. The difference
    -- between them is the sustained duration the alert rule tests.
    first_observed_at  TIMESTAMPTZ NOT NULL,
    last_observed_at   TIMESTAMPTZ NOT NULL,
    observation_count  INTEGER NOT NULL DEFAULT 1,

    -- Running total of observed power shortfall, divided by observation_count to
    -- get a mean loss rate. Averaging over the condition's life is more honest
    -- than using the latest instantaneous value, which could be an outlier.
    sum_loss_kw        DOUBLE PRECISION NOT NULL DEFAULT 0,

    PRIMARY KEY (plant_id, inverter_id, alert_type)
);

COMMENT ON TABLE alert_conditions IS 'In-progress fault observations. Promoted to `alerts` once sustained long enough.';
COMMENT ON COLUMN alert_conditions.first_observed_at IS 'Event time, not wall clock: durations must survive the compressed demo clock and replay.';
