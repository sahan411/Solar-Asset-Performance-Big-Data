-- SolarIQ serving schema — 003: business/operational alerts.
--
-- These are SOLAR OPERATIONAL alerts (an asset is losing energy and therefore
-- money). They are deliberately distinct from pipeline-health alerts, which are
-- Prometheus rules over the metrics in pipeline_health (migration 005).

CREATE TABLE IF NOT EXISTS alerts (
    id                     TEXT PRIMARY KEY,
    plant_id               TEXT NOT NULL REFERENCES plants(id) ON DELETE CASCADE,
    -- NULL means the alert is plant-wide rather than attributed to one inverter.
    inverter_id            TEXT,

    alert_type             TEXT NOT NULL
        CHECK (alert_type IN ('UNDERPERFORMANCE', 'INVERTER_OFFLINE', 'TELEMETRY_GAP')),
    severity               TEXT NOT NULL
        CHECK (severity IN ('WARNING', 'CRITICAL')),
    message                TEXT NOT NULL,

    started_at             TIMESTAMPTZ NOT NULL,
    ended_at               TIMESTAMPTZ,
    status                 TEXT NOT NULL
        CHECK (status IN ('ACTIVE', 'RESOLVED')),

    -- Financial impact accumulated over the life of the alert. This is the
    -- "so what" of the whole platform: energy lost translated into money.
    estimated_loss_kwh     DOUBLE PRECISION,
    estimated_revenue_loss DOUBLE PRECISION,

    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- A resolved alert must have an end time; an active one must not.
    CONSTRAINT alerts_ended_at_matches_status CHECK (
        (status = 'ACTIVE'   AND ended_at IS NULL) OR
        (status = 'RESOLVED' AND ended_at IS NOT NULL)
    ),
    CONSTRAINT alerts_interval_ordered CHECK (ended_at IS NULL OR ended_at >= started_at)
);

COMMENT ON TABLE alerts IS 'Business/operational solar alerts. Pipeline-health problems belong in pipeline_health, not here.';

-- Anti-spam guarantee enforced by the database rather than by hoping the stream
-- job behaves: at most ONE active alert may exist per asset+type at a time. The
-- streaming sink relies on this to upsert-or-open instead of inserting a new row
-- every microbatch. COALESCE maps plant-wide alerts (NULL inverter) onto a
-- comparable key, because NULLs are not equal to each other in a unique index.
CREATE UNIQUE INDEX IF NOT EXISTS alerts_single_active_per_asset_idx
    ON alerts (plant_id, COALESCE(inverter_id, ''), alert_type)
    WHERE status = 'ACTIVE';

-- Serving pattern: "active alerts, newest first" and the alerts screen listing.
CREATE INDEX IF NOT EXISTS alerts_status_started_idx ON alerts (status, started_at DESC);
CREATE INDEX IF NOT EXISTS alerts_plant_started_idx  ON alerts (plant_id, started_at DESC);
