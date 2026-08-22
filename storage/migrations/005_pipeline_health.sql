-- SolarIQ serving schema — 005: pipeline health.
--
-- ENGINEERING observability, not solar operations. One row per pipeline
-- component, upserted as that component makes progress. This is what backs the
-- mandatory "no telemetry processed recently" health rule and the API's /ready
-- endpoint, and it is what makes the demo able to distinguish
--   "the solar asset is broken"  (alerts table)
-- from
--   "our data pipeline is broken" (this table).

CREATE TABLE IF NOT EXISTS pipeline_health (
    component       TEXT PRIMARY KEY,
    status          TEXT NOT NULL
        CHECK (status IN ('HEALTHY', 'DEGRADED', 'STALE', 'FAILED')),

    -- Event time of the most recent telemetry the component handled. Staleness
    -- is measured against this, so a component that is running but no longer
    -- receiving data still reports as stale.
    last_event_at   TIMESTAMPTZ,
    -- Wall-clock time the component last completed a unit of work successfully.
    last_success_at TIMESTAMPTZ,

    message         TEXT,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE  pipeline_health IS 'Per-component pipeline liveness. Engineering health only; solar problems belong in alerts.';
COMMENT ON COLUMN pipeline_health.component IS 'Stable component name, e.g. spark-stream, airflow-daily-reconciliation.';
COMMENT ON COLUMN pipeline_health.last_event_at IS 'Event-time watermark of processed data, not wall clock.';
