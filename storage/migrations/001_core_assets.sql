-- SolarIQ serving schema — 001: core asset registry.
--
-- These two tables are the portfolio's static reference data. They are seeded
-- from the shared portfolio configuration owned by Member 1 (see
-- storage/seed_portfolio.py) rather than being written by the streaming job.
-- Every metric/alert table below references plants(id) so that a metric can
-- never point at an asset the platform does not know about.

CREATE TABLE IF NOT EXISTS plants (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    capacity_kw DOUBLE PRECISION NOT NULL CHECK (capacity_kw > 0),
    timezone    TEXT NOT NULL DEFAULT 'UTC',
    active      BOOLEAN NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE  plants IS 'Static solar plant registry, seeded from the shared portfolio config.';
COMMENT ON COLUMN plants.capacity_kw IS 'Installed DC/AC nameplate capacity in kW; used as the denominator of the live expected-power proxy.';

CREATE TABLE IF NOT EXISTS inverters (
    id             TEXT NOT NULL,
    plant_id       TEXT NOT NULL REFERENCES plants(id) ON DELETE CASCADE,
    name           TEXT NOT NULL,
    rated_power_kw DOUBLE PRECISION NOT NULL CHECK (rated_power_kw > 0),
    active         BOOLEAN NOT NULL DEFAULT TRUE,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (plant_id, id)
);

COMMENT ON TABLE inverters IS 'Static inverter registry. (plant_id, id) matches the Kafka message key plant_id:inverter_id.';

-- Supports "all inverters for this plant" lookups from the serving API.
CREATE INDEX IF NOT EXISTS inverters_plant_id_idx ON inverters (plant_id);
