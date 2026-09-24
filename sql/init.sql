-- PostgreSQL schema for the Smart Grid pipeline.
-- Runs once, automatically, when the postgres container starts with an empty volume.

-- Airflow keeps its own metadata in a separate database on the same server.
CREATE DATABASE airflow;

-- ---------------------------------------------------------------------------
-- Master dataset (written by the Spark speed layer, read by the Airflow batch layer)
-- ---------------------------------------------------------------------------

-- Every clean smart-meter reading. Append-only and never updated: this is the
-- immutable record the batch layer recomputes bills from. The primary key makes
-- the insert idempotent, so a duplicated or replayed Kafka message is ignored.
CREATE TABLE meter_readings (
    meter_id               TEXT             NOT NULL,
    household_id           TEXT             NOT NULL,
    grid_zone              TEXT             NOT NULL,
    event_time             TIMESTAMP        NOT NULL,  -- simulated time (UTC)
    power_consumption_kwh  DOUBLE PRECISION NOT NULL,
    solar_generation_kwh   DOUBLE PRECISION NOT NULL,
    trace_id               TEXT,                         -- from the Kafka header
    kafka_partition        INT,
    kafka_offset           BIGINT,
    ingested_at            TIMESTAMPTZ      NOT NULL DEFAULT now(),  -- real time
    PRIMARY KEY (meter_id, event_time)
);
CREATE INDEX meter_readings_event_time_idx ON meter_readings (event_time);
CREATE INDEX meter_readings_ingested_at_idx ON meter_readings (ingested_at);
CREATE INDEX meter_readings_trace_id_idx ON meter_readings (trace_id);

-- Readings that failed validation, kept with the reason for debugging.
CREATE TABLE rejected_readings (
    id               BIGSERIAL   PRIMARY KEY,
    raw_value        TEXT,
    reason           TEXT        NOT NULL,
    trace_id         TEXT,
    kafka_partition  INT,
    kafka_offset     BIGINT,
    rejected_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX rejected_readings_trace_id_idx ON rejected_readings (trace_id);

-- ---------------------------------------------------------------------------
-- Real-time view (speed layer)
-- ---------------------------------------------------------------------------

-- Grid load and renewable contribution per zone per simulated hour.
-- Upserted by Spark as each window fills up.
CREATE TABLE zone_load_hourly (
    grid_zone          TEXT             NOT NULL,
    window_start       TIMESTAMP        NOT NULL,
    window_end         TIMESTAMP        NOT NULL,
    consumption_kwh    DOUBLE PRECISION NOT NULL,
    solar_kwh          DOUBLE PRECISION NOT NULL,
    net_grid_load_kwh  DOUBLE PRECISION NOT NULL,  -- consumption minus solar
    renewable_pct      DOUBLE PRECISION NOT NULL,  -- share of demand met by solar
    readings           INT              NOT NULL,
    expected_readings  INT              NOT NULL,  -- meters in zone x 4 readings/hour
    updated_at         TIMESTAMPTZ      NOT NULL DEFAULT now(),
    PRIMARY KEY (grid_zone, window_start)
);

-- Threshold-based alerts raised by the speed layer.
CREATE TABLE alerts (
    id            BIGSERIAL        PRIMARY KEY,
    alert_type    TEXT             NOT NULL,
    grid_zone     TEXT             NOT NULL,
    window_start  TIMESTAMP        NOT NULL,
    value         DOUBLE PRECISION NOT NULL,
    threshold     DOUBLE PRECISION NOT NULL,
    message       TEXT             NOT NULL,
    created_at    TIMESTAMPTZ      NOT NULL DEFAULT now(),
    UNIQUE (alert_type, grid_zone, window_start)
);

-- ---------------------------------------------------------------------------
-- Batch views (batch layer)
-- ---------------------------------------------------------------------------

-- The daily tariff file, after validation.
CREATE TABLE tariffs (
    tariff_date   DATE          NOT NULL,
    household_id  TEXT          NOT NULL,
    tariff_rate   NUMERIC(8, 4) NOT NULL,  -- currency units per kWh
    billing_tier  TEXT          NOT NULL,
    subsidy_flag  BOOLEAN       NOT NULL,
    loaded_at     TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (tariff_date, household_id)
);

-- One bill per household per simulated day: the consolidated daily report.
CREATE TABLE daily_bills (
    bill_date               DATE             NOT NULL,
    household_id            TEXT             NOT NULL,
    grid_zone               TEXT             NOT NULL,
    consumption_kwh         DOUBLE PRECISION NOT NULL,
    solar_kwh               DOUBLE PRECISION NOT NULL,
    grid_import_kwh         DOUBLE PRECISION NOT NULL,
    solar_export_kwh        DOUBLE PRECISION NOT NULL,
    solar_contribution_pct  DOUBLE PRECISION NOT NULL,
    tariff_rate             NUMERIC(8, 4)    NOT NULL,
    billing_tier            TEXT             NOT NULL,
    subsidy_flag            BOOLEAN          NOT NULL,
    energy_charge           NUMERIC(12, 2)   NOT NULL,
    solar_credit            NUMERIC(12, 2)   NOT NULL,
    service_charge          NUMERIC(12, 2)   NOT NULL,
    subsidy_discount        NUMERIC(12, 2)   NOT NULL,
    total_bill              NUMERIC(12, 2)   NOT NULL,
    computed_at             TIMESTAMPTZ      NOT NULL DEFAULT now(),
    PRIMARY KEY (bill_date, household_id)
);

-- One row per finished billing day. The Airflow DAG uses it to know which
-- days are done.
CREATE TABLE billing_runs (
    bill_date             DATE           PRIMARY KEY,
    households_billed     INT            NOT NULL,
    households_unbilled   INT            NOT NULL,  -- had readings but no valid tariff
    tariff_rows_rejected  INT            NOT NULL,
    total_billed          NUMERIC(12, 2) NOT NULL,
    report_file           TEXT           NOT NULL,
    finished_at           TIMESTAMPTZ    NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- Observability
-- ---------------------------------------------------------------------------

-- Row counts for every Spark micro-batch and every billing run, plus
-- end-to-end latency (simulator send -> stored in PostgreSQL) for stream_ingest.
CREATE TABLE pipeline_metrics (
    id              BIGSERIAL   PRIMARY KEY,
    stage           TEXT        NOT NULL,  -- stream_ingest | stream_aggregate | batch_billing
    batch_id        BIGINT,
    rows_in         INT         NOT NULL,
    rows_valid      INT         NOT NULL,
    rows_invalid    INT         NOT NULL,
    rows_duplicate  INT         NOT NULL,
    duration_ms     INT         NOT NULL,
    avg_latency_ms  INT,
    max_latency_ms  INT,
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX pipeline_metrics_recorded_at_idx ON pipeline_metrics (stage, recorded_at);
