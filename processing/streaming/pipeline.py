"""Wiring for the SolarIQ streaming job.

The source topic fans out into three independent sinks, each with its own
checkpoint so their progress cannot become entangled:

    Kafka -> parse -> validate -+-> invalid  -> solar.telemetry.invalid
                                |
                                +-> valid -> normalize -> dedup -+-> live metrics + alerts (PostgreSQL)
                                                                 |
                                                                 +-> raw Parquet archive (MinIO)

De-duplication sits on the *streaming* DataFrame rather than inside
foreachBatch, because only there does Spark keep the watermarked state that lets
it recognise a duplicate arriving in a later microbatch.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from processing.common.config import StreamSettings
from processing.common.db import connect, fetch_all
from processing.common.logging import get_logger
from processing.streaming.alert_store import reconcile_alerts
from processing.streaming.alerts import CONDITION_COLUMNS, detect_alert_conditions
from processing.streaming.health import (
    STATUS_DEGRADED,
    STATUS_HEALTHY,
    count_active_alerts,
    record_batch_metrics,
    record_health,
)
from processing.streaming.metrics import (
    PLANT_METRIC_COLUMNS,
    PORTFOLIO_METRIC_COLUMNS,
    latest_reading_per_inverter,
    plant_metrics,
    portfolio_metrics,
    select_plant_metric_columns,
    select_portfolio_metric_columns,
)
from processing.streaming.sinks import collect_rows, execute_batch_metrics_write
from processing.streaming.validation import (
    deduplicate_events,
    invalid_events,
    normalize_valid_events,
    to_quarantine_records,
    valid_events,
    validate_telemetry,
)

log = get_logger("spark-stream")


def load_plant_reference(spark: SparkSession, database_url: str) -> DataFrame:
    """Plant capacity and configured inverter count, from the asset registry.

    Read through psycopg2 rather than JDBC: the registry is a few dozen rows, and
    this avoids adding a JDBC driver jar to the Spark image for no benefit.

    Loaded once at startup. A plant added to the registry mid-run is picked up on
    the next restart, which is acceptable for a fleet that changes on the scale of
    months.
    """
    with connect(database_url) as conn:
        rows = fetch_all(
            conn,
            """
            SELECT p.id, p.capacity_kw, COUNT(i.id)::int
              FROM plants p
              LEFT JOIN inverters i ON i.plant_id = p.id AND i.active
             WHERE p.active
             GROUP BY p.id, p.capacity_kw
            """,
        )

    return spark.createDataFrame(
        [(pid, float(cap), int(count)) for pid, cap, count in rows],
        schema="plant_id string, capacity_kw double, configured_inverters int",
    )


def load_inverter_reference(spark: SparkSession, database_url: str) -> DataFrame:
    """The configured inverter fleet — what detection is driven from."""
    with connect(database_url) as conn:
        rows = fetch_all(
            conn,
            "SELECT plant_id, id, rated_power_kw FROM inverters WHERE active",
        )

    return spark.createDataFrame(
        [(plant, inverter, float(rating)) for plant, inverter, rating in rows],
        schema="plant_id string, inverter_id string, rated_power_kw double",
    )


def build_valid_stream(parsed: DataFrame, settings: StreamSettings) -> DataFrame:
    """Validated, normalized, de-duplicated telemetry."""
    validated = validate_telemetry(parsed)
    return deduplicate_events(
        normalize_valid_events(valid_events(validated)), settings.watermark
    )


def build_invalid_stream(parsed: DataFrame) -> DataFrame:
    """Quarantine records, ready for the invalid topic."""
    return to_quarantine_records(invalid_events(validate_telemetry(parsed)))


@dataclass
class MicrobatchContext:
    """Everything the per-batch processor needs, resolved once at startup."""

    database_url: str
    settings: StreamSettings
    plant_reference: DataFrame
    inverter_reference: DataFrame


def process_microbatch(batch: DataFrame, batch_id: int, ctx: MicrobatchContext) -> None:
    """Compute and persist one microbatch's metrics, alerts and health.

    Runs on the driver. Everything inside a single transaction, so the serving
    layer never sees metrics without their corresponding alert state.
    """
    # Spark may hand the same batch back after a failure; caching avoids
    # recomputing the whole chain for each of the several actions below.
    batch.persist()
    try:
        if batch.isEmpty():
            # Alive but idle. Recorded so a consumer can tell "no data" apart
            # from "job died", which look identical from the outside.
            with connect(ctx.database_url) as conn:
                record_health(conn, STATUS_DEGRADED, None, "no telemetry in this microbatch")
            record_batch_metrics(processed=0, invalid=0, last_event_at=None)
            return

        latest = latest_reading_per_inverter(batch)
        plants = plant_metrics(batch, ctx.plant_reference, ctx.settings)
        portfolio = portfolio_metrics(plants)

        plant_rows = collect_rows(select_plant_metric_columns(plants), PLANT_METRIC_COLUMNS)
        portfolio_rows = collect_rows(
            select_portfolio_metric_columns(portfolio), PORTFOLIO_METRIC_COLUMNS
        )

        # The batch's observation clock: the latest event time it contained.
        observed_at = batch.agg({"event_time": "max"}).collect()[0][0]
        observed_at = _as_utc(observed_at)

        conditions = detect_alert_conditions(
            latest, ctx.inverter_reference, ctx.settings, observed_at
        ).select(*CONDITION_COLUMNS).collect()

        processed = batch.count()

        with connect(ctx.database_url) as conn:
            execute_batch_metrics_write(conn, plant_rows, portfolio_rows)
            outcome = reconcile_alerts(
                conn, conditions, observed_at, float(ctx.settings.alert_sustain_seconds)
            )
            active_alerts = count_active_alerts(conn)
            record_health(
                conn,
                STATUS_HEALTHY,
                observed_at,
                f"processed {processed} events across {len(plant_rows)} plants",
            )

        record_batch_metrics(
            processed=processed,
            invalid=0,
            last_event_at=observed_at,
            active_alerts=active_alerts,
        )

        log.info(
            "microbatch_processed",
            f"Batch {batch_id}: {processed} events, {len(plant_rows)} plants",
            batch_id=batch_id,
            events=processed,
            plants=len(plant_rows),
            alerts_opened=outcome.opened,
            alerts_resolved=outcome.resolved,
            active_alerts=active_alerts,
        )
    finally:
        batch.unpersist()


def _as_utc(value: Any) -> datetime | None:
    """Attach UTC to a timestamp Spark collected as a naive local datetime.

    PySpark renders TimestampType in the host's local zone with tzinfo stripped.
    The instant is correct relative to the host, so localising it back and
    converting to UTC recovers the true instant on any machine.
    """
    if value is None:
        return None
    if value.tzinfo is None:
        return value.astimezone(timezone.utc)
    return value.astimezone(timezone.utc)
