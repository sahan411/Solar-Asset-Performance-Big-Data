"""Task functions for the daily reconciliation DAG.

Every Airflow task in `orchestration/dags/` is a thin call into a function here.
The DAG file then contains only wiring — schedule, dependencies, retries — and
none of the business logic, so the logic can be tested without Airflow installed
and the DAG file stays parseable in milliseconds.

Airflow parses every DAG file on a short interval. Anything expensive at module
scope (a Spark session, a database connection) would run on every parse, so all
of that happens inside the task functions, never at import.

Each function takes an explicit `simulation_date` rather than reading Airflow's
execution context, which is what lets the same code be called from a test, from
a backfill, or from the command line.
"""

from __future__ import annotations

from datetime import date
from typing import Any

from processing.batch.actuals import (
    collect_plant_actuals,
    plant_daily_actuals,
    read_daily_archive,
)
from processing.batch.reconcile import (
    PlantSummary,
    ReconciliationResult,
    alert_counts_for_day,
    reconcile_day,
    reference_by_plant,
    write_daily_summary,
)
from processing.batch.reference import (
    ReferenceFeedError,
    configured_plant_ids,
    load_reference_file,
    load_reference_into_db,
    reference_path,
)
from processing.common.config import BatchSettings, DatabaseSettings, ObjectStoreSettings
from processing.common.db import connect, fetch_all, fetch_one
from processing.common.logging import get_logger
from processing.streaming.health import STATUS_FAILED, STATUS_HEALTHY, record_health

log = get_logger("airflow-batch")

COMPONENT_BATCH = "airflow-daily-reconciliation"


def _spark(app_name: str = "solariq-daily-reconciliation"):
    """Build a Spark session configured for the MinIO raw archive.

    Imported lazily so that a task which does not need Spark — and the DAG parse
    itself — never pays for importing PySpark.
    """
    from processing.streaming.session import create_spark_session

    return create_spark_session(
        app_name=app_name,
        object_store=ObjectStoreSettings.from_env(),
    )


def validate_reference_feed(simulation_date: date) -> dict[str, Any]:
    """Check the day's reference CSV against the contract and the asset registry.

    Deliberately separate from loading it: a feed that fails validation must stop
    the DAG before anything touches the database, so a bad file cannot half-load.
    """
    settings = BatchSettings.from_env()
    path = reference_path(settings.reference_dir, simulation_date)

    with connect(DatabaseSettings.from_env().url) as conn:
        expected_plants = configured_plant_ids(conn)

    feed = load_reference_file(path, expected_plants, simulation_date)

    log.info(
        "reference_validated",
        f"Reference feed for {simulation_date.isoformat()} is valid",
        simulation_date=simulation_date.isoformat(),
        plants=feed.plant_count,
        path=str(path),
    )
    return {"path": str(path), "plants": feed.plant_count, "warnings": feed.warnings}


def check_raw_daily_data(simulation_date: date) -> dict[str, Any]:
    """Confirm the archive holds telemetry for the day before reconciling it.

    Without this the DAG would happily reconcile an empty day and report every
    plant at zero generation — indistinguishable from a real total outage. Better
    to fail loudly: an empty archive is a pipeline problem, not a solar one.
    """
    object_store = ObjectStoreSettings.from_env()
    spark = _spark("solariq-archive-check")
    try:
        events = read_daily_archive(spark, object_store.raw_telemetry_uri, simulation_date)
        count = events.count()
        plants = [row[0] for row in events.select("plant_id").distinct().collect()]
    finally:
        spark.stop()

    if count == 0:
        raise ValueError(
            f"No archived telemetry for {simulation_date.isoformat()} at "
            f"{object_store.raw_telemetry_uri}. The streaming job writes this "
            "archive; check that it ran and that the simulated day completed."
        )

    log.info(
        "raw_archive_verified",
        f"{count} archived events across {len(plants)} plant(s)",
        simulation_date=simulation_date.isoformat(),
        events=count,
        plants=len(plants),
    )
    return {"events": count, "plants": sorted(plants)}


def load_reference(simulation_date: date) -> int:
    """Validate again, then upsert the feed into `daily_reference`.

    Re-validating rather than trusting the earlier task is intentional: Airflow
    tasks run in separate processes and can be retried or cleared individually,
    so this task must be correct when run on its own.
    """
    settings = BatchSettings.from_env()
    database_url = DatabaseSettings.from_env().url
    path = reference_path(settings.reference_dir, simulation_date)

    with connect(database_url) as conn:
        expected_plants = configured_plant_ids(conn)

    feed = load_reference_file(path, expected_plants, simulation_date)

    with connect(database_url) as conn:
        return load_reference_into_db(conn, feed)


def compute_daily_actuals(simulation_date: date) -> dict[str, dict]:
    """Aggregate the day's archived telemetry into per-plant actuals.

    Returns a plain mapping — one entry per plant — which is small enough to pass
    through XCom to the reconciliation task.
    """
    settings = BatchSettings.from_env()
    object_store = ObjectStoreSettings.from_env()

    spark = _spark()
    try:
        events = read_daily_archive(spark, object_store.raw_telemetry_uri, simulation_date)
        actuals = collect_plant_actuals(
            plant_daily_actuals(events, settings.telemetry_interval_seconds)
        )
    finally:
        # Released explicitly: an Airflow worker slot should not hold a JVM open
        # between tasks.
        spark.stop()

    log.info(
        "daily_actuals_computed",
        f"Computed actuals for {len(actuals)} plant(s)",
        simulation_date=simulation_date.isoformat(),
        plants=len(actuals),
    )
    return actuals


def reconcile_expected_actual(
    simulation_date: date, actuals: dict[str, dict]
) -> list[dict]:
    """Join actuals to expectations and price the shortfall.

    Returns JSON-safe summary dicts for XCom; `write_daily_summary_task`
    reconstitutes them.
    """
    with connect(DatabaseSettings.from_env().url) as conn:
        reference = reference_by_plant(conn, simulation_date)
        alert_counts = alert_counts_for_day(conn, simulation_date)

    result = reconcile_day(simulation_date, actuals, reference, alert_counts)

    log.info(
        "day_reconciled",
        f"Portfolio at {result.portfolio_performance_pct:.1f}% of expected"
        if result.portfolio_performance_pct is not None
        else "Portfolio performance unavailable",
        simulation_date=simulation_date.isoformat(),
        plants=len(result.summaries),
        portfolio_lost_revenue=result.portfolio_lost_revenue,
    )
    return [summary.to_dict() for summary in result.summaries]


def write_daily_summary_task(simulation_date: date, summaries: list[dict]) -> int:
    """Persist the reconciled day to `daily_plant_summary`."""
    result = ReconciliationResult(
        simulation_date=simulation_date,
        summaries=[PlantSummary.from_dict(item) for item in summaries],
    )

    with connect(DatabaseSettings.from_env().url) as conn:
        written = write_daily_summary(conn, result)
        record_health(
            conn,
            STATUS_HEALTHY,
            None,
            f"reconciled {written} plant(s) for {simulation_date.isoformat()}",
            component=COMPONENT_BATCH,
        )
    return written


def run_data_quality_checks(simulation_date: date) -> dict[str, Any]:
    """Verify what actually landed in the database, not what we intended to write.

    The reconciliation already rejects bad inputs; this checks the stored result,
    which is what Member 3's API and the daily report will serve. A summary that
    passed computation but landed wrong — a missing plant, a NULL where a number
    belongs — would otherwise surface as a broken dashboard during the demo.
    """
    database_url = DatabaseSettings.from_env().url
    problems: list[str] = []

    with connect(database_url) as conn:
        expected_plants = set(configured_plant_ids(conn))

        rows = fetch_all(
            conn,
            """
            SELECT plant_id, actual_generation_kwh, expected_generation_kwh,
                   performance_pct, estimated_lost_energy_kwh,
                   estimated_actual_revenue, estimated_lost_revenue
              FROM daily_plant_summary
             WHERE simulation_date = %s
            """,
            (simulation_date,),
        )

        reference_count = fetch_one(
            conn,
            "SELECT COUNT(*) FROM daily_reference WHERE simulation_date = %s",
            (simulation_date,),
        )[0]

    summarised = {row[0] for row in rows}
    missing = sorted(expected_plants - summarised)
    if missing:
        problems.append(f"no summary row for configured plant(s): {', '.join(missing)}")

    if len(rows) != reference_count:
        problems.append(
            f"summary has {len(rows)} row(s) against {reference_count} reference row(s)"
        )

    for plant_id, actual, expected, performance, lost, revenue, lost_revenue in rows:
        if actual is None or actual < 0:
            problems.append(f"{plant_id}: actual generation is {actual}")
        if expected is None or expected <= 0:
            problems.append(f"{plant_id}: expected generation is {expected}")
        if lost is None or lost < 0:
            problems.append(f"{plant_id}: lost energy is {lost}")
        if revenue is None or revenue < 0:
            problems.append(f"{plant_id}: actual revenue is {revenue}")
        if lost_revenue is None or lost_revenue < 0:
            problems.append(f"{plant_id}: lost revenue is {lost_revenue}")
        # Recompute the headline ratio from the stored numbers: a mismatch means
        # the row is internally inconsistent whatever the pipeline believed.
        if performance is not None and expected:
            recomputed = actual / expected * 100.0
            if abs(recomputed - performance) > 0.01:
                problems.append(
                    f"{plant_id}: stored performance {performance:.2f}% does not match "
                    f"{recomputed:.2f}% recomputed from stored energy values"
                )

    if problems:
        with connect(database_url) as conn:
            record_health(
                conn,
                STATUS_FAILED,
                None,
                f"data quality failed for {simulation_date.isoformat()}",
                component=COMPONENT_BATCH,
            )
        listed = "\n  - ".join(problems)
        raise ValueError(
            f"Data quality checks failed for {simulation_date.isoformat()} "
            f"({len(problems)} problem(s)):\n  - {listed}"
        )

    log.info(
        "data_quality_passed",
        f"{len(rows)} summary row(s) passed data quality checks",
        simulation_date=simulation_date.isoformat(),
        plants=len(rows),
    )
    return {"plants": len(rows), "problems": []}


__all__ = [
    "COMPONENT_BATCH",
    "ReferenceFeedError",
    "check_raw_daily_data",
    "compute_daily_actuals",
    "load_reference",
    "reconcile_expected_actual",
    "run_data_quality_checks",
    "validate_reference_feed",
    "write_daily_summary_task",
]
