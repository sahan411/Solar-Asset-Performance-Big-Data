"""Daily reconciliation DAG.

Orchestration only. Every task is a thin call into `processing.batch.tasks`,
which is where the logic lives and where it is tested — Airflow is not installed
in the unit-test environment, and the business rules must not depend on it.

    wait_for_reference_file      the day's CSV has landed (and is complete)
            |
    validate_reference_feed      contract, portfolio coverage, plausibility
            |
    check_raw_daily_data         the archive actually holds this day
            |
    load_reference               upsert into daily_reference
            |
    compute_daily_actuals        Spark over the day's Parquet
            |
    reconcile_expected_actual    join, price the shortfall
            |
    write_daily_summary          upsert into daily_plant_summary
            |
    run_data_quality_checks      verify what landed, not what we intended

The chain is strictly linear because each step depends on the previous one's
output. Report *rendering* is Member 3's; this DAG stops once the reconciled
summary is in the database and verified.

SCHEDULE
Set to None — triggered manually or by the demo scripts. Under the compressed
clock a simulated day passes in five real minutes, so a wall-clock cron schedule
would bear no relation to when a day actually ends. `simulation_date` is passed
as a run parameter instead, which also makes backfilling a past day trivial.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.sensors.filesystem import FileSensor

from processing.batch import tasks
from processing.common.config import BatchSettings

DAG_ID = "solariq_daily_reconciliation"

# Retries are small and explicit. A transient database blip is worth one retry;
# a validation failure is deterministic and retrying it only delays the report.
DEFAULT_ARGS = {
    "owner": "member-2",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
    "depends_on_past": False,
}


def _simulation_date(context_params: dict) -> date:
    """Resolve the run's simulated date from the DAG parameter."""
    raw = context_params.get("simulation_date")
    if not raw:
        raise ValueError(
            "simulation_date is required. Trigger the DAG with "
            '{"simulation_date": "2026-08-21"}.'
        )
    return date.fromisoformat(str(raw))


@dag(
    dag_id=DAG_ID,
    description="Reconcile a simulated day's actual solar generation against its expectation.",
    default_args=DEFAULT_ARGS,
    schedule=None,
    # In the past so a manual trigger is never rejected as being before the start.
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["solariq", "batch", "lambda"],
    params={
        "simulation_date": Param(
            default="2026-08-21",
            type="string",
            description="Simulated day to reconcile, as YYYY-MM-DD.",
        )
    },
)
def solariq_daily_reconciliation():
    settings = BatchSettings.from_env()

    # The generator writes atomically (temp file then rename), so the sensor can
    # never observe a half-written feed. The path must be a volume mounted into
    # this container, not a host-only path.
    wait_for_reference_file = FileSensor(
        task_id="wait_for_reference_file",
        filepath=f"{settings.reference_dir}/daily_reference_{{{{ params.simulation_date }}}}.csv",
        poke_interval=10,
        # Ten minutes is generous against a five-minute simulated day; beyond
        # that the generator is not running and waiting longer helps nobody.
        timeout=600,
        mode="reschedule",
    )

    @task
    def validate_reference_feed(**context):
        return tasks.validate_reference_feed(_simulation_date(context["params"]))

    @task
    def check_raw_daily_data(**context):
        return tasks.check_raw_daily_data(_simulation_date(context["params"]))

    @task
    def load_reference(**context):
        return tasks.load_reference(_simulation_date(context["params"]))

    @task
    def compute_daily_actuals(**context):
        return tasks.compute_daily_actuals(_simulation_date(context["params"]))

    @task
    def reconcile_expected_actual(actuals: dict, **context):
        return tasks.reconcile_expected_actual(_simulation_date(context["params"]), actuals)

    @task
    def write_daily_summary(summaries: list, **context):
        return tasks.write_daily_summary_task(_simulation_date(context["params"]), summaries)

    @task
    def run_data_quality_checks(**context):
        return tasks.run_data_quality_checks(_simulation_date(context["params"]))

    validated = validate_reference_feed()
    archive_checked = check_raw_daily_data()
    loaded = load_reference()
    actuals = compute_daily_actuals()
    summaries = reconcile_expected_actual(actuals)
    written = write_daily_summary(summaries)
    checked = run_data_quality_checks()

    # Explicit ordering where the dependency is a side effect rather than a
    # value: reconciliation reads `daily_reference`, so the load must precede it.
    wait_for_reference_file >> validated >> archive_checked >> loaded >> actuals
    written >> checked


solariq_daily_reconciliation()
