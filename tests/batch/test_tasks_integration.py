"""Airflow task functions, exercised without Airflow (Milestone 13).

The DAG is deliberately a thin wrapper, so these tests cover the behaviour that
matters: each task correct in isolation (they run in separate processes and can
be retried or cleared individually) and correct in sequence.

Tasks needing Spark against MinIO — check_raw_daily_data, compute_daily_actuals —
are covered by the archive tests instead; they cannot run without an object store.

Skipped unless SOLARIQ_TEST_DATABASE_URL points at a throwaway database.
"""

from __future__ import annotations

import csv
import os
from datetime import date
from pathlib import Path

import pytest

from processing.batch import tasks
from processing.batch.reconcile import PlantSummary
from processing.batch.reference import REFERENCE_COLUMNS, ReferenceFeedError
from processing.common.db import connect, fetch_one
from storage.migrate import run_migrations
from storage.seed_portfolio import load_portfolio, seed_portfolio

pytestmark = pytest.mark.integration

TEST_DATABASE_URL = os.getenv("SOLARIQ_TEST_DATABASE_URL")
if not TEST_DATABASE_URL:
    pytest.skip("set SOLARIQ_TEST_DATABASE_URL to run task tests", allow_module_level=True)

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "portfolio.test.yaml"
DAY = date(2026, 8, 21)


@pytest.fixture()
def feed_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", TEST_DATABASE_URL)
    monkeypatch.setenv("SIMULATION_OUTPUT_DIR", str(tmp_path))
    monkeypatch.setenv("TELEMETRY_INTERVAL_SECONDS", "3")

    with connect(TEST_DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
    run_migrations(TEST_DATABASE_URL)
    with connect(TEST_DATABASE_URL) as conn:
        seed_portfolio(conn, load_portfolio(FIXTURE))
    return tmp_path


def _write_feed(directory: Path, rows=None, simulation_date=DAY):
    path = directory / f"daily_reference_{simulation_date.isoformat()}.csv"
    rows = rows if rows is not None else [_row("PLANT_01"), _row("PLANT_02")]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(REFERENCE_COLUMNS))
        writer.writeheader()
        writer.writerows(rows)
    return path


def _row(plant_id, expected="20000", rate="0.15", maintenance="false"):
    return {
        "simulation_date": DAY.isoformat(),
        "plant_id": plant_id,
        "plant_capacity_kw": "4000",
        "expected_generation_kwh": expected,
        "expected_peak_power_kw": "3800",
        "forecast_irradiance_kwh_m2": "5.2",
        "ppa_rate_per_kwh": rate,
        "maintenance_flag": maintenance,
        "source_version": "v1",
    }


def _actuals(plant_01=16000.0, plant_02=18000.0):
    return {
        "PLANT_01": {
            "actual_generation_kwh": plant_01,
            "availability_pct": 98.0,
            "downtime_minutes": 5.0,
        },
        "PLANT_02": {
            "actual_generation_kwh": plant_02,
            "availability_pct": 100.0,
            "downtime_minutes": 0.0,
        },
    }


class TestValidateReferenceFeed:
    def test_a_good_feed_validates_against_the_registry(self, feed_dir):
        _write_feed(feed_dir)
        result = tasks.validate_reference_feed(DAY)

        assert result["plants"] == 2
        assert result["warnings"] == []

    def test_a_missing_file_fails_the_task(self, feed_dir):
        with pytest.raises(ReferenceFeedError, match="file not found"):
            tasks.validate_reference_feed(DAY)

    def test_a_feed_missing_a_configured_plant_fails(self, feed_dir):
        _write_feed(feed_dir, rows=[_row("PLANT_01")])
        with pytest.raises(ReferenceFeedError, match="PLANT_02"):
            tasks.validate_reference_feed(DAY)

    def test_validation_writes_nothing_to_the_database(self, feed_dir):
        """A bad feed must not half-load; validation is read-only by design."""
        _write_feed(feed_dir)
        tasks.validate_reference_feed(DAY)

        with connect(TEST_DATABASE_URL) as conn:
            assert fetch_one(conn, "SELECT COUNT(*) FROM daily_reference")[0] == 0


class TestLoadReference:
    def test_loads_the_validated_feed(self, feed_dir):
        _write_feed(feed_dir)
        assert tasks.load_reference(DAY) == 2

        with connect(TEST_DATABASE_URL) as conn:
            rate = fetch_one(
                conn,
                "SELECT ppa_rate_per_kwh FROM daily_reference WHERE plant_id = 'PLANT_01'",
            )[0]
        assert rate == pytest.approx(0.15)

    def test_revalidates_rather_than_trusting_the_earlier_task(self, feed_dir):
        """Tasks run in separate processes and can be cleared individually."""
        _write_feed(feed_dir, rows=[_row("PLANT_01", expected="-5")])
        with pytest.raises(ReferenceFeedError):
            tasks.load_reference(DAY)

    def test_is_idempotent_across_reruns(self, feed_dir):
        _write_feed(feed_dir)
        tasks.load_reference(DAY)
        tasks.load_reference(DAY)

        with connect(TEST_DATABASE_URL) as conn:
            assert fetch_one(conn, "SELECT COUNT(*) FROM daily_reference")[0] == 2


class TestReconcileAndWrite:
    def test_reconciliation_returns_json_safe_summaries(self, feed_dir):
        _write_feed(feed_dir)
        tasks.load_reference(DAY)

        summaries = tasks.reconcile_expected_actual(DAY, _actuals())

        assert len(summaries) == 2
        # XCom serialises to JSON: the date must already be a string.
        assert summaries[0]["simulation_date"] == "2026-08-21"
        assert isinstance(summaries[0]["actual_generation_kwh"], float)

    def test_summaries_survive_the_xcom_round_trip(self, feed_dir):
        _write_feed(feed_dir)
        tasks.load_reference(DAY)

        summaries = tasks.reconcile_expected_actual(DAY, _actuals())
        restored = PlantSummary.from_dict(summaries[0])

        assert restored.simulation_date == DAY
        assert restored.plant_id == "PLANT_01"
        assert restored.performance_pct == pytest.approx(80.0)

    def test_writing_persists_the_priced_day(self, feed_dir):
        _write_feed(feed_dir)
        tasks.load_reference(DAY)
        summaries = tasks.reconcile_expected_actual(DAY, _actuals())

        assert tasks.write_daily_summary_task(DAY, summaries) == 2

        with connect(TEST_DATABASE_URL) as conn:
            row = fetch_one(
                conn,
                "SELECT actual_generation_kwh, performance_pct, estimated_lost_revenue "
                "FROM daily_plant_summary WHERE plant_id = 'PLANT_01'",
            )
        assert row[0] == pytest.approx(16000.0)
        assert row[1] == pytest.approx(80.0)
        assert row[2] == pytest.approx(600.0)

    def test_writing_records_batch_health(self, feed_dir):
        _write_feed(feed_dir)
        tasks.load_reference(DAY)
        tasks.write_daily_summary_task(DAY, tasks.reconcile_expected_actual(DAY, _actuals()))

        with connect(TEST_DATABASE_URL) as conn:
            status = fetch_one(
                conn,
                "SELECT status FROM pipeline_health WHERE component = %s",
                (tasks.COMPONENT_BATCH,),
            )[0]
        assert status == "HEALTHY"


class TestDataQualityChecks:
    def _complete_run(self, feed_dir):
        _write_feed(feed_dir)
        tasks.load_reference(DAY)
        tasks.write_daily_summary_task(DAY, tasks.reconcile_expected_actual(DAY, _actuals()))

    def test_a_clean_day_passes(self, feed_dir):
        self._complete_run(feed_dir)
        assert tasks.run_data_quality_checks(DAY)["plants"] == 2

    def test_a_missing_summary_row_is_caught(self, feed_dir):
        self._complete_run(feed_dir)
        with connect(TEST_DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM daily_plant_summary WHERE plant_id = 'PLANT_02'")

        with pytest.raises(ValueError, match="PLANT_02"):
            tasks.run_data_quality_checks(DAY)

    def test_an_internally_inconsistent_row_is_caught(self, feed_dir):
        """Checks what LANDED, not what the pipeline believed it wrote."""
        self._complete_run(feed_dir)
        with connect(TEST_DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE daily_plant_summary SET performance_pct = 42.0 "
                    "WHERE plant_id = 'PLANT_01'"
                )

        with pytest.raises(ValueError, match="does not match"):
            tasks.run_data_quality_checks(DAY)

    def test_a_negative_stored_value_is_caught(self, feed_dir):
        self._complete_run(feed_dir)
        with connect(TEST_DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE daily_plant_summary SET estimated_lost_revenue = -1 "
                    "WHERE plant_id = 'PLANT_01'"
                )

        with pytest.raises(ValueError, match="lost revenue"):
            tasks.run_data_quality_checks(DAY)

    def test_failure_is_recorded_in_pipeline_health(self, feed_dir):
        self._complete_run(feed_dir)
        with connect(TEST_DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM daily_plant_summary WHERE plant_id = 'PLANT_02'")

        with pytest.raises(ValueError):
            tasks.run_data_quality_checks(DAY)

        with connect(TEST_DATABASE_URL) as conn:
            status = fetch_one(
                conn,
                "SELECT status FROM pipeline_health WHERE component = %s",
                (tasks.COMPONENT_BATCH,),
            )[0]
        assert status == "FAILED"


def test_the_whole_task_sequence_runs_end_to_end(feed_dir):
    """The DAG's chain, minus the two Spark tasks that need MinIO."""
    _write_feed(feed_dir)

    tasks.validate_reference_feed(DAY)
    tasks.load_reference(DAY)
    summaries = tasks.reconcile_expected_actual(DAY, _actuals())
    tasks.write_daily_summary_task(DAY, summaries)
    result = tasks.run_data_quality_checks(DAY)

    assert result["plants"] == 2

    with connect(TEST_DATABASE_URL) as conn:
        total_lost = fetch_one(
            conn,
            "SELECT SUM(estimated_lost_revenue) FROM daily_plant_summary "
            "WHERE simulation_date = %s",
            (DAY,),
        )[0]
    # PLANT_01 lost 4000 kWh, PLANT_02 lost 2000 kWh, both at 0.15.
    assert float(total_lost) == pytest.approx(900.0)


def test_rerunning_the_whole_sequence_is_idempotent(feed_dir):
    _write_feed(feed_dir)
    for _ in range(2):
        tasks.validate_reference_feed(DAY)
        tasks.load_reference(DAY)
        tasks.write_daily_summary_task(DAY, tasks.reconcile_expected_actual(DAY, _actuals()))
        tasks.run_data_quality_checks(DAY)

    with connect(TEST_DATABASE_URL) as conn:
        assert fetch_one(conn, "SELECT COUNT(*) FROM daily_plant_summary")[0] == 2
        assert fetch_one(conn, "SELECT COUNT(*) FROM daily_reference")[0] == 2
