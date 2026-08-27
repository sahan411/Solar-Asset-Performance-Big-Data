"""The whole batch path against a real PostgreSQL (Milestone 12).

archive telemetry -> daily actuals -> reconcile against the reference feed ->
daily_plant_summary. This is the pipeline that answers the project's central
business question, so it is exercised end to end rather than only in pieces.

Skipped unless SOLARIQ_TEST_DATABASE_URL points at a throwaway database.
"""

from __future__ import annotations

import os
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from processing.batch.actuals import collect_plant_actuals, plant_daily_actuals
from processing.batch.reconcile import (
    alert_counts_for_day,
    reconcile_day,
    reference_by_plant,
    write_daily_summary,
)
from processing.batch.reference import load_reference_into_db, validate_reference_frame
from processing.common.db import connect, fetch_all, fetch_one
from storage.migrate import run_migrations
from storage.seed_portfolio import load_portfolio, seed_portfolio
from tests.processing._events import utc_ts

pytestmark = [pytest.mark.integration, pytest.mark.spark]

TEST_DATABASE_URL = os.getenv("SOLARIQ_TEST_DATABASE_URL")
if not TEST_DATABASE_URL:
    pytest.skip("set SOLARIQ_TEST_DATABASE_URL to run reconciliation tests", allow_module_level=True)

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "portfolio.test.yaml"
DAY = date(2026, 8, 21)
INTERVAL_SECONDS = 3.0

ARCHIVE_SCHEMA = (
    "plant_id string, inverter_id string, energy_today_kwh double, "
    "active_power_kw double, status string, event_time timestamp"
)


@pytest.fixture()
def database():
    with connect(TEST_DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
    run_migrations(TEST_DATABASE_URL)
    with connect(TEST_DATABASE_URL) as conn:
        seed_portfolio(conn, load_portfolio(FIXTURE))
    return TEST_DATABASE_URL


def _load_reference(database_url, rows):
    frame = pd.DataFrame(rows)
    feed = validate_reference_frame(frame, ("PLANT_01", "PLANT_02"), DAY)
    with connect(database_url) as conn:
        load_reference_into_db(conn, feed)


def _reference_row(plant_id, expected="20000", rate="0.15", maintenance="false"):
    return {
        "simulation_date": "2026-08-21",
        "plant_id": plant_id,
        "plant_capacity_kw": "4000",
        "expected_generation_kwh": expected,
        "expected_peak_power_kw": "3800",
        "forecast_irradiance_kwh_m2": "5.2",
        "ppa_rate_per_kwh": rate,
        "maintenance_flag": maintenance,
        "source_version": "v1",
    }


def _archive(spark, rows):
    return spark.createDataFrame(
        [
            (plant, inverter, float(energy), 100.0, status, utc_ts(2026, 8, 21, hour, 0, 0))
            for plant, inverter, energy, status, hour in rows
        ],
        schema=ARCHIVE_SCHEMA,
    )


def _run_batch(spark, database_url, archive_rows):
    """The batch pipeline exactly as the Airflow DAG will call it."""
    actuals = collect_plant_actuals(
        plant_daily_actuals(_archive(spark, archive_rows), INTERVAL_SECONDS)
    )
    with connect(database_url) as conn:
        reference = reference_by_plant(conn, DAY)
        alerts = alert_counts_for_day(conn, DAY)

    result = reconcile_day(DAY, actuals, reference, alerts)

    with connect(database_url) as conn:
        write_daily_summary(conn, result)
    return result


def _summary(database_url, plant_id="PLANT_01"):
    with connect(database_url) as conn:
        return fetch_one(
            conn,
            "SELECT actual_generation_kwh, expected_generation_kwh, performance_pct, "
            "estimated_lost_energy_kwh, estimated_actual_revenue, estimated_lost_revenue, "
            "availability_pct, downtime_minutes, alert_count, maintenance_flag, "
            "ppa_rate_per_kwh FROM daily_plant_summary WHERE plant_id = %s",
            (plant_id,),
        )


def test_the_full_batch_path_produces_a_priced_summary(spark, database):
    """16000 kWh generated against 20000 expected, at 0.15 per kWh."""
    _load_reference(database, [_reference_row("PLANT_01"), _reference_row("PLANT_02")])

    archive = [
        ("PLANT_01", "INV_01", 8000, "ONLINE", 12),
        ("PLANT_01", "INV_02", 8000, "ONLINE", 12),
        ("PLANT_02", "INV_01", 10000, "ONLINE", 12),
        ("PLANT_02", "INV_02", 10000, "ONLINE", 12),
    ]
    _run_batch(spark, database, archive)

    row = _summary(database)
    assert row[0] == pytest.approx(16000.0)   # actual
    assert row[1] == pytest.approx(20000.0)   # expected
    assert row[2] == pytest.approx(80.0)      # performance
    assert row[3] == pytest.approx(4000.0)    # lost energy
    assert row[4] == pytest.approx(2400.0)    # 16000 * 0.15
    assert row[5] == pytest.approx(600.0)     # 4000 * 0.15
    assert row[10] == pytest.approx(0.15)     # rate recorded


def test_every_configured_plant_gets_a_row(spark, database):
    _load_reference(database, [_reference_row("PLANT_01"), _reference_row("PLANT_02")])
    _run_batch(spark, database, [("PLANT_01", "INV_01", 5000, "ONLINE", 12)])

    with connect(database) as conn:
        rows = fetch_all(conn, "SELECT plant_id FROM daily_plant_summary ORDER BY plant_id")
    assert [r[0] for r in rows] == ["PLANT_01", "PLANT_02"]


def test_a_plant_with_no_telemetry_is_reported_as_a_total_loss(spark, database):
    """The most commercially significant outcome must not be silently skipped."""
    _load_reference(database, [_reference_row("PLANT_01"), _reference_row("PLANT_02")])
    _run_batch(spark, database, [("PLANT_01", "INV_01", 20000, "ONLINE", 12)])

    row = _summary(database, "PLANT_02")
    assert row[0] == pytest.approx(0.0)
    assert row[2] == pytest.approx(0.0)
    assert row[3] == pytest.approx(20000.0)
    assert row[5] == pytest.approx(3000.0)  # the whole day's revenue, lost


def test_availability_and_downtime_reach_the_summary(spark, database):
    _load_reference(database, [_reference_row("PLANT_01"), _reference_row("PLANT_02")])
    archive = [
        ("PLANT_01", "INV_01", 5000, "ONLINE", 10),
        ("PLANT_01", "INV_01", 5000, "OFFLINE", 11),
        ("PLANT_02", "INV_01", 5000, "ONLINE", 10),
    ]
    _run_batch(spark, database, archive)

    row = _summary(database)
    assert row[6] == pytest.approx(50.0)              # availability
    assert row[7] == pytest.approx(3.0 / 60.0)        # one offline sample


def test_alerts_raised_that_day_are_counted_on_the_summary(spark, database):
    _load_reference(database, [_reference_row("PLANT_01"), _reference_row("PLANT_02")])
    with connect(database) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO alerts (id, plant_id, inverter_id, alert_type, severity, message,
                                    started_at, status)
                VALUES ('a1', 'PLANT_01', 'INV_01', 'UNDERPERFORMANCE', 'WARNING', 'x',
                        '2026-08-21 09:00:00+00', 'ACTIVE'),
                       ('a2', 'PLANT_01', 'INV_02', 'INVERTER_OFFLINE', 'CRITICAL', 'y',
                        '2026-08-21 11:00:00+00', 'ACTIVE'),
                       ('a3', 'PLANT_02', 'INV_01', 'UNDERPERFORMANCE', 'WARNING', 'z',
                        '2026-08-20 09:00:00+00', 'ACTIVE')
                """
            )

    _run_batch(spark, database, [("PLANT_01", "INV_01", 16000, "ONLINE", 12)])

    assert _summary(database, "PLANT_01")[8] == 2
    # The alert that began the previous day belongs to that day, not this one.
    assert _summary(database, "PLANT_02")[8] == 0


def test_maintenance_is_recorded_without_excluding_the_plant(spark, database):
    _load_reference(
        database,
        [_reference_row("PLANT_01", maintenance="true"), _reference_row("PLANT_02")],
    )
    _run_batch(spark, database, [("PLANT_01", "INV_01", 5000, "ONLINE", 12)])

    row = _summary(database)
    assert row[9] is True
    assert row[3] == pytest.approx(15000.0)  # the lost energy is still counted


def test_rerunning_the_day_updates_rather_than_duplicating(spark, database):
    """Required for the demo reset and for reprocessing a corrected feed."""
    _load_reference(database, [_reference_row("PLANT_01"), _reference_row("PLANT_02")])
    _run_batch(spark, database, [("PLANT_01", "INV_01", 10000, "ONLINE", 12)])
    _run_batch(spark, database, [("PLANT_01", "INV_01", 16000, "ONLINE", 12)])

    with connect(database) as conn:
        assert fetch_one(conn, "SELECT COUNT(*) FROM daily_plant_summary")[0] == 2
    assert _summary(database)[0] == pytest.approx(16000.0)


def test_a_corrected_rate_reprices_the_day(spark, database):
    """Reload the reference with a new rate, re-run, and revenue follows."""
    _load_reference(database, [_reference_row("PLANT_01"), _reference_row("PLANT_02")])
    _run_batch(spark, database, [("PLANT_01", "INV_01", 16000, "ONLINE", 12)])
    assert _summary(database)[4] == pytest.approx(2400.0)

    _load_reference(
        database,
        [_reference_row("PLANT_01", rate="0.20"), _reference_row("PLANT_02", rate="0.20")],
    )
    _run_batch(spark, database, [("PLANT_01", "INV_01", 16000, "ONLINE", 12)])

    assert _summary(database)[4] == pytest.approx(3200.0)
    assert _summary(database)[10] == pytest.approx(0.20)


def test_portfolio_totals_are_weighted_across_plants(spark, database):
    _load_reference(
        database,
        [
            _reference_row("PLANT_01", expected="20000", rate="0.15"),
            _reference_row("PLANT_02", expected="2000", rate="0.20"),
        ],
    )
    archive = [
        ("PLANT_01", "INV_01", 19000, "ONLINE", 12),   # 95%
        ("PLANT_02", "INV_01", 1000, "ONLINE", 12),    # 50%
    ]
    result = _run_batch(spark, database, archive)

    assert result.portfolio_actual_kwh == pytest.approx(20000.0)
    assert result.portfolio_expected_kwh == pytest.approx(22000.0)
    assert result.portfolio_performance_pct == pytest.approx(90.909, abs=0.01)
    # 1000*0.15 + 1000*0.20, each plant priced at its own rate.
    assert result.portfolio_lost_revenue == pytest.approx(350.0)
