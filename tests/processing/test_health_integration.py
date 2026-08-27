"""Pipeline health reporting (Milestone 9).

Skipped unless SOLARIQ_TEST_DATABASE_URL points at a throwaway database.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from processing.common.db import connect, fetch_one
from processing.streaming.health import (
    COMPONENT_STREAM,
    STATUS_DEGRADED,
    STATUS_FAILED,
    STATUS_HEALTHY,
    count_active_alerts,
    record_health,
)
from storage.migrate import run_migrations
from storage.seed_portfolio import load_portfolio, seed_portfolio
from tests.processing._events import utc_ts

pytestmark = pytest.mark.integration

TEST_DATABASE_URL = os.getenv("SOLARIQ_TEST_DATABASE_URL")
if not TEST_DATABASE_URL:
    pytest.skip("set SOLARIQ_TEST_DATABASE_URL to run health tests", allow_module_level=True)

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "portfolio.test.yaml"


@pytest.fixture()
def database():
    with connect(TEST_DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
    run_migrations(TEST_DATABASE_URL)
    with connect(TEST_DATABASE_URL) as conn:
        seed_portfolio(conn, load_portfolio(FIXTURE))
    return TEST_DATABASE_URL


def _health(database_url, component=COMPONENT_STREAM):
    with connect(database_url) as conn:
        return fetch_one(
            conn,
            "SELECT status, "
            "to_char(last_event_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS'), "
            "last_success_at, message FROM pipeline_health WHERE component = %s",
            (component,),
        )


def test_a_healthy_batch_records_its_event_time(database):
    with connect(database) as conn:
        record_health(conn, STATUS_HEALTHY, utc_ts(2026, 8, 21, 5, 0, 3), "processed 40 events")

    status, last_event, last_success, message = _health(database)
    assert status == STATUS_HEALTHY
    assert last_event == "2026-08-21 05:00:03"
    assert last_success is not None
    assert message == "processed 40 events"


def test_health_is_upserted_not_appended(database):
    with connect(database) as conn:
        record_health(conn, STATUS_HEALTHY, utc_ts(2026, 8, 21, 5, 0, 3), "first")
    with connect(database) as conn:
        record_health(conn, STATUS_HEALTHY, utc_ts(2026, 8, 21, 5, 0, 6), "second")

    with connect(database) as conn:
        assert fetch_one(conn, "SELECT COUNT(*) FROM pipeline_health")[0] == 1
    assert _health(database)[3] == "second"


def test_an_empty_batch_does_not_erase_the_last_seen_event_time(database):
    """A high-water mark: staleness is measured from the last real event."""
    with connect(database) as conn:
        record_health(conn, STATUS_HEALTHY, utc_ts(2026, 8, 21, 5, 0, 3), "processed")
    with connect(database) as conn:
        record_health(conn, STATUS_DEGRADED, None, "no telemetry in this batch")

    status, last_event, _, message = _health(database)
    assert status == STATUS_DEGRADED
    # The event time survives, so a consumer can still see how stale we are.
    assert last_event == "2026-08-21 05:00:03"
    assert message == "no telemetry in this batch"


def test_out_of_order_batches_do_not_move_the_clock_backwards(database):
    with connect(database) as conn:
        record_health(conn, STATUS_HEALTHY, utc_ts(2026, 8, 21, 6, 0, 0), "later")
    with connect(database) as conn:
        record_health(conn, STATUS_HEALTHY, utc_ts(2026, 8, 21, 5, 0, 0), "earlier replay")

    assert _health(database)[1] == "2026-08-21 06:00:00"


def test_a_failed_batch_does_not_advance_the_success_timestamp(database):
    """Otherwise a crash-looping job would look permanently healthy."""
    with connect(database) as conn:
        record_health(conn, STATUS_HEALTHY, utc_ts(2026, 8, 21, 5, 0, 0), "ok")
    first_success = _health(database)[2]

    with connect(database) as conn:
        record_health(conn, STATUS_FAILED, None, "database write failed")

    status, _, last_success, _ = _health(database)
    assert status == STATUS_FAILED
    assert last_success == first_success


def test_components_are_tracked_independently(database):
    with connect(database) as conn:
        record_health(conn, STATUS_HEALTHY, utc_ts(2026, 8, 21, 5, 0, 0), "stream ok")
        record_health(
            conn, STATUS_FAILED, None, "dag failed", component="airflow-daily-reconciliation"
        )

    assert _health(database)[0] == STATUS_HEALTHY
    assert _health(database, "airflow-daily-reconciliation")[0] == STATUS_FAILED


def test_active_alert_count_reflects_only_active_rows(database):
    with connect(database) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO alerts (id, plant_id, inverter_id, alert_type, severity, message,
                                    started_at, status)
                VALUES ('a1', 'PLANT_01', 'INV_01', 'UNDERPERFORMANCE', 'WARNING', 'x', NOW(), 'ACTIVE'),
                       ('a2', 'PLANT_01', 'INV_02', 'INVERTER_OFFLINE', 'CRITICAL', 'y', NOW(), 'ACTIVE')
                """
            )
            cur.execute(
                """
                INSERT INTO alerts (id, plant_id, inverter_id, alert_type, severity, message,
                                    started_at, ended_at, status)
                VALUES ('a3', 'PLANT_02', 'INV_01', 'UNDERPERFORMANCE', 'WARNING', 'z',
                        NOW(), NOW(), 'RESOLVED')
                """
            )

    with connect(database) as conn:
        assert count_active_alerts(conn) == 2
