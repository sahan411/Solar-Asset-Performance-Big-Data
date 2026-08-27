"""End-to-end migration + seed checks against a real PostgreSQL.

Skipped unless SOLARIQ_TEST_DATABASE_URL points at a throwaway database, because
these tests create and drop schema. Never point them at a database holding data
you care about.

    docker run --rm -d --name solariq-test-pg -p 55432:5432 \
        -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=solariq_test postgres:16-alpine
    SOLARIQ_TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:55432/solariq_test \
        python -m pytest tests/storage -m integration
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from processing.common.db import connect, fetch_all, fetch_one
from storage.migrate import discover_migrations, run_migrations
from storage.seed_portfolio import load_portfolio, seed_portfolio

pytestmark = pytest.mark.integration

TEST_DATABASE_URL = os.getenv("SOLARIQ_TEST_DATABASE_URL")

pytest_skip_reason = "set SOLARIQ_TEST_DATABASE_URL to run migration integration tests"
if not TEST_DATABASE_URL:
    pytest.skip(pytest_skip_reason, allow_module_level=True)

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "portfolio.test.yaml"


@pytest.fixture()
def empty_database():
    """Drop and recreate the public schema so each test starts from nothing."""
    with connect(TEST_DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
    yield TEST_DATABASE_URL


def _table_names(conn) -> set[str]:
    rows = fetch_all(
        conn,
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'",
    )
    return {name for (name,) in rows}


def test_migrations_apply_to_an_empty_database(empty_database):
    applied = run_migrations(empty_database)
    assert applied == [m.version for m in discover_migrations()]

    with connect(empty_database) as conn:
        tables = _table_names(conn)

    for expected in (
        "plants",
        "inverters",
        "live_plant_metrics",
        "live_portfolio_metrics",
        "alerts",
        "daily_reference",
        "daily_plant_summary",
        "pipeline_health",
        "schema_migrations",
    ):
        assert expected in tables


def test_migrations_are_idempotent(empty_database):
    run_migrations(empty_database)
    # A second run has nothing to do and must not error.
    assert run_migrations(empty_database) == []


def test_seed_is_idempotent_and_updates_in_place(empty_database):
    run_migrations(empty_database)
    portfolio = load_portfolio(FIXTURE)

    with connect(empty_database) as conn:
        seed_portfolio(conn, portfolio)
    with connect(empty_database) as conn:
        seed_portfolio(conn, portfolio)
        assert fetch_one(conn, "SELECT COUNT(*) FROM plants")[0] == 2
        assert fetch_one(conn, "SELECT COUNT(*) FROM inverters")[0] == 6


def test_alerts_reject_a_second_active_alert_for_the_same_asset(empty_database):
    """The partial unique index is what stops per-microbatch alert spam."""
    run_migrations(empty_database)
    with connect(empty_database) as conn:
        seed_portfolio(conn, load_portfolio(FIXTURE))

    insert = """
        INSERT INTO alerts (id, plant_id, inverter_id, alert_type, severity, message, started_at, status)
        VALUES (%s, 'PLANT_01', 'INV_01', 'UNDERPERFORMANCE', 'WARNING', 'test', NOW(), 'ACTIVE')
    """
    with connect(empty_database) as conn:
        with conn.cursor() as cur:
            cur.execute(insert, ("alert-1",))

    with pytest.raises(Exception):  # psycopg2.errors.UniqueViolation
        with connect(empty_database) as conn:
            with conn.cursor() as cur:
                cur.execute(insert, ("alert-2",))

    # Once resolved, a new active alert for the same asset is allowed again.
    with connect(empty_database) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE alerts SET status = 'RESOLVED', ended_at = NOW() WHERE id = 'alert-1'"
            )
    with connect(empty_database) as conn:
        with conn.cursor() as cur:
            cur.execute(insert, ("alert-3",))


def test_resolved_alert_must_have_an_end_time(empty_database):
    run_migrations(empty_database)
    with connect(empty_database) as conn:
        seed_portfolio(conn, load_portfolio(FIXTURE))

    with pytest.raises(Exception):  # CheckViolation on alerts_ended_at_matches_status
        with connect(empty_database) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO alerts (id, plant_id, alert_type, severity, message, started_at, status)
                    VALUES ('bad', 'PLANT_01', 'UNDERPERFORMANCE', 'WARNING', 'x', NOW(), 'RESOLVED')
                    """
                )


def test_metrics_cannot_reference_an_unknown_plant(empty_database):
    run_migrations(empty_database)
    with pytest.raises(Exception):  # ForeignKeyViolation
        with connect(empty_database) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO live_plant_metrics
                        (plant_id, window_start, window_end, current_power_kw, avg_power_kw)
                    VALUES ('GHOST_PLANT', NOW(), NOW(), 1.0, 1.0)
                    """
                )
