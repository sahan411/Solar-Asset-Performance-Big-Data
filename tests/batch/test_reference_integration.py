"""Daily reference load against a real PostgreSQL (Milestone 10).

Skipped unless SOLARIQ_TEST_DATABASE_URL points at a throwaway database.
"""

from __future__ import annotations

import os
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from processing.batch.reference import (
    configured_plant_ids,
    load_reference_into_db,
    validate_reference_frame,
)
from processing.common.db import connect, fetch_one
from storage.migrate import run_migrations
from storage.seed_portfolio import load_portfolio, seed_portfolio

pytestmark = pytest.mark.integration

TEST_DATABASE_URL = os.getenv("SOLARIQ_TEST_DATABASE_URL")
if not TEST_DATABASE_URL:
    pytest.skip("set SOLARIQ_TEST_DATABASE_URL to run reference load tests", allow_module_level=True)

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


def _feed(rate="0.15", generation="20000"):
    rows = [
        {
            "simulation_date": "2026-08-21",
            "plant_id": plant,
            "plant_capacity_kw": "4000",
            "expected_generation_kwh": generation,
            "expected_peak_power_kw": "3800",
            "forecast_irradiance_kwh_m2": "5.2",
            "ppa_rate_per_kwh": rate,
            "maintenance_flag": "false",
            "source_version": "v1",
        }
        for plant in ("PLANT_01", "PLANT_02")
    ]
    return validate_reference_frame(pd.DataFrame(rows), ("PLANT_01", "PLANT_02"))


def test_configured_plants_come_from_the_registry(database):
    with connect(database) as conn:
        assert configured_plant_ids(conn) == ["PLANT_01", "PLANT_02"]


def test_a_validated_feed_loads(database):
    with connect(database) as conn:
        assert load_reference_into_db(conn, _feed()) == 2

    with connect(database) as conn:
        row = fetch_one(
            conn,
            "SELECT expected_generation_kwh, ppa_rate_per_kwh, maintenance_flag, source_version "
            "FROM daily_reference WHERE plant_id = 'PLANT_01'",
        )

    assert row[0] == pytest.approx(20000.0)
    assert row[1] == pytest.approx(0.15)
    assert row[2] is False
    assert row[3] == "v1"


def test_the_simulation_date_is_stored_as_the_intended_day(database):
    with connect(database) as conn:
        load_reference_into_db(conn, _feed())
    with connect(database) as conn:
        stored = fetch_one(conn, "SELECT simulation_date FROM daily_reference LIMIT 1")[0]

    assert stored == date(2026, 8, 21)


def test_reloading_the_same_day_updates_rather_than_duplicating(database):
    """Re-running a day's DAG after a demo reset must converge, not fail."""
    with connect(database) as conn:
        load_reference_into_db(conn, _feed(rate="0.15"))
    with connect(database) as conn:
        load_reference_into_db(conn, _feed(rate="0.18"))

    with connect(database) as conn:
        count = fetch_one(conn, "SELECT COUNT(*) FROM daily_reference")[0]
        rate = fetch_one(
            conn, "SELECT ppa_rate_per_kwh FROM daily_reference WHERE plant_id = 'PLANT_01'"
        )[0]

    assert count == 2
    # A corrected feed wins.
    assert rate == pytest.approx(0.18)


def test_loading_is_atomic_across_the_whole_feed(database):
    """A feed that violates a database constraint must load nothing at all."""
    feed = _feed()
    # Bypass validation to simulate a constraint the DB catches but we did not.
    feed.rows.append(
        (date(2026, 8, 21), "GHOST_PLANT", 1.0, 1.0, 1.0, 1.0, 0.1, False, "v1")
    )

    with pytest.raises(Exception):
        with connect(database) as conn:
            load_reference_into_db(conn, feed)

    with connect(database) as conn:
        assert fetch_one(conn, "SELECT COUNT(*) FROM daily_reference")[0] == 0
