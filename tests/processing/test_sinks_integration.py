"""Live-metric sink against a real PostgreSQL (Milestone 7).

Proves the two properties that unit tests cannot: that UTC instants survive the
Spark -> Python -> Postgres round trip unshifted, and that replaying a microbatch
updates rather than duplicates.

Skipped unless SOLARIQ_TEST_DATABASE_URL points at a throwaway database — these
tests drop and recreate the schema.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from processing.common.db import connect, fetch_all, fetch_one
from processing.streaming.metrics import PLANT_METRIC_COLUMNS, PORTFOLIO_METRIC_COLUMNS
from processing.streaming.sinks import collect_rows, write_live_metrics
from storage.migrate import run_migrations
from storage.seed_portfolio import load_portfolio, seed_portfolio

pytestmark = [pytest.mark.integration, pytest.mark.spark]

TEST_DATABASE_URL = os.getenv("SOLARIQ_TEST_DATABASE_URL")
if not TEST_DATABASE_URL:
    pytest.skip(
        "set SOLARIQ_TEST_DATABASE_URL to run sink integration tests",
        allow_module_level=True,
    )

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "portfolio.test.yaml"

PLANT_SCHEMA = (
    "plant_id string, window_start timestamp, window_end timestamp, "
    "current_power_kw double, avg_power_kw double, expected_power_kw double, "
    "avg_irradiance_wm2 double, availability_pct double, performance_pct double, "
    "estimated_loss_kw double, online_inverters int, offline_inverters int"
)
PORTFOLIO_SCHEMA = (
    "window_start timestamp, window_end timestamp, current_power_kw double, "
    "avg_power_kw double, expected_power_kw double, online_inverters int, "
    "offline_inverters int, availability_pct double, performance_pct double, "
    "estimated_loss_kw double"
)


@pytest.fixture()
def seeded_database():
    with connect(TEST_DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
    run_migrations(TEST_DATABASE_URL)
    with connect(TEST_DATABASE_URL) as conn:
        seed_portfolio(conn, load_portfolio(FIXTURE))
    return TEST_DATABASE_URL


def _plant_batch(spark, current_power=450.0, start_second=0):
    from tests.processing._events import utc_ts

    return spark.createDataFrame(
        [
            (
                "PLANT_01",
                utc_ts(2026, 8, 21, 5, 0, start_second),
                utc_ts(2026, 8, 21, 5, 0, start_second + 3),
                current_power, 350.0, 500.0, 500.0, 100.0, 90.0, 50.0, 2, 0,
            )
        ],
        schema=PLANT_SCHEMA,
    )


def _portfolio_batch(spark, current_power=450.0, start_second=0):
    from tests.processing._events import utc_ts

    return spark.createDataFrame(
        [
            (
                utc_ts(2026, 8, 21, 5, 0, start_second),
                utc_ts(2026, 8, 21, 5, 0, start_second + 3),
                current_power, 350.0, 500.0, 2, 0, 100.0, 90.0, 50.0,
            )
        ],
        schema=PORTFOLIO_SCHEMA,
    )


def _write(spark, database_url, current_power=450.0, start_second=0):
    return write_live_metrics(
        database_url,
        collect_rows(_plant_batch(spark, current_power, start_second), PLANT_METRIC_COLUMNS),
        collect_rows(
            _portfolio_batch(spark, current_power, start_second), PORTFOLIO_METRIC_COLUMNS
        ),
    )


def test_utc_instants_survive_the_round_trip_unshifted(spark, seeded_database):
    """The whole point of formatting timestamps inside Spark.

    A collected naive datetime would be stored shifted by the host's UTC offset,
    silently moving every window boundary.
    """
    _write(spark, seeded_database)

    with connect(seeded_database) as conn:
        stored = fetch_one(
            conn,
            "SELECT to_char(window_start AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS'), "
            "to_char(window_end AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS') "
            "FROM live_plant_metrics WHERE plant_id = 'PLANT_01'",
        )

    assert stored == ("2026-08-21 05:00:00", "2026-08-21 05:00:03")


def test_metrics_are_written_with_their_values(spark, seeded_database):
    plants, portfolio = _write(spark, seeded_database)
    assert (plants, portfolio) == (1, 1)

    with connect(seeded_database) as conn:
        row = fetch_one(
            conn,
            "SELECT current_power_kw, avg_power_kw, expected_power_kw, performance_pct, "
            "availability_pct, estimated_loss_kw, online_inverters, offline_inverters "
            "FROM live_plant_metrics WHERE plant_id = 'PLANT_01'",
        )

    assert row[0] == pytest.approx(450.0)
    assert row[1] == pytest.approx(350.0)
    assert row[2] == pytest.approx(500.0)
    assert row[3] == pytest.approx(90.0)
    assert row[6] == 2
    assert row[7] == 0


def test_replaying_a_microbatch_updates_instead_of_duplicating(spark, seeded_database):
    """Structured Streaming delivers to foreachBatch at least once."""
    _write(spark, seeded_database, current_power=450.0)
    _write(spark, seeded_database, current_power=450.0)

    with connect(seeded_database) as conn:
        assert fetch_one(conn, "SELECT COUNT(*) FROM live_plant_metrics")[0] == 1
        assert fetch_one(conn, "SELECT COUNT(*) FROM live_portfolio_metrics")[0] == 1


def test_replay_refreshes_values_and_the_update_timestamp(spark, seeded_database):
    _write(spark, seeded_database, current_power=450.0)
    with connect(seeded_database) as conn:
        first = fetch_one(
            conn, "SELECT current_power_kw, updated_at FROM live_plant_metrics"
        )

    # A corrected recomputation of the same window must win.
    _write(spark, seeded_database, current_power=475.0)
    with connect(seeded_database) as conn:
        second = fetch_one(
            conn, "SELECT current_power_kw, updated_at FROM live_plant_metrics"
        )

    assert first[0] == pytest.approx(450.0)
    assert second[0] == pytest.approx(475.0)
    assert second[1] >= first[1]


def test_distinct_windows_accumulate_as_separate_rows(spark, seeded_database):
    _write(spark, seeded_database, start_second=0)
    _write(spark, seeded_database, start_second=10)

    with connect(seeded_database) as conn:
        assert fetch_one(conn, "SELECT COUNT(*) FROM live_plant_metrics")[0] == 2


def test_nulls_are_persisted_as_nulls_not_zeros(spark, seeded_database):
    """Night-time performance is unknown; storing 0 would read as total failure."""
    from tests.processing._events import utc_ts

    night = spark.createDataFrame(
        [
            (
                "PLANT_01",
                utc_ts(2026, 8, 21, 20, 0, 0),
                utc_ts(2026, 8, 21, 20, 0, 3),
                0.0, 0.0, None, 2.0, 100.0, None, None, 2, 0,
            )
        ],
        schema=PLANT_SCHEMA,
    )
    write_live_metrics(seeded_database, collect_rows(night, PLANT_METRIC_COLUMNS), [])

    with connect(seeded_database) as conn:
        row = fetch_one(
            conn,
            "SELECT expected_power_kw, performance_pct, estimated_loss_kw, current_power_kw "
            "FROM live_plant_metrics WHERE window_start = '2026-08-21 20:00:00+00'",
        )

    assert row[0] is None
    assert row[1] is None
    assert row[2] is None
    assert row[3] == pytest.approx(0.0)


def test_a_failing_write_rolls_back_the_whole_batch(spark, seeded_database):
    """Partial metric state is worse than none: the portfolio must not disagree."""
    good = collect_rows(_plant_batch(spark), PLANT_METRIC_COLUMNS)
    # A plant that does not exist violates the foreign key.
    bad = list(good) + [tuple(["GHOST_PLANT"] + list(good[0][1:]))]

    with pytest.raises(Exception):
        write_live_metrics(seeded_database, bad, [])

    with connect(seeded_database) as conn:
        assert fetch_one(conn, "SELECT COUNT(*) FROM live_plant_metrics")[0] == 0


def test_empty_batch_is_a_no_op(seeded_database):
    assert write_live_metrics(seeded_database, [], []) == (0, 0)
    with connect(seeded_database) as conn:
        assert fetch_all(conn, "SELECT * FROM live_plant_metrics") == []
