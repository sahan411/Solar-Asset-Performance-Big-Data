"""End-to-end microbatch processing: telemetry in, serving tables out.

Exercises the speed layer the way the running job does — metrics, alerts and
health written together in one transaction — against a real PostgreSQL.

Skipped unless SOLARIQ_TEST_DATABASE_URL points at a throwaway database.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from processing.common.db import connect, fetch_all, fetch_one
from processing.streaming.pipeline import (
    MicrobatchContext,
    load_inverter_reference,
    load_plant_reference,
    process_microbatch,
)
from processing.streaming.transforms import parse_telemetry
from processing.streaming.validation import (
    normalize_valid_events,
    valid_events,
    validate_telemetry,
)
from storage.migrate import run_migrations
from storage.seed_portfolio import load_portfolio, seed_portfolio
from tests.processing._events import kafka_frame, telemetry_event
from tests.processing.test_metrics_plant import SETTINGS

pytestmark = [pytest.mark.integration, pytest.mark.spark]

TEST_DATABASE_URL = os.getenv("SOLARIQ_TEST_DATABASE_URL")
if not TEST_DATABASE_URL:
    pytest.skip("set SOLARIQ_TEST_DATABASE_URL to run pipeline tests", allow_module_level=True)

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


@pytest.fixture()
def ctx(spark, database):
    return MicrobatchContext(
        database_url=database,
        settings=SETTINGS,
        plant_reference=load_plant_reference(spark, database),
        inverter_reference=load_inverter_reference(spark, database),
    )


def _batch(spark, specs):
    payloads = [telemetry_event(**spec) for spec in specs]
    return normalize_valid_events(
        valid_events(validate_telemetry(parse_telemetry(kafka_frame(spark, payloads))))
    )


def _healthy_plant_01(power=800.0):
    """PLANT_01 has four 1000 kW inverters in the test fixture."""
    return [
        dict(
            event_id=f"p1-{i}",
            plant_id="PLANT_01",
            inverter_id=f"INV_0{i}",
            active_power_kw=power,
            irradiance_wm2=800.0,
            timestamp="2026-08-21T05:00:00Z",
        )
        for i in range(1, 5)
    ]


def _shift(specs, time_of_day: str, suffix: str):
    """Re-stamp a batch at a later simulated time, with fresh event ids."""
    return [
        dict(spec, timestamp=f"2026-08-21T{time_of_day}Z", event_id=f"{spec['event_id']}-{suffix}")
        for spec in specs
    ]


def _healthy_plant_02(power=800.0):
    """PLANT_02 has two 1000 kW inverters."""
    return [
        dict(
            event_id=f"p2-{i}",
            plant_id="PLANT_02",
            inverter_id=f"INV_0{i}",
            active_power_kw=power,
            irradiance_wm2=800.0,
            timestamp="2026-08-21T05:00:00Z",
        )
        for i in range(1, 3)
    ]


class TestReferenceLoading:
    def test_plant_reference_carries_capacity_and_inverter_counts(self, spark, database):
        rows = {r.plant_id: r for r in load_plant_reference(spark, database).collect()}

        assert rows["PLANT_01"].capacity_kw == pytest.approx(4000.0)
        assert rows["PLANT_01"].configured_inverters == 4
        assert rows["PLANT_02"].configured_inverters == 2

    def test_inverter_reference_carries_the_whole_fleet(self, spark, database):
        rows = load_inverter_reference(spark, database).collect()
        assert len(rows) == 6
        assert {r.rated_power_kw for r in rows} == {1000.0}


class TestHappyPath:
    def test_metrics_land_in_both_serving_tables(self, spark, ctx, database):
        process_microbatch(_batch(spark, _healthy_plant_01() + _healthy_plant_02()), 0, ctx)

        with connect(database) as conn:
            plants = fetch_all(
                conn,
                "SELECT plant_id, current_power_kw, expected_power_kw, performance_pct, "
                "availability_pct FROM live_plant_metrics ORDER BY plant_id",
            )
            portfolio = fetch_one(
                conn,
                "SELECT current_power_kw, expected_power_kw, performance_pct, "
                "online_inverters FROM live_portfolio_metrics",
            )

        assert len(plants) == 2
        # PLANT_01: four inverters at 800 kW.
        assert plants[0][1] == pytest.approx(3200.0)
        # 4000 kW nameplate * 0.8 irradiance fraction.
        assert plants[0][2] == pytest.approx(3200.0)
        assert plants[0][3] == pytest.approx(100.0)
        assert plants[0][4] == pytest.approx(100.0)

        # Portfolio is the weighted roll-up of both plants.
        assert portfolio[0] == pytest.approx(4800.0)
        assert portfolio[1] == pytest.approx(4800.0)
        assert portfolio[2] == pytest.approx(100.0)
        assert portfolio[3] == 6

    def test_window_times_are_stored_as_the_correct_utc_instant(self, spark, ctx, database):
        """Guards the naive-datetime shift across the whole processor."""
        process_microbatch(_batch(spark, _healthy_plant_01()), 0, ctx)

        with connect(database) as conn:
            stored = fetch_one(
                conn,
                "SELECT to_char(window_start AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS') "
                "FROM live_plant_metrics WHERE plant_id = 'PLANT_01'",
            )[0]

        assert stored == "2026-08-21 05:00:00"

    def test_health_is_recorded_as_healthy(self, spark, ctx, database):
        process_microbatch(_batch(spark, _healthy_plant_01()), 0, ctx)

        with connect(database) as conn:
            status, message = fetch_one(
                conn,
                "SELECT status, message FROM pipeline_health WHERE component = 'spark-stream'",
            )

        assert status == "HEALTHY"
        assert "processed 4 events" in message

    def test_a_healthy_fleet_raises_no_alerts(self, spark, ctx, database):
        process_microbatch(_batch(spark, _healthy_plant_01() + _healthy_plant_02()), 0, ctx)

        with connect(database) as conn:
            assert fetch_one(conn, "SELECT COUNT(*) FROM alerts")[0] == 0


class TestEmptyBatch:
    def test_empty_batch_reports_degraded_without_failing(self, spark, ctx, database):
        """Alive but idle must be distinguishable from dead."""
        process_microbatch(_batch(spark, []), 0, ctx)

        with connect(database) as conn:
            status, message = fetch_one(
                conn,
                "SELECT status, message FROM pipeline_health WHERE component = 'spark-stream'",
            )
            assert fetch_one(conn, "SELECT COUNT(*) FROM live_plant_metrics")[0] == 0

        assert status == "DEGRADED"
        assert "no telemetry" in message


class TestFaultDetection:
    def test_a_sustained_degraded_inverter_becomes_an_alert(self, spark, ctx, database):
        """The demo's headline path, end to end through the real processor.

        Both plants report, so the only fault present is the degraded inverter —
        otherwise PLANT_02's silent inverters would (correctly) raise gaps too.
        """
        degraded = _healthy_plant_01() + _healthy_plant_02()
        degraded[1]["active_power_kw"] = 360.0  # PLANT_01/INV_02 at 45% of expectation

        # First observation starts the clock; no alert yet.
        process_microbatch(_batch(spark, degraded), 0, ctx)
        with connect(database) as conn:
            assert fetch_one(conn, "SELECT COUNT(*) FROM alerts")[0] == 0
            assert fetch_one(conn, "SELECT COUNT(*) FROM alert_conditions")[0] == 1

        # An hour of simulated time later, the fault has persisted.
        process_microbatch(_batch(spark, _shift(degraded, "06:00:00", "b")), 1, ctx)

        with connect(database) as conn:
            alerts = fetch_all(
                conn,
                "SELECT plant_id, inverter_id, alert_type, severity, status "
                "FROM alerts WHERE status = 'ACTIVE'",
            )

        assert alerts == [("PLANT_01", "INV_02", "UNDERPERFORMANCE", "WARNING", "ACTIVE")]

    def test_alert_interval_is_ordered_and_in_utc(self, spark, ctx, database):
        """started_at and ended_at must come from the same clock.

        They are set on different code paths; if one were taken from a Spark
        column collected as host-local and the other from a timezone-aware Python
        value, a resolved alert could appear to end before it began.
        """
        degraded = _healthy_plant_01() + _healthy_plant_02()
        degraded[1]["active_power_kw"] = 360.0

        process_microbatch(_batch(spark, degraded), 0, ctx)
        process_microbatch(_batch(spark, _shift(degraded, "06:00:00", "b")), 1, ctx)
        process_microbatch(
            _batch(spark, _shift(_healthy_plant_01() + _healthy_plant_02(), "07:00:00", "c")), 2, ctx
        )

        with connect(database) as conn:
            started, ended = fetch_one(
                conn,
                "SELECT to_char(started_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS'), "
                "to_char(ended_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS') FROM alerts",
            )

        assert started == "2026-08-21 05:00:00"
        assert ended == "2026-08-21 07:00:00"

    def test_recovery_resolves_the_alert(self, spark, ctx, database):
        degraded = _healthy_plant_01() + _healthy_plant_02()
        degraded[1]["active_power_kw"] = 360.0

        process_microbatch(_batch(spark, degraded), 0, ctx)
        process_microbatch(_batch(spark, _shift(degraded, "06:00:00", "b")), 1, ctx)

        # The inverter recovers.
        process_microbatch(
            _batch(spark, _shift(_healthy_plant_01() + _healthy_plant_02(), "07:00:00", "c")), 2, ctx
        )

        with connect(database) as conn:
            assert fetch_one(conn, "SELECT COUNT(*) FROM alerts WHERE status = 'ACTIVE'")[0] == 0
            assert fetch_one(conn, "SELECT COUNT(*) FROM alerts WHERE status = 'RESOLVED'")[0] == 1

    def test_a_silent_inverter_shows_as_lost_availability(self, spark, ctx, database):
        """PLANT_01 has four configured inverters; only three report."""
        process_microbatch(_batch(spark, _healthy_plant_01()[:3]), 0, ctx)

        with connect(database) as conn:
            online, offline, availability = fetch_one(
                conn,
                "SELECT online_inverters, offline_inverters, availability_pct "
                "FROM live_plant_metrics WHERE plant_id = 'PLANT_01'",
            )

        assert (online, offline) == (3, 1)
        assert availability == pytest.approx(75.0)


class TestIdempotency:
    def test_replaying_a_microbatch_does_not_duplicate_metrics(self, spark, ctx, database):
        batch = _batch(spark, _healthy_plant_01())
        process_microbatch(batch, 0, ctx)
        process_microbatch(batch, 0, ctx)

        with connect(database) as conn:
            assert fetch_one(conn, "SELECT COUNT(*) FROM live_plant_metrics")[0] == 1
            assert fetch_one(conn, "SELECT COUNT(*) FROM live_portfolio_metrics")[0] == 1
