"""Alert lifecycle against a real PostgreSQL (Milestone 8).

Covers the properties that make alerts trustworthy rather than noisy: a fault
must persist before anyone is told, it must be reported once rather than every
microbatch, and it must close itself when the asset recovers.

Skipped unless SOLARIQ_TEST_DATABASE_URL points at a throwaway database.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from processing.common.db import connect, fetch_all, fetch_one
from processing.streaming.alert_store import reconcile_alerts_standalone
from processing.streaming.alerts import (
    ALERT_INVERTER_OFFLINE,
    ALERT_UNDERPERFORMANCE,
    SEVERITY_CRITICAL,
    SEVERITY_WARNING,
)
from storage.migrate import run_migrations
from storage.seed_portfolio import load_portfolio, seed_portfolio
from tests.processing._events import utc_ts

pytestmark = pytest.mark.integration

TEST_DATABASE_URL = os.getenv("SOLARIQ_TEST_DATABASE_URL")
if not TEST_DATABASE_URL:
    pytest.skip(
        "set SOLARIQ_TEST_DATABASE_URL to run alert lifecycle tests",
        allow_module_level=True,
    )

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "portfolio.test.yaml"

# One simulated hour. Under the demo clock (1 day = 300 real seconds) that is
# about 12 real seconds, long enough to prove "sustained" during a live demo.
SUSTAIN_SECONDS = 3600.0

T0 = utc_ts(2026, 8, 21, 5, 0, 0)
T_HALF = utc_ts(2026, 8, 21, 5, 30, 0)   # 30 simulated minutes later
T_FULL = utc_ts(2026, 8, 21, 6, 0, 0)    # 60 simulated minutes later
T_LATER = utc_ts(2026, 8, 21, 7, 0, 0)


def _condition(observed_at, inverter_id="INV_01", alert_type=ALERT_UNDERPERFORMANCE,
               severity=SEVERITY_WARNING, loss_kw=440.0, message="degraded"):
    return (
        "PLANT_01", inverter_id, alert_type, severity, message, loss_kw, observed_at,
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


def _alerts(database_url, status=None):
    query = (
        "SELECT id, plant_id, inverter_id, alert_type, severity, status, "
        "estimated_loss_kwh, estimated_revenue_loss, message FROM alerts"
    )
    params = None
    if status:
        query += " WHERE status = %s"
        params = (status,)
    query += " ORDER BY started_at"
    with connect(database_url) as conn:
        return fetch_all(conn, query, params)


def test_a_brief_fault_does_not_open_an_alert(database):
    """A passing cloud must not page anyone."""
    reconcile_alerts_standalone(database, [_condition(T0)], T0, SUSTAIN_SECONDS)
    reconcile_alerts_standalone(database, [_condition(T_HALF)], T_HALF, SUSTAIN_SECONDS)

    assert _alerts(database) == []
    # The condition is being tracked, just not yet promoted.
    with connect(database) as conn:
        assert fetch_one(conn, "SELECT COUNT(*) FROM alert_conditions")[0] == 1


def test_a_sustained_fault_opens_exactly_one_alert(database):
    reconcile_alerts_standalone(database, [_condition(T0)], T0, SUSTAIN_SECONDS)
    reconcile_alerts_standalone(database, [_condition(T_FULL)], T_FULL, SUSTAIN_SECONDS)

    alerts = _alerts(database)
    assert len(alerts) == 1
    assert alerts[0][1:6] == ("PLANT_01", "INV_01", ALERT_UNDERPERFORMANCE, SEVERITY_WARNING, "ACTIVE")


def test_continued_observation_does_not_spam_duplicates(database):
    """The core anti-noise property: one fault, one row, however many batches."""
    reconcile_alerts_standalone(database, [_condition(T0)], T0, SUSTAIN_SECONDS)
    for _ in range(10):
        reconcile_alerts_standalone(database, [_condition(T_FULL)], T_FULL, SUSTAIN_SECONDS)

    assert len(_alerts(database)) == 1


def test_alert_start_time_is_when_the_fault_began_not_when_it_was_promoted(database):
    reconcile_alerts_standalone(database, [_condition(T0)], T0, SUSTAIN_SECONDS)
    reconcile_alerts_standalone(database, [_condition(T_FULL)], T_FULL, SUSTAIN_SECONDS)

    with connect(database) as conn:
        started = fetch_one(
            conn,
            "SELECT to_char(started_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS') FROM alerts",
        )[0]

    assert started == "2026-08-21 05:00:00"


def test_recovery_resolves_the_alert(database):
    reconcile_alerts_standalone(database, [_condition(T0)], T0, SUSTAIN_SECONDS)
    reconcile_alerts_standalone(database, [_condition(T_FULL)], T_FULL, SUSTAIN_SECONDS)
    assert len(_alerts(database, "ACTIVE")) == 1

    # The inverter recovers: nothing is observed this batch.
    reconcile_alerts_standalone(database, [], T_LATER, SUSTAIN_SECONDS)

    assert _alerts(database, "ACTIVE") == []
    resolved = _alerts(database, "RESOLVED")
    assert len(resolved) == 1

    with connect(database) as conn:
        ended = fetch_one(
            conn,
            "SELECT to_char(ended_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS') FROM alerts",
        )[0]
    assert ended == "2026-08-21 07:00:00"
    # The tracked condition is cleared too, so a recurrence starts fresh.
    with connect(database) as conn:
        assert fetch_one(conn, "SELECT COUNT(*) FROM alert_conditions")[0] == 0


def test_a_recurrence_after_recovery_opens_a_new_alert(database):
    """Two separate outages are two incidents, not one reopened row."""
    reconcile_alerts_standalone(database, [_condition(T0)], T0, SUSTAIN_SECONDS)
    reconcile_alerts_standalone(database, [_condition(T_FULL)], T_FULL, SUSTAIN_SECONDS)
    reconcile_alerts_standalone(database, [], T_LATER, SUSTAIN_SECONDS)

    later_start = utc_ts(2026, 8, 21, 9, 0, 0)
    later_end = utc_ts(2026, 8, 21, 10, 0, 0)
    reconcile_alerts_standalone(database, [_condition(later_start)], later_start, SUSTAIN_SECONDS)
    reconcile_alerts_standalone(database, [_condition(later_end)], later_end, SUSTAIN_SECONDS)

    assert len(_alerts(database)) == 2
    assert len(_alerts(database, "ACTIVE")) == 1
    assert len(_alerts(database, "RESOLVED")) == 1


def test_different_fault_types_on_one_asset_are_separate_alerts(database):
    offline = _condition(T0, alert_type=ALERT_INVERTER_OFFLINE, severity=SEVERITY_CRITICAL)
    other = _condition(T0, inverter_id="INV_02")

    reconcile_alerts_standalone(database, [offline, other], T0, SUSTAIN_SECONDS)
    reconcile_alerts_standalone(
        database,
        [
            _condition(T_FULL, alert_type=ALERT_INVERTER_OFFLINE, severity=SEVERITY_CRITICAL),
            _condition(T_FULL, inverter_id="INV_02"),
        ],
        T_FULL,
        SUSTAIN_SECONDS,
    )

    alerts = _alerts(database, "ACTIVE")
    assert len(alerts) == 2
    assert {(a[2], a[3]) for a in alerts} == {
        ("INV_01", ALERT_INVERTER_OFFLINE),
        ("INV_02", ALERT_UNDERPERFORMANCE),
    }


def test_resolving_one_fault_leaves_the_other_active(database):
    conditions = [
        _condition(T0, alert_type=ALERT_INVERTER_OFFLINE, severity=SEVERITY_CRITICAL),
        _condition(T0, inverter_id="INV_02"),
    ]
    reconcile_alerts_standalone(database, conditions, T0, SUSTAIN_SECONDS)
    reconcile_alerts_standalone(
        database,
        [
            _condition(T_FULL, alert_type=ALERT_INVERTER_OFFLINE, severity=SEVERITY_CRITICAL),
            _condition(T_FULL, inverter_id="INV_02"),
        ],
        T_FULL,
        SUSTAIN_SECONDS,
    )
    # Only INV_02 is still faulty.
    reconcile_alerts_standalone(database, [_condition(T_LATER, inverter_id="INV_02")], T_LATER, SUSTAIN_SECONDS)

    active = _alerts(database, "ACTIVE")
    assert len(active) == 1
    assert active[0][2] == "INV_02"
    assert len(_alerts(database, "RESOLVED")) == 1


class TestFinancialImpact:
    def test_lost_energy_is_mean_shortfall_times_duration(self, database):
        reconcile_alerts_standalone(database, [_condition(T0, loss_kw=400.0)], T0, SUSTAIN_SECONDS)
        reconcile_alerts_standalone(database, [_condition(T_FULL, loss_kw=400.0)], T_FULL, SUSTAIN_SECONDS)

        loss_kwh = _alerts(database)[0][6]
        # 400 kW mean shortfall sustained for one simulated hour.
        assert loss_kwh == pytest.approx(400.0)

    def test_impact_grows_while_the_fault_persists(self, database):
        reconcile_alerts_standalone(database, [_condition(T0, loss_kw=400.0)], T0, SUSTAIN_SECONDS)
        reconcile_alerts_standalone(database, [_condition(T_FULL, loss_kw=400.0)], T_FULL, SUSTAIN_SECONDS)
        first = _alerts(database)[0][6]

        reconcile_alerts_standalone(database, [_condition(T_LATER, loss_kw=400.0)], T_LATER, SUSTAIN_SECONDS)
        second = _alerts(database)[0][6]

        # Two simulated hours of the same shortfall.
        assert second > first
        assert second == pytest.approx(800.0)

    def test_revenue_loss_uses_the_plants_ppa_rate(self, database):
        """The commercial payoff: energy lost becomes money lost."""
        with connect(database) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO daily_reference (
                        simulation_date, plant_id, plant_capacity_kw, expected_generation_kwh,
                        expected_peak_power_kw, forecast_irradiance_kwh_m2, ppa_rate_per_kwh,
                        maintenance_flag, source_version
                    ) VALUES ('2026-08-21', 'PLANT_01', 4000, 20000, 3800, 5.0, 0.15, FALSE, 'v1')
                    """
                )

        reconcile_alerts_standalone(database, [_condition(T0, loss_kw=400.0)], T0, SUSTAIN_SECONDS)
        reconcile_alerts_standalone(database, [_condition(T_FULL, loss_kw=400.0)], T_FULL, SUSTAIN_SECONDS)

        alert = _alerts(database)[0]
        assert alert[6] == pytest.approx(400.0)          # kWh lost
        assert alert[7] == pytest.approx(60.0)           # 400 kWh * 0.15

    def test_energy_loss_is_still_recorded_without_a_reference_feed(self, database):
        """Before the day's rate arrives, the physical loss is still known."""
        reconcile_alerts_standalone(database, [_condition(T0, loss_kw=400.0)], T0, SUSTAIN_SECONDS)
        reconcile_alerts_standalone(database, [_condition(T_FULL, loss_kw=400.0)], T_FULL, SUSTAIN_SECONDS)

        alert = _alerts(database)[0]
        assert alert[6] == pytest.approx(400.0)
        assert alert[7] is None


def test_message_and_severity_track_the_current_condition(database):
    reconcile_alerts_standalone(database, [_condition(T0, message="down 45%")], T0, SUSTAIN_SECONDS)
    reconcile_alerts_standalone(
        database, [_condition(T_FULL, message="down 20%")], T_FULL, SUSTAIN_SECONDS
    )

    assert _alerts(database)[0][8] == "down 20%"
