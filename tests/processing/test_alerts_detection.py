"""Per-inverter fault detection (Milestone 8)."""

from __future__ import annotations

import pytest

from processing.streaming.alerts import (
    ALERT_INVERTER_OFFLINE,
    ALERT_TELEMETRY_GAP,
    ALERT_UNDERPERFORMANCE,
    CONDITION_COLUMNS,
    SEVERITY_CRITICAL,
    SEVERITY_WARNING,
    detect_alert_conditions,
)
from processing.streaming.metrics import latest_reading_per_inverter
from processing.streaming.transforms import parse_telemetry
from processing.streaming.validation import (
    normalize_valid_events,
    valid_events,
    validate_telemetry,
)
from tests.processing._events import kafka_frame, telemetry_event, utc_ts
from tests.processing.test_metrics_plant import SETTINGS

pytestmark = pytest.mark.spark

OBSERVED_AT = utc_ts(2026, 8, 21, 5, 0, 3)

# A five-inverter plant, matching the scale of the demo portfolio.
FLEET = [("PLANT_01", f"INV_0{i}", 1000.0) for i in range(1, 6)]


def _fleet(spark, rows=None):
    return spark.createDataFrame(
        list(rows if rows is not None else FLEET),
        schema="plant_id string, inverter_id string, rated_power_kw double",
    )


def _latest(spark, specs):
    payloads = [telemetry_event(**spec) for spec in specs]
    events = normalize_valid_events(
        valid_events(validate_telemetry(parse_telemetry(kafka_frame(spark, payloads))))
    )
    return latest_reading_per_inverter(events)


def _conditions(spark, specs, fleet_rows=None):
    return {
        (r.inverter_id, r.alert_type): r
        for r in detect_alert_conditions(
            _latest(spark, specs), _fleet(spark, fleet_rows), SETTINGS, OBSERVED_AT
        ).collect()
    }


def _healthy(inverter_id, power=800.0, **overrides):
    """A well-behaved inverter: 800 kW of an 800 kW expectation at 800 W/m^2."""
    base = dict(
        event_id=f"e-{inverter_id}",
        inverter_id=inverter_id,
        active_power_kw=power,
        irradiance_wm2=800.0,
        status="ONLINE",
        availability=1.0,
    )
    base.update(overrides)
    return base


def test_a_fully_healthy_fleet_raises_nothing(spark):
    specs = [_healthy(f"INV_0{i}") for i in range(1, 6)]
    assert _conditions(spark, specs) == {}


class TestUnderperformance:
    def test_single_degraded_inverter_is_detected(self, spark):
        """The demo's headline anomaly, which a plant-level rule cannot see.

        One inverter at 45% of expectation on a five-inverter plant moves plant
        output by only ~11% — nowhere near an 80% plant threshold.
        """
        specs = [_healthy(f"INV_0{i}") for i in range(1, 6)]
        specs[1] = _healthy("INV_02", power=360.0)  # 45% of the 800 kW expectation

        conditions = _conditions(spark, specs)

        assert list(conditions) == [("INV_02", ALERT_UNDERPERFORMANCE)]
        assert conditions[("INV_02", ALERT_UNDERPERFORMANCE)].severity == SEVERITY_WARNING

    def test_loss_is_the_shortfall_against_expectation(self, spark):
        specs = [_healthy(f"INV_0{i}") for i in range(1, 6)]
        specs[1] = _healthy("INV_02", power=360.0)

        condition = _conditions(spark, specs)[("INV_02", ALERT_UNDERPERFORMANCE)]
        # 1000 kW rating * 0.8 irradiance = 800 expected; 800 - 360 = 440.
        assert condition.loss_kw == pytest.approx(440.0)

    def test_message_states_the_shortfall(self, spark):
        specs = [_healthy(f"INV_0{i}") for i in range(1, 6)]
        specs[1] = _healthy("INV_02", power=360.0)

        message = _conditions(spark, specs)[("INV_02", ALERT_UNDERPERFORMANCE)].message
        assert "INV_02" in message and "PLANT_01" in message
        assert "45.0%" in message

    def test_just_above_the_threshold_is_not_flagged(self, spark):
        specs = [_healthy(f"INV_0{i}") for i in range(1, 6)]
        specs[1] = _healthy("INV_02", power=648.0)  # 81% of expected
        assert _conditions(spark, specs) == {}

    def test_just_below_the_threshold_is_flagged(self, spark):
        specs = [_healthy(f"INV_0{i}") for i in range(1, 6)]
        specs[1] = _healthy("INV_02", power=632.0)  # 79% of expected
        assert ("INV_02", ALERT_UNDERPERFORMANCE) in _conditions(spark, specs)

    def test_no_underperformance_is_raised_at_night(self, spark):
        """Zero output in darkness is correct behaviour, not a fault."""
        specs = [_healthy(f"INV_0{i}", power=0.0, irradiance_wm2=10.0) for i in range(1, 6)]
        assert _conditions(spark, specs) == {}


class TestOffline:
    def test_offline_status_is_critical(self, spark):
        specs = [_healthy(f"INV_0{i}") for i in range(1, 6)]
        specs[0] = _healthy("INV_01", power=0.0, status="OFFLINE", availability=0.0)

        conditions = _conditions(spark, specs)
        condition = conditions[("INV_01", ALERT_INVERTER_OFFLINE)]

        assert condition.severity == SEVERITY_CRITICAL
        assert "OFFLINE" in condition.message

    def test_offline_does_not_also_raise_underperformance(self, spark):
        """One fault, one alert: a dead inverter obviously produces nothing."""
        specs = [_healthy(f"INV_0{i}") for i in range(1, 6)]
        specs[0] = _healthy("INV_01", power=0.0, status="OFFLINE", availability=0.0)

        types = {alert_type for (_, alert_type) in _conditions(spark, specs)}
        assert types == {ALERT_INVERTER_OFFLINE}

    def test_zero_availability_counts_as_offline_even_if_status_disagrees(self, spark):
        specs = [_healthy(f"INV_0{i}") for i in range(1, 6)]
        specs[0] = _healthy("INV_01", power=0.0, status="WARNING", availability=0.0)
        assert ("INV_01", ALERT_INVERTER_OFFLINE) in _conditions(spark, specs)

    def test_offline_loss_is_the_full_expectation(self, spark):
        specs = [_healthy(f"INV_0{i}") for i in range(1, 6)]
        specs[0] = _healthy("INV_01", power=0.0, status="OFFLINE", availability=0.0)

        condition = _conditions(spark, specs)[("INV_01", ALERT_INVERTER_OFFLINE)]
        assert condition.loss_kw == pytest.approx(800.0)


class TestTelemetryGap:
    def test_a_silent_inverter_is_detected_from_the_registry(self, spark):
        """Detection is driven by the configured fleet, not by what arrived.

        An inverter that sends nothing cannot be found by looking at telemetry;
        it is found by noticing that the registry expected it.
        """
        specs = [_healthy(f"INV_0{i}") for i in range(1, 5)]  # INV_05 never reports

        conditions = _conditions(spark, specs)

        assert list(conditions) == [("INV_05", ALERT_TELEMETRY_GAP)]
        assert conditions[("INV_05", ALERT_TELEMETRY_GAP)].severity == SEVERITY_CRITICAL

    def test_gap_is_distinct_from_offline(self, spark):
        """The spec requires these be distinguishable: one is an asset fault,
        the other means we have lost visibility of the asset entirely."""
        specs = [_healthy(f"INV_0{i}") for i in range(1, 5)]
        specs[0] = _healthy("INV_01", power=0.0, status="OFFLINE", availability=0.0)

        conditions = _conditions(spark, specs)

        assert conditions[("INV_01", ALERT_INVERTER_OFFLINE)].alert_type == ALERT_INVERTER_OFFLINE
        assert conditions[("INV_05", ALERT_TELEMETRY_GAP)].alert_type == ALERT_TELEMETRY_GAP

    def test_gap_loss_is_zero_because_it_is_unknown(self, spark):
        """No reading means no irradiance, so any loss figure would be invented."""
        specs = [_healthy(f"INV_0{i}") for i in range(1, 5)]
        assert _conditions(spark, specs)[("INV_05", ALERT_TELEMETRY_GAP)].loss_kw == pytest.approx(0.0)

    def test_gap_message_names_the_missing_asset(self, spark):
        specs = [_healthy(f"INV_0{i}") for i in range(1, 5)]
        message = _conditions(spark, specs)[("INV_05", ALERT_TELEMETRY_GAP)].message
        assert "No telemetry" in message and "INV_05" in message


class TestShape:
    def test_output_matches_the_declared_column_contract(self, spark):
        specs = [_healthy(f"INV_0{i}") for i in range(1, 5)]
        frame = detect_alert_conditions(
            _latest(spark, specs), _fleet(spark), SETTINGS, OBSERVED_AT
        )
        assert tuple(frame.columns) == CONDITION_COLUMNS

    def test_all_conditions_share_the_batch_observation_clock(self, spark):
        """Including silent inverters, which have no event time of their own."""
        from tests.processing._events import utc

        specs = [_healthy(f"INV_0{i}") for i in range(1, 5)]
        specs[0] = _healthy("INV_01", power=0.0, status="OFFLINE", availability=0.0)

        frame = detect_alert_conditions(
            _latest(spark, specs), _fleet(spark), SETTINGS, OBSERVED_AT
        )
        observed = {r.observed_at for r in frame.select(utc("observed_at")).collect()}

        assert observed == {"2026-08-21 05:00:03"}

    def test_faults_on_multiple_plants_are_reported_separately(self, spark):
        fleet = [("PLANT_01", "INV_01", 1000.0), ("PLANT_02", "INV_01", 1000.0)]
        specs = [
            _healthy("INV_01", plant_id="PLANT_01", event_id="p1"),
            _healthy("INV_01", plant_id="PLANT_02", event_id="p2", power=0.0,
                     status="OFFLINE", availability=0.0),
        ]
        rows = detect_alert_conditions(
            _latest(spark, specs), _fleet(spark, fleet), SETTINGS, OBSERVED_AT
        ).collect()

        assert len(rows) == 1
        assert rows[0].plant_id == "PLANT_02"
