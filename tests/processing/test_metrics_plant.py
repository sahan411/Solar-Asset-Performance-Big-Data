"""Live plant metrics (Milestone 5).

The headline case is `current_power_kw`: summing every sample in a window
multiplies the plant's real output by the number of readings taken. These tests
pin the correct behaviour with hand-calculable numbers.
"""

from __future__ import annotations

import pytest

from processing.common.config import StreamSettings
from processing.streaming.metrics import (
    latest_reading_per_inverter,
    plant_metrics,
    plant_power_timeline,
)
from processing.streaming.transforms import parse_telemetry
from processing.streaming.validation import (
    normalize_valid_events,
    valid_events,
    validate_telemetry,
)
from tests.processing._events import kafka_frame, telemetry_event

pytestmark = pytest.mark.spark


SETTINGS = StreamSettings(
    checkpoint_dir="/tmp/checkpoints",
    watermark="2 minutes",
    window_duration="60 seconds",
    window_slide="15 seconds",
    min_irradiance_wm2=150.0,
    reference_irradiance_wm2=1000.0,
    underperformance_threshold_pct=80.0,
    underperformance_sustain_seconds=30,
    offline_sustain_seconds=15,
)


def _events(spark, specs):
    """Build normalized telemetry from a list of override dicts."""
    payloads = [telemetry_event(**spec) for spec in specs]
    return normalize_valid_events(
        valid_events(validate_telemetry(parse_telemetry(kafka_frame(spark, payloads))))
    )


def _reference(spark, rows=(("PLANT_01", 1000.0, 2),)):
    return spark.createDataFrame(
        list(rows), schema="plant_id string, capacity_kw double, configured_inverters int"
    )


# Two inverters, two instants. Latest readings are 200 and 250 -> 450 kW.
# Every sample summed would be 100+200+150+250 = 700 kW, which is the bug.
TWO_INVERTER_BATCH = [
    dict(event_id="a1", inverter_id="INV_01", timestamp="2026-08-21T05:00:00Z", active_power_kw=100.0, irradiance_wm2=500.0),
    dict(event_id="a2", inverter_id="INV_01", timestamp="2026-08-21T05:00:03Z", active_power_kw=200.0, irradiance_wm2=500.0),
    dict(event_id="b1", inverter_id="INV_02", timestamp="2026-08-21T05:00:00Z", active_power_kw=150.0, irradiance_wm2=500.0),
    dict(event_id="b2", inverter_id="INV_02", timestamp="2026-08-21T05:00:03Z", active_power_kw=250.0, irradiance_wm2=500.0),
]


class TestLatestReadingPerInverter:
    def test_keeps_exactly_one_row_per_inverter(self, spark):
        latest = latest_reading_per_inverter(_events(spark, TWO_INVERTER_BATCH))
        rows = {r.inverter_id: r for r in latest.collect()}

        assert latest.count() == 2
        assert rows["INV_01"].active_power_kw == pytest.approx(200.0)
        assert rows["INV_02"].active_power_kw == pytest.approx(250.0)

    def test_same_inverter_id_on_different_plants_is_not_conflated(self, spark):
        specs = [
            dict(event_id="p1", plant_id="PLANT_01", inverter_id="INV_01", active_power_kw=10.0),
            dict(event_id="p2", plant_id="PLANT_02", inverter_id="INV_01", active_power_kw=20.0),
        ]
        assert latest_reading_per_inverter(_events(spark, specs)).count() == 2

    def test_ties_on_event_time_resolve_deterministically(self, spark):
        """Replaying a batch must not pick a different 'latest' reading."""
        specs = [
            dict(event_id="t1", inverter_id="INV_01", timestamp="2026-08-21T05:00:00Z", active_power_kw=111.0),
            dict(event_id="t2", inverter_id="INV_01", timestamp="2026-08-21T05:00:00Z", active_power_kw=222.0),
        ]
        results = {
            latest_reading_per_inverter(_events(spark, specs)).collect()[0].active_power_kw
            for _ in range(3)
        }
        # Highest Kafka offset wins, every time.
        assert results == {222.0}


class TestPlantPowerTimeline:
    def test_sums_across_inverters_at_each_instant(self, spark):
        timeline = {
            str(r.event_time): r.plant_power_kw
            for r in plant_power_timeline(_events(spark, TWO_INVERTER_BATCH)).collect()
        }
        # 100+150 at the first instant, 200+250 at the second.
        assert sorted(timeline.values()) == [250.0, 450.0]


class TestCurrentPower:
    def test_is_the_sum_of_latest_readings_not_of_all_samples(self, spark):
        """The core correctness property of the whole speed layer."""
        row = plant_metrics(
            _events(spark, TWO_INVERTER_BATCH), _reference(spark), SETTINGS
        ).collect()[0]

        assert row.current_power_kw == pytest.approx(450.0)
        # The bug this guards against would report 700.
        assert row.current_power_kw != pytest.approx(700.0)

    def test_a_single_reading_per_inverter_is_unaffected(self, spark):
        specs = [
            dict(event_id="x", inverter_id="INV_01", active_power_kw=300.0, irradiance_wm2=500.0),
            dict(event_id="y", inverter_id="INV_02", active_power_kw=200.0, irradiance_wm2=500.0),
        ]
        row = plant_metrics(_events(spark, specs), _reference(spark), SETTINGS).collect()[0]
        assert row.current_power_kw == pytest.approx(500.0)


class TestAveragePower:
    def test_is_the_mean_of_plant_power_over_instants(self, spark):
        """Mean of (250, 450) = 350 — not the mean of the raw samples (175)."""
        row = plant_metrics(
            _events(spark, TWO_INVERTER_BATCH), _reference(spark), SETTINGS
        ).collect()[0]

        assert row.avg_power_kw == pytest.approx(350.0)
        assert row.avg_power_kw != pytest.approx(175.0)


class TestExpectedPowerAndPerformance:
    def test_expected_power_scales_capacity_by_irradiance_fraction(self, spark):
        row = plant_metrics(
            _events(spark, TWO_INVERTER_BATCH), _reference(spark), SETTINGS
        ).collect()[0]
        # 1000 kW nameplate * (500 / 1000 W/m^2) = 500 kW.
        assert row.expected_power_kw == pytest.approx(500.0)

    def test_performance_is_actual_over_expected(self, spark):
        row = plant_metrics(
            _events(spark, TWO_INVERTER_BATCH), _reference(spark), SETTINGS
        ).collect()[0]
        # 450 / 500 = 90%.
        assert row.performance_pct == pytest.approx(90.0)

    def test_expected_power_is_capped_at_nameplate_capacity(self, spark):
        """Cloud-edge enhancement can exceed 1000 W/m^2; the plant cannot."""
        specs = [dict(event_id="hi", inverter_id="INV_01", active_power_kw=900.0, irradiance_wm2=1200.0)]
        row = plant_metrics(_events(spark, specs), _reference(spark), SETTINGS).collect()[0]
        assert row.expected_power_kw == pytest.approx(1000.0)

    def test_performance_is_null_below_the_minimum_irradiance(self, spark):
        """At night, 0/0 is not 0% — it is unknown, and must read as such."""
        specs = [
            dict(event_id="n1", inverter_id="INV_01", active_power_kw=0.0, irradiance_wm2=5.0),
            dict(event_id="n2", inverter_id="INV_02", active_power_kw=0.0, irradiance_wm2=5.0),
        ]
        row = plant_metrics(_events(spark, specs), _reference(spark), SETTINGS).collect()[0]

        assert row.expected_power_kw is None
        assert row.performance_pct is None
        assert row.estimated_loss_kw is None
        # Current power is still reported: zero output is a fact.
        assert row.current_power_kw == pytest.approx(0.0)

    def test_performance_above_100_is_reported_not_clamped(self, spark):
        """The proxy is conservative; hiding >100% would hide model error."""
        specs = [dict(event_id="over", inverter_id="INV_01", active_power_kw=600.0, irradiance_wm2=500.0)]
        row = plant_metrics(_events(spark, specs), _reference(spark), SETTINGS).collect()[0]
        assert row.performance_pct == pytest.approx(120.0)


class TestEstimatedLoss:
    def test_loss_is_expected_minus_actual(self, spark):
        row = plant_metrics(
            _events(spark, TWO_INVERTER_BATCH), _reference(spark), SETTINGS
        ).collect()[0]
        assert row.estimated_loss_kw == pytest.approx(50.0)

    def test_loss_never_goes_negative(self, spark):
        specs = [dict(event_id="over", inverter_id="INV_01", active_power_kw=600.0, irradiance_wm2=500.0)]
        row = plant_metrics(_events(spark, specs), _reference(spark), SETTINGS).collect()[0]
        assert row.estimated_loss_kw == pytest.approx(0.0)


class TestAvailability:
    def test_all_configured_inverters_online_is_100_percent(self, spark):
        row = plant_metrics(
            _events(spark, TWO_INVERTER_BATCH), _reference(spark), SETTINGS
        ).collect()[0]

        assert row.availability_pct == pytest.approx(100.0)
        assert row.online_inverters == 2
        assert row.offline_inverters == 0

    def test_offline_inverter_halves_availability(self, spark):
        specs = [
            dict(event_id="on", inverter_id="INV_01", active_power_kw=200.0, irradiance_wm2=500.0),
            dict(
                event_id="off",
                inverter_id="INV_02",
                active_power_kw=0.0,
                availability=0.0,
                status="OFFLINE",
                irradiance_wm2=500.0,
            ),
        ]
        row = plant_metrics(_events(spark, specs), _reference(spark), SETTINGS).collect()[0]

        assert row.online_inverters == 1
        assert row.offline_inverters == 1
        assert row.availability_pct == pytest.approx(50.0)

    def test_a_silent_inverter_counts_as_unavailable(self, spark):
        """A telemetry gap must show as lost availability, not be ignored.

        Only one of two configured inverters reported. Dividing by the reporting
        count would flatter the plant at 100%; dividing by the configured count
        surfaces the gap.
        """
        specs = [dict(event_id="only", inverter_id="INV_01", active_power_kw=200.0, irradiance_wm2=500.0)]
        row = plant_metrics(_events(spark, specs), _reference(spark), SETTINGS).collect()[0]

        assert row.online_inverters == 1
        assert row.offline_inverters == 1
        assert row.availability_pct == pytest.approx(50.0)

    def test_warning_status_still_counts_as_online(self, spark):
        """A degraded inverter is still generating; that is an alert, not downtime."""
        specs = [
            dict(event_id="w", inverter_id="INV_01", status="WARNING", active_power_kw=90.0, irradiance_wm2=500.0),
            dict(event_id="o", inverter_id="INV_02", active_power_kw=200.0, irradiance_wm2=500.0),
        ]
        row = plant_metrics(_events(spark, specs), _reference(spark), SETTINGS).collect()[0]
        assert row.availability_pct == pytest.approx(100.0)


class TestWindowBounds:
    def test_window_spans_the_batch_event_times(self, spark):
        from tests.processing._events import utc

        metrics = plant_metrics(_events(spark, TWO_INVERTER_BATCH), _reference(spark), SETTINGS)
        row = metrics.select(utc("window_start"), utc("window_end")).collect()[0]

        assert row.window_start == "2026-08-21 05:00:00"
        assert row.window_end == "2026-08-21 05:00:03"


class TestMultiplePlants:
    def test_each_plant_gets_its_own_row(self, spark):
        specs = [
            dict(event_id="p1", plant_id="PLANT_01", inverter_id="INV_01", active_power_kw=400.0, irradiance_wm2=500.0),
            dict(event_id="p2", plant_id="PLANT_02", inverter_id="INV_01", active_power_kw=100.0, irradiance_wm2=500.0),
        ]
        reference = _reference(spark, (("PLANT_01", 1000.0, 1), ("PLANT_02", 400.0, 1)))
        rows = {r.plant_id: r for r in plant_metrics(_events(spark, specs), reference, SETTINGS).collect()}

        assert rows["PLANT_01"].current_power_kw == pytest.approx(400.0)
        assert rows["PLANT_01"].expected_power_kw == pytest.approx(500.0)
        # Capacity differs, so the same irradiance implies a different expectation.
        assert rows["PLANT_02"].expected_power_kw == pytest.approx(200.0)
        assert rows["PLANT_02"].performance_pct == pytest.approx(50.0)
