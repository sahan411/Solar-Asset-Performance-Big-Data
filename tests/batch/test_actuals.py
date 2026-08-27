"""Daily actual generation from the raw archive (Milestone 11).

The headline case is the cumulative-counter handling: a mid-day reset must not
silently understate the day's generation, and therefore its revenue.
"""

from __future__ import annotations

import pytest

from processing.batch.actuals import (
    PLANT_ACTUAL_COLUMNS,
    collect_plant_actuals,
    inverter_daily_actuals,
    inverter_energy_increments,
    plant_daily_actuals,
)
from tests.processing._events import utc_ts

pytestmark = pytest.mark.spark

ARCHIVE_SCHEMA = (
    "plant_id string, inverter_id string, energy_today_kwh double, "
    "active_power_kw double, status string, event_time timestamp"
)

INTERVAL_SECONDS = 3.0


def _archive(spark, rows):
    """rows: (plant, inverter, energy_kwh, status, hour)."""
    return spark.createDataFrame(
        [
            (plant, inverter, float(energy), 100.0, status, utc_ts(2026, 8, 21, hour, 0, 0))
            for plant, inverter, energy, status, hour in rows
        ],
        schema=ARCHIVE_SCHEMA,
    )


def _plant(spark, rows):
    return {
        r["plant_id"]: r
        for r in plant_daily_actuals(_archive(spark, rows), INTERVAL_SECONDS).collect()
    }


class TestEnergyFromACumulativeCounter:
    def test_a_clean_day_totals_the_final_reading(self, spark):
        """Counter starts at zero and rises: the day's energy is the last value."""
        rows = [
            ("PLANT_01", "INV_01", 0, "ONLINE", 5),
            ("PLANT_01", "INV_01", 1200, "ONLINE", 10),
            ("PLANT_01", "INV_01", 3400, "ONLINE", 15),
        ]
        assert _plant(spark, rows)["PLANT_01"].actual_generation_kwh == pytest.approx(3400.0)

    def test_a_mid_day_counter_reset_is_credited_not_lost(self, spark):
        """The reason increments are summed rather than taking max().

        The counter reaches 1000, resets (simulator restart), then climbs to 600.
        The plant genuinely produced 1600 kWh. Taking max() would report 1000 and
        understate the day's revenue by 37%.
        """
        rows = [
            ("PLANT_01", "INV_01", 400, "ONLINE", 5),
            ("PLANT_01", "INV_01", 1000, "ONLINE", 9),
            ("PLANT_01", "INV_01", 200, "ONLINE", 12),   # reset happened
            ("PLANT_01", "INV_01", 600, "ONLINE", 16),
        ]
        assert _plant(spark, rows)["PLANT_01"].actual_generation_kwh == pytest.approx(1600.0)

    def test_energy_already_on_the_counter_at_first_observation_is_included(self, spark):
        """Archive starts mid-morning; the counter already reads 500."""
        rows = [
            ("PLANT_01", "INV_01", 500, "ONLINE", 9),
            ("PLANT_01", "INV_01", 2000, "ONLINE", 15),
        ]
        # max - min would report 1500 and lose the first 500.
        assert _plant(spark, rows)["PLANT_01"].actual_generation_kwh == pytest.approx(2000.0)

    def test_a_flat_counter_contributes_nothing_further(self, spark):
        rows = [
            ("PLANT_01", "INV_01", 800, "ONLINE", 10),
            ("PLANT_01", "INV_01", 800, "ONLINE", 11),
            ("PLANT_01", "INV_01", 800, "ONLINE", 12),
        ]
        assert _plant(spark, rows)["PLANT_01"].actual_generation_kwh == pytest.approx(800.0)

    def test_increments_are_computed_per_inverter_not_across_them(self, spark):
        """Two inverters interleaved in time must not contaminate each other."""
        rows = [
            ("PLANT_01", "INV_01", 100, "ONLINE", 8),
            ("PLANT_01", "INV_02", 900, "ONLINE", 9),
            ("PLANT_01", "INV_01", 300, "ONLINE", 10),
            ("PLANT_01", "INV_02", 1500, "ONLINE", 11),
        ]
        # INV_01 contributes 300, INV_02 contributes 1500.
        assert _plant(spark, rows)["PLANT_01"].actual_generation_kwh == pytest.approx(1800.0)

    def test_increments_are_isolated_between_plants(self, spark):
        rows = [
            ("PLANT_01", "INV_01", 1000, "ONLINE", 10),
            ("PLANT_02", "INV_01", 400, "ONLINE", 10),
        ]
        plants = _plant(spark, rows)
        assert plants["PLANT_01"].actual_generation_kwh == pytest.approx(1000.0)
        assert plants["PLANT_02"].actual_generation_kwh == pytest.approx(400.0)

    def test_increment_column_is_exposed_for_inspection(self, spark):
        rows = [
            ("PLANT_01", "INV_01", 100, "ONLINE", 8),
            ("PLANT_01", "INV_01", 250, "ONLINE", 9),
        ]
        increments = sorted(
            r.energy_increment_kwh
            for r in inverter_energy_increments(_archive(spark, rows)).collect()
        )
        assert increments == pytest.approx([100.0, 150.0])


class TestPlantAggregation:
    def test_plant_energy_sums_its_inverters(self, spark):
        rows = [
            ("PLANT_01", "INV_01", 1000, "ONLINE", 12),
            ("PLANT_01", "INV_02", 1100, "ONLINE", 12),
            ("PLANT_01", "INV_03", 900, "ONLINE", 12),
        ]
        result = _plant(spark, rows)["PLANT_01"]
        assert result.actual_generation_kwh == pytest.approx(3000.0)
        assert result.reporting_inverters == 3

    def test_per_inverter_totals_are_available(self, spark):
        rows = [
            ("PLANT_01", "INV_01", 1000, "ONLINE", 12),
            ("PLANT_01", "INV_02", 1100, "ONLINE", 12),
        ]
        totals = {
            r["inverter_id"]: r["actual_generation_kwh"]
            for r in inverter_daily_actuals(_archive(spark, rows)).collect()
        }
        assert totals == {"INV_01": pytest.approx(1000.0), "INV_02": pytest.approx(1100.0)}

    def test_output_matches_the_declared_column_contract(self, spark):
        rows = [("PLANT_01", "INV_01", 100, "ONLINE", 10)]
        frame = plant_daily_actuals(_archive(spark, rows), INTERVAL_SECONDS)
        assert tuple(frame.columns) == PLANT_ACTUAL_COLUMNS


class TestAvailabilityAndDowntime:
    def test_all_online_is_full_availability_and_no_downtime(self, spark):
        rows = [
            ("PLANT_01", "INV_01", 100, "ONLINE", 8),
            ("PLANT_01", "INV_01", 200, "ONLINE", 9),
        ]
        result = _plant(spark, rows)["PLANT_01"]
        assert result.availability_pct == pytest.approx(100.0)
        assert result.downtime_minutes == pytest.approx(0.0)

    def test_offline_observations_reduce_availability(self, spark):
        rows = [
            ("PLANT_01", "INV_01", 100, "ONLINE", 8),
            ("PLANT_01", "INV_01", 100, "OFFLINE", 9),
            ("PLANT_01", "INV_01", 100, "OFFLINE", 10),
            ("PLANT_01", "INV_01", 200, "ONLINE", 11),
        ]
        result = _plant(spark, rows)["PLANT_01"]
        assert result.availability_pct == pytest.approx(50.0)
        # Two offline samples at a 3-second interval.
        assert result.downtime_minutes == pytest.approx(6.0 / 60.0)

    def test_warning_status_is_not_counted_as_online_or_as_downtime(self, spark):
        """A degraded inverter is generating, but is not reporting healthy."""
        rows = [
            ("PLANT_01", "INV_01", 100, "ONLINE", 8),
            ("PLANT_01", "INV_01", 200, "WARNING", 9),
        ]
        result = _plant(spark, rows)["PLANT_01"]
        assert result.availability_pct == pytest.approx(50.0)
        assert result.downtime_minutes == pytest.approx(0.0)

    def test_downtime_scales_with_the_telemetry_interval(self, spark):
        rows = [("PLANT_01", "INV_01", 0, "OFFLINE", h) for h in range(20)]
        frame = plant_daily_actuals(_archive(spark, rows), telemetry_interval_seconds=60.0)
        # 20 offline samples at 60 seconds each.
        assert frame.collect()[0].downtime_minutes == pytest.approx(20.0)


class TestCollection:
    def test_collects_to_a_plain_mapping_keyed_by_plant(self, spark):
        rows = [
            ("PLANT_01", "INV_01", 1000, "ONLINE", 12),
            ("PLANT_02", "INV_01", 500, "OFFLINE", 12),
        ]
        collected = collect_plant_actuals(
            plant_daily_actuals(_archive(spark, rows), INTERVAL_SECONDS)
        )

        assert set(collected) == {"PLANT_01", "PLANT_02"}
        assert collected["PLANT_01"]["actual_generation_kwh"] == pytest.approx(1000.0)
        assert collected["PLANT_01"]["availability_pct"] == pytest.approx(100.0)
        assert collected["PLANT_02"]["availability_pct"] == pytest.approx(0.0)
        assert collected["PLANT_02"]["reporting_inverters"] == 1

    def test_an_empty_day_collects_to_nothing(self, spark):
        empty = spark.createDataFrame([], schema=ARCHIVE_SCHEMA)
        assert collect_plant_actuals(plant_daily_actuals(empty, INTERVAL_SECONDS)) == {}
