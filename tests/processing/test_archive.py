"""Raw Parquet archive shape and partitioning (Milestone 4).

The archive is the batch layer's only input, so its column contract and its
partition layout are load-bearing: the daily reconciliation reads exactly these
names from exactly this directory structure.
"""

from __future__ import annotations

import pytest

from processing.streaming.archive import (
    ARCHIVE_COLUMNS,
    PARTITION_COLUMNS,
    prepare_archive_frame,
    with_simulation_date,
)
from processing.streaming.transforms import parse_telemetry
from processing.streaming.validation import (
    normalize_valid_events,
    valid_events,
    validate_telemetry,
)
from tests.processing._events import kafka_frame, telemetry_event
from tests.conftest import requires_hadoop_writes

pytestmark = pytest.mark.spark


def _normalized(spark, payloads):
    return normalize_valid_events(
        valid_events(validate_telemetry(parse_telemetry(kafka_frame(spark, payloads))))
    )


def test_simulation_date_comes_from_event_time_not_processing_time(spark):
    """Under a compressed clock, wall-clock time says nothing about the day."""
    frame = _normalized(spark, [telemetry_event(timestamp="2026-08-21T23:45:00Z")])
    row = with_simulation_date(frame).collect()[0]

    assert str(row.simulation_date) == "2026-08-21"


def test_events_late_in_the_utc_day_stay_in_that_day(spark):
    """A 23:59Z reading belongs to the 21st, not the 22nd."""
    payloads = [
        telemetry_event(event_id="a", timestamp="2026-08-21T23:59:59Z"),
        telemetry_event(event_id="b", timestamp="2026-08-22T00:00:01Z"),
    ]
    rows = {r.event_id: str(r.simulation_date) for r in with_simulation_date(_normalized(spark, payloads)).collect()}

    assert rows["a"] == "2026-08-21"
    assert rows["b"] == "2026-08-22"


def test_archive_frame_matches_the_declared_column_contract(spark):
    frame = prepare_archive_frame(_normalized(spark, [telemetry_event()]))
    assert tuple(frame.columns) == ARCHIVE_COLUMNS


def test_archive_excludes_bulky_and_internal_columns(spark):
    frame = prepare_archive_frame(_normalized(spark, [telemetry_event()]))

    # Normalized telemetry only: the original JSON would roughly double the
    # archive for data already proven valid.
    assert "raw_payload" not in frame.columns
    assert "payload_parsed" not in frame.columns
    assert "event_timestamp_raw" not in frame.columns
    # Lineage back to the source record is kept, though.
    assert "kafka_offset" in frame.columns


def test_archive_preserves_measurements_and_scenario_labels(spark):
    """The batch layer recomputes generation from these, so they must survive."""
    event = telemetry_event(
        active_power_kw=412.5,
        energy_today_kwh=1875.25,
        simulator_scenario="INV_UNDERPERFORMANCE",
    )
    row = prepare_archive_frame(_normalized(spark, [event])).collect()[0]

    assert row.active_power_kw == pytest.approx(412.5)
    assert row.energy_today_kwh == pytest.approx(1875.25)
    assert row.status == "ONLINE"
    assert row.availability == 1.0
    # Scenario labels make the demo's injected anomalies traceable in the archive.
    assert row.simulator_scenario == "INV_UNDERPERFORMANCE"


def test_partition_columns_are_present_and_ordered(spark):
    assert PARTITION_COLUMNS == ("simulation_date", "plant_id")
    frame = prepare_archive_frame(_normalized(spark, [telemetry_event()]))
    for column in PARTITION_COLUMNS:
        assert column in frame.columns


@requires_hadoop_writes
def test_round_trip_through_partitioned_parquet(spark, tmp_path):
    """Write and read back to prove the layout and types survive Parquet."""
    payloads = [
        telemetry_event(event_id="p1a", plant_id="PLANT_01"),
        telemetry_event(event_id="p1b", plant_id="PLANT_01", active_power_kw=100.0),
        telemetry_event(event_id="p2a", plant_id="PLANT_02", active_power_kw=250.0),
    ]
    archive_path = str(tmp_path / "telemetry")

    prepare_archive_frame(_normalized(spark, payloads)).write.partitionBy(
        *PARTITION_COLUMNS
    ).parquet(archive_path)

    # Partition directories are what the batch layer prunes on.
    date_dirs = [p.name for p in (tmp_path / "telemetry").iterdir() if p.is_dir()]
    assert date_dirs == ["simulation_date=2026-08-21"]
    plant_dirs = sorted(
        p.name for p in (tmp_path / "telemetry" / "simulation_date=2026-08-21").iterdir() if p.is_dir()
    )
    assert plant_dirs == ["plant_id=PLANT_01", "plant_id=PLANT_02"]

    reloaded = spark.read.parquet(archive_path)
    assert reloaded.count() == 3
    assert {r.event_id for r in reloaded.collect()} == {"p1a", "p1b", "p2a"}

    # Reading a single day/plant yields only that partition.
    one_plant = spark.read.parquet(archive_path).filter("plant_id = 'PLANT_02'")
    assert one_plant.count() == 1
    assert one_plant.collect()[0].active_power_kw == pytest.approx(250.0)


@requires_hadoop_writes
def test_appending_a_second_batch_does_not_disturb_the_first(spark, tmp_path):
    """The archive is append-only; a later microbatch must not rewrite history."""
    archive_path = str(tmp_path / "telemetry")

    prepare_archive_frame(_normalized(spark, [telemetry_event(event_id="first")])).write.partitionBy(
        *PARTITION_COLUMNS
    ).parquet(archive_path)

    prepare_archive_frame(
        _normalized(spark, [telemetry_event(event_id="second")])
    ).write.mode("append").partitionBy(*PARTITION_COLUMNS).parquet(archive_path)

    reloaded = spark.read.parquet(archive_path)
    assert {r.event_id for r in reloaded.collect()} == {"first", "second"}
