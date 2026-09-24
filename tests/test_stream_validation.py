"""Speed layer: Spark parsing, validation and windowed aggregation.

Runs the real transformation functions from spark/stream_job.py on a small
static DataFrame shaped like the Kafka source (value, partition, offset, headers).
Times are formatted inside Spark (session time zone UTC), because collecting a
timestamp into Python converts it to the local time zone of the machine.
Needs pyspark and Java; skipped if pyspark is not installed.
"""

import json
import os
import sys

import pytest

pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql import types as T  # noqa: E402

from spark.stream_job import parse_readings, zone_load_windows  # noqa: E402

KAFKA_SCHEMA = T.StructType([
    T.StructField("value", T.BinaryType()),
    T.StructField("partition", T.IntegerType()),
    T.StructField("offset", T.LongType()),
    T.StructField("headers", T.ArrayType(T.StructType([
        T.StructField("key", T.StringType()),
        T.StructField("value", T.BinaryType()),
    ]))),
])


@pytest.fixture(scope="module")
def spark():
    # Spark workers must use this same Python interpreter.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    session = (SparkSession.builder.master("local[1]").appName("tests")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.shuffle.partitions", "1")
               .getOrCreate())
    yield session
    session.stop()


def reading(**overrides):
    r = {"meter_id": "M001", "household_id": "H001", "grid_zone": "north",
         "power_consumption_kwh": 0.5, "solar_generation_kwh": 0.2,
         "timestamp": "2026-01-01T10:00:00Z"}
    r.update(overrides)
    return r


def kafka_df(spark, values):
    rows = []
    for i, (trace_id, value) in enumerate(values):
        raw = value if isinstance(value, bytes) else json.dumps(value).encode()
        headers = [("trace_id", trace_id.encode()), ("produced_at", b"1700000000000")]
        rows.append((bytearray(raw), 0, i, headers))
    return spark.createDataFrame(rows, KAFKA_SCHEMA)


def test_each_bad_record_gets_the_right_reject_reason(spark):
    df = parse_readings(kafka_df(spark, [
        ("ok", reading()),
        ("neg", reading(power_consumption_kwh=-0.3)),
        ("neg-solar", reading(solar_generation_kwh=-1.0)),
        ("no-house", reading(household_id=None)),
        ("zone", reading(grid_zone="zone-x")),
        ("broken", b'{"meter_id": "M001", "power_'),
    ]))
    reasons = {r.trace_id: r.reject_reason for r in df.collect()}
    assert reasons == {
        "ok": None,
        "neg": "invalid_consumption",
        "neg-solar": "invalid_solar_generation",
        "no-house": "missing_household_id",
        "zone": "unknown_grid_zone",
        "broken": "malformed_or_missing_fields",
    }


def test_trace_headers_and_event_time_are_extracted(spark):
    row = (parse_readings(kafka_df(spark, [("abc-123", reading())]))
           .withColumn("event_time_utc", F.date_format("event_time", "yyyy-MM-dd HH:mm"))
           .collect()[0])
    assert row.trace_id == "abc-123"
    assert row.produced_at_ms == 1700000000000
    assert row.event_time_utc == "2026-01-01 10:00"
    assert (row.kafka_partition, row.kafka_offset) == (0, 0)


def test_hourly_zone_window_sums_and_renewable_pct(spark):
    readings = parse_readings(kafka_df(spark, [
        ("a", reading(power_consumption_kwh=1.0, solar_generation_kwh=0.25)),
        ("b", reading(meter_id="M004", household_id="H004", timestamp="2026-01-01T10:45:00Z",
                      power_consumption_kwh=1.0, solar_generation_kwh=0.25)),
        ("c", reading(timestamp="2026-01-01T11:00:00Z")),      # next hour
        ("bad", reading(power_consumption_kwh=-5.0)),           # must be excluded
    ]))
    windows = {(w.grid_zone, int(w.hour)): w for w in zone_load_windows(readings)
               .withColumn("hour", F.date_format("window_start", "HH")).collect()}
    w10 = windows[("north", 10)]
    assert w10.readings == 2
    assert w10.consumption_kwh == pytest.approx(2.0)
    assert w10.solar_kwh == pytest.approx(0.5)
    assert w10.net_grid_load_kwh == pytest.approx(1.5)
    assert w10.renewable_pct == pytest.approx(25.0)
    assert windows[("north", 11)].readings == 1


def test_speed_and_batch_layers_reject_the_same_records(spark):
    """Lambda keeps two implementations of the rules; they must agree."""
    from common.validation import reject_reason

    cases = [
        reading(),
        reading(power_consumption_kwh=-0.3),
        reading(solar_generation_kwh="abc"),
        reading(household_id=None),
        reading(grid_zone="zone-x"),
        reading(timestamp="not a time"),
        reading(meter_id=None),
        b'{"meter_id": "M001", "power_',
    ]
    values = [(str(i), c) for i, c in enumerate(cases)]
    spark_reasons = {r.trace_id: r.reject_reason for r in parse_readings(kafka_df(spark, values)).collect()}
    for trace_id, case in values:
        parsed = None if isinstance(case, bytes) else case
        assert reject_reason(parsed) == spark_reasons[trace_id], case
