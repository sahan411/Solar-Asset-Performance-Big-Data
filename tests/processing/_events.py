"""Builders for telemetry test data.

Shared by the parsing, validation and aggregation tests so every one of them
starts from the same frozen contract shape, and a contract change breaks in one
place instead of a dozen.
"""

from __future__ import annotations

import json
from typing import Any

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.functions import col, date_format
from pyspark.sql.types import (
    BinaryType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Mirrors the Kafka source's DataFrame shape so tests exercise the same columns
# the real stream produces.
KAFKA_SOURCE_SCHEMA = StructType(
    [
        StructField("key", BinaryType(), True),
        StructField("value", BinaryType(), True),
        StructField("topic", StringType(), True),
        StructField("partition", IntegerType(), True),
        StructField("offset", LongType(), True),
        StructField("timestamp", TimestampType(), True),
    ]
)

# A complete, valid event exactly as specified in the frozen contract.
VALID_EVENT: dict[str, Any] = {
    "event_id": "11111111-1111-4111-8111-111111111111",
    "plant_id": "PLANT_01",
    "inverter_id": "INV_01",
    "active_power_kw": 422.7,
    "energy_today_kwh": 2150.2,
    "irradiance_wm2": 782.4,
    "module_temp_c": 47.3,
    "inverter_temp_c": 51.0,
    "status": "ONLINE",
    "availability": 1.0,
    "timestamp": "2026-08-21T05:00:00Z",
    "simulator_scenario": None,
}


def utc(column: str, alias: str | None = None) -> Column:
    """Format a timestamp column as a UTC string for assertions.

    PySpark converts TimestampType to a *timezone-naive local* datetime when
    collecting to Python, so `row.event_time` reads differently depending on the
    host's timezone (an instant of 05:00Z arrives as 10:30 on a UTC+5:30
    machine). Formatting inside Spark honours spark.sql.session.timeZone=UTC and
    is therefore deterministic everywhere.
    """
    return date_format(col(column), "yyyy-MM-dd HH:mm:ss").alias(alias or column)


def telemetry_event(**overrides: Any) -> dict[str, Any]:
    """A valid contract event with selected fields replaced.

    Pass `field=None` to null a value, or use `drop=[...]` semantics by popping
    from the returned dict.
    """
    event = dict(VALID_EVENT)
    event.update(overrides)
    return event


def kafka_frame(
    spark: SparkSession,
    payloads: list[str | dict[str, Any] | None],
    topic: str = "solar.telemetry.raw",
    partition: int = 0,
) -> DataFrame:
    """Build a DataFrame shaped like the Kafka source from raw payloads.

    Accepts dicts (encoded as JSON), raw strings (for malformed-input tests) and
    None (a tombstone-style null value).
    """
    from datetime import datetime, timezone

    rows = []
    ingest_time = datetime(2026, 8, 21, 5, 0, 0, tzinfo=timezone.utc)

    for offset, payload in enumerate(payloads):
        if payload is None:
            value = None
            key = None
        else:
            text = payload if isinstance(payload, str) else json.dumps(payload)
            value = text.encode("utf-8")
            # Key follows the contract: plant_id:inverter_id.
            if isinstance(payload, dict):
                key = f"{payload.get('plant_id')}:{payload.get('inverter_id')}".encode("utf-8")
            else:
                key = b"unknown:unknown"

        rows.append((key, value, topic, partition, offset, ingest_time))

    return spark.createDataFrame(rows, schema=KAFKA_SOURCE_SCHEMA)
