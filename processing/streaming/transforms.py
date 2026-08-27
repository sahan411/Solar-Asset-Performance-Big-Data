"""Pure DataFrame transformations for the SolarIQ speed layer.

Everything here is an ordinary DataFrame -> DataFrame function with no streaming
or I/O dependency, so each step can be tested against a small batch DataFrame in
local Spark. The streaming job is then just these functions wired together.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json, to_timestamp

from processing.streaming.schema import (
    CORRUPT_RECORD_COLUMN,
    TELEMETRY_COLUMNS,
    TELEMETRY_PARSE_OPTIONS,
    TELEMETRY_PARSE_SCHEMA,
)

# Raw payload text is carried through the pipeline so a rejected record can be
# quarantined with its original bytes, and so an operator can see exactly what
# arrived rather than a partially-parsed reconstruction.
RAW_PAYLOAD_COLUMN = "raw_payload"

# Set when the JSON itself could not be decoded at all.
UNPARSEABLE_REASON = "UNPARSEABLE_JSON"


def parse_telemetry(raw: DataFrame) -> DataFrame:
    """Decode Kafka records into typed telemetry columns.

    Input is Kafka's native shape (key, value, topic, partition, offset,
    timestamp). Output carries every contract field, the Kafka metadata, the
    original payload text, a typed `event_time`, and `payload_parsed` marking
    whether the JSON decoded at all.

    Nothing is dropped here. Deciding what is valid is the validation step's job;
    conflating the two would silently discard records with no audit trail.
    """
    decoded = raw.select(
        col("key").cast("string").alias("kafka_key"),
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
        col("value").cast("string").alias(RAW_PAYLOAD_COLUMN),
    ).withColumn(
        "payload",
        from_json(col(RAW_PAYLOAD_COLUMN), TELEMETRY_PARSE_SCHEMA, TELEMETRY_PARSE_OPTIONS),
    )

    flattened = decoded.select(
        # Contract fields, lifted out of the parsed struct.
        *[col(f"payload.{name}").alias(name) for name in TELEMETRY_COLUMNS],
        "kafka_key",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        RAW_PAYLOAD_COLUMN,
        # Malformed JSON produces an all-null struct rather than a null struct,
        # so the corrupt-record column is the only trustworthy signal. A null
        # Kafka value is not "corrupt" by that measure but is equally unusable,
        # hence the explicit payload check.
        (
            col(RAW_PAYLOAD_COLUMN).isNotNull()
            & col(f"payload.{CORRUPT_RECORD_COLUMN}").isNull()
        ).alias("payload_parsed"),
    )

    return (
        flattened
        # Keep the original text so an unparseable timestamp can be reported with
        # the offending value rather than just "it was null".
        .withColumnRenamed("timestamp", "event_timestamp_raw")
        # NULL when the string is not a valid instant; validation turns that into
        # a rejection reason.
        .withColumn("event_time", to_timestamp(col("event_timestamp_raw")))
    )
