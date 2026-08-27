"""Boundary validation and quarantine for incoming telemetry.

Kafka is an untrusted boundary: the producer is a separate service owned by
another member, and a schema-valid message can still carry physically impossible
readings. Every event is therefore checked before it can influence a metric.

The policy is quarantine, never silent drop. A rejected event keeps its original
payload, its Kafka coordinates and a specific machine-readable reason, and is
republished to `solar.telemetry.invalid`. "The numbers looked wrong and we don't
know why" is the failure mode this is designed to prevent.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import coalesce, col, current_timestamp, lit, struct, to_json, when
from pyspark.sql.types import DoubleType

from processing.streaming.schema import ALLOWED_STATUSES

# Column added by `validate_telemetry`: NULL for a good event, otherwise the
# reason it was rejected.
REJECTION_REASON_COLUMN = "rejection_reason"

# Machine-readable rejection reasons. Stable strings — dashboards and the
# quarantine topic key off these, so treat them as part of the contract.
REASON_UNPARSEABLE_JSON = "UNPARSEABLE_JSON"
REASON_MISSING_EVENT_ID = "MISSING_EVENT_ID"
REASON_MISSING_PLANT_ID = "MISSING_PLANT_ID"
REASON_MISSING_INVERTER_ID = "MISSING_INVERTER_ID"
REASON_INVALID_TIMESTAMP = "INVALID_TIMESTAMP"
REASON_INVALID_STATUS = "INVALID_STATUS"
REASON_NEGATIVE_ACTIVE_POWER = "NEGATIVE_ACTIVE_POWER"
REASON_NEGATIVE_ENERGY = "NEGATIVE_ENERGY"
REASON_NEGATIVE_IRRADIANCE = "NEGATIVE_IRRADIANCE"
REASON_IRRADIANCE_OUT_OF_RANGE = "IRRADIANCE_OUT_OF_RANGE"
REASON_AVAILABILITY_OUT_OF_RANGE = "AVAILABILITY_OUT_OF_RANGE"

# Physical plausibility bound. Terrestrial global irradiance peaks near
# 1000 W/m^2 at standard test conditions; brief cloud-edge enhancement can push
# real sensors above that, so the bound is generous. Anything beyond it is a
# broken sensor or a unit error, not weather.
MAX_PLAUSIBLE_IRRADIANCE_WM2 = 1500.0


def validate_telemetry(df: DataFrame) -> DataFrame:
    """Attach a `rejection_reason` column. NULL means the event is usable.

    Checks are ordered from most fundamental to most specific and only the first
    failure is reported, so a corrupt record yields UNPARSEABLE_JSON rather than
    a cascade of "missing field" noise. `coalesce` over `when` gives first-match
    semantics because an unmatched `when` evaluates to NULL.
    """
    reason = coalesce(
        # Structural failures first — nothing else can be trusted.
        when(~col("payload_parsed"), lit(REASON_UNPARSEABLE_JSON)),
        # Identity: without these an event cannot be attributed to an asset or
        # de-duplicated, so it is unusable no matter how good the readings are.
        when(col("event_id").isNull(), lit(REASON_MISSING_EVENT_ID)),
        when(col("plant_id").isNull(), lit(REASON_MISSING_PLANT_ID)),
        when(col("inverter_id").isNull(), lit(REASON_MISSING_INVERTER_ID)),
        # Event time drives every window; an unparseable instant cannot be placed.
        when(col("event_time").isNull(), lit(REASON_INVALID_TIMESTAMP)),
        # Enum outside the frozen contract.
        when(
            col("status").isNull() | ~col("status").isin(list(ALLOWED_STATUSES)),
            lit(REASON_INVALID_STATUS),
        ),
        # Physically impossible measurements. A negative reading is a fault or a
        # sign error; letting it through would understate portfolio output and
        # corrupt the loss/revenue figures the whole platform exists to produce.
        when(col("active_power_kw") < 0, lit(REASON_NEGATIVE_ACTIVE_POWER)),
        when(col("energy_today_kwh") < 0, lit(REASON_NEGATIVE_ENERGY)),
        when(col("irradiance_wm2") < 0, lit(REASON_NEGATIVE_IRRADIANCE)),
        when(
            col("irradiance_wm2") > MAX_PLAUSIBLE_IRRADIANCE_WM2,
            lit(REASON_IRRADIANCE_OUT_OF_RANGE),
        ),
        # Availability is a 0-or-1 flag at event level in the contract; anything
        # outside [0, 1] would skew the availability percentage.
        when(
            (col("availability") < 0) | (col("availability") > 1),
            lit(REASON_AVAILABILITY_OUT_OF_RANGE),
        ),
    )
    return df.withColumn(REJECTION_REASON_COLUMN, reason)


def valid_events(df: DataFrame) -> DataFrame:
    """Events that passed validation, with the reason column dropped."""
    return df.filter(col(REJECTION_REASON_COLUMN).isNull()).drop(REJECTION_REASON_COLUMN)


def invalid_events(df: DataFrame) -> DataFrame:
    """Events that failed validation, keeping the reason."""
    return df.filter(col(REJECTION_REASON_COLUMN).isNotNull())


def normalize_valid_events(df: DataFrame) -> DataFrame:
    """Apply type/shape normalisation that only makes sense on trusted events.

    Kept separate from validation: validation decides *whether* to accept an
    event, normalisation makes an accepted event uniform. Missing optional
    numerics become explicit zeros/defaults here so downstream aggregations do not
    have to defend against nulls in arithmetic.
    """
    return (
        df
        # A missing power reading from an accepted event means "no output
        # observed", which is 0 kW — not "unknown", which would silently drop the
        # inverter out of a SUM and overstate plant performance.
        .withColumn("active_power_kw", coalesce(col("active_power_kw"), lit(0.0)))
        .withColumn("energy_today_kwh", coalesce(col("energy_today_kwh"), lit(0.0)))
        .withColumn("irradiance_wm2", coalesce(col("irradiance_wm2"), lit(0.0)))
        # Availability defaults from status rather than to a constant: an OFFLINE
        # inverter that omitted the field is unambiguously unavailable.
        .withColumn(
            "availability",
            coalesce(
                col("availability"),
                when(col("status") == lit("OFFLINE"), lit(0.0)).otherwise(lit(1.0)),
            ).cast(DoubleType()),
        )
    )


def deduplicate_events(df: DataFrame, watermark: str) -> DataFrame:
    """Drop repeated `event_id`s within the watermark window.

    The producer retries on delivery failure, so at-least-once delivery is
    expected and duplicates are normal. Without this, one retried event would be
    counted twice in a window average.

    A watermark bounds the state store: Spark can forget an event_id once the
    watermark passes it, otherwise the deduplication set would grow forever.
    Events later than the watermark are dropped as too-late rather than
    reprocessed.
    """
    watermarked = df.withWatermark("event_time", watermark)

    if df.isStreaming:
        # Spark 3.5's purpose-built operator: de-duplicates on a business key
        # without forcing the event-time column into the key, which would defeat
        # the point when a duplicate carries a slightly different timestamp.
        return watermarked.dropDuplicatesWithinWatermark(["event_id"])

    # Batch DataFrames (unit tests) have no watermark semantics; plain
    # dropDuplicates is equivalent over a bounded set.
    return watermarked.dropDuplicates(["event_id"])


def to_quarantine_records(df: DataFrame) -> DataFrame:
    """Shape rejected events for publication to `solar.telemetry.invalid`.

    Produces Kafka's expected key/value columns. The record carries enough
    context to diagnose the fault without going back to the source topic: the
    reason, the asset (when known), the exact Kafka coordinates, and the original
    payload bytes as text.
    """
    return df.select(
        # Keyed by asset where identifiable so related failures land on the same
        # partition; unattributable records get a stable placeholder rather than
        # a null key, which would round-robin them across partitions.
        coalesce(
            col("kafka_key"),
            lit("unattributed"),
        ).alias("key"),
        to_json(
            struct(
                col(REJECTION_REASON_COLUMN).alias("rejection_reason"),
                current_timestamp().alias("rejected_at"),
                col("event_id"),
                col("plant_id"),
                col("inverter_id"),
                col("event_timestamp_raw"),
                col("kafka_topic").alias("source_topic"),
                col("kafka_partition").alias("source_partition"),
                col("kafka_offset").alias("source_offset"),
                col("raw_payload"),
            ),
            # Spark omits null fields from JSON by default, which would give the
            # quarantine topic a shape that changes with the failure mode. An
            # explicit null is information ("we could not determine the plant"),
            # so consumers get a stable set of keys either way.
            {"ignoreNullFields": "false"},
        ).alias("value"),
    )
