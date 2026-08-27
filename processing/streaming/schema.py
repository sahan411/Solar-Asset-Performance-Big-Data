"""Spark schema for the frozen telemetry contract.

This mirrors the streaming telemetry event defined in the master specification
(section 9.1). It is the team's shared interface: changing a field name or type
here without agreeing a contract version breaks Member 1's producer and
Member 3's serving layer at the same time.

Design note — `timestamp` is read as a STRING rather than a TimestampType.
Spark's `from_json` is all-or-nothing per record: had the timestamp been typed,
one malformed date would null out the entire event and we would lose the plant
id, the power reading and any chance of reporting *why* it failed. Reading it as
text and converting explicitly downstream lets a bad timestamp be quarantined
with a precise reason while the rest of the record stays inspectable.
"""

from __future__ import annotations

from pyspark.sql.types import DoubleType, StringType, StructField, StructType

# Allowed values for the `status` enum in the frozen contract.
STATUS_ONLINE = "ONLINE"
STATUS_OFFLINE = "OFFLINE"
STATUS_WARNING = "WARNING"
ALLOWED_STATUSES = (STATUS_ONLINE, STATUS_OFFLINE, STATUS_WARNING)

# Fields that must be present and non-null for an event to be usable at all.
REQUIRED_FIELDS = ("event_id", "plant_id", "inverter_id", "timestamp")

TELEMETRY_JSON_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=True),
        StructField("plant_id", StringType(), nullable=True),
        StructField("inverter_id", StringType(), nullable=True),
        StructField("active_power_kw", DoubleType(), nullable=True),
        StructField("energy_today_kwh", DoubleType(), nullable=True),
        StructField("irradiance_wm2", DoubleType(), nullable=True),
        StructField("module_temp_c", DoubleType(), nullable=True),
        StructField("inverter_temp_c", DoubleType(), nullable=True),
        StructField("status", StringType(), nullable=True),
        StructField("availability", DoubleType(), nullable=True),
        StructField("timestamp", StringType(), nullable=True),
        StructField("simulator_scenario", StringType(), nullable=True),
    ]
)
# Every field is declared nullable on purpose: nullable=False would make Spark
# assume the field is always present without enforcing it, which hides exactly
# the malformed records this pipeline is supposed to catch. Presence is enforced
# explicitly in processing.streaming.validation instead.

# Payload columns, in contract order, as they appear after flattening.
TELEMETRY_COLUMNS = tuple(field.name for field in TELEMETRY_JSON_SCHEMA.fields)

# Spark's `from_json` does NOT return a NULL struct for malformed JSON — it
# returns a struct with every field set to null, which is indistinguishable from
# a well-formed but empty event. The only reliable way to tell the two apart is
# PERMISSIVE mode's corrupt-record column, which captures the original text when
# (and only when) the parser failed.
CORRUPT_RECORD_COLUMN = "_corrupt_record"

TELEMETRY_PARSE_SCHEMA = StructType(
    [*TELEMETRY_JSON_SCHEMA.fields, StructField(CORRUPT_RECORD_COLUMN, StringType(), nullable=True)]
)

# Options that activate the corrupt-record column above.
TELEMETRY_PARSE_OPTIONS = {
    "mode": "PERMISSIVE",
    "columnNameOfCorruptRecord": CORRUPT_RECORD_COLUMN,
}

# Kafka metadata carried alongside each event. Kept because "which partition and
# offset did this bad record come from" is the first question when debugging a
# stream, and because the offset makes a record uniquely traceable in the archive.
KAFKA_METADATA_COLUMNS = (
    "kafka_key",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
)
