"""Immutable raw telemetry archive — the Lambda architecture's batch source.

Normalized, validated events are appended to Parquet in MinIO. This is what makes
the batch layer genuinely a *batch layer* rather than a nightly re-read of the
live tables: the end-of-day reconciliation recomputes from complete, immutable
history, so a change to the performance model can be replayed over past days and
a late-arriving event still lands in the day it belongs to.

Layout:

    s3a://solariq-raw/telemetry/
        simulation_date=2026-08-21/
            plant_id=PLANT_01/
                part-*.parquet

Partitioning by (simulation_date, plant_id) matches how the batch layer queries:
always one simulated day, usually grouped per plant. Spark prunes to a single
date directory instead of scanning the whole archive.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date
from pyspark.sql.streaming import StreamingQuery

from processing.common.logging import get_logger

log = get_logger("spark-stream")

# Partition columns, in directory order.
PARTITION_COLUMNS = ("simulation_date", "plant_id")

# Columns written to the archive. Declared explicitly and shared with the batch
# layer so the two cannot drift apart: the daily job reads exactly these names.
#
# `raw_payload` is deliberately excluded. The specification asks for *normalized*
# telemetry, and duplicating the original JSON alongside the parsed columns would
# roughly double archive size for data already proven valid. Kafka coordinates
# are kept instead, which give per-record lineage back to the source topic at a
# fraction of the cost.
ARCHIVE_COLUMNS = (
    "event_id",
    "plant_id",
    "inverter_id",
    "active_power_kw",
    "energy_today_kwh",
    "irradiance_wm2",
    "module_temp_c",
    "inverter_temp_c",
    "status",
    "availability",
    "event_time",
    "simulator_scenario",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "simulation_date",
)


def with_simulation_date(df: DataFrame) -> DataFrame:
    """Derive the partition date from event time, not wall-clock time.

    Under the compressed demo clock a simulated day passes in five real minutes,
    so processing time says nothing about which day an event belongs to. Using
    event time also means a late-arriving record is filed under the day it was
    measured rather than the day it happened to be processed.
    """
    return df.withColumn("simulation_date", to_date(col("event_time")))


def prepare_archive_frame(df: DataFrame) -> DataFrame:
    """Project normalized events onto the archive's column contract."""
    return with_simulation_date(df).select(*[col(name) for name in ARCHIVE_COLUMNS])


def write_raw_archive(
    events: DataFrame,
    path: str,
    checkpoint_location: str,
    trigger_interval: str | None = None,
) -> StreamingQuery:
    """Append normalized telemetry to the partitioned Parquet archive.

    Uses its own checkpoint, separate from every other sink. Sharing a checkpoint
    between sinks makes their progress interdependent: one falling behind or
    being restarted would silently skip or replay data in the other.
    """
    log.info("raw_archive_starting", f"Archiving normalized telemetry to {path}", path=path)

    writer = (
        prepare_archive_frame(events)
        .writeStream.format("parquet")
        .option("path", path)
        .option("checkpointLocation", checkpoint_location)
        .partitionBy(*PARTITION_COLUMNS)
        # Append is the only mode an immutable archive should ever use; the raw
        # layer is never updated in place.
        .outputMode("append")
    )

    if trigger_interval:
        writer = writer.trigger(processingTime=trigger_interval)

    return writer.start()
