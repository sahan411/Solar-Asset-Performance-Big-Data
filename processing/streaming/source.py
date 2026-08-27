"""Kafka source for the SolarIQ streaming job."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from processing.common.config import KafkaSettings
from processing.common.logging import get_logger

log = get_logger("spark-stream")


def read_telemetry_stream(spark: SparkSession, kafka: KafkaSettings) -> DataFrame:
    """Open the raw telemetry topic as a streaming DataFrame.

    Returns Kafka's native shape (key/value/topic/partition/offset/timestamp);
    decoding happens in `processing.streaming.transforms.parse_telemetry` so the
    parsing logic stays testable without a broker.
    """
    log.info(
        "kafka_source_opening",
        f"Subscribing to {kafka.telemetry_topic}",
        topic=kafka.telemetry_topic,
        bootstrap_servers=kafka.bootstrap_servers,
        starting_offsets=kafka.starting_offsets,
    )

    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka.bootstrap_servers)
        .option("subscribe", kafka.telemetry_topic)
        # Configured, never hard-coded: the demo replays from `earliest` so a
        # restarted job still reflects the whole simulated day, while a
        # long-running deployment would start at `latest`.
        .option("startingOffsets", kafka.starting_offsets)
        # A truncated/compacted topic must not kill the job mid-demo; the
        # checkpoint's stored offsets can legitimately fall off the retention
        # window when the simulator is reset.
        .option("failOnDataLoss", "false")
        .load()
    )
