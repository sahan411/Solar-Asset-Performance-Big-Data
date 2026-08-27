"""SolarIQ speed layer — Spark Structured Streaming entry point.

Starts three queries against one Kafka source:

  * live metrics + alerts + health -> PostgreSQL (foreachBatch)
  * normalized telemetry           -> Parquet/MinIO raw archive
  * rejected telemetry             -> solar.telemetry.invalid

Each has its own checkpoint directory. Sharing one would couple their progress,
so restarting or falling behind in one would silently skip or replay data in
another.

Run locally (from the repository root):

    python -m processing.streaming.job --master 'local[2]'
    python -m processing.streaming.job --console      # decode only, no writes

Run under spark-submit:

    spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,\
org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
      processing/streaming/job.py
"""

from __future__ import annotations

import argparse
import os
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

from processing.common.config import (
    ConfigError,
    DatabaseSettings,
    KafkaSettings,
    ObjectStoreSettings,
    StreamSettings,
)
from processing.common.logging import get_logger
from processing.streaming.archive import write_raw_archive
from processing.streaming.health import (
    EVENTS_INVALID,
    start_metrics_server,
)
from processing.streaming.pipeline import (
    MicrobatchContext,
    build_invalid_stream,
    build_valid_stream,
    load_inverter_reference,
    load_plant_reference,
    process_microbatch,
)
from processing.streaming.session import create_spark_session
from processing.streaming.source import read_telemetry_stream
from processing.streaming.transforms import parse_telemetry

log = get_logger("spark-stream")

_CONSOLE_COLUMNS = (
    "event_time",
    "plant_id",
    "inverter_id",
    "active_power_kw",
    "irradiance_wm2",
    "status",
    "simulator_scenario",
    "payload_parsed",
)


def _start_metrics_sink(
    valid: DataFrame, ctx: MicrobatchContext, checkpoint_dir: str
) -> StreamingQuery:
    return (
        valid.writeStream.foreachBatch(
            lambda batch, batch_id: process_microbatch(batch, batch_id, ctx)
        )
        .option("checkpointLocation", f"{checkpoint_dir}/live-metrics")
        .outputMode("append")
        .start()
    )


def _start_quarantine_sink(
    invalid: DataFrame, kafka: KafkaSettings, checkpoint_dir: str
) -> StreamingQuery:
    """Publish rejected records, counting them on the way out.

    foreachBatch rather than a plain Kafka sink so the invalid counter can be
    incremented on the driver — a silent quarantine stream is indistinguishable
    from a healthy one.
    """

    def write(batch: DataFrame, batch_id: int) -> None:
        count = batch.count()
        if not count:
            return
        (
            batch.write.format("kafka")
            .option("kafka.bootstrap.servers", kafka.bootstrap_servers)
            .option("topic", kafka.invalid_topic)
            .save()
        )
        EVENTS_INVALID.inc(count)
        log.warning(
            "telemetry_quarantined",
            f"Quarantined {count} invalid event(s) to {kafka.invalid_topic}",
            batch_id=batch_id,
            count=count,
            topic=kafka.invalid_topic,
        )

    return (
        invalid.writeStream.foreachBatch(write)
        .option("checkpointLocation", f"{checkpoint_dir}/quarantine")
        .outputMode("append")
        .start()
    )


def _run_console(parsed: DataFrame, checkpoint_dir: str) -> None:
    """Debug sink: print decoded telemetry so connectivity can be eyeballed."""
    query = (
        parsed.select(*_CONSOLE_COLUMNS)
        .writeStream.format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("numRows", 20)
        .option("checkpointLocation", f"{checkpoint_dir}/console")
        .start()
    )
    log.info("stream_started", "Console stream running; press Ctrl-C to stop")
    query.awaitTermination()


def run(spark: SparkSession, args: argparse.Namespace) -> None:
    kafka = KafkaSettings.from_env()
    settings = StreamSettings.from_env()

    parsed = parse_telemetry(read_telemetry_stream(spark, kafka))

    if args.console:
        _run_console(parsed, settings.checkpoint_dir)
        return

    database_url = DatabaseSettings.from_env().url
    object_store = ObjectStoreSettings.from_env()

    ctx = MicrobatchContext(
        database_url=database_url,
        settings=settings,
        plant_reference=load_plant_reference(spark, database_url),
        inverter_reference=load_inverter_reference(spark, database_url),
    )
    if ctx.inverter_reference.isEmpty():
        raise ConfigError(
            "The inverter registry is empty. Run `python -m storage.migrate` and "
            "`python -m storage.seed_portfolio` before starting the stream — "
            "without it, telemetry gaps cannot be detected."
        )

    valid = build_valid_stream(parsed, settings)

    queries = [
        _start_metrics_sink(valid, ctx, settings.checkpoint_dir),
        write_raw_archive(
            valid,
            path=object_store.raw_telemetry_uri,
            checkpoint_location=f"{settings.checkpoint_dir}/raw-archive",
        ),
        _start_quarantine_sink(build_invalid_stream(parsed), kafka, settings.checkpoint_dir),
    ]

    log.info(
        "stream_started",
        f"{len(queries)} streaming queries running",
        queries=[q.name or q.id for q in queries],
    )

    # Any query terminating means the pipeline is no longer whole; surface it
    # rather than silently continuing with partial coverage.
    spark.streams.awaitAnyTermination()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="SolarIQ telemetry stream processor.")
    parser.add_argument(
        "--console",
        action="store_true",
        help="Print decoded telemetry to stdout instead of writing to the serving store.",
    )
    parser.add_argument("--master", help="Spark master URL (defaults to the submit environment).")
    args = parser.parse_args(argv)

    spark = None
    try:
        if not args.console:
            start_metrics_server(int(os.getenv("STREAM_METRICS_PORT", "9102")))

        spark = create_spark_session(
            object_store=None if args.console else ObjectStoreSettings.from_env(),
            master=args.master,
        )
        run(spark, args)
    except ConfigError as exc:
        log.error("stream_config_error", str(exc))
        return 2
    except KeyboardInterrupt:
        log.info("stream_stopped", "Stream stopped by operator")
        return 0
    except Exception as exc:  # noqa: BLE001 - top-level job boundary
        log.exception("stream_failed", f"Streaming job failed: {exc}")
        return 1
    finally:
        if spark is not None:
            spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
