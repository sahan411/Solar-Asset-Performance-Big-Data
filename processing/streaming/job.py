"""SolarIQ speed layer — Spark Structured Streaming entry point.

Wires the pipeline together: Kafka source -> parse -> (validation, windowing,
sinks added in later milestones). Each stage lives in its own module so it can be
tested against a batch DataFrame without a broker.

Run locally (from the repository root):

    python -m processing.streaming.job --console

Run under spark-submit:

    spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
      processing/streaming/job.py
"""

from __future__ import annotations

import argparse
import sys

from pyspark.sql import DataFrame

from processing.common.config import ConfigError, KafkaSettings, StreamSettings
from processing.common.logging import get_logger
from processing.streaming.session import create_spark_session
from processing.streaming.source import read_telemetry_stream
from processing.streaming.transforms import parse_telemetry

log = get_logger("spark-stream")

# Columns worth eyeballing when running with --console.
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


def build_pipeline(spark, kafka: KafkaSettings) -> DataFrame:
    """Kafka -> decoded telemetry. Later milestones extend this chain."""
    raw = read_telemetry_stream(spark, kafka)
    return parse_telemetry(raw)


def _run_console(parsed: DataFrame, checkpoint_dir: str) -> None:
    """Debug sink: print decoded events so connectivity can be eyeballed."""
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
        kafka = KafkaSettings.from_env()
        stream = StreamSettings.from_env()
        spark = create_spark_session(master=args.master)

        parsed = build_pipeline(spark, kafka)

        if args.console:
            _run_console(parsed, stream.checkpoint_dir)
        else:
            # Serving-store sinks arrive with the validation and aggregation
            # milestones; failing loudly beats pretending to run.
            raise ConfigError(
                "No production sink is wired yet. Run with --console until the "
                "validation and aggregation milestones land."
            )
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
