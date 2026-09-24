"""Speed layer: Spark Structured Streaming job.

Reads smart-meter readings from Kafka and runs two streaming queries:

1. store_readings  (cleaning)
   Parse JSON -> validate -> reject bad records -> drop duplicates ->
   append clean readings to PostgreSQL `meter_readings`.
   That table is the master dataset the Airflow batch layer bills from.

2. store_zone_load  (real-time aggregation)
   1-hour tumbling windows (event time, 1-hour watermark) per grid zone:
   total consumption, total solar, net grid load, renewable %.
   Upserted into `zone_load_hourly`, and a LOW_RENEWABLE alert is raised when
   daytime renewable contribution drops below the threshold.

All times are simulated event time from the reading's `timestamp` field.

Tracing: every Kafka message has a `trace_id` header (set by the simulator).
It is stored with the reading (or with its rejection) and written in the logs,
so one reading can be followed from the simulator log, through Kafka
(partition/offset) and Spark, into PostgreSQL and the daily bill
(GET /api/trace/<trace_id>). The `produced_at` header gives the end-to-end
latency, recorded per micro-batch in `pipeline_metrics`.
"""

import logging
import os
import time
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from common.households import ZONES, expected_readings_per_hour
from common.log import get_logger, log_event

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "meter-readings")
PG_DSN = os.getenv("PG_DSN", "host=postgres dbname=smartgrid user=grid password=grid")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/checkpoints")

LOW_RENEWABLE_PCT = float(os.getenv("LOW_RENEWABLE_PCT", "20"))
DAYTIME_HOURS = (9, 15)  # alerts only make sense while the sun is up
MIN_WINDOW_COVERAGE = 0.75  # judge a window only once 75% of its readings arrived

log = get_logger("spark-streaming")

READING_SCHEMA = T.StructType([
    T.StructField("meter_id", T.StringType()),
    T.StructField("household_id", T.StringType()),
    T.StructField("grid_zone", T.StringType()),
    T.StructField("power_consumption_kwh", T.DoubleType()),
    T.StructField("solar_generation_kwh", T.DoubleType()),
    T.StructField("timestamp", T.TimestampType()),
])


@contextmanager
def db():
    conn = psycopg2.connect(PG_DSN)
    try:
        with conn:  # commits on success, rolls back on error
            yield conn
    finally:
        conn.close()


def record_metrics(cur, stage, batch_id, rows_in, valid, invalid, duplicate, started,
                   avg_latency_ms=None, max_latency_ms=None):
    duration_ms = int((time.time() - started) * 1000)
    cur.execute(
        """INSERT INTO pipeline_metrics
           (stage, batch_id, rows_in, rows_valid, rows_invalid, rows_duplicate, duration_ms,
            avg_latency_ms, max_latency_ms)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        (stage, batch_id, rows_in, valid, invalid, duplicate, duration_ms,
         avg_latency_ms, max_latency_ms),
    )
    return duration_ms


def header(name):
    """Value of one Kafka header as a string (null if missing)."""
    return F.expr(f"filter(headers, h -> h.key = '{name}')[0].value").cast("string")


def parse_readings(kafka_df):
    """Kafka bytes -> typed columns, plus a `reject_reason` (null = valid)."""
    raw_value = F.col("value").cast("string")
    parsed = (
        kafka_df
        .select(
            raw_value.alias("raw_value"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            header("trace_id").alias("trace_id"),
            header("produced_at").cast("long").alias("produced_at_ms"),
            F.from_json(raw_value, READING_SCHEMA).alias("r"),
        )
        .select("raw_value", "kafka_partition", "kafka_offset", "trace_id", "produced_at_ms",
                "r.*")
        .withColumnRenamed("timestamp", "event_time")
    )
    c = F.col
    reject_reason = (
        F.when(c("meter_id").isNull() | c("event_time").isNull(), "malformed_or_missing_fields")
        .when(c("household_id").isNull(), "missing_household_id")
        .when(c("grid_zone").isNull() | ~c("grid_zone").isin(*ZONES), "unknown_grid_zone")
        .when(c("power_consumption_kwh").isNull() | (c("power_consumption_kwh") < 0),
              "invalid_consumption")
        .when(c("solar_generation_kwh").isNull() | (c("solar_generation_kwh") < 0),
              "invalid_solar_generation")
    )
    return parsed.withColumn("reject_reason", reject_reason)


# ---------------------------------------------------------------------------
# Query 1: clean readings -> meter_readings (master dataset)
# ---------------------------------------------------------------------------

def store_readings(batch_df, batch_id):
    started = time.time()
    batch_df.persist()
    rows_in = batch_df.count()
    if rows_in == 0:
        batch_df.unpersist()
        return

    rejected = batch_df.filter(F.col("reject_reason").isNotNull()).collect()
    clean = (
        batch_df.filter(F.col("reject_reason").isNull())
        .dropDuplicates(["meter_id", "event_time"])  # duplicates inside this batch
        .collect()
    )
    batch_df.unpersist()

    # End-to-end latency: simulator send time -> now (just before the DB write).
    now_ms = time.time() * 1000
    latencies = [now_ms - r.produced_at_ms for r in clean if r.produced_at_ms]
    avg_latency = int(sum(latencies) / len(latencies)) if latencies else None
    max_latency = int(max(latencies)) if latencies else None

    with db() as conn, conn.cursor() as cur:
        # ON CONFLICT DO NOTHING drops duplicates across batches (and replays).
        inserted = execute_values(
            cur,
            """INSERT INTO meter_readings
               (meter_id, household_id, grid_zone, event_time, power_consumption_kwh,
                solar_generation_kwh, trace_id, kafka_partition, kafka_offset)
               VALUES %s ON CONFLICT (meter_id, event_time) DO NOTHING RETURNING 1""",
            [(r.meter_id, r.household_id, r.grid_zone, r.event_time, r.power_consumption_kwh,
              r.solar_generation_kwh, r.trace_id, r.kafka_partition, r.kafka_offset)
             for r in clean],
            page_size=1000, fetch=True,
        )
        if rejected:
            execute_values(
                cur,
                """INSERT INTO rejected_readings
                   (raw_value, reason, trace_id, kafka_partition, kafka_offset)
                   VALUES %s""",
                [(r.raw_value, r.reject_reason, r.trace_id, r.kafka_partition, r.kafka_offset)
                 for r in rejected],
            )
        valid = len(inserted)
        duplicate = rows_in - len(rejected) - valid
        duration_ms = record_metrics(cur, "stream_ingest", batch_id, rows_in, valid,
                                     len(rejected), duplicate, started, avg_latency, max_latency)

    log_event(log, "readings_stored", batch_id=batch_id, rows_in=rows_in, stored=valid,
              rejected=len(rejected), duplicates=duplicate, duration_ms=duration_ms,
              avg_latency_ms=avg_latency, max_latency_ms=max_latency)
    for r in rejected:
        log_event(log, "reading_rejected", level=logging.WARNING, trace_id=r.trace_id,
                  reason=r.reject_reason, batch_id=batch_id, partition=r.kafka_partition,
                  offset=r.kafka_offset)


# ---------------------------------------------------------------------------
# Query 2: windowed zone load -> zone_load_hourly + alerts
# ---------------------------------------------------------------------------

def zone_load_windows(readings):
    c = F.col
    renewable_pct = F.when(
        c("consumption_kwh") > 0,
        F.least(c("solar_kwh"), c("consumption_kwh")) / c("consumption_kwh") * 100,
    ).otherwise(0.0)
    return (
        readings.filter(c("reject_reason").isNull())
        .withWatermark("event_time", "1 hour")  # accept readings up to 1 simulated hour late
        .groupBy(F.window("event_time", "1 hour").alias("w"), "grid_zone")
        .agg(
            F.sum("power_consumption_kwh").alias("consumption_kwh"),
            F.sum("solar_generation_kwh").alias("solar_kwh"),
            F.count("*").alias("readings"),
        )
        .select(
            "grid_zone",
            c("w.start").alias("window_start"),
            c("w.end").alias("window_end"),
            "consumption_kwh",
            "solar_kwh",
            (c("consumption_kwh") - c("solar_kwh")).alias("net_grid_load_kwh"),
            renewable_pct.alias("renewable_pct"),
            "readings",
        )
    )


def store_zone_load(batch_df, batch_id):
    started = time.time()
    rows = batch_df.collect()
    if not rows:
        return

    alerts = []
    for r in rows:
        expected = expected_readings_per_hour(r.grid_zone)
        daytime = DAYTIME_HOURS[0] <= r.window_start.hour < DAYTIME_HOURS[1]
        enough_data = r.readings >= MIN_WINDOW_COVERAGE * expected
        if daytime and enough_data and r.renewable_pct < LOW_RENEWABLE_PCT:
            alerts.append((
                "LOW_RENEWABLE", r.grid_zone, r.window_start, round(r.renewable_pct, 1),
                LOW_RENEWABLE_PCT,
                f"Renewable contribution in zone {r.grid_zone} is {r.renewable_pct:.1f}% "
                f"(threshold {LOW_RENEWABLE_PCT:.0f}%) for hour starting {r.window_start:%Y-%m-%d %H:%M}",
            ))

    with db() as conn, conn.cursor() as cur:
        execute_values(
            cur,
            """INSERT INTO zone_load_hourly
               (grid_zone, window_start, window_end, consumption_kwh, solar_kwh,
                net_grid_load_kwh, renewable_pct, readings, expected_readings)
               VALUES %s
               ON CONFLICT (grid_zone, window_start) DO UPDATE SET
                 consumption_kwh = EXCLUDED.consumption_kwh,
                 solar_kwh = EXCLUDED.solar_kwh,
                 net_grid_load_kwh = EXCLUDED.net_grid_load_kwh,
                 renewable_pct = EXCLUDED.renewable_pct,
                 readings = EXCLUDED.readings,
                 updated_at = now()""",
            [(r.grid_zone, r.window_start, r.window_end, r.consumption_kwh, r.solar_kwh,
              r.net_grid_load_kwh, r.renewable_pct, r.readings,
              expected_readings_per_hour(r.grid_zone)) for r in rows],
        )
        new_alerts = []
        if alerts:
            new_alerts = execute_values(
                cur,
                """INSERT INTO alerts (alert_type, grid_zone, window_start, value, threshold, message)
                   VALUES %s ON CONFLICT (alert_type, grid_zone, window_start) DO NOTHING
                   RETURNING grid_zone, value""",
                alerts, fetch=True,
            )
        duration_ms = record_metrics(cur, "stream_aggregate", batch_id, len(rows), len(rows),
                                     0, 0, started)

    log_event(log, "zone_load_updated", batch_id=batch_id, windows=len(rows),
              duration_ms=duration_ms)
    for zone, value in new_alerts:
        log_event(log, "alert_low_renewable", level=logging.WARNING, grid_zone=zone, renewable_pct=value,
                  threshold=LOW_RENEWABLE_PCT)


def main():
    spark = (
        SparkSession.builder.appName("smart-grid-streaming")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "3")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("includeHeaders", "true")  # trace_id and produced_at
        .load()
    )
    readings = parse_readings(kafka_df)

    (readings.writeStream.foreachBatch(store_readings)
     .option("checkpointLocation", f"{CHECKPOINT_DIR}/readings")
     .trigger(processingTime="3 seconds")
     .start())

    (zone_load_windows(readings).writeStream.foreachBatch(store_zone_load)
     .outputMode("update")  # emit a window every time its totals change
     .option("checkpointLocation", f"{CHECKPOINT_DIR}/zone_load")
     .trigger(processingTime="3 seconds")
     .start())

    log_event(log, "streaming_started", topic=TOPIC, low_renewable_pct=LOW_RENEWABLE_PCT)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
