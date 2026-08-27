"""Daily actual generation, computed from the immutable raw archive.

This is the authoritative view of what a plant produced. It reads the full day's
Parquet history — never the live serving tables — which is what makes the batch
layer meaningful: it sees late-arriving events the speed layer dropped past its
watermark, and a change to the calculation can be replayed over past days.

HOW DAILY ENERGY IS DERIVED
`energy_today_kwh` is a cumulative counter that resets at the simulated-day
boundary. Three approaches were considered:

  max(energy)          Correct only if the counter starts at exactly zero and
                       never resets mid-day. A simulator restart or demo reset
                       during the day silently truncates the total to whatever
                       accumulated after the restart.

  max - min            Robust to a non-zero starting point, but undercounts by
                       the energy already on the counter at first observation —
                       everything generated before the archive's first record
                       for that inverter is lost.

  sum of increments    What a real energy meter does. Adds each observed rise in
                       the counter, and on a reset (the counter falling) credits
                       the new reading as generation since the reset. Degrades to
                       exactly max(energy) in the simple case, and stays correct
                       when the counter restarts.

The third is implemented here. The extra cost is one window function; the benefit
is that a mid-day simulator restart — which happens routinely during development
and demo resets — does not quietly understate the day's revenue.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    coalesce,
    col,
    count,
    countDistinct,
    lag,
    lit,
    sum as spark_sum,
    when,
)

from processing.common.logging import get_logger
from processing.streaming.schema import STATUS_OFFLINE, STATUS_ONLINE

log = get_logger("batch-actuals")

# Columns produced per plant.
PLANT_ACTUAL_COLUMNS = (
    "plant_id",
    "actual_generation_kwh",
    "availability_pct",
    "downtime_minutes",
    "observation_count",
    "reporting_inverters",
)


def read_daily_archive(
    spark: SparkSession, archive_path: str, simulation_date: date
) -> DataFrame:
    """Load one simulated day from the partitioned Parquet archive.

    Filtering on the partition column lets Spark prune to a single date directory
    rather than scanning the whole archive.
    """
    log.info(
        "archive_read",
        f"Reading {simulation_date.isoformat()} from {archive_path}",
        simulation_date=simulation_date.isoformat(),
        path=archive_path,
    )
    return spark.read.parquet(archive_path).filter(
        col("simulation_date") == lit(simulation_date)
    )


def inverter_energy_increments(events: DataFrame) -> DataFrame:
    """Attach each observation's contribution to its inverter's daily energy.

    Ordered by event time within each inverter, an observation contributes:

      * its full reading, if it is the first seen for that inverter — the counter
        resets to zero at the day boundary, so the reading is energy generated
        so far today;
      * the rise since the previous reading, in the normal case;
      * its full reading again if the counter FELL, which means it reset and the
        new value is generation since that reset. Clamping the negative delta to
        zero instead would silently discard that energy.
    """
    ordered = Window.partitionBy("plant_id", "inverter_id").orderBy("event_time")
    previous = lag("energy_today_kwh").over(ordered)

    return events.withColumn(
        "energy_increment_kwh",
        when(previous.isNull(), col("energy_today_kwh"))
        .when(col("energy_today_kwh") >= previous, col("energy_today_kwh") - previous)
        .otherwise(col("energy_today_kwh")),
    )


def inverter_daily_actuals(events: DataFrame) -> DataFrame:
    """Per-inverter daily totals."""
    return (
        inverter_energy_increments(events)
        .groupBy("plant_id", "inverter_id")
        .agg(
            spark_sum("energy_increment_kwh").alias("actual_generation_kwh"),
            count("*").alias("observation_count"),
            spark_sum(when(col("status") == lit(STATUS_ONLINE), 1).otherwise(0)).alias(
                "online_observations"
            ),
            spark_sum(when(col("status") == lit(STATUS_OFFLINE), 1).otherwise(0)).alias(
                "offline_observations"
            ),
        )
    )


def plant_daily_actuals(
    events: DataFrame, telemetry_interval_seconds: float
) -> DataFrame:
    """Aggregate a day's telemetry into per-plant actuals.

    AVAILABILITY is the share of received observations reporting ONLINE.

    Limitation, stated because it matters for interpretation: this is measured
    over telemetry that ARRIVED. An inverter that stopped reporting entirely
    contributes no observations, so it neither raises nor lowers this figure —
    its absence is captured by the speed layer's telemetry-gap alerts and by
    reporting_inverters below, not here. A production system would compare
    against expected observation counts.

    DOWNTIME converts OFFLINE observations into minutes using the simulator's
    telemetry interval. It is therefore a sampled estimate: an outage shorter
    than one interval can be missed, and the resolution is one interval.
    """
    per_inverter = inverter_daily_actuals(events)

    return per_inverter.groupBy("plant_id").agg(
        spark_sum("actual_generation_kwh").alias("actual_generation_kwh"),
        spark_sum("observation_count").alias("observation_count"),
        countDistinct("inverter_id").alias("reporting_inverters"),
        spark_sum("online_observations").alias("_online"),
        spark_sum("offline_observations").alias("_offline"),
    ).withColumn(
        "availability_pct",
        when(
            col("observation_count") > 0,
            col("_online") / col("observation_count") * lit(100.0),
        ).otherwise(lit(None)),
    ).withColumn(
        "downtime_minutes",
        col("_offline") * lit(telemetry_interval_seconds) / lit(60.0),
    ).select(*PLANT_ACTUAL_COLUMNS)


def collect_plant_actuals(actuals: DataFrame) -> dict[str, dict]:
    """Collect the day's actuals into plain Python, keyed by plant.

    Safe to collect: at most one row per plant. Returned as a mapping so the
    reconciliation step can join it against the reference feed without a second
    Spark job.
    """
    return {
        row["plant_id"]: {
            "actual_generation_kwh": float(row["actual_generation_kwh"] or 0.0),
            "availability_pct": (
                float(row["availability_pct"]) if row["availability_pct"] is not None else None
            ),
            "downtime_minutes": float(row["downtime_minutes"] or 0.0),
            "observation_count": int(row["observation_count"] or 0),
            "reporting_inverters": int(row["reporting_inverters"] or 0),
        }
        for row in actuals.collect()
    }
