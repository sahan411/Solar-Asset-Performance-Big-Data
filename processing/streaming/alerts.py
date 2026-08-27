"""Business alert engine — turning telemetry into "this asset is costing money".

These are SOLAR OPERATIONAL alerts, distinct from pipeline-health alerts. The
difference matters during the demo and in production: "the inverter is broken"
and "our ingestion is broken" look similar on a dashboard and need completely
different responses.

DETECTION IS PER-INVERTER, NOT PER-PLANT
The obvious rule — plant performance below 80% — cannot see the fault it most
needs to catch. Degrading one inverter to 45% on a five-inverter plant moves
plant output by about 11%, so a plant-level threshold stays silent while an
asset quietly loses money all day. Evaluating each inverter against its own
nameplate rating catches it immediately.

CONDITIONS ARE MUTUALLY EXCLUSIVE
An offline inverter produces zero power, which also looks like severe
underperformance. Raising both would double-report one fault, so exactly one
condition is assigned per inverter in priority order:

    TELEMETRY_GAP > INVERTER_OFFLINE > UNDERPERFORMANCE

A gap outranks the rest because when an asset stops reporting, every other
judgement about it is guesswork.

SUSTAINED, IN EVENT TIME
A condition must persist before it becomes an alert (see migration 006).
Durations are event time, so they mean the same thing under the compressed demo
clock, during replay, and in production.
"""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    coalesce,
    col,
    concat,
    greatest,
    least,
    lit,
    round as spark_round,
    when,
)
from pyspark.sql.types import TimestampType

from processing.common.config import StreamSettings
from processing.streaming.schema import STATUS_OFFLINE

ALERT_UNDERPERFORMANCE = "UNDERPERFORMANCE"
ALERT_INVERTER_OFFLINE = "INVERTER_OFFLINE"
ALERT_TELEMETRY_GAP = "TELEMETRY_GAP"

SEVERITY_WARNING = "WARNING"
SEVERITY_CRITICAL = "CRITICAL"

# Columns of the inverter reference frame, sourced from the asset registry.
INVERTER_REFERENCE_COLUMNS = ("plant_id", "inverter_id", "rated_power_kw")

# Columns produced by `detect_alert_conditions`.
CONDITION_COLUMNS = (
    "plant_id",
    "inverter_id",
    "alert_type",
    "severity",
    "message",
    "loss_kw",
    "observed_at",
)


def detect_alert_conditions(
    latest_readings: DataFrame,
    inverter_reference: DataFrame,
    settings: StreamSettings,
    observed_at: "datetime",
) -> DataFrame:
    """Evaluate every configured inverter and return those in a fault condition.

    `latest_readings` is one row per reporting inverter (the output of
    `latest_reading_per_inverter`). `inverter_reference` is the full configured
    fleet, which is what allows a *silent* inverter to be detected: the join is
    driven by the registry, not by what happened to arrive.

    `observed_at` is the batch's observation clock — normally the maximum event
    time it contained. It is passed in rather than taken per row because a silent
    inverter has no event time of its own, and because every condition in a batch
    should share one logical instant.
    """
    # LEFT join from the registry: an inverter that sent nothing still appears.
    fleet = inverter_reference.join(
        latest_readings.select(
            "plant_id",
            "inverter_id",
            "active_power_kw",
            "irradiance_wm2",
            "status",
            "availability",
            "event_time",
        ),
        on=["plant_id", "inverter_id"],
        how="left",
    )

    # Each inverter is judged against its own rating, not the plant's capacity.
    irradiance_fraction = least(
        coalesce(col("irradiance_wm2"), lit(0.0)) / lit(settings.reference_irradiance_wm2),
        lit(1.0),
    )
    expected_kw = col("rated_power_kw") * irradiance_fraction
    performance_pct = col("active_power_kw") / expected_kw * lit(100.0)

    sun_is_up = col("irradiance_wm2") >= lit(settings.min_irradiance_wm2)
    is_silent = col("event_time").isNull()
    is_offline = (col("status") == lit(STATUS_OFFLINE)) | (col("availability") == 0)
    is_underperforming = (
        sun_is_up
        & (expected_kw > 0)
        & (performance_pct < lit(settings.underperformance_threshold_pct))
    )

    evaluated = fleet.withColumn(
        "alert_type",
        # Priority order: a silent asset cannot be judged on anything else, and
        # an offline one already explains its own zero output.
        when(is_silent, lit(ALERT_TELEMETRY_GAP))
        .when(is_offline, lit(ALERT_INVERTER_OFFLINE))
        .when(is_underperforming, lit(ALERT_UNDERPERFORMANCE))
        .otherwise(lit(None)),
    )

    with_detail = (
        evaluated.withColumn(
            "severity",
            # A silent or dead inverter is producing nothing at all; a degraded
            # one is still earning, just less than it should.
            when(col("alert_type") == lit(ALERT_UNDERPERFORMANCE), lit(SEVERITY_WARNING)).otherwise(
                lit(SEVERITY_CRITICAL)
            ),
        )
        .withColumn(
            "loss_kw",
            # Shortfall against what the available sunlight should have produced.
            # A silent inverter's loss is unknown — we have no irradiance reading
            # from it — so it is reported as zero rather than guessed.
            when(
                col("alert_type") == lit(ALERT_TELEMETRY_GAP),
                lit(0.0),
            ).otherwise(
                greatest(expected_kw - coalesce(col("active_power_kw"), lit(0.0)), lit(0.0))
            ),
        )
        .withColumn(
            "message",
            when(
                col("alert_type") == lit(ALERT_TELEMETRY_GAP),
                concat(
                    lit("No telemetry received from inverter "),
                    col("inverter_id"),
                    lit(" at plant "),
                    col("plant_id"),
                ),
            )
            .when(
                col("alert_type") == lit(ALERT_INVERTER_OFFLINE),
                concat(
                    lit("Inverter "),
                    col("inverter_id"),
                    lit(" at plant "),
                    col("plant_id"),
                    lit(" reported OFFLINE"),
                ),
            )
            .otherwise(
                concat(
                    lit("Inverter "),
                    col("inverter_id"),
                    lit(" at plant "),
                    col("plant_id"),
                    lit(" generating "),
                    spark_round(performance_pct, 1).cast("string"),
                    lit("% of expected under current irradiance"),
                )
            ),
        )
    )

    return (
        with_detail.filter(col("alert_type").isNotNull())
        .withColumn("observed_at", lit(observed_at).cast(TimestampType()))
        .select(*CONDITION_COLUMNS)
    )
