"""Live plant and portfolio metrics for the SolarIQ speed layer.

Every function here is a pure batch DataFrame transformation. They run inside
`foreachBatch`, which is a deliberate architectural choice rather than a
convenience — see "Why not a sliding window" below.

WINDOW SEMANTICS
Each microbatch produces exactly one row per plant, whose window is the
event-time range of the telemetry that batch received. The rows are therefore
self-contained summaries of disjoint slices, which is what makes them safe to
upsert: replaying a microbatch recomputes the identical row under the identical
key, so recovery cannot double-count.

WHY NOT A SLIDING WINDOW
The obvious `groupBy(window(...), plant_id).agg(sum("active_power_kw"))` is
wrong for current power. With telemetry every 3 seconds, a 60-second window
holds ~20 readings per inverter, so the sum is roughly 20x the plant's actual
output — it adds up repeated observations of the same physical quantity.
Current power is an *instantaneous* measure: the sum of each inverter's most
recent reading, one value per asset. That requires a latest-per-inverter step
followed by a per-plant sum, which is two chained aggregations. Structured
Streaming does not allow two aggregations on a streaming DataFrame, but does
allow them on the batch DataFrame inside `foreachBatch`. Hence this design.

Event-time watermarking is still used, in the de-duplication step upstream.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    avg,
    coalesce,
    col,
    count,
    greatest,
    least,
    lit,
    max as spark_max,
    min as spark_min,
    row_number,
    sum as spark_sum,
    when,
)

from processing.common.config import StreamSettings
from processing.streaming.schema import STATUS_OFFLINE

# Columns of the plant reference frame joined in from the asset registry.
# Sourced from `plants`/`inverters` rather than from telemetry, so that an
# inverter which has stopped reporting still counts against availability.
PLANT_REFERENCE_COLUMNS = ("plant_id", "capacity_kw", "configured_inverters")


def latest_reading_per_inverter(events: DataFrame) -> DataFrame:
    """Reduce a batch to one row per inverter: its most recent reading.

    Ties on event time are broken by Kafka offset so the result is deterministic;
    without a tie-break, replaying a batch could pick a different row and produce
    a different "current" power for identical input.
    """
    most_recent = Window.partitionBy("plant_id", "inverter_id").orderBy(
        col("event_time").desc(), col("kafka_offset").desc()
    )
    return (
        events.withColumn("_rank", row_number().over(most_recent))
        .filter(col("_rank") == 1)
        .drop("_rank")
    )


def plant_power_timeline(events: DataFrame) -> DataFrame:
    """Plant-level power at each observed instant.

    Summing across inverters at a single timestamp is legitimate — those are
    different physical assets producing simultaneously. Averaging *this* over
    time gives a true mean plant power, unlike averaging raw samples.
    """
    return events.groupBy("plant_id", "event_time").agg(
        spark_sum("active_power_kw").alias("plant_power_kw")
    )


def plant_metrics(
    events: DataFrame,
    plant_reference: DataFrame,
    settings: StreamSettings,
) -> DataFrame:
    """Compute one live metric row per plant for a single microbatch.

    `events` must be validated, normalized, de-duplicated telemetry.
    `plant_reference` carries capacity and configured inverter count per plant.
    """
    latest = latest_reading_per_inverter(events)

    # Instantaneous state, from one reading per asset.
    current = latest.groupBy("plant_id").agg(
        spark_sum("active_power_kw").alias("current_power_kw"),
        avg("irradiance_wm2").alias("avg_irradiance_wm2"),
        # An inverter counts as online only if it says so *and* reports itself
        # available; the two disagree during a fault the firmware has noticed.
        spark_sum(
            when((col("status") != lit(STATUS_OFFLINE)) & (col("availability") > 0), 1).otherwise(0)
        ).alias("reporting_online_inverters"),
        count("*").alias("reporting_inverters"),
    )

    # Window bounds describe the slice of telemetry this batch summarised, so
    # they must come from every event received — not from `latest`, whose
    # earliest row is by construction each inverter's most recent reading.
    bounds = events.groupBy("plant_id").agg(
        spark_min("event_time").alias("window_start"),
        spark_max("event_time").alias("window_end"),
    )

    # Time-averaged power across the batch's instants.
    averaged = plant_power_timeline(events).groupBy("plant_id").agg(
        avg("plant_power_kw").alias("avg_power_kw")
    )

    combined = (
        current.join(bounds, on="plant_id", how="inner")
        .join(averaged, on="plant_id", how="inner")
        .join(plant_reference, on="plant_id", how="left")
    )

    return _apply_performance_model(combined, settings)


def _apply_performance_model(df: DataFrame, settings: StreamSettings) -> DataFrame:
    """Derive expected power, performance and loss.

    EXPECTED POWER (simplified proxy, not a bankable PV model):

        expected_kw = capacity_kw * min(irradiance / 1000, 1)

    Irradiance at standard test conditions is 1000 W/m^2, the level at which a
    panel is rated. So the ratio is the fraction of nameplate capacity the
    available sunlight can support. It is capped at 1 because brief cloud-edge
    enhancement can exceed 1000 W/m^2 without the plant being able to exceed its
    inverter rating.

    Deliberately excluded: temperature derate, soiling, shading, cable and
    inverter losses, and angle of incidence. A real performance-ratio
    calculation includes all of them. This is documented as a Phase 1
    approximation in docs/architecture.md.
    """
    irradiance_fraction = least(
        col("avg_irradiance_wm2") / lit(settings.reference_irradiance_wm2), lit(1.0)
    )
    expected_power = col("capacity_kw") * irradiance_fraction

    # Below the minimum irradiance the sun is not up enough to judge the asset:
    # expected power approaches zero and the ratio becomes meaningless or
    # explosive. Reporting NULL is honest; reporting 0% at night is not.
    sun_is_up = col("avg_irradiance_wm2") >= lit(settings.min_irradiance_wm2)

    with_expected = df.withColumn(
        "expected_power_kw", when(sun_is_up, expected_power).otherwise(lit(None))
    )

    return (
        with_expected
        .withColumn(
            "performance_pct",
            when(
                col("expected_power_kw").isNotNull() & (col("expected_power_kw") > 0),
                col("current_power_kw") / col("expected_power_kw") * lit(100.0),
            ).otherwise(lit(None)),
        )
        .withColumn(
            "estimated_loss_kw",
            when(
                col("expected_power_kw").isNotNull(),
                # Never negative: a plant outperforming the simplified proxy is
                # not "producing negative loss", it just means the model is
                # conservative.
                greatest(col("expected_power_kw") - col("current_power_kw"), lit(0.0)),
            ).otherwise(lit(None)),
        )
        .withColumn(
            # Non-reporting inverters count as offline. This is what turns a
            # telemetry gap into visible unavailability instead of silently
            # shrinking the denominator and flattering the plant.
            "online_inverters",
            col("reporting_online_inverters"),
        )
        .withColumn(
            "offline_inverters",
            greatest(
                coalesce(col("configured_inverters"), col("reporting_inverters"))
                - col("reporting_online_inverters"),
                lit(0),
            ),
        )
        .withColumn(
            "availability_pct",
            when(
                coalesce(col("configured_inverters"), col("reporting_inverters")) > 0,
                col("reporting_online_inverters")
                / coalesce(col("configured_inverters"), col("reporting_inverters"))
                * lit(100.0),
            ).otherwise(lit(None)),
        )
    )


# Columns written to `live_plant_metrics`, in table order.
PLANT_METRIC_COLUMNS = (
    "plant_id",
    "window_start",
    "window_end",
    "current_power_kw",
    "avg_power_kw",
    "expected_power_kw",
    "avg_irradiance_wm2",
    "availability_pct",
    "performance_pct",
    "estimated_loss_kw",
    "online_inverters",
    "offline_inverters",
)


def select_plant_metric_columns(df: DataFrame) -> DataFrame:
    """Project onto the serving table's column contract."""
    return df.select(*[col(name) for name in PLANT_METRIC_COLUMNS])
