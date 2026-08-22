"""Live portfolio roll-up (Milestone 6).

The headline case is weighting: a portfolio percentage must be
sum(actual)/sum(expected), never the mean of the plants' percentages.
"""

from __future__ import annotations

import pytest

from processing.streaming.metrics import (
    PORTFOLIO_METRIC_COLUMNS,
    portfolio_metrics,
    select_portfolio_metric_columns,
)
from tests.processing._events import utc, utc_ts

pytestmark = pytest.mark.spark

PLANT_METRIC_SCHEMA = (
    "plant_id string, window_start timestamp, window_end timestamp, "
    "current_power_kw double, avg_power_kw double, expected_power_kw double, "
    "estimated_loss_kw double, online_inverters int, offline_inverters int"
)


def _plant_rows(spark, rows):
    start = utc_ts(2026, 8, 21, 5, 0, 0)
    end = utc_ts(2026, 8, 21, 5, 0, 3)
    return spark.createDataFrame(
        [(r[0], start, end, *r[1:]) for r in rows], schema=PLANT_METRIC_SCHEMA
    )


def test_power_and_loss_are_summed_across_plants(spark):
    rows = [
        ("PLANT_01", 4750.0, 4700.0, 5000.0, 250.0, 5, 0),
        ("PLANT_02", 250.0, 240.0, 500.0, 250.0, 1, 1),
    ]
    row = portfolio_metrics(_plant_rows(spark, rows)).collect()[0]

    assert row.current_power_kw == pytest.approx(5000.0)
    assert row.avg_power_kw == pytest.approx(4940.0)
    assert row.expected_power_kw == pytest.approx(5500.0)
    assert row.estimated_loss_kw == pytest.approx(500.0)


def test_performance_is_capacity_weighted_not_a_mean_of_percentages(spark):
    """A big plant at 95% and a small one at 50% is ~91%, not 72.5%."""
    rows = [
        ("PLANT_01", 4750.0, 4750.0, 5000.0, 250.0, 5, 0),  # 95% of 5000
        ("PLANT_02", 250.0, 250.0, 500.0, 250.0, 1, 0),     # 50% of 500
    ]
    row = portfolio_metrics(_plant_rows(spark, rows)).collect()[0]

    # 5000 / 5500 = 90.909...
    assert row.performance_pct == pytest.approx(90.909, abs=0.01)
    # The unweighted mean would be (95 + 50) / 2 = 72.5.
    assert row.performance_pct != pytest.approx(72.5, abs=0.01)


def test_night_plants_are_excluded_from_both_sides_of_the_ratio(spark):
    """A dark plant's zero output must not drag the portfolio toward zero.

    One plant is producing in daylight, another is after sunset with an unknown
    expectation. The portfolio percentage should describe only the plant that can
    meaningfully be judged.
    """
    rows = [
        ("PLANT_01", 450.0, 450.0, 500.0, 50.0, 2, 0),
        ("PLANT_02", 0.0, 0.0, None, None, 2, 0),  # night: expected is NULL
    ]
    row = portfolio_metrics(_plant_rows(spark, rows)).collect()[0]

    # 450 / 500 = 90%, unaffected by the dark plant's zero.
    assert row.performance_pct == pytest.approx(90.0)
    # Current power still reflects the whole portfolio.
    assert row.current_power_kw == pytest.approx(450.0)


def test_performance_is_null_when_the_whole_portfolio_is_dark(spark):
    rows = [
        ("PLANT_01", 0.0, 0.0, None, None, 2, 0),
        ("PLANT_02", 0.0, 0.0, None, None, 2, 0),
    ]
    row = portfolio_metrics(_plant_rows(spark, rows)).collect()[0]

    assert row.performance_pct is None
    assert row.expected_power_kw is None
    assert row.current_power_kw == pytest.approx(0.0)


def test_inverter_counts_and_availability_aggregate(spark):
    rows = [
        ("PLANT_01", 400.0, 400.0, 500.0, 100.0, 4, 1),
        ("PLANT_02", 100.0, 100.0, 200.0, 100.0, 3, 2),
    ]
    row = portfolio_metrics(_plant_rows(spark, rows)).collect()[0]

    assert row.online_inverters == 7
    assert row.offline_inverters == 3
    # 7 of 10 configured inverters online.
    assert row.availability_pct == pytest.approx(70.0)


def test_window_spans_every_plant_window(spark):
    frame = spark.createDataFrame(
        [
            ("PLANT_01", utc_ts(2026, 8, 21, 5, 0, 0), utc_ts(2026, 8, 21, 5, 0, 3),
             100.0, 100.0, 200.0, 100.0, 1, 0),
            ("PLANT_02", utc_ts(2026, 8, 21, 5, 0, 1), utc_ts(2026, 8, 21, 5, 0, 9),
             100.0, 100.0, 200.0, 100.0, 1, 0),
        ],
        schema=PLANT_METRIC_SCHEMA,
    )
    row = portfolio_metrics(frame).select(utc("window_start"), utc("window_end")).collect()[0]

    assert row.window_start == "2026-08-21 05:00:00"
    assert row.window_end == "2026-08-21 05:00:09"


def test_single_plant_portfolio_matches_that_plant(spark):
    rows = [("PLANT_01", 450.0, 440.0, 500.0, 50.0, 2, 0)]
    row = portfolio_metrics(_plant_rows(spark, rows)).collect()[0]

    assert row.current_power_kw == pytest.approx(450.0)
    assert row.performance_pct == pytest.approx(90.0)
    assert row.availability_pct == pytest.approx(100.0)


def test_projection_matches_the_serving_table_contract(spark):
    rows = [("PLANT_01", 450.0, 440.0, 500.0, 50.0, 2, 0)]
    projected = select_portfolio_metric_columns(portfolio_metrics(_plant_rows(spark, rows)))

    assert tuple(projected.columns) == PORTFOLIO_METRIC_COLUMNS
    # Exactly one portfolio row per microbatch.
    assert projected.count() == 1
