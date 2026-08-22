"""PostgreSQL sink: statement construction and UTC handling (Milestone 7)."""

from __future__ import annotations

import pytest

from processing.streaming.metrics import PLANT_METRIC_COLUMNS, PORTFOLIO_METRIC_COLUMNS
from processing.streaming.sinks import (
    PLANT_METRICS_KEY,
    PORTFOLIO_METRICS_KEY,
    UPSERT_PLANT_METRICS,
    UPSERT_PORTFOLIO_METRICS,
    build_upsert,
    collect_rows,
    with_utc_timestamp_strings,
)


class TestUpsertConstruction:
    def test_conflict_target_matches_the_tables_primary_key(self):
        assert "ON CONFLICT (plant_id, window_start, window_end)" in UPSERT_PLANT_METRICS
        assert "ON CONFLICT (window_start, window_end)" in UPSERT_PORTFOLIO_METRICS

    def test_key_columns_are_never_reassigned_on_update(self):
        """Updating a key column in the DO UPDATE clause is meaningless."""
        for key in PLANT_METRICS_KEY:
            assert f"{key} = EXCLUDED.{key}" not in UPSERT_PLANT_METRICS

    def test_every_metric_column_is_updated_on_conflict(self):
        """A replayed batch must refresh every value, not just some."""
        for column in PLANT_METRIC_COLUMNS:
            if column in PLANT_METRICS_KEY:
                continue
            assert f"{column} = EXCLUDED.{column}" in UPSERT_PLANT_METRICS

        for column in PORTFOLIO_METRIC_COLUMNS:
            if column in PORTFOLIO_METRICS_KEY:
                continue
            assert f"{column} = EXCLUDED.{column}" in UPSERT_PORTFOLIO_METRICS

    def test_updated_at_is_refreshed(self):
        assert "updated_at = NOW()" in UPSERT_PLANT_METRICS

    def test_values_use_bound_parameters_not_interpolation(self):
        placeholders = UPSERT_PLANT_METRICS.count("%s")
        assert placeholders == len(PLANT_METRIC_COLUMNS)

    def test_builder_is_generic(self):
        statement = build_upsert("t", ("a", "b", "c"), ("a",))
        assert statement.startswith("INSERT INTO t (a, b, c) VALUES (%s, %s, %s)")
        assert "ON CONFLICT (a) DO UPDATE SET b = EXCLUDED.b, c = EXCLUDED.c" in statement


@pytest.mark.spark
class TestUtcTimestampHandling:
    def test_timestamps_become_explicit_utc_strings(self, spark):
        """Guards the bug where a collected datetime shifts by the host offset."""
        from tests.processing._events import utc_ts

        frame = spark.createDataFrame(
            [("PLANT_01", utc_ts(2026, 8, 21, 5, 0, 0), utc_ts(2026, 8, 21, 5, 0, 3))],
            schema="plant_id string, window_start timestamp, window_end timestamp",
        )
        row = with_utc_timestamp_strings(frame, ("window_start", "window_end")).collect()[0]

        assert row.window_start == "2026-08-21 05:00:00.000+00"
        assert row.window_end == "2026-08-21 05:00:03.000+00"

    def test_collected_rows_match_the_column_order_of_the_statement(self, spark):
        from tests.processing._events import utc_ts

        frame = spark.createDataFrame(
            [(utc_ts(2026, 8, 21, 5, 0, 0), utc_ts(2026, 8, 21, 5, 0, 3), 100.0, 90.0,
              120.0, 4, 1, 80.0, 83.3, 20.0)],
            schema=(
                "window_start timestamp, window_end timestamp, current_power_kw double, "
                "avg_power_kw double, expected_power_kw double, online_inverters int, "
                "offline_inverters int, availability_pct double, performance_pct double, "
                "estimated_loss_kw double"
            ),
        )
        rows = collect_rows(frame, PORTFOLIO_METRIC_COLUMNS)

        assert len(rows) == 1
        assert len(rows[0]) == len(PORTFOLIO_METRIC_COLUMNS)
        # Order matters: these are positional bind parameters.
        assert rows[0][0] == "2026-08-21 05:00:00.000+00"
        assert rows[0][2] == pytest.approx(100.0)

    def test_empty_frame_collects_to_no_rows(self, spark):
        frame = spark.createDataFrame(
            [], schema="window_start timestamp, window_end timestamp, current_power_kw double"
        )
        assert collect_rows(frame, ("window_start", "current_power_kw")) == []
