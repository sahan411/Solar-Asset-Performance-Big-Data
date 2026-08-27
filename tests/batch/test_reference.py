"""Daily reference feed validation (Milestone 10).

Every financial figure the platform reports traces back to this file, so these
tests are about refusing to trust a feed that could produce a confident wrong
revenue report.
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from processing.batch.reference import (
    REFERENCE_COLUMNS,
    ReferenceFeedError,
    load_reference_file,
    reference_path,
    validate_reference_frame,
)

PLANTS = ("PLANT_01", "PLANT_02")


def _row(plant_id="PLANT_01", **overrides):
    row = {
        "simulation_date": "2026-08-21",
        "plant_id": plant_id,
        "plant_capacity_kw": "4000",
        "expected_generation_kwh": "20000",
        "expected_peak_power_kw": "3800",
        "forecast_irradiance_kwh_m2": "5.2",
        "ppa_rate_per_kwh": "0.15",
        "maintenance_flag": "false",
        "source_version": "v1",
    }
    row.update({k: str(v) for k, v in overrides.items()})
    return row


def _frame(rows=None):
    return pd.DataFrame(rows if rows is not None else [_row("PLANT_01"), _row("PLANT_02")])


def _validate(rows=None, plants=PLANTS, simulation_date=None):
    return validate_reference_frame(_frame(rows), plants, simulation_date)


def _problems(rows=None, plants=PLANTS, simulation_date=None):
    with pytest.raises(ReferenceFeedError) as exc:
        _validate(rows, plants, simulation_date)
    return exc.value.problems


class TestHappyPath:
    def test_a_well_formed_feed_validates(self):
        feed = _validate()

        assert feed.simulation_date == date(2026, 8, 21)
        assert feed.plant_count == 2
        assert feed.rows[0][1] == "PLANT_01"
        assert feed.rows[0][6] == pytest.approx(0.15)

    def test_values_are_typed_not_left_as_text(self):
        feed = _validate()
        row = feed.rows[0]

        assert row[0] == date(2026, 8, 21)
        assert isinstance(row[2], float)
        assert row[7] is False

    @pytest.mark.parametrize("raw,expected", [
        ("true", True), ("True", True), ("TRUE", True), ("yes", True), ("1", True),
        ("false", False), ("False", False), ("no", False), ("0", False), ("", False),
    ])
    def test_maintenance_flag_accepts_common_boolean_spellings(self, raw, expected):
        feed = _validate([_row("PLANT_01", maintenance_flag=raw), _row("PLANT_02")])
        assert feed.rows[0][7] is expected

    def test_a_maintenance_plant_is_kept_not_dropped(self):
        """Planned maintenance still appears in the report, flagged."""
        feed = _validate([_row("PLANT_01", maintenance_flag="true"), _row("PLANT_02")])
        assert feed.plant_count == 2
        assert feed.rows[0][7] is True

    def test_zero_ppa_rate_is_allowed(self):
        """A plant may legitimately have no commercial rate configured."""
        feed = _validate([_row("PLANT_01", ppa_rate_per_kwh="0"), _row("PLANT_02")])
        assert feed.rows[0][6] == pytest.approx(0.0)


class TestSchema:
    def test_missing_contract_column_is_rejected(self):
        frame = _frame().drop(columns=["ppa_rate_per_kwh"])
        with pytest.raises(ReferenceFeedError, match="missing required column"):
            validate_reference_frame(frame, PLANTS)

    def test_unexpected_column_is_tolerated_with_a_warning(self):
        """A generator adding a field should not fail the day's reconciliation."""
        frame = _frame()
        frame["experimental_field"] = "x"
        feed = validate_reference_frame(frame, PLANTS)

        assert feed.plant_count == 2
        assert any("experimental_field" in w for w in feed.warnings)

    def test_empty_feed_is_rejected(self):
        with pytest.raises(ReferenceFeedError, match="no rows"):
            validate_reference_frame(pd.DataFrame(columns=list(REFERENCE_COLUMNS)), PLANTS)

    def test_missing_source_version_is_rejected(self):
        assert any("source_version" in p for p in _problems([_row("PLANT_01", source_version=""), _row("PLANT_02")]))


class TestPortfolioCoverage:
    def test_a_missing_plant_is_rejected(self):
        """Silently dropping a plant would understate the day's revenue."""
        problems = _problems([_row("PLANT_01")])
        assert any("PLANT_02" in p and "no reference row" in p for p in problems)

    def test_an_unknown_plant_is_rejected(self):
        problems = _problems([_row("PLANT_01"), _row("PLANT_02"), _row("PLANT_99")])
        assert any("PLANT_99" in p and "unknown" in p for p in problems)

    def test_duplicate_plant_rows_are_rejected(self):
        problems = _problems([_row("PLANT_01"), _row("PLANT_01"), _row("PLANT_02")])
        assert any("duplicate" in p for p in problems)


class TestDates:
    def test_mixed_dates_in_one_feed_are_rejected(self):
        problems = _problems(
            [_row("PLANT_01"), _row("PLANT_02", simulation_date="2026-08-22")]
        )
        assert any("multiple simulation_dates" in p for p in problems)

    def test_feed_for_the_wrong_day_is_rejected(self):
        problems = _problems(simulation_date=date(2026, 8, 22))
        assert any("2026-08-22 was requested" in p for p in problems)

    def test_feed_for_the_requested_day_passes(self):
        feed = _validate(simulation_date=date(2026, 8, 21))
        assert feed.simulation_date == date(2026, 8, 21)

    def test_malformed_date_is_rejected(self):
        problems = _problems([_row("PLANT_01", simulation_date="21/08/2026"), _row("PLANT_02")])
        assert any("not an ISO date" in p for p in problems)


class TestPhysicalPlausibility:
    @pytest.mark.parametrize("column", [
        "plant_capacity_kw", "expected_generation_kwh", "expected_peak_power_kw",
    ])
    def test_non_positive_quantities_are_rejected(self, column):
        problems = _problems([_row("PLANT_01", **{column: "0"}), _row("PLANT_02")])
        assert any(column in p and "greater than zero" in p for p in problems)

    def test_negative_rate_is_rejected(self):
        problems = _problems([_row("PLANT_01", ppa_rate_per_kwh="-0.1"), _row("PLANT_02")])
        assert any("ppa_rate_per_kwh" in p for p in problems)

    def test_peak_above_nameplate_capacity_is_rejected(self):
        """A plant cannot be expected to exceed its own rating."""
        problems = _problems(
            [_row("PLANT_01", plant_capacity_kw="4000", expected_peak_power_kw="5000"),
             _row("PLANT_02")]
        )
        assert any("exceeds" in p for p in problems)

    def test_implausible_daily_generation_is_rejected(self):
        """Catches a MWh/kWh unit error before it inflates portfolio revenue.

        4000 kW producing 200000 kWh implies 50 equivalent full-power hours in a
        24-hour day, which no solar plant achieves.
        """
        problems = _problems(
            [_row("PLANT_01", expected_generation_kwh="200000"), _row("PLANT_02")]
        )
        assert any("equivalent full-power hours" in p for p in problems)

    def test_a_realistic_yield_passes(self):
        """4000 kW at 5 equivalent sun hours is an ordinary day."""
        feed = _validate([_row("PLANT_01", expected_generation_kwh="20000"), _row("PLANT_02")])
        assert feed.plant_count == 2

    def test_non_numeric_value_is_rejected(self):
        problems = _problems([_row("PLANT_01", expected_generation_kwh="lots"), _row("PLANT_02")])
        assert any("not a number" in p for p in problems)

    def test_unparseable_maintenance_flag_is_rejected(self):
        problems = _problems([_row("PLANT_01", maintenance_flag="maybe"), _row("PLANT_02")])
        assert any("not a boolean" in p for p in problems)


class TestErrorReporting:
    def test_every_problem_is_reported_not_just_the_first(self):
        """One Airflow run should tell the operator everything to fix."""
        problems = _problems([
            _row("PLANT_01", expected_generation_kwh="-1", ppa_rate_per_kwh="-2"),
            _row("PLANT_02", expected_peak_power_kw="0"),
        ])
        assert len(problems) >= 3

    def test_the_message_names_the_offending_row_and_plant(self):
        problems = _problems([_row("PLANT_01", ppa_rate_per_kwh="-1"), _row("PLANT_02")])
        assert any("PLANT_01" in p for p in problems)


class TestFileLoading:
    def test_reads_a_csv_from_disk(self, tmp_path):
        path = tmp_path / "daily_reference_2026-08-21.csv"
        _frame().to_csv(path, index=False)

        feed = load_reference_file(path, PLANTS, date(2026, 8, 21))
        assert feed.plant_count == 2
        assert feed.source == str(path)

    def test_a_missing_file_names_the_producing_component(self, tmp_path):
        with pytest.raises(ReferenceFeedError, match="Member 1"):
            load_reference_file(tmp_path / "absent.csv", PLANTS)

    def test_filename_convention(self):
        assert reference_path("/data/daily", date(2026, 8, 21)).name == (
            "daily_reference_2026-08-21.csv"
        )
