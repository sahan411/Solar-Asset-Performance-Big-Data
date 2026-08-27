"""Expected-versus-actual reconciliation and revenue impact (Milestone 12).

Every figure here is hand-calculable, because these are the numbers the project
exists to produce and the ones a viva will ask about.
"""

from __future__ import annotations

from datetime import date

import pytest

from processing.batch.reconcile import (
    IMPLAUSIBLE_PERFORMANCE_PCT,
    ReconciliationError,
    reconcile_day,
)

DAY = date(2026, 8, 21)


def _actual(generation=800.0, availability=95.0, downtime=12.0):
    return {
        "actual_generation_kwh": generation,
        "availability_pct": availability,
        "downtime_minutes": downtime,
    }


def _reference(expected=1000.0, rate=0.15, maintenance=False):
    return {
        "expected_generation_kwh": expected,
        "expected_peak_power_kw": 800.0,
        "ppa_rate_per_kwh": rate,
        "maintenance_flag": maintenance,
    }


def _one_plant(actual=None, reference=None, alert_counts=None):
    return reconcile_day(
        DAY,
        {"PLANT_01": actual or _actual()},
        {"PLANT_01": reference or _reference()},
        alert_counts,
    )


class TestTheWorkedExample:
    """The specification's own example: 1000 expected, 800 actual, 0.15 rate."""

    def test_performance_is_eighty_percent(self):
        assert _one_plant().summaries[0].performance_pct == pytest.approx(80.0)

    def test_lost_energy_is_two_hundred_kwh(self):
        assert _one_plant().summaries[0].estimated_lost_energy_kwh == pytest.approx(200.0)

    def test_actual_revenue_is_one_hundred_and_twenty(self):
        assert _one_plant().summaries[0].estimated_actual_revenue == pytest.approx(120.0)

    def test_lost_revenue_is_thirty(self):
        assert _one_plant().summaries[0].estimated_lost_revenue == pytest.approx(30.0)


class TestCarriedFields:
    def test_availability_and_downtime_pass_through(self):
        summary = _one_plant(_actual(availability=91.5, downtime=42.0)).summaries[0]
        assert summary.availability_pct == pytest.approx(91.5)
        assert summary.downtime_minutes == pytest.approx(42.0)

    def test_the_rate_used_is_recorded_on_the_summary(self):
        """The report must stay reproducible if the rate is later revised."""
        summary = _one_plant(reference=_reference(rate=0.22)).summaries[0]
        assert summary.ppa_rate_per_kwh == pytest.approx(0.22)

    def test_alert_counts_are_attached(self):
        summary = _one_plant(alert_counts={"PLANT_01": 3}).summaries[0]
        assert summary.alert_count == 3

    def test_missing_alert_count_defaults_to_zero(self):
        assert _one_plant().summaries[0].alert_count == 0

    def test_row_ordering_matches_the_table_contract(self):
        row = _one_plant().summaries[0].as_row()
        assert row[0] == DAY
        assert row[1] == "PLANT_01"
        assert row[2] == pytest.approx(800.0)
        assert len(row) == 13


class TestEdgeCases:
    def test_a_plant_that_generated_nothing_is_reported_not_skipped(self):
        """Zero generation is the worst day a plant can have — and a valid result."""
        result = reconcile_day(DAY, {}, {"PLANT_01": _reference()})
        summary = result.summaries[0]

        assert summary.actual_generation_kwh == pytest.approx(0.0)
        assert summary.performance_pct == pytest.approx(0.0)
        assert summary.estimated_lost_energy_kwh == pytest.approx(1000.0)
        assert summary.estimated_lost_revenue == pytest.approx(150.0)
        assert summary.availability_pct == pytest.approx(0.0)
        assert any("no telemetry" in w for w in result.warnings)

    def test_beating_the_forecast_does_not_produce_negative_loss(self):
        result = _one_plant(_actual(generation=1100.0))
        summary = result.summaries[0]

        assert summary.performance_pct == pytest.approx(110.0)
        assert summary.estimated_lost_energy_kwh == pytest.approx(0.0)
        assert summary.estimated_lost_revenue == pytest.approx(0.0)
        # Revenue still reflects everything actually generated.
        assert summary.estimated_actual_revenue == pytest.approx(165.0)
        assert any("clearer than expected" in w for w in result.warnings)

    def test_a_maintenance_plant_stays_in_the_report(self):
        """Excluding planned outages would flatter the portfolio."""
        result = _one_plant(reference=_reference(maintenance=True))
        summary = result.summaries[0]

        assert summary.maintenance_flag is True
        assert summary.estimated_lost_energy_kwh == pytest.approx(200.0)
        assert any("maintenance" in w for w in result.warnings)

    def test_a_zero_rate_yields_zero_revenue_without_error(self):
        summary = _one_plant(reference=_reference(rate=0.0)).summaries[0]
        assert summary.estimated_actual_revenue == pytest.approx(0.0)
        assert summary.estimated_lost_revenue == pytest.approx(0.0)


class TestDataQuality:
    def test_negative_actual_generation_aborts_the_day(self):
        with pytest.raises(ReconciliationError, match="negative"):
            _one_plant(_actual(generation=-5.0))

    def test_non_positive_expectation_aborts_the_day(self):
        with pytest.raises(ReconciliationError, match="must be positive"):
            _one_plant(reference=_reference(expected=0.0))

    def test_telemetry_for_an_unreferenced_plant_aborts_the_day(self):
        """It cannot be valued, so the portfolio total would be wrong."""
        with pytest.raises(ReconciliationError, match="no reference row"):
            reconcile_day(DAY, {"PLANT_99": _actual()}, {"PLANT_01": _reference()})

    def test_a_missing_reference_feed_aborts_the_day(self):
        with pytest.raises(ReconciliationError, match="no reference rows"):
            reconcile_day(DAY, {"PLANT_01": _actual()}, {})

    def test_implausible_output_aborts_rather_than_reporting_a_windfall(self):
        """Weather can beat a forecast; it cannot quadruple it."""
        with pytest.raises(ReconciliationError, match="unit or scaling error"):
            _one_plant(_actual(generation=4000.0))

    def test_the_plausibility_boundary_is_respected(self):
        just_under = IMPLAUSIBLE_PERFORMANCE_PCT / 100.0 * 1000.0 - 1
        result = _one_plant(_actual(generation=just_under))
        assert result.summaries[0].performance_pct < IMPLAUSIBLE_PERFORMANCE_PCT

    def test_every_problem_is_reported_together(self):
        with pytest.raises(ReconciliationError) as exc:
            reconcile_day(
                DAY,
                {"PLANT_01": _actual(generation=-1.0), "PLANT_99": _actual()},
                {"PLANT_01": _reference(), "PLANT_02": _reference(expected=-5.0)},
            )
        assert len(exc.value.problems) >= 3


class TestPortfolioRollUp:
    def _portfolio(self):
        return reconcile_day(
            DAY,
            {
                "PLANT_01": _actual(generation=4750.0),
                "PLANT_02": _actual(generation=250.0),
            },
            {
                "PLANT_01": _reference(expected=5000.0, rate=0.15),
                "PLANT_02": _reference(expected=500.0, rate=0.20),
            },
        )

    def test_totals_sum_across_plants(self):
        result = self._portfolio()
        assert result.portfolio_actual_kwh == pytest.approx(5000.0)
        assert result.portfolio_expected_kwh == pytest.approx(5500.0)

    def test_portfolio_performance_is_energy_weighted(self):
        """95% and 50% weighted by size is ~91%, not the 72.5% mean."""
        result = self._portfolio()
        assert result.portfolio_performance_pct == pytest.approx(90.909, abs=0.01)
        assert result.portfolio_performance_pct != pytest.approx(72.5, abs=0.01)

    def test_lost_revenue_uses_each_plants_own_rate(self):
        """Rates differ per plant, so the portfolio total is not one rate times one loss."""
        result = self._portfolio()
        # PLANT_01: 250 kWh lost * 0.15 = 37.50
        # PLANT_02: 250 kWh lost * 0.20 = 50.00
        assert result.portfolio_lost_revenue == pytest.approx(87.50)


def test_a_day_with_no_reference_rows_raises_rather_than_reporting_zero():
    """An empty portfolio total would look like a real result. It is not."""
    with pytest.raises(ReconciliationError, match="no reference rows"):
        reconcile_day(DAY, {}, {})


def test_summaries_are_ordered_by_plant_for_stable_reports():
    result = reconcile_day(
        DAY,
        {},
        {
            "PLANT_03": _reference(),
            "PLANT_01": _reference(),
            "PLANT_02": _reference(),
        },
    )
    assert [s.plant_id for s in result.summaries] == ["PLANT_01", "PLANT_02", "PLANT_03"]
