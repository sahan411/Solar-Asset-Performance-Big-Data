"""Daily reconciliation: expected versus actual, and what the gap costs.

This is where the platform answers its central business question — which assets
underperformed, by how much energy, and what that was worth. It joins the day's
actual generation (from the raw archive) against the day's expectation and
commercial rate (from the reference feed).

The join happens in plain Python rather than Spark. Both sides are at most one
row per plant — five rows in the demo portfolio — so a distributed join would add
latency and obscurity for no benefit, and the arithmetic stays trivially
testable against hand-calculated figures.

DATA QUALITY POLICY
Two categories, deliberately distinct:

  problems  make the day's reconciliation untrustworthy and abort it. Publishing
            a confidently wrong revenue figure is worse than publishing none.
  warnings  are recorded and reported but do not stop the run, because the
            resulting summary is still correct and useful.

A plant that produced nothing all day is a WARNING, not a problem: zero
generation against a positive expectation is a perfectly valid — and highly
significant — business result. Refusing to report it would hide the worst day a
plant can have.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any, Mapping, Sequence

from processing.common.db import execute_batch, fetch_all
from processing.common.logging import get_logger

log = get_logger("batch-reconcile")

# A day's generation this far above forecast is not a sunny day, it is a unit or
# scaling error. Weather can beat a forecast; it cannot triple it.
IMPLAUSIBLE_PERFORMANCE_PCT = 300.0


class ReconciliationError(ValueError):
    """Raised when the day cannot be reconciled trustworthily."""

    def __init__(self, problems: Sequence[str], simulation_date: date) -> None:
        self.problems = list(problems)
        self.simulation_date = simulation_date
        listed = "\n  - ".join(self.problems)
        super().__init__(
            f"Daily reconciliation for {simulation_date.isoformat()} failed "
            f"({len(self.problems)} problem(s)):\n  - {listed}"
        )


@dataclass(frozen=True)
class PlantSummary:
    """One plant's reconciled day."""

    simulation_date: date
    plant_id: str
    actual_generation_kwh: float
    expected_generation_kwh: float
    performance_pct: float | None
    availability_pct: float | None
    downtime_minutes: float
    estimated_lost_energy_kwh: float
    ppa_rate_per_kwh: float
    estimated_actual_revenue: float
    estimated_lost_revenue: float
    alert_count: int
    maintenance_flag: bool

    def as_row(self) -> tuple:
        """Bind parameters in `daily_plant_summary` column order."""
        return (
            self.simulation_date,
            self.plant_id,
            self.actual_generation_kwh,
            self.expected_generation_kwh,
            self.performance_pct,
            self.availability_pct,
            self.downtime_minutes,
            self.estimated_lost_energy_kwh,
            self.ppa_rate_per_kwh,
            self.estimated_actual_revenue,
            self.estimated_lost_revenue,
            self.alert_count,
            self.maintenance_flag,
        )


@dataclass
class ReconciliationResult:
    simulation_date: date
    summaries: list[PlantSummary]
    warnings: list[str] = field(default_factory=list)

    @property
    def portfolio_actual_kwh(self) -> float:
        return sum(s.actual_generation_kwh for s in self.summaries)

    @property
    def portfolio_expected_kwh(self) -> float:
        return sum(s.expected_generation_kwh for s in self.summaries)

    @property
    def portfolio_lost_revenue(self) -> float:
        return sum(s.estimated_lost_revenue for s in self.summaries)

    @property
    def portfolio_performance_pct(self) -> float | None:
        """Weighted, never a mean of the plants' percentages."""
        expected = self.portfolio_expected_kwh
        if expected <= 0:
            return None
        return self.portfolio_actual_kwh / expected * 100.0


def reconcile_day(
    simulation_date: date,
    actuals: Mapping[str, Mapping[str, Any]],
    reference: Mapping[str, Mapping[str, Any]],
    alert_counts: Mapping[str, int] | None = None,
) -> ReconciliationResult:
    """Join actuals to expectations and price the difference.

    `actuals` is keyed by plant id (from `collect_plant_actuals`), `reference` by
    plant id (from `reference_by_plant`). The reference feed defines the
    portfolio: a plant present in telemetry but absent from the feed cannot be
    valued and is a problem.
    """
    problems: list[str] = []
    warnings: list[str] = []
    alert_counts = alert_counts or {}

    unknown = sorted(set(actuals) - set(reference))
    if unknown:
        problems.append(
            f"telemetry for plant(s) with no reference row: {', '.join(unknown)}"
        )

    if not reference:
        problems.append("no reference rows for this date; load the daily feed first")

    summaries: list[PlantSummary] = []

    for plant_id in sorted(reference):
        expectation = reference[plant_id]
        expected_kwh = float(expectation["expected_generation_kwh"])
        rate = float(expectation["ppa_rate_per_kwh"])
        maintenance = bool(expectation["maintenance_flag"])

        measured = actuals.get(plant_id)
        if measured is None:
            # No telemetry at all: a real and serious outcome, not a bad input.
            warnings.append(
                f"{plant_id}: no telemetry archived for this day; "
                "reported as zero generation"
            )
            measured = {
                "actual_generation_kwh": 0.0,
                "availability_pct": 0.0,
                "downtime_minutes": 0.0,
            }

        actual_kwh = float(measured["actual_generation_kwh"])

        if actual_kwh < 0:
            problems.append(f"{plant_id}: actual generation is negative ({actual_kwh:g} kWh)")
            continue
        if expected_kwh <= 0:
            problems.append(
                f"{plant_id}: expected generation must be positive, got {expected_kwh:g} kWh"
            )
            continue

        performance_pct = actual_kwh / expected_kwh * 100.0

        if performance_pct > IMPLAUSIBLE_PERFORMANCE_PCT:
            problems.append(
                f"{plant_id}: actual generation is {performance_pct:.0f}% of expected, "
                f"which indicates a unit or scaling error rather than weather"
            )
            continue
        if performance_pct > 100.0:
            # Plausible: a clearer day than forecast. Worth noting, not blocking.
            warnings.append(
                f"{plant_id}: generated {performance_pct:.1f}% of forecast "
                "(clearer than expected)"
            )
        if maintenance:
            # Kept in the report rather than excluded — the energy really was
            # lost, and hiding planned outages would flatter portfolio figures.
            warnings.append(
                f"{plant_id}: planned maintenance was flagged for this day; "
                "performance reflects it"
            )

        # Floored at zero: beating forecast is not "negative loss".
        lost_kwh = max(expected_kwh - actual_kwh, 0.0)

        summaries.append(
            PlantSummary(
                simulation_date=simulation_date,
                plant_id=plant_id,
                actual_generation_kwh=actual_kwh,
                expected_generation_kwh=expected_kwh,
                performance_pct=performance_pct,
                availability_pct=measured.get("availability_pct"),
                downtime_minutes=float(measured.get("downtime_minutes") or 0.0),
                estimated_lost_energy_kwh=lost_kwh,
                ppa_rate_per_kwh=rate,
                estimated_actual_revenue=actual_kwh * rate,
                estimated_lost_revenue=lost_kwh * rate,
                alert_count=int(alert_counts.get(plant_id, 0)),
                maintenance_flag=maintenance,
            )
        )

    if problems:
        raise ReconciliationError(problems, simulation_date)

    for warning in warnings:
        log.warning("reconciliation_warning", warning, simulation_date=simulation_date.isoformat())

    return ReconciliationResult(
        simulation_date=simulation_date, summaries=summaries, warnings=warnings
    )


def reference_by_plant(conn, simulation_date: date) -> dict[str, dict]:
    """Load the day's expectations from `daily_reference`, keyed by plant."""
    rows = fetch_all(
        conn,
        """
        SELECT plant_id, expected_generation_kwh, expected_peak_power_kw,
               ppa_rate_per_kwh, maintenance_flag
          FROM daily_reference
         WHERE simulation_date = %s
        """,
        (simulation_date,),
    )
    return {
        plant_id: {
            "expected_generation_kwh": expected,
            "expected_peak_power_kw": peak,
            "ppa_rate_per_kwh": rate,
            "maintenance_flag": maintenance,
        }
        for plant_id, expected, peak, rate, maintenance in rows
    }


def alert_counts_for_day(conn, simulation_date: date) -> dict[str, int]:
    """Alerts raised per plant on a simulated day.

    Counted by the alert's START date in UTC, so a fault is attributed to the day
    it began rather than the day it happened to be resolved.
    """
    rows = fetch_all(
        conn,
        """
        SELECT plant_id, COUNT(*)
          FROM alerts
         WHERE (started_at AT TIME ZONE 'UTC')::date = %s
         GROUP BY plant_id
        """,
        (simulation_date,),
    )
    return {plant_id: int(count) for plant_id, count in rows}


_UPSERT_SUMMARY = """
INSERT INTO daily_plant_summary (
    simulation_date, plant_id, actual_generation_kwh, expected_generation_kwh,
    performance_pct, availability_pct, downtime_minutes, estimated_lost_energy_kwh,
    ppa_rate_per_kwh, estimated_actual_revenue, estimated_lost_revenue,
    alert_count, maintenance_flag, computed_at
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
ON CONFLICT (simulation_date, plant_id) DO UPDATE SET
    actual_generation_kwh     = EXCLUDED.actual_generation_kwh,
    expected_generation_kwh   = EXCLUDED.expected_generation_kwh,
    performance_pct           = EXCLUDED.performance_pct,
    availability_pct          = EXCLUDED.availability_pct,
    downtime_minutes          = EXCLUDED.downtime_minutes,
    estimated_lost_energy_kwh = EXCLUDED.estimated_lost_energy_kwh,
    ppa_rate_per_kwh          = EXCLUDED.ppa_rate_per_kwh,
    estimated_actual_revenue  = EXCLUDED.estimated_actual_revenue,
    estimated_lost_revenue    = EXCLUDED.estimated_lost_revenue,
    alert_count               = EXCLUDED.alert_count,
    maintenance_flag          = EXCLUDED.maintenance_flag,
    computed_at               = NOW()
"""


def write_daily_summary(conn, result: ReconciliationResult) -> int:
    """Upsert the day's summaries, inside the caller's transaction.

    Upsert so re-running a day's DAG converges on the current answer instead of
    failing on the primary key — required for the demo reset and for reprocessing
    after a corrected reference feed.
    """
    rows = [summary.as_row() for summary in result.summaries]
    count = execute_batch(conn, _UPSERT_SUMMARY, rows)

    log.info(
        "daily_summary_written",
        f"Reconciled {count} plant(s) for {result.simulation_date.isoformat()}",
        simulation_date=result.simulation_date.isoformat(),
        plants=count,
        portfolio_performance_pct=result.portfolio_performance_pct,
        portfolio_lost_revenue=result.portfolio_lost_revenue,
    )
    return count
