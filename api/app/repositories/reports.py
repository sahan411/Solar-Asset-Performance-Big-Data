"""Parameterized SQL backing the daily reconciliation report.

Deliberately reuses app.repositories.portfolio's daily-totals/rows queries —
the report is a superset of the daily portfolio endpoint (adds best/worst
performer), and building it from a second, independent query path would risk
the two disagreeing over rounding or filters.
"""

from __future__ import annotations

from app.repositories.portfolio import get_daily_plant_rows, get_daily_totals

__all__ = ["get_daily_totals", "get_daily_plant_rows", "best_and_worst_performer"]


def best_and_worst_performer(plant_rows: list[dict]) -> tuple[dict | None, dict | None]:
    """Rank by performance_pct, excluding plants where it is NULL (no meaningful
    expectation for the day, per docs/member-2-handoff.md)."""
    ranked = [row for row in plant_rows if row["performance_pct"] is not None]
    if not ranked:
        return None, None
    ranked_sorted = sorted(ranked, key=lambda row: row["performance_pct"])
    return ranked_sorted[-1], ranked_sorted[0]
