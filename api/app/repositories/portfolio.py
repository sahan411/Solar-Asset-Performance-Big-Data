"""Parameterized SQL for the portfolio endpoints.

Reference queries are the ones documented in the Member 3 playbook (section 26)
and docs/member-2-handoff.md — repository functions are thin wrappers around
them, never SQL built from string interpolation.
"""

from __future__ import annotations

from datetime import date
from typing import Any

from app.db import dict_cursor

_LATEST_PORTFOLIO_METRIC = """
    SELECT window_start, window_end, current_power_kw, avg_power_kw,
           expected_power_kw, availability_pct, performance_pct,
           estimated_loss_kw, online_inverters, offline_inverters
      FROM live_portfolio_metrics
     ORDER BY window_end DESC
     LIMIT 1
"""

_INSTALLED_CAPACITY = "SELECT COALESCE(SUM(capacity_kw), 0) AS capacity_kw FROM plants WHERE active"

_DAILY_TOTALS = """
    SELECT SUM(actual_generation_kwh)                                            AS actual_kwh,
           SUM(expected_generation_kwh)                                          AS expected_kwh,
           SUM(actual_generation_kwh) / NULLIF(SUM(expected_generation_kwh), 0) * 100
                                                                                  AS performance_pct,
           SUM(estimated_lost_energy_kwh)                                        AS lost_kwh,
           SUM(estimated_actual_revenue)                                         AS revenue,
           SUM(estimated_lost_revenue)                                           AS lost_revenue,
           COUNT(*)                                                              AS plant_count
      FROM daily_plant_summary
     WHERE simulation_date = %s
"""

_DAILY_PLANT_ROWS = """
    SELECT s.plant_id, p.name AS plant_name, s.actual_generation_kwh,
           s.expected_generation_kwh, s.performance_pct, s.availability_pct,
           s.estimated_lost_energy_kwh, s.estimated_lost_revenue, s.alert_count,
           s.maintenance_flag
      FROM daily_plant_summary s
      JOIN plants p ON p.id = s.plant_id
     WHERE s.simulation_date = %s
     ORDER BY s.performance_pct ASC NULLS LAST
"""


def get_latest_portfolio_metric(conn: Any) -> dict | None:
    with dict_cursor(conn) as cur:
        cur.execute(_LATEST_PORTFOLIO_METRIC)
        return cur.fetchone()


def get_installed_capacity_kw(conn: Any) -> float:
    with dict_cursor(conn) as cur:
        cur.execute(_INSTALLED_CAPACITY)
        row = cur.fetchone()
        return float(row["capacity_kw"]) if row else 0.0


def get_daily_totals(conn: Any, simulation_date: date) -> dict | None:
    """None when no rows exist for the date — the batch has not run yet."""
    with dict_cursor(conn) as cur:
        cur.execute(_DAILY_TOTALS, (simulation_date,))
        row = cur.fetchone()
        if row is None or row["plant_count"] == 0:
            return None
        return row


def get_daily_plant_rows(conn: Any, simulation_date: date) -> list[dict]:
    with dict_cursor(conn) as cur:
        cur.execute(_DAILY_PLANT_ROWS, (simulation_date,))
        return list(cur.fetchall())
