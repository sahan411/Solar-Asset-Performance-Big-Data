"""Parameterized SQL for the plant endpoints."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from app.db import dict_cursor

_LIST_PLANTS_WITH_LATEST = """
    SELECT p.id, p.name, p.capacity_kw, p.active,
           m.current_power_kw, m.performance_pct, m.availability_pct, m.window_end AS last_update
      FROM plants p
      LEFT JOIN LATERAL (
          SELECT current_power_kw, performance_pct, availability_pct, window_end
            FROM live_plant_metrics lpm
           WHERE lpm.plant_id = p.id
           ORDER BY lpm.window_end DESC
           LIMIT 1
      ) m ON TRUE
     ORDER BY p.id
"""

_GET_PLANT = "SELECT id, name, capacity_kw, active FROM plants WHERE id = %s"

_LATEST_PLANT_METRIC = """
    SELECT window_start, window_end, current_power_kw, avg_power_kw,
           expected_power_kw, availability_pct, performance_pct,
           estimated_loss_kw, online_inverters, offline_inverters
      FROM live_plant_metrics
     WHERE plant_id = %s
     ORDER BY window_end DESC
     LIMIT 1
"""

_PLANT_HISTORY = """
    SELECT window_start, window_end, current_power_kw, avg_power_kw,
           expected_power_kw, performance_pct, availability_pct
      FROM live_plant_metrics
     WHERE plant_id = %s AND window_end BETWEEN %s AND %s
     ORDER BY window_end ASC
"""

_LIST_INVERTERS = """
    SELECT id, plant_id, name, rated_power_kw, active
      FROM inverters
     WHERE plant_id = %s
     ORDER BY id
"""


def list_plants(conn: Any) -> list[dict]:
    with dict_cursor(conn) as cur:
        cur.execute(_LIST_PLANTS_WITH_LATEST)
        return list(cur.fetchall())


def get_plant(conn: Any, plant_id: str) -> dict | None:
    with dict_cursor(conn) as cur:
        cur.execute(_GET_PLANT, (plant_id,))
        return cur.fetchone()


def get_latest_plant_metric(conn: Any, plant_id: str) -> dict | None:
    with dict_cursor(conn) as cur:
        cur.execute(_LATEST_PLANT_METRIC, (plant_id,))
        return cur.fetchone()


def get_plant_history(conn: Any, plant_id: str, from_ts: datetime, to_ts: datetime) -> list[dict]:
    with dict_cursor(conn) as cur:
        cur.execute(_PLANT_HISTORY, (plant_id, from_ts, to_ts))
        return list(cur.fetchall())


def list_inverters(conn: Any, plant_id: str) -> list[dict]:
    with dict_cursor(conn) as cur:
        cur.execute(_LIST_INVERTERS, (plant_id,))
        return list(cur.fetchall())
