"""Parameterized SQL for the alerts endpoint.

Filters are appended conditionally rather than built with string formatting,
so every value stays a bind parameter regardless of which filters are active.
"""

from __future__ import annotations

from typing import Any

from app.db import dict_cursor

_BASE_QUERY = """
    SELECT id, plant_id, inverter_id, alert_type, severity, message,
           started_at, ended_at, status, estimated_loss_kwh, estimated_revenue_loss
      FROM alerts
"""

# ACTIVE first (most relevant to an operator), then newest-started within each group.
_ORDER_BY = " ORDER BY (status = 'ACTIVE') DESC, started_at DESC LIMIT %s"


def list_alerts(
    conn: Any,
    status: str | None = None,
    plant_id: str | None = None,
    severity: str | None = None,
    limit: int = 100,
) -> list[dict]:
    clauses: list[str] = []
    params: list[Any] = []

    if status is not None:
        clauses.append("status = %s")
        params.append(status)
    if plant_id is not None:
        clauses.append("plant_id = %s")
        params.append(plant_id)
    if severity is not None:
        clauses.append("severity = %s")
        params.append(severity)

    query = _BASE_QUERY
    if clauses:
        query += " WHERE " + " AND ".join(clauses)
    query += _ORDER_BY
    params.append(limit)

    with dict_cursor(conn) as cur:
        cur.execute(query, tuple(params))
        return list(cur.fetchall())
