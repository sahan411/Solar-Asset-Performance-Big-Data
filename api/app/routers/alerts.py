"""`GET /api/v1/alerts` — operational alerts, with optional status/plant/severity filters."""

from __future__ import annotations

from typing import Any, Literal

from fastapi import APIRouter, Depends, Query

from app.dependencies import get_connection
from app.models.alert import Alert
from app.repositories import alerts as alerts_repo

router = APIRouter(prefix="/api/v1/alerts", tags=["alerts"])

# Keep optional filters small, per the playbook — nothing beyond what the
# dashboard's alerts screen actually needs.
_MAX_LIMIT = 500


@router.get("", response_model=list[Alert])
def list_alerts(
    status: Literal["active", "resolved"] | None = Query(None),
    plant_id: str | None = Query(None),
    severity: Literal["WARNING", "CRITICAL"] | None = Query(None),
    limit: int = Query(100, ge=1, le=_MAX_LIMIT),
    conn: Any = Depends(get_connection),
) -> list[Alert]:
    db_status = status.upper() if status is not None else None
    rows = alerts_repo.list_alerts(conn, status=db_status, plant_id=plant_id, severity=severity, limit=limit)
    return [Alert(**row) for row in rows]
