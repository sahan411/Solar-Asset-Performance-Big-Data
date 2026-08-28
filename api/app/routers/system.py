"""`/health`, `/ready` and `/metrics` — the three endpoints outside `/api/v1`."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Response

from app.db import Database
from app.dependencies import get_database
from app.metrics import render_latest

router = APIRouter(tags=["system"])


@router.get("/health")
def health() -> dict:
    """The process is alive. Must not fail merely because PostgreSQL is down —
    that distinction is what /ready is for."""
    return {"status": "ok", "service": "solariq-api"}


@router.get("/ready")
def ready(response: Response, database: Database = Depends(get_database)) -> dict:
    """The API is ready to serve real data: DB reachable and a key table query
    succeeds."""
    if database.key_table_query_ok():
        return {"status": "ready", "database": "ok"}

    response.status_code = 503
    return {"status": "not_ready", "database": "error"}


@router.get("/metrics")
def metrics() -> Response:
    body, content_type = render_latest()
    return Response(content=body, media_type=content_type)
