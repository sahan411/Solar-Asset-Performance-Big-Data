"""Response model for the alerts endpoint."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel

AlertType = Literal["UNDERPERFORMANCE", "INVERTER_OFFLINE", "TELEMETRY_GAP"]
Severity = Literal["WARNING", "CRITICAL"]
AlertStatus = Literal["ACTIVE", "RESOLVED"]


class Alert(BaseModel):
    """A row of `alerts`. Field names and enum values match the DB exactly
    (see storage/migrations/003_alerts.sql) — the API does not relabel them."""

    id: str
    plant_id: str
    inverter_id: str | None
    alert_type: AlertType
    severity: Severity
    message: str
    started_at: datetime
    ended_at: datetime | None
    status: AlertStatus
    estimated_loss_kwh: float | None
    estimated_revenue_loss: float | None
