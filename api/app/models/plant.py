"""Response models for the plant endpoints."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel

from app.freshness import DataStatus


class PlantSummary(BaseModel):
    """One row of `GET /api/v1/plants`."""

    id: str
    name: str
    capacity_kw: float
    active: bool
    current_power_kw: float | None = None
    performance_pct: float | None = None
    data_status: DataStatus | None = None
    last_update: datetime | None = None


class PlantLive(BaseModel):
    """`GET /api/v1/plants/{plant_id}/live`."""

    plant_id: str
    plant_name: str
    capacity_kw: float
    timestamp: datetime | None
    current_power_kw: float | None
    avg_power_kw: float | None
    expected_power_kw: float | None
    availability_pct: float | None
    performance_pct: float | None
    estimated_loss_kw: float | None
    online_inverters: int | None
    offline_inverters: int | None
    data_status: DataStatus


class PlantHistoryPoint(BaseModel):
    """One live_plant_metrics window, ordered ascending by window_end."""

    window_start: datetime
    window_end: datetime
    current_power_kw: float
    avg_power_kw: float
    expected_power_kw: float | None
    performance_pct: float | None
    availability_pct: float | None


class PlantHistoryResponse(BaseModel):
    plant_id: str
    points: list[PlantHistoryPoint]


class InverterInfo(BaseModel):
    """`GET /api/v1/plants/{plant_id}/inverters`.

    No per-inverter live metrics table exists (docs/member-2-handoff.md,
    section 9.7), so this is configuration only, plus the plant's aggregate
    online/offline counts rather than fabricated per-inverter state.
    """

    id: str
    plant_id: str
    name: str
    rated_power_kw: float
    active: bool
