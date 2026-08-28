"""Response models for the portfolio endpoints."""

from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel

from app.freshness import DataStatus


class PortfolioLive(BaseModel):
    """`GET /api/v1/portfolio/live` — latest live_portfolio_metrics row plus
    static plant metadata. Nullable fields follow docs/member-2-handoff.md:
    NULL means "not meaningful right now" (e.g. night), never zero."""

    timestamp: datetime | None
    installed_capacity_kw: float
    current_power_kw: float | None
    avg_power_kw: float | None
    expected_power_kw: float | None
    availability_pct: float | None
    performance_pct: float | None
    online_inverters: int | None
    offline_inverters: int | None
    estimated_loss_kw: float | None
    data_status: DataStatus


class DailyPortfolioTotals(BaseModel):
    """Portfolio totals for one simulated day, aggregated energy-first per the
    master specification: sum the kWh columns, then divide — never average
    each plant's percentage."""

    simulation_date: date
    actual_generation_kwh: float
    expected_generation_kwh: float
    performance_pct: float | None
    lost_energy_kwh: float | None
    actual_revenue: float | None
    lost_revenue: float | None


class DailyPlantRow(BaseModel):
    plant_id: str
    plant_name: str
    actual_generation_kwh: float
    expected_generation_kwh: float
    performance_pct: float | None
    availability_pct: float | None
    lost_energy_kwh: float | None
    lost_revenue: float | None
    alert_count: int
    maintenance_flag: bool


class PortfolioDailyResponse(BaseModel):
    """`GET /api/v1/portfolio/daily?date=`."""

    portfolio: DailyPortfolioTotals
    plants: list[DailyPlantRow]
