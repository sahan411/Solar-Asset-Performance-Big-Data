"""Response model for the daily reconciliation report."""

from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel

from app.models.portfolio import DailyPlantRow, DailyPortfolioTotals


class PerformerRef(BaseModel):
    plant_id: str
    plant_name: str
    performance_pct: float


class DailyReport(BaseModel):
    """`GET /api/v1/reports/daily?date=`. 404s upstream when no
    daily_plant_summary rows exist for the date — the DAG simply has not run
    for that simulated day yet, which is a normal, expected state."""

    simulation_date: date
    portfolio: DailyPortfolioTotals
    plants: list[DailyPlantRow]
    best_performer: PerformerRef | None
    worst_performer: PerformerRef | None
    generated_at: datetime
