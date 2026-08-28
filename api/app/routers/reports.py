"""`GET /api/v1/reports/daily` — the structured daily reconciliation report."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException

from app.dependencies import get_connection
from app.models.portfolio import DailyPlantRow, DailyPortfolioTotals
from app.models.report import DailyReport, PerformerRef
from app.repositories import reports as reports_repo

router = APIRouter(prefix="/api/v1/reports", tags=["reports"])


@router.get("/daily", response_model=DailyReport)
def get_daily_report(date: date, conn: Any = Depends(get_connection)) -> DailyReport:
    totals = reports_repo.get_daily_totals(conn, date)
    if totals is None:
        raise HTTPException(
            status_code=404,
            detail=f"No daily reconciliation report exists yet for {date.isoformat()}.",
        )

    plant_rows = reports_repo.get_daily_plant_rows(conn, date)
    best, worst = reports_repo.best_and_worst_performer(plant_rows)

    return DailyReport(
        simulation_date=date,
        portfolio=DailyPortfolioTotals(
            simulation_date=date,
            actual_generation_kwh=totals["actual_kwh"],
            expected_generation_kwh=totals["expected_kwh"],
            performance_pct=totals["performance_pct"],
            lost_energy_kwh=totals["lost_kwh"],
            actual_revenue=totals["revenue"],
            lost_revenue=totals["lost_revenue"],
        ),
        plants=[
            DailyPlantRow(
                plant_id=row["plant_id"],
                plant_name=row["plant_name"],
                actual_generation_kwh=row["actual_generation_kwh"],
                expected_generation_kwh=row["expected_generation_kwh"],
                performance_pct=row["performance_pct"],
                availability_pct=row["availability_pct"],
                lost_energy_kwh=row["estimated_lost_energy_kwh"],
                lost_revenue=row["estimated_lost_revenue"],
                alert_count=row["alert_count"],
                maintenance_flag=row["maintenance_flag"],
            )
            for row in plant_rows
        ],
        best_performer=PerformerRef(
            plant_id=best["plant_id"], plant_name=best["plant_name"], performance_pct=best["performance_pct"]
        )
        if best is not None
        else None,
        worst_performer=PerformerRef(
            plant_id=worst["plant_id"], plant_name=worst["plant_name"], performance_pct=worst["performance_pct"]
        )
        if worst is not None
        else None,
        generated_at=datetime.now(timezone.utc),
    )
