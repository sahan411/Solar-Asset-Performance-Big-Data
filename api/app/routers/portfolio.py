"""`GET /api/v1/portfolio/live` and `GET /api/v1/portfolio/daily`."""

from __future__ import annotations

from datetime import date
from typing import Any

from fastapi import APIRouter, Depends, HTTPException

from app.config import Settings
from app.dependencies import get_connection, get_settings
from app.freshness import data_status
from app.models.portfolio import DailyPlantRow, DailyPortfolioTotals, PortfolioDailyResponse, PortfolioLive
from app.repositories import portfolio as portfolio_repo

router = APIRouter(prefix="/api/v1/portfolio", tags=["portfolio"])


@router.get("/live", response_model=PortfolioLive)
def get_portfolio_live(
    conn: Any = Depends(get_connection),
    settings: Settings = Depends(get_settings),
) -> PortfolioLive:
    metric = portfolio_repo.get_latest_portfolio_metric(conn)
    installed_capacity_kw = portfolio_repo.get_installed_capacity_kw(conn)

    if metric is None:
        return PortfolioLive(
            timestamp=None,
            installed_capacity_kw=installed_capacity_kw,
            current_power_kw=None,
            avg_power_kw=None,
            expected_power_kw=None,
            availability_pct=None,
            performance_pct=None,
            online_inverters=None,
            offline_inverters=None,
            estimated_loss_kw=None,
            data_status="NO_DATA",
        )

    return PortfolioLive(
        timestamp=metric["window_end"],
        installed_capacity_kw=installed_capacity_kw,
        current_power_kw=metric["current_power_kw"],
        avg_power_kw=metric["avg_power_kw"],
        expected_power_kw=metric["expected_power_kw"],
        availability_pct=metric["availability_pct"],
        performance_pct=metric["performance_pct"],
        online_inverters=metric["online_inverters"],
        offline_inverters=metric["offline_inverters"],
        estimated_loss_kw=metric["estimated_loss_kw"],
        data_status=data_status(metric["window_end"], settings.stale_data_seconds),
    )


@router.get("/daily", response_model=PortfolioDailyResponse)
def get_portfolio_daily(date: date, conn: Any = Depends(get_connection)) -> PortfolioDailyResponse:
    totals = portfolio_repo.get_daily_totals(conn, date)
    if totals is None:
        raise HTTPException(
            status_code=404,
            detail=f"No daily reconciliation exists yet for {date.isoformat()}.",
        )

    plant_rows = portfolio_repo.get_daily_plant_rows(conn, date)

    return PortfolioDailyResponse(
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
    )
