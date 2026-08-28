"""Plant list, live snapshot, history and inverter-configuration endpoints."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query

from app.config import Settings
from app.dependencies import get_connection, get_settings
from app.freshness import data_status
from app.models.plant import InverterInfo, PlantHistoryPoint, PlantHistoryResponse, PlantLive, PlantSummary
from app.repositories import plants as plants_repo

router = APIRouter(prefix="/api/v1/plants", tags=["plants"])

# Guards against an accidentally unbounded query (e.g. from=2000-01-01) turning
# into a full-table scan and a multi-megabyte response.
_MAX_HISTORY_RANGE_DAYS = 31


@router.get("", response_model=list[PlantSummary])
def list_plants(
    conn: Any = Depends(get_connection),
    settings: Settings = Depends(get_settings),
) -> list[PlantSummary]:
    rows = plants_repo.list_plants(conn)
    return [
        PlantSummary(
            id=row["id"],
            name=row["name"],
            capacity_kw=row["capacity_kw"],
            active=row["active"],
            current_power_kw=row["current_power_kw"],
            performance_pct=row["performance_pct"],
            availability_pct=row["availability_pct"],
            data_status=data_status(row["last_update"], settings.stale_data_seconds)
            if row["last_update"] is not None
            else "NO_DATA",
            last_update=row["last_update"],
        )
        for row in rows
    ]


def _require_plant(conn: Any, plant_id: str) -> dict:
    plant = plants_repo.get_plant(conn, plant_id)
    if plant is None:
        raise HTTPException(status_code=404, detail=f"Unknown plant: {plant_id}")
    return plant


@router.get("/{plant_id}/live", response_model=PlantLive)
def get_plant_live(
    plant_id: str,
    conn: Any = Depends(get_connection),
    settings: Settings = Depends(get_settings),
) -> PlantLive:
    plant = _require_plant(conn, plant_id)
    metric = plants_repo.get_latest_plant_metric(conn, plant_id)

    if metric is None:
        return PlantLive(
            plant_id=plant["id"],
            plant_name=plant["name"],
            capacity_kw=plant["capacity_kw"],
            timestamp=None,
            current_power_kw=None,
            avg_power_kw=None,
            expected_power_kw=None,
            availability_pct=None,
            performance_pct=None,
            estimated_loss_kw=None,
            online_inverters=None,
            offline_inverters=None,
            data_status="NO_DATA",
        )

    return PlantLive(
        plant_id=plant["id"],
        plant_name=plant["name"],
        capacity_kw=plant["capacity_kw"],
        timestamp=metric["window_end"],
        current_power_kw=metric["current_power_kw"],
        avg_power_kw=metric["avg_power_kw"],
        expected_power_kw=metric["expected_power_kw"],
        availability_pct=metric["availability_pct"],
        performance_pct=metric["performance_pct"],
        estimated_loss_kw=metric["estimated_loss_kw"],
        online_inverters=metric["online_inverters"],
        offline_inverters=metric["offline_inverters"],
        data_status=data_status(metric["window_end"], settings.stale_data_seconds),
    )


@router.get("/{plant_id}/history", response_model=PlantHistoryResponse)
def get_plant_history(
    plant_id: str,
    from_: datetime = Query(..., alias="from"),
    to: datetime = Query(...),
    conn: Any = Depends(get_connection),
) -> PlantHistoryResponse:
    _require_plant(conn, plant_id)

    if from_ > to:
        raise HTTPException(status_code=422, detail="'from' must not be after 'to'.")
    if (to - from_).days > _MAX_HISTORY_RANGE_DAYS:
        raise HTTPException(
            status_code=422,
            detail=f"Requested range exceeds the maximum of {_MAX_HISTORY_RANGE_DAYS} days.",
        )

    rows = plants_repo.get_plant_history(conn, plant_id, from_, to)
    return PlantHistoryResponse(
        plant_id=plant_id,
        points=[
            PlantHistoryPoint(
                window_start=row["window_start"],
                window_end=row["window_end"],
                current_power_kw=row["current_power_kw"],
                avg_power_kw=row["avg_power_kw"],
                expected_power_kw=row["expected_power_kw"],
                performance_pct=row["performance_pct"],
                availability_pct=row["availability_pct"],
            )
            for row in rows
        ],
    )


@router.get("/{plant_id}/inverters", response_model=list[InverterInfo])
def get_plant_inverters(plant_id: str, conn: Any = Depends(get_connection)) -> list[InverterInfo]:
    _require_plant(conn, plant_id)
    rows = plants_repo.list_inverters(conn, plant_id)
    return [
        InverterInfo(
            id=row["id"],
            plant_id=row["plant_id"],
            name=row["name"],
            rated_power_kw=row["rated_power_kw"],
            active=row["active"],
        )
        for row in rows
    ]
