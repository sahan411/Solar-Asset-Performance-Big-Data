"""Daily report endpoint: no-data 404 and best/worst performer ranking."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.dependencies import get_connection
from app.main import app
from app.repositories import reports as reports_repo

client = TestClient(app)


@pytest.fixture(autouse=True)
def _override_connection():
    app.dependency_overrides[get_connection] = lambda: None
    yield
    app.dependency_overrides.clear()


def test_daily_report_returns_404_when_the_dag_has_not_run(monkeypatch):
    monkeypatch.setattr(reports_repo, "get_daily_totals", lambda conn, date: None)

    response = client.get("/api/v1/reports/daily", params={"date": "2026-08-21"})

    assert response.status_code == 404


def test_daily_report_ranks_best_and_worst_performer(monkeypatch):
    monkeypatch.setattr(
        reports_repo,
        "get_daily_totals",
        lambda conn, date: {
            "actual_kwh": 100.0,
            "expected_kwh": 110.0,
            "performance_pct": 90.9,
            "lost_kwh": 10.0,
            "revenue": 15.0,
            "lost_revenue": 1.5,
        },
    )
    monkeypatch.setattr(
        reports_repo,
        "get_daily_plant_rows",
        lambda conn, date: [
            {
                "plant_id": "PLANT_01",
                "plant_name": "North Ridge",
                "actual_generation_kwh": 60.0,
                "expected_generation_kwh": 60.0,
                "performance_pct": 100.0,
                "availability_pct": 100.0,
                "estimated_lost_energy_kwh": 0.0,
                "estimated_lost_revenue": 0.0,
                "alert_count": 0,
                "maintenance_flag": False,
            },
            {
                "plant_id": "PLANT_02",
                "plant_name": "South Field",
                "actual_generation_kwh": 40.0,
                "expected_generation_kwh": 50.0,
                "performance_pct": 80.0,
                "availability_pct": 95.0,
                "estimated_lost_energy_kwh": 10.0,
                "estimated_lost_revenue": 1.5,
                "alert_count": 1,
                "maintenance_flag": False,
            },
        ],
    )

    response = client.get("/api/v1/reports/daily", params={"date": "2026-08-21"})

    assert response.status_code == 200
    body = response.json()
    assert body["best_performer"]["plant_id"] == "PLANT_01"
    assert body["worst_performer"]["plant_id"] == "PLANT_02"


def test_daily_report_has_no_ranking_when_every_plant_lacks_a_performance_figure(monkeypatch):
    monkeypatch.setattr(
        reports_repo,
        "get_daily_totals",
        lambda conn, date: {
            "actual_kwh": 0.0,
            "expected_kwh": 0.0,
            "performance_pct": None,
            "lost_kwh": None,
            "revenue": None,
            "lost_revenue": None,
        },
    )
    monkeypatch.setattr(
        reports_repo,
        "get_daily_plant_rows",
        lambda conn, date: [
            {
                "plant_id": "PLANT_01",
                "plant_name": "North Ridge",
                "actual_generation_kwh": 0.0,
                "expected_generation_kwh": 0.0,
                "performance_pct": None,
                "availability_pct": None,
                "estimated_lost_energy_kwh": None,
                "estimated_lost_revenue": None,
                "alert_count": 0,
                "maintenance_flag": True,
            }
        ],
    )

    response = client.get("/api/v1/reports/daily", params={"date": "2026-08-21"})

    assert response.status_code == 200
    body = response.json()
    assert body["best_performer"] is None
    assert body["worst_performer"] is None
