"""Portfolio endpoints, with the repository layer monkeypatched.

Route tests exercise HTTP-shape concerns (status codes, freshness labelling,
404-on-no-data) — not SQL correctness, which belongs to the seeded-database
integration suite under tests/integration/.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.dependencies import get_connection, get_settings
from app.main import app
from app.repositories import portfolio as portfolio_repo

client = TestClient(app)

_SETTINGS = Settings(
    database_url="postgresql://unused",
    host="0.0.0.0",
    port=8000,
    log_level="INFO",
    cors_origins=("http://localhost:5173",),
    stale_data_seconds=60,
)


@pytest.fixture(autouse=True)
def _override_connection_and_settings():
    app.dependency_overrides[get_connection] = lambda: None
    app.dependency_overrides[get_settings] = lambda: _SETTINGS
    yield
    app.dependency_overrides.clear()


def test_portfolio_live_reports_no_data_when_the_pipeline_has_never_written(monkeypatch):
    monkeypatch.setattr(portfolio_repo, "get_latest_portfolio_metric", lambda conn: None)
    monkeypatch.setattr(portfolio_repo, "get_installed_capacity_kw", lambda conn: 25000.0)

    response = client.get("/api/v1/portfolio/live")

    assert response.status_code == 200
    body = response.json()
    assert body["data_status"] == "NO_DATA"
    assert body["current_power_kw"] is None
    assert body["installed_capacity_kw"] == 25000.0


def test_portfolio_live_is_live_when_the_latest_window_is_recent(monkeypatch):
    recent = datetime.now(timezone.utc) - timedelta(seconds=5)
    monkeypatch.setattr(
        portfolio_repo,
        "get_latest_portfolio_metric",
        lambda conn: {
            "window_start": recent - timedelta(seconds=15),
            "window_end": recent,
            "current_power_kw": 18700.0,
            "avg_power_kw": 18120.0,
            "expected_power_kw": 20200.0,
            "availability_pct": 98.1,
            "performance_pct": 92.6,
            "online_inverters": 28,
            "offline_inverters": 2,
            "estimated_loss_kw": 1510.0,
        },
    )
    monkeypatch.setattr(portfolio_repo, "get_installed_capacity_kw", lambda conn: 25000.0)

    response = client.get("/api/v1/portfolio/live")

    assert response.status_code == 200
    assert response.json()["data_status"] == "LIVE"


def test_portfolio_live_is_stale_past_the_configured_threshold(monkeypatch):
    old = datetime.now(timezone.utc) - timedelta(seconds=600)
    monkeypatch.setattr(
        portfolio_repo,
        "get_latest_portfolio_metric",
        lambda conn: {
            "window_start": old,
            "window_end": old,
            "current_power_kw": 400.0,
            "avg_power_kw": 400.0,
            "expected_power_kw": None,
            "availability_pct": None,
            "performance_pct": None,
            "online_inverters": 1,
            "offline_inverters": 0,
            "estimated_loss_kw": None,
        },
    )
    monkeypatch.setattr(portfolio_repo, "get_installed_capacity_kw", lambda conn: 25000.0)

    response = client.get("/api/v1/portfolio/live")

    assert response.status_code == 200
    body = response.json()
    assert body["data_status"] == "STALE"
    # Stale still shows the last known figure — it must never be replaced with 0.
    assert body["current_power_kw"] == 400.0


def test_portfolio_daily_returns_404_when_the_batch_has_not_run(monkeypatch):
    monkeypatch.setattr(portfolio_repo, "get_daily_totals", lambda conn, date: None)

    response = client.get("/api/v1/portfolio/daily", params={"date": "2026-08-21"})

    assert response.status_code == 404


def test_portfolio_daily_reports_energy_weighted_totals_from_the_repository(monkeypatch):
    monkeypatch.setattr(
        portfolio_repo,
        "get_daily_totals",
        lambda conn, date: {
            "actual_kwh": 101200.0,
            "expected_kwh": 110000.0,
            "performance_pct": 92.0,
            "lost_kwh": 8800.0,
            "revenue": 15180.0,
            "lost_revenue": 1320.0,
        },
    )
    monkeypatch.setattr(portfolio_repo, "get_daily_plant_rows", lambda conn, date: [])

    response = client.get("/api/v1/portfolio/daily", params={"date": "2026-08-21"})

    assert response.status_code == 200
    portfolio = response.json()["portfolio"]
    assert portfolio["performance_pct"] == 92.0
    assert portfolio["actual_generation_kwh"] == 101200.0
