"""Plant endpoints: unknown-plant 404s and history range validation."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.dependencies import get_connection, get_settings
from app.main import app
from app.repositories import plants as plants_repo

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


def test_unknown_plant_live_returns_404(monkeypatch):
    monkeypatch.setattr(plants_repo, "get_plant", lambda conn, plant_id: None)

    response = client.get("/api/v1/plants/GHOST/live")

    assert response.status_code == 404


def test_unknown_plant_history_returns_404_before_validating_the_range(monkeypatch):
    monkeypatch.setattr(plants_repo, "get_plant", lambda conn, plant_id: None)

    response = client.get(
        "/api/v1/plants/GHOST/history",
        params={"from": "2026-08-21T00:00:00Z", "to": "2026-08-20T00:00:00Z"},
    )

    assert response.status_code == 404


def test_plant_history_rejects_from_after_to(monkeypatch):
    monkeypatch.setattr(
        plants_repo, "get_plant", lambda conn, plant_id: {"id": plant_id, "name": "x", "capacity_kw": 1.0, "active": True}
    )

    response = client.get(
        "/api/v1/plants/PLANT_01/history",
        params={"from": "2026-08-21T10:00:00Z", "to": "2026-08-21T09:00:00Z"},
    )

    assert response.status_code == 422


def test_plant_history_rejects_an_excessive_range(monkeypatch):
    monkeypatch.setattr(
        plants_repo, "get_plant", lambda conn, plant_id: {"id": plant_id, "name": "x", "capacity_kw": 1.0, "active": True}
    )

    response = client.get(
        "/api/v1/plants/PLANT_01/history",
        params={"from": "2026-01-01T00:00:00Z", "to": "2026-08-21T00:00:00Z"},
    )

    assert response.status_code == 422


def test_plant_history_returns_ordered_points(monkeypatch):
    monkeypatch.setattr(
        plants_repo, "get_plant", lambda conn, plant_id: {"id": plant_id, "name": "x", "capacity_kw": 1.0, "active": True}
    )
    monkeypatch.setattr(
        plants_repo,
        "get_plant_history",
        lambda conn, plant_id, from_ts, to_ts: [
            {
                "window_start": "2026-08-21T09:00:00Z",
                "window_end": "2026-08-21T09:01:00Z",
                "current_power_kw": 100.0,
                "avg_power_kw": 95.0,
                "expected_power_kw": 110.0,
                "performance_pct": 90.9,
                "availability_pct": 100.0,
            }
        ],
    )

    response = client.get(
        "/api/v1/plants/PLANT_01/history",
        params={"from": "2026-08-21T09:00:00Z", "to": "2026-08-21T10:00:00Z"},
    )

    assert response.status_code == 200
    assert len(response.json()["points"]) == 1


def test_unknown_plant_inverters_returns_404(monkeypatch):
    monkeypatch.setattr(plants_repo, "get_plant", lambda conn, plant_id: None)

    response = client.get("/api/v1/plants/GHOST/inverters")

    assert response.status_code == 404
