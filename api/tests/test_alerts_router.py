"""Alerts endpoint: filter translation and response shape."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.dependencies import get_connection
from app.main import app
from app.repositories import alerts as alerts_repo

client = TestClient(app)


@pytest.fixture(autouse=True)
def _override_connection():
    app.dependency_overrides[get_connection] = lambda: None
    yield
    app.dependency_overrides.clear()


def _alert_row(**overrides) -> dict:
    row = {
        "id": "alert-1",
        "plant_id": "PLANT_03",
        "inverter_id": "INV_02",
        "alert_type": "UNDERPERFORMANCE",
        "severity": "WARNING",
        "message": "Inverter underperforming",
        "started_at": "2026-08-21T05:10:00Z",
        "ended_at": None,
        "status": "ACTIVE",
        "estimated_loss_kwh": 12.5,
        "estimated_revenue_loss": None,
    }
    row.update(overrides)
    return row


def test_alerts_active_filter_is_translated_to_the_db_status_value(monkeypatch):
    captured = {}

    def fake_list_alerts(conn, status=None, plant_id=None, severity=None, limit=100):
        captured["status"] = status
        return [_alert_row()]

    monkeypatch.setattr(alerts_repo, "list_alerts", fake_list_alerts)

    response = client.get("/api/v1/alerts", params={"status": "active"})

    assert response.status_code == 200
    assert captured["status"] == "ACTIVE"
    assert response.json()[0]["status"] == "ACTIVE"


def test_alerts_with_no_filters_returns_everything(monkeypatch):
    monkeypatch.setattr(alerts_repo, "list_alerts", lambda conn, **kwargs: [_alert_row(), _alert_row(id="alert-2", status="RESOLVED")])

    response = client.get("/api/v1/alerts")

    assert response.status_code == 200
    assert len(response.json()) == 2


def test_alerts_reject_an_invalid_severity(monkeypatch):
    monkeypatch.setattr(alerts_repo, "list_alerts", lambda conn, **kwargs: [])

    response = client.get("/api/v1/alerts", params={"severity": "URGENT"})

    assert response.status_code == 422
