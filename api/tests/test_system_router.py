"""/health and /ready — process liveness vs. real readiness."""

from __future__ import annotations

from fastapi.testclient import TestClient

from app.dependencies import get_database
from app.main import app


class _FakeDatabase:
    def __init__(self, ok: bool) -> None:
        self._ok = ok

    def key_table_query_ok(self) -> bool:
        return self._ok


def test_health_does_not_touch_the_database():
    # No dependency override at all: /health must succeed even though nothing
    # backs get_database in this test process.
    client = TestClient(app)
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": "solariq-api"}


def test_ready_reports_ready_when_the_database_answers():
    app.dependency_overrides[get_database] = lambda: _FakeDatabase(ok=True)
    try:
        client = TestClient(app)
        response = client.get("/ready")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json() == {"status": "ready", "database": "ok"}


def test_ready_reports_not_ready_when_the_database_is_unreachable():
    app.dependency_overrides[get_database] = lambda: _FakeDatabase(ok=False)
    try:
        client = TestClient(app)
        response = client.get("/ready")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 503
    assert response.json() == {"status": "not_ready", "database": "error"}


def test_metrics_endpoint_exposes_prometheus_text_format():
    client = TestClient(app)
    response = client.get("/metrics")
    assert response.status_code == 200
    assert "solariq_api_requests_total" in response.text
