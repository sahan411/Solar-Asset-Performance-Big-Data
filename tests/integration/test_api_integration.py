"""End-to-end serving-API checks against a real, seeded PostgreSQL.

Skipped unless SOLARIQ_TEST_DATABASE_URL points at a throwaway database — see
tests/storage/test_migrations_integration.py for how to start one locally.
These are the tests that actually prove SQL correctness (the weighted daily
aggregation, the alerts active-filter, freshness against a real timestamp);
api/tests/ covers HTTP-shape concerns with the repository layer mocked out.
"""

from __future__ import annotations

import os
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

TEST_DATABASE_URL = os.getenv("SOLARIQ_TEST_DATABASE_URL")

if not TEST_DATABASE_URL:
    pytest.skip("set SOLARIQ_TEST_DATABASE_URL to run API integration tests", allow_module_level=True)

# api/app imports itself as the top-level package `app` (see api/tests/conftest.py);
# add api/ to sys.path the same way before importing it here.
API_ROOT = Path(__file__).resolve().parents[2] / "api"
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

# The app reads DATABASE_URL at lifespan startup; point it at the throwaway
# integration database before any test constructs a TestClient.
os.environ["DATABASE_URL"] = TEST_DATABASE_URL

from fastapi.testclient import TestClient  # noqa: E402

from processing.common.db import connect  # noqa: E402
from storage.migrate import run_migrations  # noqa: E402
from storage.seed_portfolio import load_portfolio, seed_portfolio  # noqa: E402

from app.main import app  # noqa: E402

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "portfolio.test.yaml"


@pytest.fixture()
def seeded_database():
    with connect(TEST_DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
    run_migrations(TEST_DATABASE_URL)
    with connect(TEST_DATABASE_URL) as conn:
        seed_portfolio(conn, load_portfolio(FIXTURE))
    yield TEST_DATABASE_URL


@pytest.fixture()
def client(seeded_database):
    with TestClient(app) as test_client:
        yield test_client


def test_portfolio_live_returns_the_latest_seeded_window(seeded_database, client):
    now = datetime.now(timezone.utc)
    with connect(seeded_database) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO live_portfolio_metrics
                    (window_start, window_end, current_power_kw, avg_power_kw,
                     expected_power_kw, online_inverters, offline_inverters,
                     availability_pct, performance_pct, estimated_loss_kw)
                VALUES (%s, %s, 1000.0, 950.0, 1100.0, 6, 0, 100.0, 90.9, 100.0)
                """,
                (now - timedelta(seconds=15), now),
            )

    response = client.get("/api/v1/portfolio/live")

    assert response.status_code == 200
    body = response.json()
    assert body["data_status"] == "LIVE"
    assert body["current_power_kw"] == 1000.0
    assert body["installed_capacity_kw"] == 6000.0  # 4000 + 2000 from the fixture


def test_plants_list_returns_the_seeded_plants(seeded_database, client):
    response = client.get("/api/v1/plants")

    assert response.status_code == 200
    ids = {row["id"] for row in response.json()}
    assert ids == {"PLANT_01", "PLANT_02"}


def test_alerts_active_filter_excludes_resolved_rows(seeded_database, client):
    with connect(seeded_database) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO alerts (id, plant_id, alert_type, severity, message, started_at, status)
                VALUES (%s, 'PLANT_01', 'UNDERPERFORMANCE', 'WARNING', 'test', NOW(), 'ACTIVE')
                """,
                (str(uuid.uuid4()),),
            )
            cur.execute(
                """
                INSERT INTO alerts (id, plant_id, alert_type, severity, message, started_at, ended_at, status)
                VALUES (%s, 'PLANT_01', 'INVERTER_OFFLINE', 'CRITICAL', 'test', NOW(), NOW(), 'RESOLVED')
                """,
                (str(uuid.uuid4()),),
            )

    response = client.get("/api/v1/alerts", params={"status": "active"})

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    assert body[0]["status"] == "ACTIVE"


def test_daily_portfolio_sums_energy_then_divides_rather_than_averaging_percentages(seeded_database, client):
    # PLANT_01 scores 90%, PLANT_02 scores 40% — naive averaging would report
    # 65%. Weighted by energy it must report sum(940)/sum(1100)*100 = 85.45%.
    with connect(seeded_database) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO daily_plant_summary
                    (simulation_date, plant_id, actual_generation_kwh, expected_generation_kwh,
                     performance_pct, estimated_lost_energy_kwh, estimated_actual_revenue,
                     estimated_lost_revenue, alert_count, maintenance_flag)
                VALUES
                    ('2026-08-21', 'PLANT_01', 900.0, 1000.0, 90.0, 100.0, 90.0, 10.0, 0, FALSE),
                    ('2026-08-21', 'PLANT_02', 40.0, 100.0, 40.0, 60.0, 4.0, 6.0, 1, FALSE)
                """
            )

    response = client.get("/api/v1/portfolio/daily", params={"date": "2026-08-21"})

    assert response.status_code == 200
    portfolio = response.json()["portfolio"]
    assert portfolio["actual_generation_kwh"] == 940.0
    assert portfolio["expected_generation_kwh"] == 1100.0
    assert round(portfolio["performance_pct"], 1) == 85.5


def test_daily_report_returns_404_before_the_dag_has_run(seeded_database, client):
    response = client.get("/api/v1/reports/daily", params={"date": "2099-01-01"})
    assert response.status_code == 404


def test_ready_reflects_real_database_connectivity(seeded_database, client):
    response = client.get("/ready")
    assert response.status_code == 200
    assert response.json() == {"status": "ready", "database": "ok"}
