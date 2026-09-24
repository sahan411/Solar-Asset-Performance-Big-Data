"""Serving layer: REST API over the PostgreSQL serving store.

Endpoints
  GET /health              health check (database reachable, data fresh?)
  GET /api/grid/current    real-time grid load & renewable mix by zone   (speed layer)
  GET /api/grid/history    hourly load by zone for the last N simulated hours
  GET /api/alerts          recent low-renewable alerts
  GET /api/bills/latest    daily billing report for the latest billed day (batch layer)
  GET /api/bills/{date}    daily billing report for one simulated day
  GET /api/metrics         pipeline metrics (throughput, invalid rate, latency, freshness)
  GET /api/trace/{id}      follow one reading end to end by its trace_id
  POST /api/alert-webhook  receives Grafana alert notifications (logged)

Every response carries an X-Request-ID header, which is also in the request log.

Interactive docs: http://localhost:8000/docs
"""

import logging
import os
import time
import uuid
from contextlib import contextmanager
from datetime import date

import psycopg2
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from psycopg2.extras import RealDictCursor

from common.log import get_logger, log_event

PG_DSN = os.getenv("PG_DSN", "host=postgres dbname=smartgrid user=grid password=grid")
STALE_AFTER_SECONDS = int(os.getenv("STALE_AFTER_SECONDS", "60"))

log = get_logger("api")
app = FastAPI(title="Smart Grid Monitoring & Billing API")


@contextmanager
def db():
    conn = psycopg2.connect(PG_DSN, cursor_factory=RealDictCursor)
    try:
        with conn, conn.cursor() as cur:
            yield cur
    finally:
        conn.close()


def query(sql: str, params=None) -> list[dict]:
    with db() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


@app.middleware("http")
async def log_requests(request: Request, call_next):
    started = time.time()
    request_id = request.headers.get("X-Request-ID") or uuid.uuid4().hex[:12]
    response = await call_next(request)
    response.headers["X-Request-ID"] = request_id
    log_event(log, "http_request", request_id=request_id, method=request.method,
              path=request.url.path, status=response.status_code,
              duration_ms=int((time.time() - started) * 1000))
    return response


@app.get("/health")
def health():
    """Health-check rule: 'stale' if no meter reading arrived in STALE_AFTER_SECONDS."""
    try:
        row = query("""SELECT EXTRACT(EPOCH FROM now() - MAX(ingested_at)) AS age
                       FROM meter_readings""")[0]
    except psycopg2.Error as exc:
        log_event(log, "health_db_error", level=logging.ERROR, error=str(exc))
        return JSONResponse(status_code=503, content={"status": "down", "database": "unreachable"})

    age = row["age"]
    if age is None:
        status = "no_data"
    elif age > STALE_AFTER_SECONDS:
        status = "stale"
    else:
        status = "ok"
    return {
        "status": status,
        "database": "ok",
        "seconds_since_last_reading": None if age is None else round(float(age), 1),
        "stale_after_seconds": STALE_AFTER_SECONDS,
    }


@app.post("/api/alert-webhook")
async def alert_webhook(request: Request):
    """Grafana sends alert notifications here; each one becomes a structured log line."""
    payload = await request.json()
    alerts = payload.get("alerts", [])
    for a in alerts:
        log_event(log, "grafana_alert", level=logging.WARNING, status=a.get("status"),
                  alertname=a.get("labels", {}).get("alertname"),
                  severity=a.get("labels", {}).get("severity"),
                  summary=a.get("annotations", {}).get("summary"))
    return {"received": len(alerts)}


@app.get("/api/grid/current")
def grid_current():
    """Latest (at least 75% complete) simulated hour for each zone."""
    zones = query("""
        SELECT DISTINCT ON (grid_zone)
               grid_zone, window_start, window_end,
               ROUND(consumption_kwh::numeric, 2) AS grid_load_kwh,
               ROUND(solar_kwh::numeric, 2) AS solar_kwh,
               ROUND(net_grid_load_kwh::numeric, 2) AS net_grid_load_kwh,
               ROUND(renewable_pct::numeric, 1) AS renewable_pct,
               readings, expected_readings
        FROM zone_load_hourly
        WHERE readings >= 0.75 * expected_readings
        ORDER BY grid_zone, window_start DESC""")
    total_load = sum(float(z["grid_load_kwh"]) for z in zones)
    total_solar = sum(float(z["solar_kwh"]) for z in zones)
    return {
        "zones": zones,
        "total": {
            "grid_load_kwh": round(total_load, 2),
            "solar_kwh": round(total_solar, 2),
            "renewable_pct": round(min(total_solar, total_load) / total_load * 100, 1)
            if total_load else 0.0,
        },
    }


@app.get("/api/grid/history")
def grid_history(hours: int = 24):
    return query("""
        SELECT grid_zone, window_start,
               ROUND(consumption_kwh::numeric, 2) AS grid_load_kwh,
               ROUND(solar_kwh::numeric, 2) AS solar_kwh,
               ROUND(renewable_pct::numeric, 1) AS renewable_pct
        FROM zone_load_hourly
        WHERE window_start > (SELECT MAX(window_start) FROM zone_load_hourly)
                             - make_interval(hours => %s)
        ORDER BY window_start, grid_zone""", (hours,))


@app.get("/api/alerts")
def alerts(limit: int = 20):
    return query("""SELECT alert_type, grid_zone, window_start, value, threshold, message,
                           created_at
                    FROM alerts ORDER BY created_at DESC LIMIT %s""", (limit,))


def billing_report(bill_date: date):
    run = query("SELECT * FROM billing_runs WHERE bill_date = %s", (bill_date,))
    if not run:
        raise HTTPException(404, f"no billing report for {bill_date}")
    bills = query("""SELECT household_id, grid_zone,
                            ROUND(consumption_kwh::numeric, 2) AS consumption_kwh,
                            ROUND(solar_kwh::numeric, 2) AS solar_kwh,
                            ROUND(solar_contribution_pct::numeric, 1) AS solar_contribution_pct,
                            tariff_rate, billing_tier, subsidy_flag, total_bill
                     FROM daily_bills WHERE bill_date = %s ORDER BY household_id""",
                  (bill_date,))
    return {"summary": run[0], "bills": bills}


@app.get("/api/bills/latest")
def bills_latest():
    latest = query("SELECT MAX(bill_date) AS d FROM billing_runs")[0]["d"]
    if latest is None:
        raise HTTPException(404, "no day has been billed yet")
    return billing_report(latest)


@app.get("/api/bills/{bill_date}")
def bills_for_day(bill_date: date):
    return billing_report(bill_date)


@app.get("/api/metrics")
def metrics():
    """Metrics export: pipeline throughput and data quality over the last 5 minutes."""
    stream = query("""
        SELECT COUNT(*) AS micro_batches,
               COALESCE(SUM(rows_in), 0) AS rows_in,
               COALESCE(SUM(rows_valid), 0) AS rows_stored,
               COALESCE(SUM(rows_invalid), 0) AS rows_rejected,
               COALESCE(SUM(rows_duplicate), 0) AS rows_duplicate,
               ROUND(AVG(duration_ms)) AS avg_batch_ms,
               ROUND(AVG(avg_latency_ms)) AS avg_end_to_end_latency_ms,
               MAX(max_latency_ms) AS max_end_to_end_latency_ms
        FROM pipeline_metrics
        WHERE stage = 'stream_ingest' AND recorded_at > now() - INTERVAL '5 minutes'""")[0]
    rows_in = int(stream["rows_in"])
    stream["invalid_rate_pct"] = round(int(stream["rows_rejected"]) / rows_in * 100, 2) if rows_in else 0.0

    totals = query("""
        SELECT (SELECT COUNT(*) FROM meter_readings) AS readings_total,
               (SELECT COUNT(*) FROM rejected_readings) AS rejected_total,
               (SELECT COUNT(*) FROM alerts) AS alerts_total,
               (SELECT MAX(event_time) FROM meter_readings) AS latest_sim_time,
               (SELECT ROUND(EXTRACT(EPOCH FROM now() - MAX(recorded_at))::numeric, 1)
                FROM pipeline_metrics WHERE stage = 'batch_ingest') AS seconds_since_last_batch_ingest,
               (SELECT COUNT(*) FROM billing_runs) AS days_billed,
               (SELECT MAX(bill_date) FROM billing_runs) AS latest_bill_date""")[0]
    return {"last_5_minutes": stream, "totals": totals, "health": health()}


@app.get("/api/trace/{trace_id}")
def trace(trace_id: str):
    """Where did this reading go? simulator -> Kafka -> Spark -> PostgreSQL -> bill.

    trace_id values appear in the simulator, Spark and rejected_readings logs,
    and in the meter_readings / rejected_readings tables.
    """
    stored = query("""SELECT meter_id, household_id, grid_zone, event_time,
                             power_consumption_kwh, solar_generation_kwh,
                             kafka_partition, kafka_offset, ingested_at
                      FROM meter_readings WHERE trace_id = %s""", (trace_id,))
    rejected = query("""SELECT reason, raw_value, kafka_partition, kafka_offset, rejected_at
                        FROM rejected_readings WHERE trace_id = %s""", (trace_id,))
    if not stored and not rejected:
        raise HTTPException(404, f"trace_id {trace_id} not found")

    result = {"trace_id": trace_id, "rejected": rejected, "stored_reading": None,
              "zone_window": None, "bill": None}
    if stored:
        r = stored[0]
        result["stored_reading"] = r
        window = query("""SELECT grid_zone, window_start, consumption_kwh, solar_kwh,
                                 renewable_pct, readings
                          FROM zone_load_hourly
                          WHERE grid_zone = %s AND window_start = date_trunc('hour', %s)""",
                       (r["grid_zone"], r["event_time"]))
        bill = query("""SELECT bill_date, household_id, total_bill, computed_at
                        FROM daily_bills WHERE household_id = %s AND bill_date = %s""",
                     (r["household_id"], r["event_time"].date()))
        result["zone_window"] = window[0] if window else None
        result["bill"] = bill[0] if bill else "not billed yet (day not complete or no tariff)"
    return result
