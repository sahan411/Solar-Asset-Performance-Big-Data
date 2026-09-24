"""Batch layer: daily household billing and solar-contribution report.

Runs every real minute and bills at most one simulated day per run:

    pick_day -> load_tariffs -> compute_bills -> export_report

pick_day chooses the earliest simulated day that
  * has a tariff file in data/incoming and at least one meter reading,
  * is complete: readings exist for at least 1 simulated hour after midnight
    (the same allowance for late data as the streaming watermark), and
  * has not been billed yet (no row in billing_runs).
If no day is ready, the run is skipped.

Bills are computed from the complete, de-duplicated `meter_readings` table
(the master dataset), not from the approximate real-time view. SQL sums each
household's day and joins it with that day's tariff; the pricing rules live in
common/billing.py (unit-tested). Re-running a day gives the same result.
"""

import csv
import json
import logging
import os
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import psycopg2
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException

from common.billing import compute_bill, validate_tariff_row

DATA_DIR = Path(os.getenv("SMARTGRID_DATA_DIR", "/opt/airflow/data"))
INCOMING = DATA_DIR / "incoming"
REPORTS = DATA_DIR / "reports"
PG_DSN = os.getenv("SMARTGRID_DB_DSN", "host=postgres dbname=smartgrid user=grid password=grid")

LATE_DATA_ALLOWANCE = timedelta(hours=1)

log = logging.getLogger("airflow.task")


def log_event(event: str, level: int = logging.INFO, **fields) -> None:
    """Structured log line (JSON) inside the Airflow task log."""
    log.log(level, json.dumps({"service": "airflow-billing", "event": event, **fields},
                              default=str))


def connect():
    return psycopg2.connect(PG_DSN)


# Each household's totals for the day, joined with that day's tariff.
# Households with readings but no valid tariff drop out of the join (= unbilled).
USAGE_SQL = """
WITH usage AS (
    SELECT household_id,
           MAX(grid_zone) AS grid_zone,
           SUM(power_consumption_kwh) AS consumption_kwh,
           SUM(solar_generation_kwh) AS solar_kwh,
           SUM(GREATEST(power_consumption_kwh - solar_generation_kwh, 0)) AS grid_import_kwh,
           SUM(GREATEST(solar_generation_kwh - power_consumption_kwh, 0)) AS solar_export_kwh
    FROM meter_readings
    WHERE event_time >= %(day)s AND event_time < %(day)s::date + INTERVAL '1 day'
    GROUP BY household_id
)
SELECT u.household_id, u.grid_zone, u.consumption_kwh, u.solar_kwh, u.grid_import_kwh,
       u.solar_export_kwh, t.tariff_rate, t.billing_tier, t.subsidy_flag
FROM usage u
JOIN tariffs t ON t.household_id = u.household_id AND t.tariff_date = %(day)s
"""

INSERT_BILL_SQL = """
INSERT INTO daily_bills (
    bill_date, household_id, grid_zone, consumption_kwh, solar_kwh, grid_import_kwh,
    solar_export_kwh, solar_contribution_pct, tariff_rate, billing_tier, subsidy_flag,
    energy_charge, solar_credit, service_charge, subsidy_discount, total_bill)
VALUES (%(bill_date)s, %(household_id)s, %(grid_zone)s, %(consumption_kwh)s, %(solar_kwh)s,
        %(grid_import_kwh)s, %(solar_export_kwh)s, %(solar_contribution_pct)s, %(tariff_rate)s,
        %(billing_tier)s, %(subsidy_flag)s, %(energy_charge)s, %(solar_credit)s,
        %(service_charge)s, %(subsidy_discount)s, %(total_bill)s)
ON CONFLICT (bill_date, household_id) DO UPDATE SET
    grid_zone = EXCLUDED.grid_zone,
    consumption_kwh = EXCLUDED.consumption_kwh,
    solar_kwh = EXCLUDED.solar_kwh,
    grid_import_kwh = EXCLUDED.grid_import_kwh,
    solar_export_kwh = EXCLUDED.solar_export_kwh,
    solar_contribution_pct = EXCLUDED.solar_contribution_pct,
    tariff_rate = EXCLUDED.tariff_rate,
    billing_tier = EXCLUDED.billing_tier,
    subsidy_flag = EXCLUDED.subsidy_flag,
    energy_charge = EXCLUDED.energy_charge,
    solar_credit = EXCLUDED.solar_credit,
    service_charge = EXCLUDED.service_charge,
    subsidy_discount = EXCLUDED.subsidy_discount,
    total_bill = EXCLUDED.total_bill,
    computed_at = now()
"""

REPORT_COLUMNS = [
    "household_id", "grid_zone", "consumption_kwh", "solar_kwh", "grid_import_kwh",
    "solar_export_kwh", "solar_contribution_pct", "tariff_rate", "billing_tier",
    "subsidy_flag", "energy_charge", "solar_credit", "service_charge", "subsidy_discount",
    "total_bill",
]


@dag(
    dag_id="smart_grid_daily_billing",
    schedule=timedelta(minutes=1),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(seconds=15)},
    tags=["smart-grid", "batch-layer"],
)
def smart_grid_daily_billing():

    @task
    def pick_day() -> str:
        tariff_days = sorted(
            date.fromisoformat(p.stem.removeprefix("tariffs_"))
            for p in INCOMING.glob("tariffs_*.csv")
        )
        with connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT bill_date FROM billing_runs")
            billed = {row[0] for row in cur.fetchall()}
            cur.execute("SELECT MAX(event_time) FROM meter_readings")
            latest_reading = cur.fetchone()[0]
            # Days with no readings at all (e.g. the system was stopped) are not billed.
            cur.execute("SELECT DISTINCT event_time::date FROM meter_readings")
            days_with_readings = {row[0] for row in cur.fetchall()}
        conn.close()

        if latest_reading is None:
            raise AirflowSkipException("no meter readings yet")
        for day in tariff_days:
            day_end = datetime.combine(day, datetime.min.time()) + timedelta(days=1)
            if (day not in billed and day in days_with_readings
                    and latest_reading >= day_end + LATE_DATA_ALLOWANCE):
                log_event("day_selected", sim_date=day, latest_reading=latest_reading)
                return day.isoformat()
        raise AirflowSkipException(f"no complete, unbilled day (latest reading {latest_reading})")

    @task
    def load_tariffs(day: str) -> int:
        path = INCOMING / f"tariffs_{day}.csv"
        with path.open(newline="") as f:
            rows = list(csv.DictReader(f))

        clean, rejected = [], 0
        for i, row in enumerate(rows, start=2):  # line 1 is the header
            parsed = validate_tariff_row(row)
            if parsed is None:
                rejected += 1
                log_event("tariff_row_rejected", level=logging.WARNING, file=path.name,
                          line=i, row=row)
            else:
                clean.append((day, *parsed))

        with connect() as conn, conn.cursor() as cur:
            cur.executemany(
                """INSERT INTO tariffs (tariff_date, household_id, tariff_rate, billing_tier, subsidy_flag)
                   VALUES (%s, %s, %s, %s, %s)
                   ON CONFLICT (tariff_date, household_id) DO UPDATE SET
                     tariff_rate = EXCLUDED.tariff_rate,
                     billing_tier = EXCLUDED.billing_tier,
                     subsidy_flag = EXCLUDED.subsidy_flag,
                     loaded_at = now()""",
                clean,
            )
        conn.close()
        log_event("tariffs_loaded", sim_date=day, file=path.name, loaded=len(clean),
                  rejected=rejected)
        return rejected

    @task
    def compute_bills(day: str, tariff_rows_rejected: int) -> dict:
        started = time.time()
        params = {"day": day}
        with connect() as conn, conn.cursor() as cur:
            cur.execute(USAGE_SQL, params)
            columns = [c.name for c in cur.description]
            bills = []
            for row in cur.fetchall():
                u = dict(zip(columns, row))
                bill = compute_bill(u["consumption_kwh"], u["grid_import_kwh"],
                                    u["solar_export_kwh"], float(u["tariff_rate"]),
                                    u["billing_tier"], u["subsidy_flag"])
                bills.append({**u, **bill, "bill_date": day})
            cur.executemany(INSERT_BILL_SQL, bills)
            billed = len(bills)
            total = round(sum(b["total_bill"] for b in bills), 2)
            cur.execute(
                """SELECT COUNT(DISTINCT household_id) FROM meter_readings
                   WHERE event_time >= %(day)s AND event_time < %(day)s::date + INTERVAL '1 day'""",
                params)
            with_readings = cur.fetchone()[0]
            unbilled = with_readings - billed
            cur.execute(
                """INSERT INTO pipeline_metrics
                   (stage, batch_id, rows_in, rows_valid, rows_invalid, rows_duplicate, duration_ms)
                   VALUES ('batch_billing', NULL, %s, %s, %s, 0, %s)""",
                (with_readings, billed, unbilled, int((time.time() - started) * 1000)))
        conn.close()

        level = logging.WARNING if unbilled else logging.INFO
        log_event("bills_computed", level=level, sim_date=day, households_billed=billed,
                  households_unbilled=unbilled, total_billed=float(total))
        return {"billed": billed, "unbilled": unbilled, "total": float(total),
                "tariff_rows_rejected": tariff_rows_rejected}

    @task
    def export_report(day: str, summary: dict) -> str:
        REPORTS.mkdir(parents=True, exist_ok=True)
        path = REPORTS / f"daily_billing_report_{day}.csv"
        with connect() as conn, conn.cursor() as cur:
            cur.execute(
                f"SELECT {', '.join(REPORT_COLUMNS)} FROM daily_bills "
                "WHERE bill_date = %s ORDER BY household_id", (day,))
            rows = cur.fetchall()
            with path.open("w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(REPORT_COLUMNS)
                for row in rows:
                    writer.writerow([round(v, 3) if isinstance(v, float) else v for v in row])

            # Marking the day as done is the last step, so a failed run is simply retried.
            cur.execute(
                """INSERT INTO billing_runs (bill_date, households_billed, households_unbilled,
                     tariff_rows_rejected, total_billed, report_file)
                   VALUES (%s, %s, %s, %s, %s, %s)
                   ON CONFLICT (bill_date) DO NOTHING""",
                (day, summary["billed"], summary["unbilled"], summary["tariff_rows_rejected"],
                 summary["total"], path.name))
        conn.close()
        log_event("report_exported", sim_date=day, file=str(path), rows=len(rows))
        return str(path)

    day = pick_day()
    rejected = load_tariffs(day)
    summary = compute_bills(day, rejected)
    export_report(day, summary)


smart_grid_daily_billing()
