"""Batch layer: daily household billing and solar-contribution report.

The batch layer does not use anything the speed layer (Spark) produces. It
subscribes to the Kafka topic itself (consumer group "airflow-batch") and has
two inputs:
  * the meter readings from Kafka, saved unchanged to the raw archive
    (data/raw/date=<day>/*.jsonl) -- the master dataset;
  * the daily tariff file (data/incoming/tariffs_<day>.csv).

Runs every real minute:

    ingest_from_kafka ── pick_day ──┬── load_tariffs ─────────┬── compute_bills ── export_report
                                    └── process_raw_readings ─┘

ingest_from_kafka reads every new Kafka message since the last run and appends
it to the raw archive (common/kafka_ingest.py). It runs every time. The billing
tasks after it bill at most one simulated day per run.

pick_day chooses the earliest simulated day that
  * has a tariff file and archived raw readings,
  * is complete: the archive already holds readings from at least 1 simulated
    hour after that day (the same allowance for late data as Spark's watermark),
  * has not been billed yet (no row in billing_runs).
If no day is ready, the run is skipped.

process_raw_readings cleans and de-duplicates the day's raw readings itself
(common/validation.py, the same rules as Spark) and totals each household's
usage into daily_usage. compute_bills then JOINs daily_usage with that day's
tariffs and prices each row with common/billing.py. Re-running a day always
gives the same result, because it is recomputed from the raw data.
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
from common.kafka_ingest import ingest_new_messages
from common.raw_archive import archived_days, latest_event_time, read_day, summarise_day

DATA_DIR = Path(os.getenv("SMARTGRID_DATA_DIR", "/opt/airflow/data"))
INCOMING = DATA_DIR / "incoming"
RAW_DIR = DATA_DIR / "raw"
REPORTS = DATA_DIR / "reports"
PG_DSN = os.getenv("SMARTGRID_DB_DSN", "host=postgres dbname=smartgrid user=grid password=grid")
KAFKA_BOOTSTRAP = os.getenv("SMARTGRID_KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("SMARTGRID_KAFKA_TOPIC", "meter-readings")

LATE_DATA_ALLOWANCE = timedelta(hours=1)

log = logging.getLogger("airflow.task")


def log_event(event: str, level: int = logging.INFO, **fields) -> None:
    """Structured log line (JSON) inside the Airflow task log."""
    log.log(level, json.dumps({"service": "airflow-billing", "event": event, **fields},
                              default=str))


def connect():
    return psycopg2.connect(PG_DSN)


# That day's usage (from the raw archive) joined with that day's tariff.
# Households with usage but no valid tariff row drop out of the join (= unbilled).
USAGE_JOIN_TARIFF_SQL = """
SELECT u.household_id, u.grid_zone, u.consumption_kwh, u.solar_kwh, u.grid_import_kwh,
       u.solar_export_kwh, t.tariff_rate, t.billing_tier, t.subsidy_flag
FROM daily_usage u
JOIN tariffs t ON t.household_id = u.household_id AND t.tariff_date = u.usage_date
WHERE u.usage_date = %(day)s
"""

INSERT_USAGE_SQL = """
INSERT INTO daily_usage (usage_date, household_id, grid_zone, consumption_kwh, solar_kwh,
                         grid_import_kwh, solar_export_kwh, readings)
VALUES (%(usage_date)s, %(household_id)s, %(grid_zone)s, %(consumption_kwh)s, %(solar_kwh)s,
        %(grid_import_kwh)s, %(solar_export_kwh)s, %(readings)s)
ON CONFLICT (usage_date, household_id) DO UPDATE SET
    grid_zone = EXCLUDED.grid_zone,
    consumption_kwh = EXCLUDED.consumption_kwh,
    solar_kwh = EXCLUDED.solar_kwh,
    grid_import_kwh = EXCLUDED.grid_import_kwh,
    solar_export_kwh = EXCLUDED.solar_export_kwh,
    readings = EXCLUDED.readings,
    computed_at = now()
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
    def ingest_from_kafka() -> dict:
        stats = ingest_new_messages(KAFKA_BOOTSTRAP, KAFKA_TOPIC, RAW_DIR)
        with connect() as conn, conn.cursor() as cur:
            cur.execute(
                """INSERT INTO pipeline_metrics
                   (stage, rows_in, rows_valid, rows_invalid, rows_duplicate, duration_ms,
                    avg_latency_ms, max_latency_ms)
                   VALUES ('batch_ingest', %s, %s, 0, 0, %s, %s, %s)""",
                (stats["messages"], stats["messages"], stats["duration_ms"],
                 stats["avg_latency_ms"], stats["max_latency_ms"]))
        conn.close()
        log_event("kafka_messages_ingested", topic=KAFKA_TOPIC, **stats)
        return stats

    @task
    def pick_day() -> str:
        tariff_days = {
            date.fromisoformat(p.stem.removeprefix("tariffs_"))
            for p in INCOMING.glob("tariffs_*.csv")
        }
        raw_days = archived_days(RAW_DIR)
        latest = latest_event_time(RAW_DIR)
        with connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT bill_date FROM billing_runs")
            billed = {row[0] for row in cur.fetchall()}
        conn.close()

        if latest is None:
            raise AirflowSkipException("raw archive is empty")
        for day in sorted(tariff_days & raw_days):
            day_end = datetime.combine(day, datetime.min.time()) + timedelta(days=1)
            if day not in billed and latest >= day_end + LATE_DATA_ALLOWANCE:
                log_event("day_selected", sim_date=day, archive_latest_event=latest)
                return day.isoformat()
        raise AirflowSkipException(f"no complete, unbilled day (archive up to {latest})")

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
    def process_raw_readings(day: str) -> dict:
        started = time.time()
        usage, stats = summarise_day(read_day(RAW_DIR, day))
        with connect() as conn, conn.cursor() as cur:
            cur.execute("DELETE FROM daily_usage WHERE usage_date = %s", (day,))
            cur.executemany(INSERT_USAGE_SQL, [{**u, "usage_date": day} for u in usage])
            cur.execute(
                """INSERT INTO pipeline_metrics
                   (stage, rows_in, rows_valid, rows_invalid, rows_duplicate, duration_ms)
                   VALUES ('batch_billing', %s, %s, %s, %s, %s)""",
                (stats["raw_records"], stats["valid"], stats["rejected"], stats["duplicates"],
                 int((time.time() - started) * 1000)))
        conn.close()
        log_event("raw_readings_processed", sim_date=day, households=len(usage), **stats)
        return stats

    @task
    def compute_bills(day: str) -> dict:
        with connect() as conn, conn.cursor() as cur:
            cur.execute(USAGE_JOIN_TARIFF_SQL, {"day": day})
            columns = [c.name for c in cur.description]
            bills = []
            for row in cur.fetchall():
                u = dict(zip(columns, row))
                bill = compute_bill(u["consumption_kwh"], u["grid_import_kwh"],
                                    u["solar_export_kwh"], float(u["tariff_rate"]),
                                    u["billing_tier"], u["subsidy_flag"])
                bills.append({**u, **bill, "bill_date": day})
            cur.executemany(INSERT_BILL_SQL, bills)
            cur.execute("SELECT COUNT(*) FROM daily_usage WHERE usage_date = %s", (day,))
            with_usage = cur.fetchone()[0]
        conn.close()

        billed = len(bills)
        total = round(sum(b["total_bill"] for b in bills), 2)
        unbilled = with_usage - billed
        level = logging.WARNING if unbilled else logging.INFO
        log_event("bills_computed", level=level, sim_date=day, households_billed=billed,
                  households_unbilled=unbilled, total_billed=total)
        return {"billed": billed, "unbilled": unbilled, "total": total}

    @task
    def export_report(day: str, bills: dict, tariff_rows_rejected: int, raw_stats: dict) -> str:
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
                     tariff_rows_rejected, raw_records, readings_rejected, readings_duplicate,
                     total_billed, report_file)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT (bill_date) DO NOTHING""",
                (day, bills["billed"], bills["unbilled"], tariff_rows_rejected,
                 raw_stats["raw_records"], raw_stats["rejected"], raw_stats["duplicates"],
                 bills["total"], path.name))
        conn.close()
        log_event("report_exported", sim_date=day, file=str(path), rows=len(rows))
        return str(path)

    day = pick_day()
    ingest_from_kafka() >> day
    tariff_rejected = load_tariffs(day)
    raw_stats = process_raw_readings(day)
    bills = compute_bills(day)
    [tariff_rejected, raw_stats] >> bills
    export_report(day, bills, tariff_rejected, raw_stats)


smart_grid_daily_billing()
