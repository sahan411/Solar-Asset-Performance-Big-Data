"""The raw archive: the batch layer's immutable master dataset.

Airflow's first task (ingest_from_kafka, see common/kafka_ingest.py) writes
every Kafka message, exactly as received (including bad ones), to JSON-lines
files, one folder per simulated day:

    data/raw/date=2026-01-01/part-0.jsonl
    data/raw/date=2026-01-01/part-1.jsonl
    ...
    data/raw/date=unparsed/part-2.jsonl      (messages with no readable timestamp)
    data/raw/_latest_event_time              (newest event time archived so far)

Each line wraps one Kafka message:

    {"partition": 0, "offset": 42, "trace_id": "...", "produced_at": "...",
     "archived_at": "...", "value": "<the original message text, unchanged>"}

summarise_day() is the batch layer's processing of one archived day: it cleans,
de-duplicates and totals each household's usage, independently of Spark.
"""

from __future__ import annotations

import json
from collections import Counter
from datetime import date, datetime
from pathlib import Path

from common.validation import parse_timestamp, reject_reason

LATEST_EVENT_FILE = "_latest_event_time"
UNPARSED = "unparsed"


def event_time(value: str) -> datetime | None:
    """Best-effort event time of a raw message (None if it can't be read)."""
    try:
        return parse_timestamp(json.loads(value).get("timestamp"))
    except (ValueError, AttributeError):
        return None


def partition_date(ts: datetime | None) -> str:
    """Folder name for a raw message: its event date, or 'unparsed'."""
    return ts.date().isoformat() if ts else UNPARSED


def day_dir(raw_dir: Path, day: date | str) -> Path:
    return raw_dir / f"date={day}"


def archived_days(raw_dir: Path) -> set[date]:
    days = set()
    for p in raw_dir.glob("date=*"):
        try:
            days.add(date.fromisoformat(p.name.removeprefix("date=")))
        except ValueError:
            pass  # date=unparsed
    return days


def latest_event_time(raw_dir: Path) -> datetime | None:
    try:
        return datetime.fromisoformat((raw_dir / LATEST_EVENT_FILE).read_text().strip())
    except (FileNotFoundError, ValueError):
        return None


def read_day(raw_dir: Path, day: date | str) -> list[str]:
    lines = []
    for path in sorted(day_dir(raw_dir, day).glob("part-*.jsonl")):
        with path.open(encoding="utf-8") as f:
            lines.extend(f)
    return lines


def summarise_day(raw_lines: list[str]) -> tuple[list[dict], dict]:
    """Clean + de-duplicate one day of raw lines and total usage per household.

    Returns (usage rows, stats). Each usage row has household_id, grid_zone,
    consumption_kwh, solar_kwh, grid_import_kwh, solar_export_kwh, readings.
    """
    usage: dict[str, dict] = {}
    seen = set()
    reasons = Counter()
    stats = {"raw_records": 0, "valid": 0, "rejected": 0, "duplicates": 0, "unreadable_lines": 0}

    for line in raw_lines:
        if not line.strip():
            continue
        stats["raw_records"] += 1
        try:
            value = json.loads(line)["value"]
        except (ValueError, KeyError, TypeError):
            stats["unreadable_lines"] += 1  # e.g. a half-written last line
            continue
        try:
            reading = json.loads(value)
        except ValueError:
            reading = None
        reason = reject_reason(reading)
        if reason:
            stats["rejected"] += 1
            reasons[reason] += 1
            continue

        key = (reading["meter_id"], parse_timestamp(reading["timestamp"]))
        if key in seen:
            stats["duplicates"] += 1
            continue
        seen.add(key)
        stats["valid"] += 1

        consumption = float(reading["power_consumption_kwh"])
        solar = float(reading["solar_generation_kwh"])
        u = usage.setdefault(reading["household_id"], {
            "household_id": reading["household_id"], "grid_zone": reading["grid_zone"],
            "consumption_kwh": 0.0, "solar_kwh": 0.0, "grid_import_kwh": 0.0,
            "solar_export_kwh": 0.0, "readings": 0,
        })
        u["consumption_kwh"] += consumption
        u["solar_kwh"] += solar
        u["grid_import_kwh"] += max(consumption - solar, 0.0)
        u["solar_export_kwh"] += max(solar - consumption, 0.0)
        u["readings"] += 1

    stats["reasons"] = dict(reasons)
    return sorted(usage.values(), key=lambda u: u["household_id"]), stats
