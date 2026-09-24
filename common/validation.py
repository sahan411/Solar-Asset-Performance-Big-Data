"""Validation rules for one meter reading, used by the batch layer.

The speed layer (spark/stream_job.py) applies the same rules in Spark SQL.
Keeping two implementations in step is a known cost of the Lambda
architecture; tests/test_batch_readings.py checks they give the same reasons.
"""

from __future__ import annotations

from datetime import datetime, timezone

from common.households import ZONES


def parse_timestamp(value) -> datetime | None:
    """'2026-01-01T10:15:00Z' -> naive UTC datetime, or None if not a valid timestamp."""
    if not isinstance(value, str):
        return None
    try:
        ts = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if ts.tzinfo is not None:
        ts = ts.astimezone(timezone.utc).replace(tzinfo=None)
    return ts


def _is_number(value) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def reject_reason(reading) -> str | None:
    """Why this reading is invalid, or None if it is valid. Same order as Spark."""
    if (not isinstance(reading, dict) or reading.get("meter_id") is None
            or parse_timestamp(reading.get("timestamp")) is None):
        return "malformed_or_missing_fields"
    if reading.get("household_id") is None:
        return "missing_household_id"
    if reading.get("grid_zone") not in ZONES:
        return "unknown_grid_zone"
    consumption = reading.get("power_consumption_kwh")
    if not _is_number(consumption) or consumption < 0:
        return "invalid_consumption"
    solar = reading.get("solar_generation_kwh")
    if not _is_number(solar) or solar < 0:
        return "invalid_solar_generation"
    return None
