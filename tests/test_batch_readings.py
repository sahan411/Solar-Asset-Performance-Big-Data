"""Batch layer: Kafka ingestion into the raw archive, and the batch layer's own cleaning."""

import json
from datetime import date, datetime

import pytest

from common.kafka_ingest import to_line
from common.raw_archive import (archived_days, event_time, latest_event_time,
                                partition_date, summarise_day)
from common.validation import reject_reason


def reading(**overrides):
    r = {"meter_id": "M001", "household_id": "H001", "grid_zone": "north",
         "power_consumption_kwh": 0.5, "solar_generation_kwh": 0.2,
         "timestamp": "2026-01-01T10:00:00Z"}
    r.update(overrides)
    return r


def raw_line(value, offset=0):
    """One raw-archive line, as the archiver writes it."""
    if not isinstance(value, str):
        value = json.dumps(value)
    return json.dumps({"partition": 0, "offset": offset, "trace_id": f"t{offset}", "value": value})


@pytest.mark.parametrize("value, reason", [
    (reading(), None),
    (reading(power_consumption_kwh=-0.3), "invalid_consumption"),
    (reading(solar_generation_kwh="abc"), "invalid_solar_generation"),
    (reading(household_id=None), "missing_household_id"),
    (reading(grid_zone="zone-x"), "unknown_grid_zone"),
    (reading(timestamp="not a time"), "malformed_or_missing_fields"),
    (None, "malformed_or_missing_fields"),                 # broken JSON
])
def test_reject_reason(value, reason):
    assert reject_reason(value) == reason


def test_summarise_day_cleans_dedupes_and_totals():
    lines = [
        raw_line(reading(power_consumption_kwh=1.0, solar_generation_kwh=0.25), 0),
        raw_line(reading(power_consumption_kwh=1.0, solar_generation_kwh=0.25), 1),  # duplicate
        raw_line(reading(timestamp="2026-01-01T12:00:00Z", power_consumption_kwh=0.5,
                         solar_generation_kwh=1.5), 2),                               # exports 1.0
        raw_line(reading(meter_id="M002", household_id="H002", grid_zone="south"), 3),
        raw_line(reading(grid_zone="zone-x"), 4),                                     # rejected
        raw_line('{"meter_id": "M001", "power_', 5),                                  # broken JSON
        '{"partition": 0, "offs',                                                     # half-written line
        "",
    ]
    usage, stats = summarise_day(lines)

    assert stats == {"raw_records": 7, "valid": 3, "rejected": 2, "duplicates": 1,
                     "unreadable_lines": 1,
                     "reasons": {"unknown_grid_zone": 1, "malformed_or_missing_fields": 1}}
    h1 = next(u for u in usage if u["household_id"] == "H001")
    assert h1["readings"] == 2
    assert h1["consumption_kwh"] == pytest.approx(1.5)
    assert h1["solar_kwh"] == pytest.approx(1.75)
    assert h1["grid_import_kwh"] == pytest.approx(0.75)   # 1.0 - 0.25
    assert h1["solar_export_kwh"] == pytest.approx(1.0)   # 1.5 - 0.5
    assert [u["household_id"] for u in usage] == ["H001", "H002"]


def test_partition_folder_uses_event_date():
    assert partition_date(event_time(json.dumps(reading()))) == "2026-01-01"
    assert partition_date(event_time('{"meter_id": "M0')) == "unparsed"
    assert partition_date(event_time(json.dumps(reading(timestamp=None)))) == "unparsed"


def test_archived_days_and_latest_event_time(tmp_path):
    for name in ("date=2026-01-01", "date=2026-01-02", "date=unparsed"):
        (tmp_path / name).mkdir()
    (tmp_path / "_latest_event_time").write_text("2026-01-02T01:15:00")
    assert archived_days(tmp_path) == {date(2026, 1, 1), date(2026, 1, 2)}
    assert latest_event_time(tmp_path) == datetime(2026, 1, 2, 1, 15)
    assert latest_event_time(tmp_path / "missing") is None


class FakeMessage:
    def __init__(self, value: bytes):
        self._value = value

    def value(self):
        return self._value

    def partition(self):
        return 2

    def offset(self):
        return 48

    def key(self):
        return b"M014"

    def headers(self):
        return [("trace_id", b"abc-123"), ("produced_at", b"1700000000000")]


def test_batch_ingest_keeps_the_message_exactly_as_received():
    broken = b'{"meter_id": "M014", "grid_zone": "zone-x", "power_'
    ts, line = to_line(FakeMessage(broken))
    record = json.loads(line)
    assert ts is None                                  # -> goes to date=unparsed
    assert record["value"] == broken.decode()          # unchanged, even though it is invalid
    assert (record["partition"], record["offset"], record["key"]) == (2, 48, "M014")
    assert record["trace_id"] == "abc-123"

    ts, _ = to_line(FakeMessage(json.dumps(reading()).encode()))
    assert ts == datetime(2026, 1, 1, 10, 0)
