"""Data sources: the simulated readings and tariff files."""

import csv
import json
import random
from datetime import datetime, timedelta

from common.billing import validate_tariff_row
from common.households import HOUSEHOLDS, ZONES
from simulators import meter_stream, tariff_batch

ASSIGNMENT_FIELDS = {"meter_id", "household_id", "power_consumption_kwh",
                     "solar_generation_kwh", "grid_zone", "timestamp"}


def readings_for_day(day: datetime):
    """All readings of one simulated day, as the simulator would send them."""
    out = []
    for slot in range(96):
        t = day + timedelta(minutes=15 * slot)
        rng = random.Random(f"{meter_stream.SEED}:{t.isoformat()}")
        out += [meter_stream.make_reading(h, t, rng) for h in HOUSEHOLDS]
    return out


def test_households_cover_every_zone_with_some_solar():
    assert len(HOUSEHOLDS) == 30
    for zone in ZONES:
        in_zone = [h for h in HOUSEHOLDS if h.grid_zone == zone]
        assert len(in_zone) == 10
        assert any(h.solar_kw > 0 for h in in_zone)


def test_reading_has_exactly_the_assignment_fields():
    reading = readings_for_day(datetime(2026, 1, 1))[0]
    assert set(reading) == ASSIGNMENT_FIELDS
    assert reading["timestamp"] == "2026-01-01T00:00:00Z"
    assert reading["power_consumption_kwh"] > 0


def test_same_seed_gives_same_data():
    assert readings_for_day(datetime(2026, 1, 2)) == readings_for_day(datetime(2026, 1, 2))


def test_no_solar_at_night():
    night = [r for r in readings_for_day(datetime(2026, 1, 1)) if r["timestamp"][11:13] in ("00", "03", "22")]
    assert all(r["solar_generation_kwh"] == 0 for r in night)


def renewable_pct(readings, zone, hour):
    rows = [r for r in readings if r["grid_zone"] == zone and int(r["timestamp"][11:13]) == hour]
    used = sum(r["power_consumption_kwh"] for r in rows)
    solar = sum(r["solar_generation_kwh"] for r in rows)
    return min(solar, used) / used * 100


def test_daily_storm_drops_renewable_share_below_alert_threshold():
    # Day 1 (index 0): the storm is over "north" from 10:00 to 14:00.
    readings = readings_for_day(datetime(2026, 1, 1))
    for hour in (10, 11, 12, 13):
        assert renewable_pct(readings, "north", hour) < 20
        assert renewable_pct(readings, "south", hour) > 20  # sunny zone, no alert


def test_corrupted_records_break_a_rule():
    reading = readings_for_day(datetime(2026, 1, 1))[0]
    rng = random.Random(1)
    for _ in range(40):
        raw = meter_stream.corrupt(reading, rng)
        try:
            bad = json.loads(raw)
        except json.JSONDecodeError:
            continue  # broken JSON
        assert (bad["power_consumption_kwh"] < 0 or bad["household_id"] is None
                or bad["grid_zone"] not in ZONES)


def test_trace_id_is_stable_per_reading():
    t = datetime(2026, 1, 1, 10, 15)
    assert meter_stream.trace_id_for("M001", t) == meter_stream.trace_id_for("M001", t)
    assert meter_stream.trace_id_for("M001", t) != meter_stream.trace_id_for("M002", t)


def test_tariff_file_has_one_valid_row_per_household(tmp_path, monkeypatch):
    monkeypatch.setattr(tariff_batch, "INCOMING", tmp_path)
    path = tariff_batch.write_tariff_file(datetime(2026, 1, 1).date())  # day index 0: all valid
    with path.open() as f:
        rows = list(csv.DictReader(f))
    assert list(rows[0]) == ["household_id", "tariff_rate", "billing_tier", "subsidy_flag"]
    assert len(rows) == 30
    assert all(validate_tariff_row(r) for r in rows)


def test_every_third_tariff_file_has_one_bad_row(tmp_path, monkeypatch):
    monkeypatch.setattr(tariff_batch, "INCOMING", tmp_path)
    path = tariff_batch.write_tariff_file(datetime(2026, 1, 3).date())  # day index 2
    with path.open() as f:
        rows = list(csv.DictReader(f))
    assert sum(validate_tariff_row(r) is None for r in rows) == 1
