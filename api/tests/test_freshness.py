"""Pure-function coverage for freshness classification."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.freshness import data_status


def test_no_data_when_there_is_no_timestamp():
    assert data_status(None, stale_after_seconds=60) == "NO_DATA"


def test_live_within_the_threshold():
    now = datetime(2026, 8, 21, 5, 0, 0, tzinfo=timezone.utc)
    latest = now - timedelta(seconds=30)
    assert data_status(latest, stale_after_seconds=60, now=now) == "LIVE"


def test_stale_past_the_threshold():
    now = datetime(2026, 8, 21, 5, 0, 0, tzinfo=timezone.utc)
    latest = now - timedelta(seconds=61)
    assert data_status(latest, stale_after_seconds=60, now=now) == "STALE"


def test_naive_timestamps_are_treated_as_utc():
    now = datetime(2026, 8, 21, 5, 0, 0, tzinfo=timezone.utc)
    latest = datetime(2026, 8, 21, 4, 59, 45)  # naive, no tzinfo
    assert data_status(latest, stale_after_seconds=60, now=now) == "LIVE"
