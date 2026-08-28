"""Data-freshness classification shared by every live endpoint.

There is no `is_stale` column anywhere in the serving schema (see
docs/member-2-handoff.md, section 3) — staleness is a property of the reader's
threshold, not the writer's, so the API computes it against
`STALE_DATA_SECONDS` rather than trusting a stored flag.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal

DataStatus = Literal["LIVE", "STALE", "NO_DATA"]


def data_status(latest_timestamp: datetime | None, stale_after_seconds: int, now: datetime | None = None) -> DataStatus:
    """Classify the freshness of the most recent metric row.

    `NO_DATA` when the pipeline has never written anything yet (a cold start,
    or a brand-new plant); `STALE` when it has written data but not recently;
    `LIVE` otherwise. Never fabricate current values for a STALE/NO_DATA
    result — callers still return the last known figures, just labelled.
    """
    if latest_timestamp is None:
        return "NO_DATA"

    reference = now or datetime.now(timezone.utc)
    if latest_timestamp.tzinfo is None:
        latest_timestamp = latest_timestamp.replace(tzinfo=timezone.utc)

    age_seconds = (reference - latest_timestamp).total_seconds()
    return "STALE" if age_seconds > stale_after_seconds else "LIVE"
