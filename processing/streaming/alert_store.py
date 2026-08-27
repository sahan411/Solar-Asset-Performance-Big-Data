"""Alert lifecycle in PostgreSQL: observe -> sustain -> open -> refresh -> resolve.

Detection (processing.streaming.alerts) says what is wrong *right now*. This
module decides whether that is worth telling anyone, and keeps the resulting
alert up to date until it clears.

The whole cycle runs in one transaction per microbatch, in this order:

  1. record every currently-observed condition, preserving when it first appeared
  2. forget conditions that have cleared
  3. open alerts for conditions sustained past the threshold
  4. refresh the financial impact of alerts that are still open
  5. resolve alerts whose condition has gone

Step 2 must precede step 5, so that resolution sees the cleared state.

Requires PostgreSQL 13+ for the built-in gen_random_uuid().
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Sequence

from processing.common.db import connect, execute_batch
from processing.common.logging import get_logger

log = get_logger("spark-stream")

# Mean shortfall over the condition's life, times its duration in hours.
# Averaging is more honest than using the latest instantaneous reading, which
# could be an outlier from a single cloudy sample.
_LOST_ENERGY_KWH = """
    (c.sum_loss_kw / NULLIF(c.observation_count, 0))
    * (EXTRACT(EPOCH FROM (c.last_observed_at - c.first_observed_at)) / 3600.0)
"""

# The daily reference carries the plant's commercial rate. Joined on the
# simulated date in UTC, matching how simulation_date is derived everywhere else.
# LEFT joined: before the day's reference feed arrives the energy loss is still
# known, only its monetary value is not.
_REFERENCE_JOIN = """
    LEFT JOIN daily_reference r
           ON r.plant_id = c.plant_id
          AND r.simulation_date = (c.first_observed_at AT TIME ZONE 'UTC')::date
"""

_UPSERT_CONDITION = """
INSERT INTO alert_conditions (
    plant_id, inverter_id, alert_type, severity, message,
    first_observed_at, last_observed_at, observation_count, sum_loss_kw
)
VALUES (%s, %s, %s, %s, %s, %s, %s, 1, %s)
ON CONFLICT (plant_id, inverter_id, alert_type) DO UPDATE SET
    severity          = EXCLUDED.severity,
    message           = EXCLUDED.message,
    -- first_observed_at is deliberately NOT updated: it anchors the duration
    -- that the sustain rule measures.
    last_observed_at  = GREATEST(alert_conditions.last_observed_at, EXCLUDED.last_observed_at),
    observation_count = alert_conditions.observation_count + 1,
    sum_loss_kw       = alert_conditions.sum_loss_kw + EXCLUDED.sum_loss_kw
"""

_DELETE_CLEARED = """
DELETE FROM alert_conditions
WHERE (plant_id, inverter_id, alert_type) NOT IN %s
"""

_DELETE_ALL_CONDITIONS = "DELETE FROM alert_conditions"

_OPEN_SUSTAINED_ALERTS = f"""
INSERT INTO alerts (
    id, plant_id, inverter_id, alert_type, severity, message,
    started_at, status, estimated_loss_kwh, estimated_revenue_loss
)
SELECT
    gen_random_uuid()::text,
    c.plant_id,
    -- Plant-wide conditions are stored with '' and surface as NULL in `alerts`.
    NULLIF(c.inverter_id, ''),
    c.alert_type,
    c.severity,
    c.message,
    c.first_observed_at,
    'ACTIVE',
    {_LOST_ENERGY_KWH},
    {_LOST_ENERGY_KWH} * r.ppa_rate_per_kwh
FROM alert_conditions c
{_REFERENCE_JOIN}
WHERE EXTRACT(EPOCH FROM (c.last_observed_at - c.first_observed_at)) >= %s
  AND NOT EXISTS (
      SELECT 1 FROM alerts a
       WHERE a.plant_id = c.plant_id
         AND COALESCE(a.inverter_id, '') = c.inverter_id
         AND a.alert_type = c.alert_type
         AND a.status = 'ACTIVE'
  )
"""

# Keeps the impact figures growing while a fault persists, so an operator sees
# the cost of leaving it unfixed rather than a number frozen at detection time.
_REFRESH_OPEN_ALERTS = f"""
UPDATE alerts a SET
    message                = c.message,
    severity               = c.severity,
    estimated_loss_kwh     = sub.lost_kwh,
    estimated_revenue_loss = sub.lost_kwh * sub.rate,
    updated_at             = NOW()
FROM (
    SELECT c.plant_id, c.inverter_id, c.alert_type, c.message, c.severity,
           {_LOST_ENERGY_KWH} AS lost_kwh,
           r.ppa_rate_per_kwh AS rate
      FROM alert_conditions c
      {_REFERENCE_JOIN}
) AS sub
JOIN alert_conditions c
  ON c.plant_id = sub.plant_id
 AND c.inverter_id = sub.inverter_id
 AND c.alert_type = sub.alert_type
WHERE a.status = 'ACTIVE'
  AND a.plant_id = sub.plant_id
  AND COALESCE(a.inverter_id, '') = sub.inverter_id
  AND a.alert_type = sub.alert_type
"""

_RESOLVE_CLEARED_ALERTS = """
UPDATE alerts a SET
    status     = 'RESOLVED',
    ended_at   = %s,
    updated_at = NOW()
WHERE a.status = 'ACTIVE'
  AND NOT EXISTS (
      SELECT 1 FROM alert_conditions c
       WHERE c.plant_id = a.plant_id
         AND c.inverter_id = COALESCE(a.inverter_id, '')
         AND c.alert_type = a.alert_type
  )
"""


@dataclass(frozen=True)
class AlertOutcome:
    """What the reconciliation did, for logging and metrics."""

    observed: int
    opened: int
    resolved: int


def condition_rows(
    records: Sequence[Sequence[Any]], observed_at: datetime
) -> list[tuple]:
    """Shape detected conditions into bind parameters for the condition upsert.

    Expects rows in CONDITION_COLUMNS order:
    (plant_id, inverter_id, alert_type, severity, message, loss_kw, observed_at).

    The row's own `observed_at` is deliberately IGNORED in favour of the caller's
    value. Collecting a Spark timestamp to Python yields a timezone-naive
    datetime in the host's local zone; handing that to a TIMESTAMPTZ column would
    store it shifted by the host's offset, while `ended_at` — taken from the
    caller's timezone-aware value — would be stored correctly. The two would then
    disagree, and a resolved alert could appear to end before it started. Keeping
    one authoritative clock per batch removes the whole class of bug.
    """
    rows = []
    for plant_id, inverter_id, alert_type, severity, message, loss_kw, _row_observed_at in records:
        rows.append(
            (
                plant_id,
                # Plant-wide conditions use '' so the composite key compares.
                inverter_id or "",
                alert_type,
                severity,
                message,
                observed_at,
                observed_at,
                float(loss_kw or 0.0),
            )
        )
    return rows


def reconcile_alerts(
    conn,
    conditions: Sequence[Sequence[Any]],
    observed_at: datetime,
    sustain_seconds: float,
) -> AlertOutcome:
    """Run the full alert lifecycle for one microbatch, inside the caller's transaction."""
    rows = condition_rows(conditions, observed_at)

    # 1. Record what is currently wrong.
    execute_batch(conn, _UPSERT_CONDITION, rows)

    # 2. Forget what has recovered.
    with conn.cursor() as cur:
        if rows:
            observed_keys = tuple((r[0], r[1], r[2]) for r in rows)
            cur.execute(_DELETE_CLEARED, (observed_keys,))
        else:
            # Nothing is wrong anywhere: every tracked condition has cleared.
            cur.execute(_DELETE_ALL_CONDITIONS)

        # 3. Promote conditions that have lasted long enough.
        cur.execute(_OPEN_SUSTAINED_ALERTS, (sustain_seconds,))
        opened = cur.rowcount

        # 4. Keep open alerts' impact figures current.
        cur.execute(_REFRESH_OPEN_ALERTS)

        # 5. Close alerts whose condition is gone.
        cur.execute(_RESOLVE_CLEARED_ALERTS, (observed_at,))
        resolved = cur.rowcount

    return AlertOutcome(observed=len(rows), opened=opened, resolved=resolved)


def reconcile_alerts_standalone(
    database_url: str,
    conditions: Sequence[Sequence[Any]],
    observed_at: datetime,
    sustain_seconds: float,
) -> AlertOutcome:
    """Convenience wrapper that owns its own connection and transaction."""
    with connect(database_url) as conn:
        outcome = reconcile_alerts(conn, conditions, observed_at, sustain_seconds)

    if outcome.opened or outcome.resolved:
        log.info(
            "alerts_reconciled",
            f"Opened {outcome.opened}, resolved {outcome.resolved} alert(s)",
            observed=outcome.observed,
            opened=outcome.opened,
            resolved=outcome.resolved,
        )
    return outcome
