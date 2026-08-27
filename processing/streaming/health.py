"""Pipeline health and Prometheus metrics for the streaming job.

ENGINEERING health, deliberately separate from the business alerts in
`alerts`/`alert_store`. The distinction matters operationally and is one the
demo has to show: "the inverter is broken" and "our ingestion is broken" look
identical on a dashboard showing zero generation, but need completely different
responses. Solar faults go to the `alerts` table; pipeline faults go here.

WHY PROCESSED-EVENT STALENESS BEATS PRODUCED-EVENT STALENESS
Member 1's producer exposes when it last *published* telemetry. This module
exposes when the stream last *processed* it. Alerting on the processed timestamp
covers far more of the system: the producer can be happily publishing while
Kafka, Spark, or the database write is broken, and only the processed timestamp
notices.

REPORTED STATUS
The job reports what it directly knows, and leaves staleness to the reader:

    HEALTHY  - this microbatch processed at least one event
    DEGRADED - the job is alive but the batch was empty
    FAILED   - the batch raised

STALE is computed by consumers (the Prometheus rule and the API's readiness
check) by comparing `last_success_at` against wall-clock now, because only they
can know how long is too long.
"""

from __future__ import annotations

from datetime import datetime, timezone

from prometheus_client import Counter, Gauge, start_http_server

from processing.common.logging import get_logger

log = get_logger("spark-stream")

COMPONENT_STREAM = "spark-stream"

STATUS_HEALTHY = "HEALTHY"
STATUS_DEGRADED = "DEGRADED"
STATUS_FAILED = "FAILED"

# Metric names agreed with Member 1's observability configuration. Note that
# `solariq_events_invalid_total` also appears in the producer's metric set: the
# producer counts events it refused to publish, this counts events the stream
# quarantined. Prometheus separates them by `job` label; the distinction is
# documented in the handoff notes.
EVENTS_PROCESSED = Counter(
    "solariq_events_processed_total",
    "Telemetry events that passed validation and reached the metric computation stage",
)
EVENTS_INVALID = Counter(
    "solariq_events_invalid_total",
    "Telemetry events quarantined by the stream's validation step",
)
STREAM_LAST_PROCESSED = Gauge(
    "solariq_stream_last_processed_timestamp_seconds",
    "Event time of the most recent telemetry the stream processed, as a Unix timestamp",
)
MICROBATCHES = Counter(
    "solariq_stream_microbatches_total",
    "Microbatches completed by the streaming job",
)
MICROBATCH_FAILURES = Counter(
    "solariq_stream_microbatch_failures_total",
    "Microbatches that raised and were retried",
)
ACTIVE_ALERTS = Gauge(
    "solariq_active_alerts",
    "Business alerts currently in ACTIVE status",
)

_UPSERT_HEALTH = """
INSERT INTO pipeline_health (component, status, last_event_at, last_success_at, message, updated_at)
VALUES (%s, %s, %s, %s, %s, NOW())
ON CONFLICT (component) DO UPDATE SET
    status          = EXCLUDED.status,
    -- GREATEST keeps the high-water mark: an empty batch reports no event time
    -- of its own and must not erase the last one we genuinely saw.
    last_event_at   = GREATEST(pipeline_health.last_event_at, EXCLUDED.last_event_at),
    last_success_at = GREATEST(pipeline_health.last_success_at, EXCLUDED.last_success_at),
    message         = EXCLUDED.message,
    updated_at      = NOW()
"""


def start_metrics_server(port: int) -> None:
    """Expose the Prometheus scrape endpoint.

    Failure is logged but not fatal: losing observability should not take down a
    pipeline that is otherwise processing correctly.
    """
    try:
        start_http_server(port)
        log.info("metrics_server_started", f"Prometheus metrics on :{port}", port=port)
    except OSError as exc:
        log.error(
            "metrics_server_failed",
            f"Could not bind the metrics port; continuing without metrics: {exc}",
            port=port,
            error=str(exc),
        )


def record_batch_metrics(
    processed: int,
    invalid: int,
    last_event_at: datetime | None,
    active_alerts: int | None = None,
) -> None:
    """Update Prometheus counters/gauges after a microbatch."""
    MICROBATCHES.inc()
    if processed:
        EVENTS_PROCESSED.inc(processed)
    if invalid:
        EVENTS_INVALID.inc(invalid)
    if last_event_at is not None:
        STREAM_LAST_PROCESSED.set(last_event_at.replace(tzinfo=timezone.utc).timestamp())
    if active_alerts is not None:
        ACTIVE_ALERTS.set(active_alerts)


def record_health(
    conn,
    status: str,
    last_event_at: datetime | None,
    message: str,
    component: str = COMPONENT_STREAM,
) -> None:
    """Upsert this component's health row, inside the caller's transaction."""
    now = datetime.now(tz=timezone.utc)
    # A failed batch did not succeed, so it must not advance last_success_at.
    last_success = now if status != STATUS_FAILED else None
    with conn.cursor() as cur:
        cur.execute(_UPSERT_HEALTH, (component, status, last_event_at, last_success, message))


def count_active_alerts(conn) -> int:
    """Current ACTIVE alert count, for the gauge and for logging."""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM alerts WHERE status = 'ACTIVE'")
        return cur.fetchone()[0]
