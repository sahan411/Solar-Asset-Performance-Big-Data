"""Prometheus metrics for the SolarIQ serving API.

Route labels use the matched route *template* (e.g. `/api/v1/plants/{plant_id}/live`),
never the raw request path, so a per-plant path parameter cannot explode
cardinality into one series per plant.
"""

from __future__ import annotations

from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest

REQUEST_COUNT = Counter(
    "solariq_api_requests_total",
    "Total HTTP requests handled by the SolarIQ API.",
    ["method", "route", "status_code"],
)

REQUEST_DURATION = Histogram(
    "solariq_api_request_duration_seconds",
    "HTTP request duration in seconds.",
    ["method", "route"],
)

ERROR_COUNT = Counter(
    "solariq_api_errors_total",
    "Total HTTP requests that resulted in a server error (5xx).",
    ["method", "route"],
)


def render_latest() -> tuple[bytes, str]:
    """Render current metrics and their content type for the /metrics endpoint."""
    return generate_latest(), CONTENT_TYPE_LATEST
