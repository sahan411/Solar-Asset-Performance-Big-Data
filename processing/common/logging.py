"""Structured JSON logging for SolarIQ processing components.

The project requires every service to emit machine-readable logs with a stable
`event` name so the pipeline can be traced across Kafka, Spark, Airflow and the
API. This module deliberately uses only the standard library: a logging
framework would be a dependency the project does not need.

Log shape (matching the master specification, section 17):

    {"timestamp": "...", "level": "INFO", "service": "spark-stream",
     "event": "microbatch_processed", "message": "...", "plant_id": "PLANT_01"}

Usage:

    log = get_logger("spark-stream")
    log.info("microbatch_processed", "Wrote 5 plant metric rows", rows=5)
"""

from __future__ import annotations

import json
import logging
import os
import sys
import traceback
from datetime import datetime, timezone
from typing import Any

# Extra fields ride in a single namespaced dict so they can never collide with
# reserved logging.LogRecord attributes such as `message`, `module` or `args`.
_FIELDS_KEY = "solariq_fields"


def _utc_iso(epoch_seconds: float) -> str:
    """Render a log timestamp as UTC ISO-8601 with a trailing Z."""
    return (
        datetime.fromtimestamp(epoch_seconds, tz=timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


class JsonLogFormatter(logging.Formatter):
    """Renders each LogRecord as a single-line JSON object."""

    def __init__(self, service: str) -> None:
        super().__init__()
        self.service = service

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": _utc_iso(record.created),
            "level": record.levelname,
            "service": self.service,
            "event": getattr(record, "solariq_event", record.name),
            "message": record.getMessage(),
        }

        fields = getattr(record, _FIELDS_KEY, None)
        if fields:
            # Caller-supplied context (plant_id, inverter_id, row counts, ...).
            payload.update(fields)

        if record.exc_info:
            exc_type, exc_value, exc_tb = record.exc_info
            payload["error"] = {
                "type": exc_type.__name__ if exc_type else "UnknownError",
                "message": str(exc_value),
                # Bounded: a full Spark/py4j traceback can be thousands of lines
                # and would drown the log aggregator.
                "traceback": "".join(traceback.format_exception(*record.exc_info))[-4000:],
            }

        return json.dumps(payload, default=str)


class StructuredLogger:
    """Thin wrapper that forces every log line to carry an `event` name.

    Free-text-only logs are hard to alert on; requiring a stable event slug keeps
    the pipeline greppable and lets Prometheus/Loki rules key off `event`.
    """

    def __init__(self, logger: logging.Logger) -> None:
        self._logger = logger

    def _log(self, level: int, event: str, message: str, exc_info: bool = False, **fields: Any) -> None:
        self._logger.log(
            level,
            message,
            exc_info=exc_info,
            extra={"solariq_event": event, _FIELDS_KEY: fields},
        )

    def debug(self, event: str, message: str, **fields: Any) -> None:
        self._log(logging.DEBUG, event, message, **fields)

    def info(self, event: str, message: str, **fields: Any) -> None:
        self._log(logging.INFO, event, message, **fields)

    def warning(self, event: str, message: str, **fields: Any) -> None:
        self._log(logging.WARNING, event, message, **fields)

    def error(self, event: str, message: str, exc_info: bool = False, **fields: Any) -> None:
        self._log(logging.ERROR, event, message, exc_info=exc_info, **fields)

    def exception(self, event: str, message: str, **fields: Any) -> None:
        """Log an error together with the active exception's traceback."""
        self._log(logging.ERROR, event, message, exc_info=True, **fields)


def get_logger(service: str, level: str | None = None) -> StructuredLogger:
    """Return a structured logger for a named service.

    `service` becomes the `service` field on every line (e.g. "spark-stream",
    "airflow-batch"). The level defaults to $LOG_LEVEL, then INFO.
    """
    resolved_level = (level or os.getenv("LOG_LEVEL") or "INFO").upper()

    logger = logging.getLogger(f"solariq.{service}")
    logger.setLevel(resolved_level)
    # Root handlers would emit a second, unstructured copy of every line.
    logger.propagate = False

    # get_logger may be called repeatedly (e.g. per Spark microbatch); attaching a
    # handler each time would duplicate output geometrically.
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonLogFormatter(service))
        logger.addHandler(handler)

    return StructuredLogger(logger)
