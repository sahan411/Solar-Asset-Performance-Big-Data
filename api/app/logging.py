"""Structured JSON logging for the SolarIQ serving API.

Mirrors processing/common/logging.py so every subsystem emits the same log
shape (see the master specification, section 17), duplicated rather than
imported because the API is a separately deployed service with its own,
lighter dependency set.

    {"timestamp": "...", "level": "INFO", "service": "api",
     "event": "request_completed", "message": "...", "route": "/api/v1/plants"}
"""

from __future__ import annotations

import json
import logging
import os
import sys
import traceback
from datetime import datetime, timezone
from typing import Any

_FIELDS_KEY = "solariq_fields"


def _utc_iso(epoch_seconds: float) -> str:
    return (
        datetime.fromtimestamp(epoch_seconds, tz=timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


class JsonLogFormatter(logging.Formatter):
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
            payload.update(fields)

        if record.exc_info:
            exc_type, exc_value, exc_tb = record.exc_info
            payload["error"] = {
                "type": exc_type.__name__ if exc_type else "UnknownError",
                "message": str(exc_value),
                "traceback": "".join(traceback.format_exception(*record.exc_info))[-4000:],
            }

        return json.dumps(payload, default=str)


class StructuredLogger:
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
        self._log(logging.ERROR, event, message, exc_info=True, **fields)


def get_logger(service: str = "api", level: str | None = None) -> StructuredLogger:
    resolved_level = (level or os.getenv("LOG_LEVEL") or "INFO").upper()

    logger = logging.getLogger(f"solariq.{service}")
    logger.setLevel(resolved_level)
    logger.propagate = False

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonLogFormatter(service))
        logger.addHandler(handler)

    return StructuredLogger(logger)
