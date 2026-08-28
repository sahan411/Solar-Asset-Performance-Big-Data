"""Typed configuration for the SolarIQ serving API.

Every external address and tuning constant is read from the environment,
mirroring the pattern in processing/common/config.py. The API is a separately
deployed service (its own Dockerfile/requirements), so this module is
self-contained rather than importing the processing package.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


class ConfigError(RuntimeError):
    """Raised when required configuration is missing or unusable."""


def _require(name: str, hint: str = "") -> str:
    value = os.getenv(name)
    if value is None or not value.strip():
        suffix = f" {hint}" if hint else ""
        raise ConfigError(f"Required environment variable {name} is not set.{suffix}")
    return value.strip()


def _optional(name: str, default: str) -> str:
    value = os.getenv(name)
    return value.strip() if value and value.strip() else default


def _as_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigError(f"Environment variable {name}={raw!r} is not an integer.") from exc


def _as_origins(name: str, default: str) -> tuple[str, ...]:
    raw = _optional(name, default)
    return tuple(origin.strip() for origin in raw.split(",") if origin.strip())


def cors_origins_from_env() -> tuple[str, ...]:
    """CORS origins alone, independent of DATABASE_URL.

    The app builds its CORS middleware at import time (before the lifespan
    handler runs), so this must not require the full `Settings` — which would
    force every import, including test collection, to have a database
    configured.
    """
    return _as_origins("CORS_ORIGINS", "http://localhost:5173")


@dataclass(frozen=True)
class Settings:
    """Runtime configuration for the FastAPI service."""

    database_url: str
    host: str
    port: int
    log_level: str
    cors_origins: tuple[str, ...]
    stale_data_seconds: int

    @staticmethod
    def from_env() -> "Settings":
        return Settings(
            database_url=_require(
                "DATABASE_URL",
                "Expected e.g. postgresql://solariq:<password>@postgres:5432/solariq",
            ),
            host=_optional("API_HOST", "0.0.0.0"),
            port=_as_int("API_PORT", 8000),
            log_level=_optional("LOG_LEVEL", "INFO"),
            cors_origins=_as_origins("CORS_ORIGINS", "http://localhost:5173"),
            stale_data_seconds=_as_int("STALE_DATA_SECONDS", 60),
        )
