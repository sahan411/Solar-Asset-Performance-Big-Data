"""Typed configuration for the SolarIQ simulation subsystem.

Every external address and tuning constant is read from the environment —
nothing is hard-coded — so the same code runs unchanged on a laptop, in Docker
Compose and against any other broker.

Settings are grouped by the system they describe and loaded explicitly via
`from_env()` rather than at import time, so importing a module never fails
because an unrelated variable is missing.

The structure here deliberately mirrors `processing/common/config.py` (Member 2):
same `ConfigError`, same `_require`/`_optional`/`_as_*` helpers, same frozen
dataclass with a `from_env()` staticmethod and a `validate()`. Two subsystems
that read the same environment should read it the same way, and the shared
variable names below are a contract — renaming one needs team agreement.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path

# Env var names shared with Member 2's processing subsystem. Keep them in sync
# with processing/common/config.py and .env.example; renaming one is a contract
# change, not a local edit.
ENV_KAFKA_BOOTSTRAP = "KAFKA_BOOTSTRAP_SERVERS"
ENV_TELEMETRY_TOPIC = "KAFKA_TELEMETRY_TOPIC"
ENV_INVALID_TOPIC = "KAFKA_INVALID_TOPIC"
ENV_ALERT_TOPIC = "KAFKA_ALERT_TOPIC"
ENV_OUTPUT_DIR = "SIMULATION_OUTPUT_DIR"
ENV_PORTFOLIO_PATH = "PORTFOLIO_CONFIG_PATH"
ENV_TELEMETRY_INTERVAL = "TELEMETRY_INTERVAL_SECONDS"

# A simulated day is always 24 hours of event time, however few real seconds it
# is compressed into.
SECONDS_PER_SIMULATED_DAY = 86_400.0


class ConfigError(RuntimeError):
    """Raised when configuration is missing or unusable.

    Deliberately fatal: a mistyped bootstrap server or an impossible interval
    should stop the simulator at startup with a clear message, not surface later
    as a confusing connection timeout or a silently wrong solar curve.
    """


def _require(name: str, hint: str = "") -> str:
    """Read a mandatory environment variable or fail with an actionable message."""
    value = os.getenv(name)
    if value is None or not value.strip():
        suffix = f" {hint}" if hint else ""
        raise ConfigError(f"Required environment variable {name} is not set.{suffix}")
    return value.strip()


def _optional(name: str, default: str) -> str:
    value = os.getenv(name)
    return value.strip() if value and value.strip() else default


def _as_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise ConfigError(f"Environment variable {name}={raw!r} is not a number.") from exc
    # NaN and infinity parse happily as floats and would poison every downstream
    # calculation rather than failing here.
    if value != value or value in (float("inf"), float("-inf")):
        raise ConfigError(f"Environment variable {name}={raw!r} must be a finite number.")
    return value


def _as_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigError(f"Environment variable {name}={raw!r} is not an integer.") from exc


def _as_date(name: str, default: str) -> date:
    raw = _optional(name, default)
    try:
        return date.fromisoformat(raw)
    except ValueError as exc:
        raise ConfigError(
            f"Environment variable {name}={raw!r} is not an ISO date (expected YYYY-MM-DD)."
        ) from exc


def _as_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    normalized = raw.strip().lower()
    if normalized in {"true", "t", "yes", "y", "1"}:
        return True
    if normalized in {"false", "f", "no", "n", "0"}:
        return False
    raise ConfigError(f"Environment variable {name}={raw!r} is not a boolean.")


@dataclass(frozen=True)
class KafkaSettings:
    """Kafka destination for published telemetry.

    The producer side of the topics Member 2's Spark job consumes.
    """

    bootstrap_servers: str
    telemetry_topic: str
    invalid_topic: str
    alert_topic: str

    @staticmethod
    def from_env() -> "KafkaSettings":
        settings = KafkaSettings(
            bootstrap_servers=_optional(ENV_KAFKA_BOOTSTRAP, "kafka:9092"),
            telemetry_topic=_optional(ENV_TELEMETRY_TOPIC, "solar.telemetry.raw"),
            invalid_topic=_optional(ENV_INVALID_TOPIC, "solar.telemetry.invalid"),
            alert_topic=_optional(ENV_ALERT_TOPIC, "solar.alerts"),
        )
        settings.validate()
        return settings

    def validate(self) -> None:
        topics = {
            ENV_TELEMETRY_TOPIC: self.telemetry_topic,
            ENV_INVALID_TOPIC: self.invalid_topic,
            ENV_ALERT_TOPIC: self.alert_topic,
        }
        if len(set(topics.values())) != len(topics):
            raise ConfigError(
                "Kafka topics must be distinct; valid and quarantined telemetry "
                f"cannot share a topic. Got {topics}."
            )


@dataclass(frozen=True)
class SimulationSettings:
    """The simulated clock, the deterministic seed and where output lands.

    `day_seconds` is how many REAL seconds one simulated day is compressed into.
    Event timestamps still span a full 24 hours — see simulators.common.time.
    """

    day_seconds: float
    telemetry_interval_seconds: float
    seed: int
    start_date: date
    output_dir: Path
    portfolio_config_path: Path
    # Publishing deliberately malformed records is a test affordance and must
    # stay off in the assessment demo unless explicitly switched on.
    emit_invalid_events: bool

    @property
    def ticks_per_day(self) -> float:
        """How many telemetry ticks fall inside one simulated day."""
        return self.day_seconds / self.telemetry_interval_seconds

    @property
    def compression_factor(self) -> float:
        """Simulated seconds elapsed per real second (288x at the defaults)."""
        return SECONDS_PER_SIMULATED_DAY / self.day_seconds

    @staticmethod
    def from_env() -> "SimulationSettings":
        settings = SimulationSettings(
            day_seconds=_as_float("SIMULATION_DAY_SECONDS", 300.0),
            telemetry_interval_seconds=_as_float(ENV_TELEMETRY_INTERVAL, 3.0),
            seed=_as_int("SIMULATION_SEED", 8203),
            start_date=_as_date("SIMULATION_START_DATE", "2026-08-21"),
            output_dir=Path(_optional(ENV_OUTPUT_DIR, "/data/daily")),
            portfolio_config_path=Path(
                _optional(ENV_PORTFOLIO_PATH, "simulators/config/portfolio.yaml")
            ),
            emit_invalid_events=_as_bool("SIMULATION_EMIT_INVALID_EVENTS", False),
        )
        settings.validate()
        return settings

    def validate(self) -> None:
        if self.day_seconds <= 0:
            raise ConfigError("SIMULATION_DAY_SECONDS must be greater than zero.")
        if self.telemetry_interval_seconds <= 0:
            raise ConfigError(f"{ENV_TELEMETRY_INTERVAL} must be greater than zero.")
        if self.telemetry_interval_seconds > self.day_seconds:
            raise ConfigError(
                f"{ENV_TELEMETRY_INTERVAL}={self.telemetry_interval_seconds} exceeds "
                f"SIMULATION_DAY_SECONDS={self.day_seconds}: a simulated day would end "
                "before its first telemetry tick."
            )
        # Under ~24 samples a day the solar curve is too coarse to read as a
        # curve at all, and windowed aggregation has almost nothing to average.
        if self.ticks_per_day < 24:
            raise ConfigError(
                f"SIMULATION_DAY_SECONDS={self.day_seconds} with "
                f"{ENV_TELEMETRY_INTERVAL}={self.telemetry_interval_seconds} yields only "
                f"{self.ticks_per_day:.1f} ticks per simulated day; at least 24 are needed "
                "for a usable generation curve."
            )


@dataclass(frozen=True)
class ObservabilitySettings:
    """Prometheus exposure for the simulator process."""

    prometheus_port: int
    # Staleness budget for the no-telemetry health rule. Kept here so the
    # exporter and the alert rule cannot drift apart.
    no_telemetry_alert_seconds: int

    @staticmethod
    def from_env() -> "ObservabilitySettings":
        settings = ObservabilitySettings(
            prometheus_port=_as_int("PROMETHEUS_PORT", 9101),
            no_telemetry_alert_seconds=_as_int("NO_TELEMETRY_ALERT_SECONDS", 60),
        )
        settings.validate()
        return settings

    def validate(self) -> None:
        if not 1 <= self.prometheus_port <= 65_535:
            raise ConfigError(
                f"PROMETHEUS_PORT must be between 1 and 65535, got {self.prometheus_port}."
            )
        if self.no_telemetry_alert_seconds <= 0:
            raise ConfigError("NO_TELEMETRY_ALERT_SECONDS must be greater than zero.")
