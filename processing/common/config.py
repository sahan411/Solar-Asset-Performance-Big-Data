"""Typed configuration for the SolarIQ processing subsystem.

Every external address, credential and tuning constant is read from the
environment — nothing is hard-coded — so the same code runs unchanged on a
laptop, in Docker Compose and (later) against real infrastructure.

Settings are grouped by the system they describe and loaded explicitly via
`load_*()` calls rather than at import time, so that importing a module never
fails because an unrelated variable is missing.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

# Env var names shared with Member 1's simulator/.env.example. Keep them in sync;
# renaming one is a contract change that needs team agreement.
ENV_KAFKA_BOOTSTRAP = "KAFKA_BOOTSTRAP_SERVERS"
ENV_TELEMETRY_TOPIC = "KAFKA_TELEMETRY_TOPIC"
ENV_INVALID_TOPIC = "KAFKA_INVALID_TOPIC"
ENV_ALERT_TOPIC = "KAFKA_ALERT_TOPIC"


class ConfigError(RuntimeError):
    """Raised when configuration is missing or unusable.

    Deliberately fatal: a mistyped bootstrap server or a missing credential
    should stop the job at startup with a clear message, not surface later as a
    confusing connection timeout mid-stream.
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
        return float(raw)
    except ValueError as exc:
        raise ConfigError(f"Environment variable {name}={raw!r} is not a number.") from exc


def _as_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigError(f"Environment variable {name}={raw!r} is not an integer.") from exc


@dataclass(frozen=True)
class DatabaseSettings:
    """PostgreSQL serving store."""

    url: str

    @staticmethod
    def from_env() -> "DatabaseSettings":
        return DatabaseSettings(
            url=_require(
                "DATABASE_URL",
                "Expected e.g. postgresql://solariq:<password>@postgres:5432/solariq",
            )
        )


@dataclass(frozen=True)
class KafkaSettings:
    """Kafka source, as published by Member 1."""

    bootstrap_servers: str
    telemetry_topic: str
    invalid_topic: str
    alert_topic: str
    # "earliest" replays the whole retained stream; "latest" starts at the tail.
    # Demo default is earliest so a restarted job still shows the current day.
    starting_offsets: str

    @staticmethod
    def from_env() -> "KafkaSettings":
        return KafkaSettings(
            bootstrap_servers=_optional(ENV_KAFKA_BOOTSTRAP, "kafka:9092"),
            telemetry_topic=_optional(ENV_TELEMETRY_TOPIC, "solar.telemetry.raw"),
            invalid_topic=_optional(ENV_INVALID_TOPIC, "solar.telemetry.invalid"),
            alert_topic=_optional(ENV_ALERT_TOPIC, "solar.alerts"),
            starting_offsets=_optional("STREAM_STARTING_OFFSETS", "earliest"),
        )


@dataclass(frozen=True)
class ObjectStoreSettings:
    """MinIO / S3-compatible raw archive — the Lambda batch layer's source of truth."""

    endpoint: str
    access_key: str
    secret_key: str
    raw_bucket: str

    @property
    def raw_telemetry_uri(self) -> str:
        """s3a:// URI of the normalized telemetry archive."""
        return f"s3a://{self.raw_bucket}/telemetry"

    @staticmethod
    def from_env() -> "ObjectStoreSettings":
        # Credentials are intentionally required rather than defaulted: no
        # credential, however weak, belongs in version control. .env.example
        # documents the local Docker values.
        return ObjectStoreSettings(
            endpoint=_optional("MINIO_ENDPOINT", "http://minio:9000"),
            access_key=_require("MINIO_ACCESS_KEY", "See .env.example for the local demo value."),
            secret_key=_require("MINIO_SECRET_KEY", "See .env.example for the local demo value."),
            raw_bucket=_optional("MINIO_RAW_BUCKET", "solariq-raw"),
        )


@dataclass(frozen=True)
class StreamSettings:
    """Spark Structured Streaming tuning and the live performance model.

    Window defaults are sized for the compressed demo clock (1 simulated day =
    300 real seconds, telemetry every 3 seconds), where a 60-second window holds
    roughly 20 samples per inverter. Under a real 24-hour clock these would be
    minutes-to-hours.
    """

    checkpoint_dir: str
    watermark: str
    window_duration: str
    window_slide: str

    # Below this irradiance the sun is not up enough to judge a plant: expected
    # power approaches zero and performance ratios become meaningless. Straight
    # from the specification's underperformance rule.
    min_irradiance_wm2: float
    # Standard Test Conditions irradiance — the denominator that turns measured
    # irradiance into a fraction of nameplate capacity.
    reference_irradiance_wm2: float

    underperformance_threshold_pct: float

    # How long a fault must persist before it becomes an alert, measured in
    # EVENT-TIME seconds (simulated plant time), not wall-clock seconds. Event
    # time keeps the rule meaningful under the compressed demo clock and makes
    # alerting replay-safe: reprocessing history yields the same alerts.
    #
    # The default is one simulated hour. Under the default demo clock (one
    # simulated day = 300 real seconds) that is roughly 12 real seconds after a
    # fault begins — long enough to demonstrate that brief dips are ignored,
    # short enough to fit a live demo. A production deployment would use hours.
    alert_sustain_seconds: int

    @staticmethod
    def from_env() -> "StreamSettings":
        settings = StreamSettings(
            checkpoint_dir=_optional("SPARK_CHECKPOINT_DIR", "/spark-checkpoints"),
            watermark=_optional("STREAM_WATERMARK", "2 minutes"),
            window_duration=_optional("STREAM_WINDOW_DURATION", "60 seconds"),
            window_slide=_optional("STREAM_WINDOW_SLIDE", "15 seconds"),
            min_irradiance_wm2=_as_float("MIN_IRRADIANCE_WM2", 150.0),
            reference_irradiance_wm2=_as_float("REFERENCE_IRRADIANCE_WM2", 1000.0),
            underperformance_threshold_pct=_as_float("UNDERPERFORMANCE_THRESHOLD_PCT", 80.0),
            alert_sustain_seconds=_as_int("ALERT_SUSTAIN_SECONDS", 3600),
        )
        settings.validate()
        return settings

    def validate(self) -> None:
        if self.reference_irradiance_wm2 <= 0:
            raise ConfigError("REFERENCE_IRRADIANCE_WM2 must be greater than zero.")
        if self.min_irradiance_wm2 < 0:
            raise ConfigError("MIN_IRRADIANCE_WM2 must not be negative.")
        if not 0 < self.underperformance_threshold_pct <= 100:
            raise ConfigError("UNDERPERFORMANCE_THRESHOLD_PCT must be in (0, 100].")
        if self.alert_sustain_seconds < 0:
            raise ConfigError("ALERT_SUSTAIN_SECONDS must not be negative.")


@dataclass(frozen=True)
class BatchSettings:
    """Daily batch layer inputs."""

    # Directory Member 1's generator writes daily_reference_YYYY-MM-DD.csv into,
    # mounted into the Airflow containers.
    reference_dir: str
    # Shared portfolio definition (plants/inverters) owned by Member 1; used to
    # seed the asset registry and to validate the reference feed's plant list.
    portfolio_config_path: str
    # Assumed spacing between telemetry samples, used to convert OFFLINE sample
    # counts into downtime minutes.
    telemetry_interval_seconds: float

    @staticmethod
    def from_env() -> "BatchSettings":
        return BatchSettings(
            reference_dir=_optional("SIMULATION_OUTPUT_DIR", "/data/daily"),
            portfolio_config_path=_optional(
                "PORTFOLIO_CONFIG_PATH", "simulators/config/portfolio.yaml"
            ),
            telemetry_interval_seconds=_as_float("TELEMETRY_INTERVAL_SECONDS", 3.0),
        )
