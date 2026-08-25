"""Tests for environment-driven simulator configuration.

Every case sets the environment explicitly via monkeypatch rather than relying on
whatever the developer happens to have exported, so the suite behaves the same
on a laptop and in a container.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from simulators.common.config import (
    SECONDS_PER_SIMULATED_DAY,
    ConfigError,
    KafkaSettings,
    ObservabilitySettings,
    SimulationSettings,
    _as_bool,
    _as_date,
    _as_float,
    _as_int,
    _optional,
    _require,
)

# Every variable these settings read. Cleared before each test so a stray value
# in the developer's shell cannot make a test pass or fail spuriously.
MANAGED_VARS = (
    "KAFKA_BOOTSTRAP_SERVERS",
    "KAFKA_TELEMETRY_TOPIC",
    "KAFKA_INVALID_TOPIC",
    "KAFKA_ALERT_TOPIC",
    "SIMULATION_DAY_SECONDS",
    "TELEMETRY_INTERVAL_SECONDS",
    "SIMULATION_SEED",
    "SIMULATION_START_DATE",
    "SIMULATION_OUTPUT_DIR",
    "PORTFOLIO_CONFIG_PATH",
    "SIMULATION_EMIT_INVALID_EVENTS",
    "PROMETHEUS_PORT",
    "NO_TELEMETRY_ALERT_SECONDS",
)


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for name in MANAGED_VARS:
        monkeypatch.delenv(name, raising=False)


class TestHelpers:
    def test_require_returns_a_stripped_value(self, monkeypatch):
        monkeypatch.setenv("SOLARIQ_TEST_VAR", "  value  ")
        assert _require("SOLARIQ_TEST_VAR") == "value"

    @pytest.mark.parametrize("raw", ["", "   "])
    def test_require_treats_blank_as_missing(self, monkeypatch, raw):
        monkeypatch.setenv("SOLARIQ_TEST_VAR", raw)
        with pytest.raises(ConfigError, match="is not set"):
            _require("SOLARIQ_TEST_VAR")

    def test_require_includes_the_hint(self, monkeypatch):
        monkeypatch.delenv("SOLARIQ_TEST_VAR", raising=False)
        with pytest.raises(ConfigError, match="Expected something useful"):
            _require("SOLARIQ_TEST_VAR", "Expected something useful.")

    def test_optional_falls_back_on_blank(self, monkeypatch):
        monkeypatch.setenv("SOLARIQ_TEST_VAR", "   ")
        assert _optional("SOLARIQ_TEST_VAR", "fallback") == "fallback"

    def test_as_float_rejects_non_numbers(self, monkeypatch):
        monkeypatch.setenv("SOLARIQ_TEST_VAR", "abc")
        with pytest.raises(ConfigError, match="is not a number"):
            _as_float("SOLARIQ_TEST_VAR", 1.0)

    @pytest.mark.parametrize("raw", ["nan", "inf", "-inf", "Infinity"])
    def test_as_float_rejects_nan_and_infinity(self, monkeypatch, raw):
        # These parse cleanly as floats and would silently poison the generation
        # model rather than failing at startup.
        monkeypatch.setenv("SOLARIQ_TEST_VAR", raw)
        with pytest.raises(ConfigError, match="must be a finite number"):
            _as_float("SOLARIQ_TEST_VAR", 1.0)

    def test_as_int_rejects_non_integers(self, monkeypatch):
        monkeypatch.setenv("SOLARIQ_TEST_VAR", "3.5")
        with pytest.raises(ConfigError, match="is not an integer"):
            _as_int("SOLARIQ_TEST_VAR", 1)

    def test_as_date_rejects_non_iso_dates(self, monkeypatch):
        monkeypatch.setenv("SOLARIQ_TEST_VAR", "21-08-2026")
        with pytest.raises(ConfigError, match="is not an ISO date"):
            _as_date("SOLARIQ_TEST_VAR", "2026-08-21")

    @pytest.mark.parametrize(
        "raw,expected",
        [("true", True), ("TRUE", True), ("yes", True), ("1", True),
         ("false", False), ("no", False), ("0", False)],
    )
    def test_as_bool_accepts_the_usual_spellings(self, monkeypatch, raw, expected):
        monkeypatch.setenv("SOLARIQ_TEST_VAR", raw)
        assert _as_bool("SOLARIQ_TEST_VAR", not expected) is expected

    def test_as_bool_rejects_anything_else(self, monkeypatch):
        monkeypatch.setenv("SOLARIQ_TEST_VAR", "maybe")
        with pytest.raises(ConfigError, match="is not a boolean"):
            _as_bool("SOLARIQ_TEST_VAR", False)


class TestKafkaSettings:
    def test_defaults_match_the_frozen_contract(self):
        settings = KafkaSettings.from_env()

        assert settings.bootstrap_servers == "kafka:9092"
        assert settings.telemetry_topic == "solar.telemetry.raw"
        assert settings.invalid_topic == "solar.telemetry.invalid"
        assert settings.alert_topic == "solar.alerts"

    def test_environment_overrides_defaults(self, monkeypatch):
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
        assert KafkaSettings.from_env().bootstrap_servers == "localhost:29092"

    def test_topics_must_be_distinct(self, monkeypatch):
        # Quarantining invalid records into the valid topic would feed malformed
        # events straight to Spark as if they had passed validation.
        monkeypatch.setenv("KAFKA_INVALID_TOPIC", "solar.telemetry.raw")
        with pytest.raises(ConfigError, match="must be distinct"):
            KafkaSettings.from_env()


class TestSimulationSettings:
    def test_defaults(self):
        settings = SimulationSettings.from_env()

        assert settings.day_seconds == 300.0
        assert settings.telemetry_interval_seconds == 3.0
        assert settings.seed == 8203
        assert settings.start_date == date(2026, 8, 21)
        assert settings.output_dir == Path("/data/daily")
        assert settings.portfolio_config_path == Path("simulators/config/portfolio.yaml")
        assert settings.emit_invalid_events is False

    def test_invalid_event_emission_is_off_by_default(self):
        # It must never be on during the assessment unless switched on knowingly.
        assert SimulationSettings.from_env().emit_invalid_events is False

    def test_derived_tick_and_compression_values(self):
        settings = SimulationSettings.from_env()

        assert settings.ticks_per_day == 100.0
        assert settings.compression_factor == SECONDS_PER_SIMULATED_DAY / 300.0
        assert settings.compression_factor == 288.0

    @pytest.mark.parametrize("bad", ["0", "-1"])
    def test_non_positive_day_length_fails(self, monkeypatch, bad):
        monkeypatch.setenv("SIMULATION_DAY_SECONDS", bad)
        with pytest.raises(ConfigError, match="SIMULATION_DAY_SECONDS must be greater than zero"):
            SimulationSettings.from_env()

    @pytest.mark.parametrize("bad", ["0", "-3"])
    def test_non_positive_interval_fails(self, monkeypatch, bad):
        monkeypatch.setenv("TELEMETRY_INTERVAL_SECONDS", bad)
        with pytest.raises(ConfigError, match="must be greater than zero"):
            SimulationSettings.from_env()

    def test_interval_longer_than_the_day_fails(self, monkeypatch):
        monkeypatch.setenv("SIMULATION_DAY_SECONDS", "10")
        monkeypatch.setenv("TELEMETRY_INTERVAL_SECONDS", "30")
        with pytest.raises(ConfigError, match="before its first telemetry tick"):
            SimulationSettings.from_env()

    def test_too_few_ticks_per_day_fails(self, monkeypatch):
        # 60/3 = 20 ticks, below the 24 needed to read as a curve.
        monkeypatch.setenv("SIMULATION_DAY_SECONDS", "60")
        monkeypatch.setenv("TELEMETRY_INTERVAL_SECONDS", "3")
        with pytest.raises(ConfigError, match="ticks per simulated day"):
            SimulationSettings.from_env()

    def test_exactly_the_minimum_tick_count_is_accepted(self, monkeypatch):
        monkeypatch.setenv("SIMULATION_DAY_SECONDS", "72")
        monkeypatch.setenv("TELEMETRY_INTERVAL_SECONDS", "3")
        assert SimulationSettings.from_env().ticks_per_day == 24.0

    def test_malformed_start_date_fails(self, monkeypatch):
        monkeypatch.setenv("SIMULATION_START_DATE", "not-a-date")
        with pytest.raises(ConfigError, match="is not an ISO date"):
            SimulationSettings.from_env()

    def test_a_negative_seed_is_allowed(self, monkeypatch):
        # Any integer seeds a PRNG; only non-integers are a mistake.
        monkeypatch.setenv("SIMULATION_SEED", "-1")
        assert SimulationSettings.from_env().seed == -1


class TestObservabilitySettings:
    def test_defaults(self):
        settings = ObservabilitySettings.from_env()

        assert settings.prometheus_port == 9101
        assert settings.no_telemetry_alert_seconds == 60

    @pytest.mark.parametrize("bad", ["0", "65536", "-1"])
    def test_out_of_range_port_fails(self, monkeypatch, bad):
        monkeypatch.setenv("PROMETHEUS_PORT", bad)
        with pytest.raises(ConfigError, match="PROMETHEUS_PORT must be between"):
            ObservabilitySettings.from_env()

    def test_non_positive_staleness_budget_fails(self, monkeypatch):
        monkeypatch.setenv("NO_TELEMETRY_ALERT_SECONDS", "0")
        with pytest.raises(ConfigError, match="NO_TELEMETRY_ALERT_SECONDS"):
            ObservabilitySettings.from_env()


class TestSettingsAreImmutable:
    @pytest.mark.parametrize(
        "settings,field,value",
        [
            (KafkaSettings.from_env, "bootstrap_servers", "elsewhere:9092"),
            (SimulationSettings.from_env, "seed", 1),
            (ObservabilitySettings.from_env, "prometheus_port", 1),
        ],
    )
    def test_settings_cannot_be_mutated_at_runtime(self, settings, field, value):
        instance = settings()
        with pytest.raises(Exception):
            setattr(instance, field, value)


class TestEnvExampleStaysInStep:
    """.env.example is the documentation for these variables; drift makes it a lie."""

    @pytest.fixture(scope="class")
    def documented(self):
        text = (Path(__file__).resolve().parents[2] / ".env.example").read_text(encoding="utf-8")
        names = set()
        for line in text.splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                names.add(line.split("=", 1)[0].strip())
        return names

    def test_every_variable_the_code_reads_is_documented(self, documented):
        missing = sorted(set(MANAGED_VARS) - documented)
        assert not missing, f".env.example is missing: {missing}"

    def test_log_level_is_documented(self, documented):
        # Read by simulators.common.logging rather than by a settings class.
        assert "LOG_LEVEL" in documented

    def test_no_secret_looking_values_are_committed(self, documented):
        text = (Path(__file__).resolve().parents[2] / ".env.example").read_text(encoding="utf-8")
        for line in text.splitlines():
            stripped = line.strip()
            if stripped.startswith("#") or "=" not in stripped:
                continue
            name, _, value = stripped.partition("=")
            if any(word in name.upper() for word in ("PASSWORD", "SECRET", "TOKEN", "KEY")):
                assert not value.strip(), f"{name} must not carry a value in .env.example"
