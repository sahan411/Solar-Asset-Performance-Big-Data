"""Tests for structured JSON logging.

Logs are an assessed deliverable and the pipeline's only narrative when something
goes wrong mid-demo, so the shape is pinned here: one JSON object per line, a
stable `event` slug on every line, and UTC timestamps.
"""

from __future__ import annotations

import json
import logging

import pytest

from simulators.common.logging import JsonLogFormatter, get_logger


def _clear_solariq_loggers() -> None:
    manager = logging.Logger.manager
    for name in [n for n in manager.loggerDict if n.startswith("solariq.")]:
        logger = logging.getLogger(name)
        logger.handlers.clear()
        logger.setLevel(logging.NOTSET)


@pytest.fixture(autouse=True)
def reset_loggers():
    """Drop handlers before AND after each test.

    get_logger caches by name on the global logging registry and attaches a
    StreamHandler bound to whatever `sys.stdout` was at the time. Modules such as
    simulators.streaming.simulator call get_logger at import, so by the time
    these tests run a handler may already exist pointing at the real stdout —
    and capsys, which swaps sys.stdout afterwards, would capture nothing.
    Clearing first forces get_logger to rebind to the captured stream.
    """
    _clear_solariq_loggers()
    yield
    _clear_solariq_loggers()


def emitted(capsys) -> list[dict]:
    """Parse captured stdout into JSON objects, one per line."""
    out = capsys.readouterr().out.strip()
    return [json.loads(line) for line in out.splitlines() if line]


class TestLogShape:
    def test_every_required_field_is_present(self, capsys):
        log = get_logger("streaming-simulator")
        log.info("telemetry_published", "Published solar telemetry event")

        record = emitted(capsys)[0]
        assert set(record) >= {"timestamp", "level", "service", "event", "message"}
        assert record["level"] == "INFO"
        assert record["service"] == "streaming-simulator"
        assert record["event"] == "telemetry_published"
        assert record["message"] == "Published solar telemetry event"

    def test_each_call_emits_exactly_one_line(self, capsys):
        log = get_logger("streaming-simulator")
        log.info("a", "one")
        log.info("b", "two")

        assert len(emitted(capsys)) == 2

    def test_timestamps_are_utc(self, capsys):
        log = get_logger("streaming-simulator")
        log.info("telemetry_published", "x")

        assert emitted(capsys)[0]["timestamp"].endswith("Z")

    def test_context_fields_are_included(self, capsys):
        log = get_logger("streaming-simulator")
        log.info(
            "telemetry_published",
            "Published solar telemetry event",
            plant_id="PLANT_01",
            inverter_id="INV_03",
            event_id="abc-123",
        )

        record = emitted(capsys)[0]
        assert record["plant_id"] == "PLANT_01"
        assert record["inverter_id"] == "INV_03"
        assert record["event_id"] == "abc-123"

    def test_non_serialisable_context_does_not_break_logging(self, capsys):
        # A logging call must never be the thing that crashes the simulator.
        class Opaque:
            def __repr__(self) -> str:
                return "<opaque>"

        log = get_logger("streaming-simulator")
        log.info("telemetry_published", "x", weird=Opaque())

        assert emitted(capsys)[0]["weird"] == "<opaque>"

    def test_context_cannot_clobber_reserved_record_attributes(self, capsys):
        # `args`, `module` and `name` are real LogRecord attributes. Passed
        # straight into logging's `extra=`, each raises
        # "Attempt to overwrite ... in LogRecord". Bundling context under one
        # namespaced key is what stops a field name from crashing the simulator.
        log = get_logger("streaming-simulator")
        log.info("telemetry_published", "real message", args="ctx", module="ctx", name="ctx")

        record = emitted(capsys)[0]
        assert record["message"] == "real message"
        assert record["event"] == "telemetry_published"
        assert record["args"] == "ctx"
        assert record["module"] == "ctx"
        assert record["name"] == "ctx"


class TestLevels:
    @pytest.mark.parametrize(
        "method,expected",
        [("debug", "DEBUG"), ("info", "INFO"), ("warning", "WARNING"), ("error", "ERROR")],
    )
    def test_each_level_is_labelled(self, capsys, method, expected):
        log = get_logger("streaming-simulator", level="DEBUG")
        getattr(log, method)("some_event", "message")

        assert emitted(capsys)[0]["level"] == expected

    def test_level_defaults_to_info_and_filters_debug(self, capsys, monkeypatch):
        monkeypatch.delenv("LOG_LEVEL", raising=False)
        log = get_logger("streaming-simulator")
        log.debug("noisy_event", "should not appear")

        assert emitted(capsys) == []

    def test_log_level_env_var_is_honoured(self, capsys, monkeypatch):
        monkeypatch.setenv("LOG_LEVEL", "debug")
        log = get_logger("streaming-simulator")
        log.debug("noisy_event", "should appear")

        assert emitted(capsys)[0]["event"] == "noisy_event"


class TestErrors:
    def test_exception_captures_type_message_and_traceback(self, capsys):
        log = get_logger("streaming-simulator")
        try:
            raise ValueError("kafka unreachable")
        except ValueError:
            log.exception("producer_failed", "Could not publish telemetry")

        error = emitted(capsys)[0]["error"]
        assert error["type"] == "ValueError"
        assert error["message"] == "kafka unreachable"
        assert "ValueError: kafka unreachable" in error["traceback"]

    def test_error_without_exc_info_carries_no_error_object(self, capsys):
        log = get_logger("streaming-simulator")
        log.error("producer_failed", "Bounded retry budget exhausted", attempts=5)

        record = emitted(capsys)[0]
        assert "error" not in record
        assert record["attempts"] == 5

    def test_traceback_is_bounded(self, capsys):
        def recurse(n: int):
            if n == 0:
                raise RuntimeError("deep")
            recurse(n - 1)

        log = get_logger("streaming-simulator")
        try:
            recurse(200)
        except RuntimeError:
            log.exception("producer_failed", "deep failure")

        assert len(emitted(capsys)[0]["error"]["traceback"]) <= 4000


class TestLoggerWiring:
    def test_repeated_get_logger_calls_do_not_duplicate_output(self, capsys):
        # get_logger runs once per tick in some call sites; a handler added each
        # time would multiply every line.
        for _ in range(5):
            get_logger("streaming-simulator")
        get_logger("streaming-simulator").info("telemetry_published", "once")

        assert len(emitted(capsys)) == 1

    def test_records_do_not_propagate_to_the_root_logger(self):
        get_logger("streaming-simulator")
        assert logging.getLogger("solariq.streaming-simulator").propagate is False

    def test_services_are_namespaced_separately(self, capsys):
        get_logger("streaming-simulator").info("e", "from stream")
        get_logger("batch-simulator").info("e", "from batch")

        assert [r["service"] for r in emitted(capsys)] == [
            "streaming-simulator",
            "batch-simulator",
        ]


class TestFormatterDirectly:
    def test_falls_back_to_the_record_name_when_no_event_is_set(self):
        # Third-party libraries log through plain logging without our wrapper.
        formatter = JsonLogFormatter("streaming-simulator")
        record = logging.LogRecord(
            name="confluent_kafka", level=logging.WARNING, pathname=__file__,
            lineno=1, msg="broker down", args=(), exc_info=None,
        )

        payload = json.loads(formatter.format(record))
        assert payload["event"] == "confluent_kafka"
        assert payload["message"] == "broker down"

    def test_output_is_a_single_line(self):
        formatter = JsonLogFormatter("streaming-simulator")
        record = logging.LogRecord(
            name="x", level=logging.INFO, pathname=__file__,
            lineno=1, msg="multi\nline\nmessage", args=(), exc_info=None,
        )

        assert "\n" not in formatter.format(record)
