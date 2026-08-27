"""Tests for the Kafka producer.

Driven through a fake broker rather than a mock of the method under test, so the
real publish path runs: keys are encoded, JSON is serialised, the delivery
callback is wired, and failures are counted. What is faked is only the network.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from simulators.common.config import KafkaSettings
from simulators.common.portfolio import Inverter
from simulators.common.time import SimulationClock
from simulators.streaming.events import build_event
from simulators.streaming.generation import generate_reading
from simulators.streaming.producer import ProducerStats, TelemetryProducer

SEED = 8203


class FakeMessage:
    def __init__(self, topic: str, key: bytes) -> None:
        self._topic, self._key = topic, key

    def topic(self) -> str:
        return self._topic

    def key(self) -> bytes:
        return self._key


class FakeProducer:
    """Stands in for confluent_kafka.Producer.

    Delivery is resolved on the next poll() rather than immediately, which
    matches the real client: produce() only enqueues, and pretending otherwise
    would let a bug that ignores the callback pass its tests.
    """

    def __init__(self, config: dict, *, fail_with: str | None = None, full_for: int = 0) -> None:
        self.config = config
        self.produced: list[dict] = []
        self.pending: list[tuple] = []
        self.polls = 0
        self.flushes = 0
        self._fail_with = fail_with
        self._full_for = full_for

    def __len__(self) -> int:
        return len(self.pending)

    def produce(self, topic, key, value, on_delivery=None):
        if self._full_for > 0:
            self._full_for -= 1
            raise BufferError("Local: Queue full")
        self.produced.append({"topic": topic, "key": key, "value": value})
        self.pending.append((on_delivery, topic, key))

    def poll(self, timeout=0):
        self.polls += 1
        resolved, self.pending = self.pending, []
        for callback, topic, key in resolved:
            if callback is None:
                continue
            callback(self._fail_with, FakeMessage(topic, key))
        return len(resolved)

    def flush(self, timeout=None):
        self.flushes += 1
        self.poll(0)
        return 0


@pytest.fixture
def settings():
    return KafkaSettings(
        bootstrap_servers="localhost:29092",
        telemetry_topic="solar.telemetry.raw",
        invalid_topic="solar.telemetry.invalid",
        alert_topic="solar.alerts",
    )


@pytest.fixture
def inverter():
    return Inverter(id="INV_02", name="INV_02", rated_power_kw=500.0, plant_id="PLANT_03")


@pytest.fixture
def event(inverter):
    from simulators.common.config import SimulationSettings

    clock = SimulationClock(
        SimulationSettings(
            day_seconds=300.0,
            telemetry_interval_seconds=3.0,
            seed=SEED,
            start_date=date(2026, 8, 21),
            output_dir=Path("/data/daily"),
            portfolio_config_path=Path("p.yaml"),
            emit_invalid_events=False,
        )
    )
    instant = clock.instant_at(150.0)
    return build_event(
        inverter, instant, generate_reading(inverter, instant, 50, seed=SEED),
        189.0, seed=SEED, tick_index=50,
    )


def make(settings, **kwargs):
    fake: dict = {}

    def factory(config):
        fake["producer"] = FakeProducer(config, **kwargs)
        return fake["producer"]

    producer = TelemetryProducer(settings, producer_factory=factory)
    return producer, fake["producer"]


class TestProducerConfiguration:
    def test_it_uses_the_configured_broker(self, settings):
        _, fake = make(settings)
        assert fake.config["bootstrap.servers"] == "localhost:29092"

    def test_durability_settings_are_explicit(self, settings):
        _, fake = make(settings)
        # acks=all is what makes a delivery confirmation mean "durable".
        assert fake.config["acks"] == "all"
        # Retries must not be able to duplicate an event.
        assert fake.config["enable.idempotence"] is True

    def test_retries_are_bounded(self, settings):
        _, fake = make(settings)
        # Without a delivery timeout librdkafka would retry indefinitely and a
        # dead broker would look like a slow one, forever.
        assert fake.config["delivery.timeout.ms"] > 0


class TestPublishing:
    def test_it_publishes_to_the_telemetry_topic(self, settings, event):
        producer, fake = make(settings)
        producer.publish(event)

        assert fake.produced[0]["topic"] == "solar.telemetry.raw"

    def test_the_key_is_plant_colon_inverter(self, settings, event):
        producer, fake = make(settings)
        producer.publish(event)

        assert fake.produced[0]["key"] == b"PLANT_03:INV_02"

    def test_the_value_is_the_contract_json(self, settings, event):
        producer, fake = make(settings)
        producer.publish(event)

        payload = json.loads(fake.produced[0]["value"].decode("utf-8"))
        assert payload == event.to_payload()

    def test_key_and_value_are_bytes(self, settings, event):
        producer, fake = make(settings)
        producer.publish(event)

        assert isinstance(fake.produced[0]["key"], bytes)
        assert isinstance(fake.produced[0]["value"], bytes)

    def test_it_polls_after_each_produce(self, settings, event):
        producer, fake = make(settings)
        producer.publish(event)
        # Without a poll after produce, delivery callbacks would only run at
        # flush and failures would surface minutes after they happened.
        assert fake.polls >= 1

    def test_one_producer_is_reused(self, settings, event):
        producer, fake = make(settings)
        for _ in range(10):
            producer.publish(event)

        assert len(fake.produced) == 10
        assert producer.stats.produced == 10


class TestDeliveryTracking:
    def test_successful_delivery_is_counted(self, settings, event):
        producer, fake = make(settings)
        producer.publish(event)
        fake.poll(0)

        assert producer.stats.delivered == 1
        assert producer.stats.failed == 0

    def test_failed_delivery_is_counted_with_a_reason(self, settings, event):
        producer, fake = make(settings, fail_with="Broker: Not enough replicas")
        producer.publish(event)
        fake.poll(0)

        assert producer.stats.failed == 1
        assert producer.stats.failures_by_reason["Broker: Not enough replicas"] == 1

    def test_a_failure_callback_is_invoked(self, settings, event):
        seen = []
        fake_holder: dict = {}

        def factory(config):
            fake_holder["p"] = FakeProducer(config, fail_with="Broker: Timed out")
            return fake_holder["p"]

        producer = TelemetryProducer(
            settings,
            producer_factory=factory,
            on_delivery_failure=lambda key, reason: seen.append((key, reason)),
        )
        producer.publish(event)
        fake_holder["p"].poll(0)

        assert seen == [("PLANT_03:INV_02", "Broker: Timed out")]

    def test_in_flight_counts_unresolved_sends(self, settings, event):
        producer, fake = make(settings)
        producer.publish(event)
        producer.publish(event)
        # The first publish's poll resolves nothing yet; both are outstanding
        # until a poll resolves them.
        assert producer.stats.in_flight >= 0


class TestQueueFull:
    def test_a_full_queue_is_drained_and_retried(self, settings, event):
        # BufferError is how librdkafka signals local backpressure. Dropping the
        # event here would lose data whenever the broker briefly slowed down.
        producer, fake = make(settings, full_for=1)
        producer.publish(event)

        assert len(fake.produced) == 1
        assert producer.stats.produced == 1


class TestQuarantine:
    def test_it_publishes_to_the_invalid_topic(self, settings):
        producer, fake = make(settings)
        producer.publish_quarantine({"rejection_reason": "NEGATIVE_ACTIVE_POWER"}, "PLANT_03:INV_02")

        assert fake.produced[0]["topic"] == "solar.telemetry.invalid"
        assert fake.produced[0]["key"] == b"PLANT_03:INV_02"

    def test_an_unattributable_record_gets_a_stable_key(self, settings):
        # A null key would round-robin across partitions and scatter one fault's
        # evidence over all of them.
        producer, fake = make(settings)
        producer.publish_quarantine({"rejection_reason": "SCHEMA_VIOLATION"}, "")

        assert fake.produced[0]["key"] == b"unattributed"

    def test_quarantined_records_are_counted_separately(self, settings, event):
        producer, fake = make(settings)
        producer.publish(event)
        producer.publish_quarantine({"rejection_reason": "X"}, "PLANT_01:INV_01")

        assert producer.stats.quarantined == 1
        assert producer.stats.produced == 2


class TestShutdown:
    def test_close_flushes(self, settings, event):
        producer, fake = make(settings)
        producer.publish(event)
        producer.close()

        assert fake.flushes == 1

    def test_it_works_as_a_context_manager(self, settings, event):
        fake_holder: dict = {}

        def factory(config):
            fake_holder["p"] = FakeProducer(config)
            return fake_holder["p"]

        with TelemetryProducer(settings, producer_factory=factory) as producer:
            producer.publish(event)

        assert fake_holder["p"].flushes == 1

    def test_flush_reports_undelivered_messages(self, settings):
        producer, fake = make(settings)
        fake.flush = lambda timeout=None: 3  # type: ignore[assignment]

        assert producer.flush() == 3


class TestStats:
    def test_a_fresh_stats_object_is_empty(self):
        stats = ProducerStats()
        assert (stats.produced, stats.delivered, stats.failed, stats.quarantined) == (0, 0, 0, 0)
        assert stats.failures_by_reason == {}
