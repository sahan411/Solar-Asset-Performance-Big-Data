"""Kafka producer for validated telemetry.

Publishing is asynchronous and that shapes everything here. `produce()` only
enqueues into librdkafka's internal buffer; it does not talk to the broker.
Delivery succeeds or fails later, on a background thread, and the only way to
learn which is the delivery callback. A producer that ignores that callback
cannot tell "published" from "silently dropped", which is the failure mode this
module exists to avoid.

Reliability choices, all deliberate:

  * `acks=all` — the broker confirms only once the write is durable. On a
    single-node cluster that is one replica, but the setting is what makes the
    code correct if the cluster ever grows.
  * bounded retries with a delivery timeout — librdkafka retries transient
    failures itself, then gives up and reports the failure through the callback
    rather than blocking the simulation forever.
  * `enable.idempotence` — retries cannot produce duplicates, so at-least-once
    delivery does not become at-least-twice on a flaky connection.

One producer instance is reused for the whole run. Constructing one per event
would open a new connection and rediscover the cluster every three seconds.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Callable

from confluent_kafka import KafkaException, Producer

from simulators.common.config import KafkaSettings
from simulators.common.logging import get_logger
from simulators.streaming.events import TelemetryEvent

log = get_logger("kafka-producer")

# How long librdkafka keeps retrying one message before reporting failure.
# Long enough to ride out a broker restart, short enough that a genuinely dead
# broker is reported inside a demo rather than after it.
DELIVERY_TIMEOUT_MS = 30_000
# Bound on how long produce() may block when the local queue is full, so a
# stalled broker slows the simulation instead of hanging it.
ENQUEUE_TIMEOUT_SECONDS = 5.0


@dataclass
class ProducerStats:
    """Counters for the run. Milestone 9 exports these to Prometheus."""

    produced: int = 0
    delivered: int = 0
    failed: int = 0
    quarantined: int = 0
    # Keyed by the reason librdkafka reported, so a demo failure is diagnosable
    # from the counter alone.
    failures_by_reason: dict[str, int] = field(default_factory=dict)

    @property
    def in_flight(self) -> int:
        return self.produced - self.delivered - self.failed


class TelemetryProducer:
    """Publishes validated telemetry, keyed by asset, with delivery tracking."""

    def __init__(
        self,
        settings: KafkaSettings,
        *,
        producer_factory: Callable[[dict[str, Any]], Producer] = Producer,
        on_delivery_failure: Callable[[str, str], None] | None = None,
    ) -> None:
        self.settings = settings
        self.stats = ProducerStats()
        self._on_delivery_failure = on_delivery_failure
        # Injectable so tests exercise the real publish path against a fake
        # broker rather than mocking out the method under test.
        self._producer = producer_factory(self._config())

    def _config(self) -> dict[str, Any]:
        return {
            "bootstrap.servers": self.settings.bootstrap_servers,
            "client.id": "solariq-streaming-simulator",
            # Durability over latency: this is the only copy of the event.
            "acks": "all",
            "enable.idempotence": True,
            "delivery.timeout.ms": DELIVERY_TIMEOUT_MS,
            # Small linger batches the 35 events of one tick into few requests
            # without adding latency a human would notice in a demo.
            "linger.ms": 20,
            "compression.type": "snappy",
        }

    def _delivery_callback(self, err: Any, msg: Any) -> None:
        """Called on the producer's thread once delivery resolves."""
        if err is None:
            self.stats.delivered += 1
            return

        reason = str(err)
        self.stats.failed += 1
        self.stats.failures_by_reason[reason] = (
            self.stats.failures_by_reason.get(reason, 0) + 1
        )
        key = msg.key().decode("utf-8", "replace") if msg and msg.key() else "unknown"
        # Never swallowed: a delivery failure means an event the rest of the
        # pipeline will never see, and silence here would look like a gap in the
        # data rather than a fault in the producer.
        log.error(
            "telemetry_delivery_failed",
            f"Kafka rejected an event for {key}: {reason}",
            asset_key=key,
            topic=msg.topic() if msg else None,
            error_reason=reason,
        )
        if self._on_delivery_failure is not None:
            self._on_delivery_failure(key, reason)

    def _produce(self, topic: str, key: str, value: str) -> None:
        try:
            self._producer.produce(
                topic=topic,
                key=key.encode("utf-8"),
                value=value.encode("utf-8"),
                on_delivery=self._delivery_callback,
            )
        except BufferError:
            # The local queue is full: the broker is not keeping up. Serve the
            # delivery callbacks to drain it, then try once more before giving
            # up on this event.
            log.warning(
                "producer_queue_full",
                "Local producer queue is full; draining before retry",
                queue_length=len(self._producer),
            )
            self._producer.poll(ENQUEUE_TIMEOUT_SECONDS)
            self._producer.produce(
                topic=topic,
                key=key.encode("utf-8"),
                value=value.encode("utf-8"),
                on_delivery=self._delivery_callback,
            )

        self.stats.produced += 1
        # Serves delivery callbacks for already-completed sends. Zero timeout, so
        # this never blocks the tick loop. Without it callbacks would only run at
        # flush() and failures would surface minutes late.
        self._producer.poll(0)

    def publish(self, event: TelemetryEvent) -> None:
        """Enqueue one validated telemetry event.

        The caller validates first: this method assumes the event is already
        known good, and publishing an invalid one to the raw topic would break
        the contract every downstream consumer relies on.
        """
        self._produce(self.settings.telemetry_topic, event.kafka_key, event.to_json())

    def publish_quarantine(self, record: dict[str, Any], key: str) -> None:
        """Send a rejected record to the quarantine topic.

        Keyed by asset where known so a failing inverter's rejects stay together;
        `unattributed` rather than a null key, which would round-robin them
        across partitions and scatter one fault's evidence.
        """
        self.stats.quarantined += 1
        self._produce(
            self.settings.invalid_topic,
            key or "unattributed",
            json.dumps(record, separators=(",", ":"), default=str),
        )

    def flush(self, timeout_seconds: float = 15.0) -> int:
        """Block until the queue drains. Returns messages still undelivered."""
        remaining = self._producer.flush(timeout_seconds)
        if remaining:
            log.error(
                "producer_flush_incomplete",
                f"{remaining} message(s) still undelivered after "
                f"{timeout_seconds}s; they are lost",
                undelivered=remaining,
            )
        return remaining

    def close(self) -> None:
        """Flush and report. Losing buffered events on exit is a real data loss."""
        remaining = self.flush()
        log.info(
            "producer_closed",
            f"Producer stopped: {self.stats.delivered} delivered, "
            f"{self.stats.failed} failed, {self.stats.quarantined} quarantined",
            produced=self.stats.produced,
            delivered=self.stats.delivered,
            failed=self.stats.failed,
            quarantined=self.stats.quarantined,
            undelivered=remaining,
        )

    def __enter__(self) -> "TelemetryProducer":
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.close()


def check_broker_reachable(settings: KafkaSettings, timeout_seconds: float = 10.0) -> None:
    """Fail fast if Kafka is not there.

    Without this the simulator starts happily, buffers events locally, and only
    reports failure once the delivery timeout expires — by which point a demo
    has been running for half a minute with an empty dashboard and no visible
    reason. Raises KafkaException.
    """
    probe = Producer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "client.id": "solariq-startup-probe",
        }
    )
    metadata = probe.list_topics(timeout=timeout_seconds)
    missing = [
        topic
        for topic in (settings.telemetry_topic, settings.invalid_topic)
        if topic not in metadata.topics
    ]
    if missing:
        # Auto-creation is disabled on the broker, so a missing topic will never
        # appear on its own and every publish would fail.
        raise KafkaException(
            f"Kafka at {settings.bootstrap_servers} is missing topic(s): "
            f"{', '.join(missing)}. Run kafka/scripts/create_topics.sh."
        )
