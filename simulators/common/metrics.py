"""Prometheus metrics for the simulation subsystem.

Deliberately few, and deliberately low-cardinality. Every distinct label
combination is a separate time series held in memory forever, so labelling by
`event_id` would create one series per event — 3500 per simulated day — and
eventually take Prometheus down. `plant_id` gives five, which is useful and
bounded. Inverter id would give thirty-five, which is affordable but adds
nothing a plant-level view does not already show during a demo.

`solariq_last_event_timestamp_seconds` is the important one. It carries REAL
time, not simulated time, because the no-telemetry alert asks "when did we last
hear anything?" and compares against `time()`. Exporting simulated time here
would make the metric jump 288 seconds per real second and the staleness rule
meaningless.
"""

from __future__ import annotations

import time
from typing import Any

from prometheus_client import CollectorRegistry, Counter, Gauge, start_http_server

from simulators.common.logging import get_logger

log = get_logger("metrics")

# Scenario names mapped to a numeric gauge. Prometheus gauges hold numbers, not
# strings, so the active scenario is exported as a code with the name carried in
# a label. 0 means healthy, which makes `> 0` the "something is scripted right
# now" query.
SCENARIO_CODES = {
    None: 0,
    "NORMAL": 0,
    "RECOVERY": 1,
    "INV_UNDERPERFORMANCE": 2,
    "INV_OFFLINE": 3,
    "TELEMETRY_GAP": 4,
}


class SimulatorMetrics:
    """The simulator's Prometheus surface.

    Takes its own registry rather than using the global default, so tests can
    build a fresh instance without the duplicate-timeseries error that the
    global registry raises on re-registration.
    """

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry if registry is not None else CollectorRegistry()

        self.events_produced = Counter(
            "solariq_events_produced_total",
            "Telemetry events published to Kafka",
            ["plant_id"],
            registry=self.registry,
        )
        self.events_invalid = Counter(
            "solariq_events_invalid_total",
            "Events rejected by source-side validation and quarantined",
            ["reason"],
            registry=self.registry,
        )
        self.producer_failures = Counter(
            "solariq_producer_failures_total",
            "Kafka delivery failures reported by the delivery callback",
            registry=self.registry,
        )
        self.last_event_timestamp = Gauge(
            "solariq_last_event_timestamp_seconds",
            "Unix time (REAL, not simulated) of the most recent published event",
            registry=self.registry,
        )
        self.simulation_day = Gauge(
            "solariq_simulation_day",
            "Zero-based index of the simulated day currently being generated",
            registry=self.registry,
        )
        self.active_scenario = Gauge(
            "solariq_active_simulation_scenario",
            "Active demo scenario as a numeric code; 0 means normal operation",
            ["scenario"],
            registry=self.registry,
        )
        self.telemetry_suppressed = Counter(
            "solariq_telemetry_suppressed_total",
            "Events deliberately not published because a TELEMETRY_GAP was active",
            ["plant_id"],
            registry=self.registry,
        )
        self.daily_reference_written = Counter(
            "solariq_daily_reference_written_total",
            "Daily reference feed files written",
            registry=self.registry,
        )

        self._served_port: int | None = None
        # Seed the scenario gauge so the series exists before the first anomaly.
        # A query against a metric that has never been set returns nothing at
        # all, which is indistinguishable from "the exporter is down".
        self.active_scenario.labels(scenario="NORMAL").set(0)

    def record_published(self, plant_id: str, *, now: float | None = None) -> None:
        self.events_produced.labels(plant_id=plant_id).inc()
        self.last_event_timestamp.set(now if now is not None else time.time())

    def record_quarantined(self, reason: str) -> None:
        self.events_invalid.labels(reason=reason).inc()

    def record_producer_failure(self, count: int = 1) -> None:
        self.producer_failures.inc(count)

    def record_suppressed(self, plant_id: str) -> None:
        self.telemetry_suppressed.labels(plant_id=plant_id).inc()

    def record_daily_reference(self) -> None:
        self.daily_reference_written.inc()

    def set_simulation_day(self, day_index: int) -> None:
        self.simulation_day.set(day_index)

    def set_active_scenario(self, scenario: str | None) -> None:
        """Publish the active scenario, clearing whichever was set before.

        Every known scenario is written on each change rather than only the
        active one. Leaving a stale label at a non-zero value would show two
        scenarios running at once on the dashboard.
        """
        active = scenario or "NORMAL"
        for name, code in SCENARIO_CODES.items():
            if name is None:
                continue
            self.active_scenario.labels(scenario=name).set(
                code if name == active else 0
            )

    def serve(self, port: int) -> None:
        """Expose /metrics over HTTP on a background thread."""
        if self._served_port is not None:
            return
        start_http_server(port, registry=self.registry)
        self._served_port = port
        log.info(
            "metrics_server_started",
            f"Prometheus metrics available on port {port}",
            port=port,
        )

    def snapshot(self) -> dict[str, Any]:
        """Current values, for logs and tests rather than for Prometheus."""
        return {
            "produced": sum(
                sample.value
                for metric in self.events_produced.collect()
                for sample in metric.samples
                if sample.name.endswith("_total")
            ),
            "invalid": sum(
                sample.value
                for metric in self.events_invalid.collect()
                for sample in metric.samples
                if sample.name.endswith("_total")
            ),
        }
