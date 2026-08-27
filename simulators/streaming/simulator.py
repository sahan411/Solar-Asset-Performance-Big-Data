"""The streaming simulator: the process that actually runs.

Ties the subsystem together. Each tick, for every inverter:

    generate a healthy reading  (generation.py)
    apply the scripted scenario (scenarios.py)
    advance the energy meter    (generation.EnergyLedger)
    build and validate an event (events.py)
    publish it, or not          (producer.py)

and at each simulated-day boundary, write that day's reference feed for the
batch layer.

Two ordering decisions here are worth stating, because getting either wrong
produces data that looks plausible and is wrong:

  * The energy meter advances even when a TELEMETRY_GAP suppresses publication.
    The plant kept generating; we merely stopped hearing about it. Freezing the
    meter would understate the day's total once telemetry resumes and invent a
    shortfall in the batch reconciliation that nothing caused.

  * The daily reference feed is written when a day *ends*, from the model rather
    than from what was published. It is a forecast of a healthy day, and the
    whole point of the reconciliation is to compare it against actuals that
    include the faults.

Run it with:

    python -m simulators.streaming.simulator
    python -m simulators.streaming.simulator --days 1 --dry-run
"""

from __future__ import annotations

import argparse
import signal
import sys
import time
from dataclasses import dataclass
from types import FrameType
from typing import Any

from simulators.batch.reference import generate_daily_reference
from simulators.common.config import (
    ConfigError,
    KafkaSettings,
    ObservabilitySettings,
    SimulationSettings,
)
from simulators.common.logging import get_logger
from simulators.common.metrics import SimulatorMetrics
from simulators.common.portfolio import Portfolio, load_portfolio
from simulators.common.time import SimulatedInstant, SimulationClock
from simulators.streaming.events import (
    EventValidationError,
    build_event,
    corrupt_event,
    to_quarantine_record,
    validate_payload,
)
from simulators.streaming.generation import EnergyLedger, SolarModel, generate_reading
from simulators.streaming.producer import TelemetryProducer, check_broker_reachable
from simulators.streaming.scenarios import ScenarioSchedule, apply_scenario, load_schedule

log = get_logger("streaming-simulator")


class _Shutdown:
    """Cooperative stop flag, set by SIGINT/SIGTERM.

    The handler only flips a flag: the loop finishes the tick it is in and then
    exits through the normal path, so buffered events are flushed. Killing the
    process mid-tick would lose whatever librdkafka had not yet delivered.
    """

    def __init__(self) -> None:
        self.requested = False

    def request(self, signum: int, _frame: FrameType | None) -> None:
        if self.requested:
            # Second interrupt: the operator means it.
            log.warning("shutdown_forced", "Second signal received, exiting immediately")
            sys.exit(130)
        self.requested = True
        log.info(
            "shutdown_requested",
            f"Signal {signum} received; finishing the current tick and flushing",
            signal=signum,
        )

    def install(self) -> None:
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, self.request)


@dataclass
class TickResult:
    published: int
    suppressed: int
    quarantined: int


class StreamingSimulator:
    """One simulated portfolio, publishing telemetry on a compressed clock."""

    def __init__(
        self,
        *,
        simulation: SimulationSettings,
        portfolio: Portfolio,
        schedule: ScenarioSchedule,
        clock: SimulationClock,
        producer: TelemetryProducer | None,
        metrics: SimulatorMetrics,
        model: SolarModel | None = None,
    ) -> None:
        self.simulation = simulation
        self.portfolio = portfolio
        self.schedule = schedule
        self.clock = clock
        self.producer = producer
        self.metrics = metrics
        self.model = model or SolarModel()
        self.ledger = EnergyLedger()
        self._current_day: int | None = None
        self._last_scenario: str | None = "__unset__"

    def tick(self, tick_index: int, instant: SimulatedInstant) -> TickResult:
        """Generate and publish one tick for every inverter."""
        published = suppressed = quarantined = 0

        window = self.schedule.active_window(instant.seconds_into_day)
        scenario_name = window.scenario if window else None
        if scenario_name != self._last_scenario:
            # Logged on change rather than every tick: 100 identical lines per
            # day would bury the transitions that matter.
            log.info(
                "scenario_changed",
                f"Scenario is now {scenario_name or 'NORMAL'}"
                + (f" on {window.target_label}" if window else ""),
                scenario=scenario_name or "NORMAL",
                target=window.target_label if window else None,
                seconds_into_day=round(instant.seconds_into_day, 1),
            )
            self.metrics.set_active_scenario(scenario_name)
            self._last_scenario = scenario_name

        for inverter in self.portfolio.inverters():
            reading = generate_reading(
                inverter, instant, tick_index, seed=self.simulation.seed, model=self.model
            )
            asset_window = window if window and window.targets(inverter) else None
            outcome = apply_scenario(inverter, reading, asset_window)

            # Before the publish check: the meter tracks generation, not
            # reporting. See the module docstring.
            energy_kwh = self.ledger.accumulate(
                inverter.asset_key,
                instant.day_index,
                outcome.reading.active_power_kw,
                self.clock.tick_simulated_hours,
            )

            if not outcome.publish:
                suppressed += 1
                self.metrics.record_suppressed(inverter.plant_id)
                continue

            event = build_event(
                inverter,
                instant,
                outcome.reading,
                energy_kwh,
                seed=self.simulation.seed,
                tick_index=tick_index,
                status=outcome.status,
                availability=outcome.availability,
                scenario=outcome.scenario,
            )

            payload = (
                corrupt_event(event)
                if self.simulation.emit_invalid_events
                else event.to_payload()
            )

            try:
                validate_payload(payload, inverter=inverter)
            except EventValidationError as exc:
                # Quarantine, never drop: an event nobody can explain is worse
                # than one labelled with why it was refused.
                quarantined += 1
                self.metrics.record_quarantined(exc.reason)
                log.warning(
                    "telemetry_quarantined",
                    f"Refused to publish an event for {inverter.asset_key}: {exc.reason}",
                    plant_id=inverter.plant_id,
                    inverter_id=inverter.id,
                    rejection_reason=exc.reason,
                )
                if self.producer is not None:
                    self.producer.publish_quarantine(
                        to_quarantine_record(payload, exc), inverter.asset_key
                    )
                continue

            if self.producer is not None:
                self.producer.publish(event)
            published += 1
            self.metrics.record_published(inverter.plant_id)

        return TickResult(published, suppressed, quarantined)

    def on_day_boundary(self, instant: SimulatedInstant, *, overwrite: bool) -> None:
        """Write the reference feed for the day that just ended."""
        if self._current_day is None:
            self._current_day = instant.day_index
            self.metrics.set_simulation_day(instant.day_index)
            return
        if instant.day_index == self._current_day:
            return

        finished_date = self.simulation.start_date
        try:
            from datetime import timedelta

            finished_date = self.simulation.start_date + timedelta(days=self._current_day)
            path = generate_daily_reference(
                self.portfolio,
                self.simulation,
                finished_date,
                model=self.model,
                overwrite=overwrite,
            )
            self.metrics.record_daily_reference()
            log.info(
                "daily_reference_ready",
                f"Wrote the reference feed for {finished_date}",
                simulation_date=str(finished_date),
                path=str(path),
            )
        except Exception as exc:  # noqa: BLE001 - the run must survive this
            # A failed reference file costs the batch layer one day. Stopping
            # the stream over it would cost the whole demo, so it is reported
            # loudly and the simulation continues.
            log.exception(
                "daily_reference_failed",
                f"Could not write the reference feed for {finished_date}: {exc}",
                simulation_date=str(finished_date),
            )

        self._current_day = instant.day_index
        self.metrics.set_simulation_day(instant.day_index)

    def run(
        self,
        *,
        shutdown: _Shutdown,
        max_days: float | None = None,
        overwrite_reference: bool = True,
        sleep: Any = time.sleep,
    ) -> None:
        """The tick loop. Runs until shutdown or `max_days` simulated days."""
        self.clock.start()
        interval = self.simulation.telemetry_interval_seconds
        tick_index = 0
        totals = TickResult(0, 0, 0)

        log.info(
            "simulation_started",
            f"Simulating {len(self.portfolio.plants)} plants / "
            f"{self.portfolio.inverter_count} inverters, "
            f"1 simulated day = {self.simulation.day_seconds:g}s real",
            plants=len(self.portfolio.plants),
            inverters=self.portfolio.inverter_count,
            day_seconds=self.simulation.day_seconds,
            interval_seconds=interval,
            seed=self.simulation.seed,
        )

        while not shutdown.requested:
            instant = self.clock.instant_at(tick_index * interval)
            if max_days is not None and instant.real_elapsed_seconds >= (
                max_days * self.simulation.day_seconds
            ):
                break

            self.on_day_boundary(instant, overwrite=overwrite_reference)
            result = self.tick(tick_index, instant)
            totals = TickResult(
                totals.published + result.published,
                totals.suppressed + result.suppressed,
                totals.quarantined + result.quarantined,
            )

            if self.producer is not None:
                self.metrics.record_producer_failure(
                    self.producer.stats.failed
                    - int(self.metrics.producer_failures._value.get())
                )

            tick_index += 1
            # Pace against the clock's own origin rather than sleeping a fixed
            # interval: fixed sleeps accumulate the time each tick took to
            # compute, and the simulated day would gradually run long.
            drift = (tick_index * interval) - self.clock.elapsed()
            if drift > 0 and not shutdown.requested:
                sleep(drift)

        log.info(
            "simulation_stopped",
            f"Published {totals.published} events, suppressed {totals.suppressed}, "
            f"quarantined {totals.quarantined} over {tick_index} ticks",
            published=totals.published,
            suppressed=totals.suppressed,
            quarantined=totals.quarantined,
            ticks=tick_index,
        )


def build_simulator(
    *,
    dry_run: bool = False,
    metrics: SimulatorMetrics | None = None,
) -> tuple[StreamingSimulator, TelemetryProducer | None]:
    """Load configuration and assemble the simulator, or fail with a clear error."""
    simulation = SimulationSettings.from_env()
    portfolio = load_portfolio(simulation.portfolio_config_path)
    schedule = load_schedule(
        day_seconds=simulation.day_seconds, portfolio=portfolio
    )
    clock = SimulationClock(simulation)
    metrics = metrics or SimulatorMetrics()

    producer: TelemetryProducer | None = None
    if not dry_run:
        kafka = KafkaSettings.from_env()
        # Fail before the loop starts rather than buffering into a void.
        check_broker_reachable(kafka)
        producer = TelemetryProducer(
            kafka,
            on_delivery_failure=lambda _key, _reason: None,
        )

    simulator = StreamingSimulator(
        simulation=simulation,
        portfolio=portfolio,
        schedule=schedule,
        clock=clock,
        producer=producer,
        metrics=metrics,
    )
    return simulator, producer


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="SolarIQ streaming telemetry simulator.")
    parser.add_argument(
        "--days",
        type=float,
        default=None,
        help="Stop after this many simulated days (default: run until interrupted).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate and validate everything without connecting to Kafka.",
    )
    parser.add_argument(
        "--no-metrics", action="store_true", help="Do not expose the Prometheus endpoint."
    )
    args = parser.parse_args(argv)

    shutdown = _Shutdown()
    shutdown.install()

    try:
        metrics = SimulatorMetrics()
        simulator, producer = build_simulator(dry_run=args.dry_run, metrics=metrics)

        if not args.no_metrics:
            metrics.serve(ObservabilitySettings.from_env().prometheus_port)

        for line in simulator.schedule.timeline():
            log.info("demo_timeline", line.strip())

        try:
            simulator.run(shutdown=shutdown, max_days=args.days)
        finally:
            if producer is not None:
                producer.close()

    except ConfigError as exc:
        log.error("simulator_config_error", str(exc))
        return 2
    except Exception as exc:  # noqa: BLE001 - top-level CLI boundary
        log.exception("simulator_failed", f"Simulator stopped with an error: {exc}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
