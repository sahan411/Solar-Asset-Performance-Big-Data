"""Tests for the simulator loop.

This is where the subsystem is assembled, so the tests here are about ordering
and wiring rather than about any single component: does the energy meter keep
running through a telemetry gap, does the reference feed get written when a day
ends, does a shutdown flush rather than drop.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
from prometheus_client import CollectorRegistry

from simulators.common.config import SimulationSettings
from simulators.common.metrics import SimulatorMetrics
from simulators.common.portfolio import load_portfolio
from simulators.common.time import SimulationClock
from simulators.streaming.scenarios import load_schedule
from simulators.streaming.simulator import StreamingSimulator, _Shutdown

SEED = 8203
REPO_ROOT = Path(__file__).resolve().parents[2]


class RecordingProducer:
    """Captures what would have been published."""

    def __init__(self) -> None:
        self.events = []
        self.quarantined = []
        self.closed = False

    def publish(self, event):
        self.events.append(event)

    def publish_quarantine(self, record, key):
        self.quarantined.append((record, key))

    def close(self):
        self.closed = True

    @property
    def stats(self):
        class _S:
            failed = 0

        return _S()


def settings(tmp_path: Path, **overrides) -> SimulationSettings:
    base = dict(
        day_seconds=300.0,
        telemetry_interval_seconds=3.0,
        seed=SEED,
        start_date=date(2026, 8, 21),
        output_dir=tmp_path,
        portfolio_config_path=REPO_ROOT / "simulators/config/portfolio.yaml",
        emit_invalid_events=False,
    )
    base.update(overrides)
    return SimulationSettings(**base)


@pytest.fixture(scope="module")
def portfolio():
    return load_portfolio(REPO_ROOT / "simulators/config/portfolio.yaml")


@pytest.fixture
def build(tmp_path, portfolio):
    def _build(**overrides):
        sim_settings = settings(tmp_path, **overrides)
        clock = SimulationClock(sim_settings)
        producer = RecordingProducer()
        simulator = StreamingSimulator(
            simulation=sim_settings,
            portfolio=portfolio,
            schedule=load_schedule(
                REPO_ROOT / "simulators/config/scenarios.yaml",
                day_seconds=sim_settings.day_seconds,
                portfolio=portfolio,
            ),
            clock=clock,
            producer=producer,
            metrics=SimulatorMetrics(CollectorRegistry()),
        )
        return simulator, producer, clock

    return _build


class TestTick:
    def test_a_normal_tick_publishes_every_inverter(self, build):
        simulator, producer, clock = build()
        result = simulator.tick(0, clock.instant_at(0.0))

        assert result.published == 35
        assert result.suppressed == 0
        assert result.quarantined == 0
        assert len(producer.events) == 35

    def test_a_gap_suppresses_only_the_targeted_plant(self, build):
        simulator, producer, clock = build()
        result = simulator.tick(80, clock.instant_at(240.0))  # inside TELEMETRY_GAP

        assert result.published == 25
        assert result.suppressed == 10
        assert all(e.plant_id != "PLANT_05" for e in producer.events)

    def test_offline_publishes_a_zero_rather_than_going_silent(self, build):
        simulator, producer, clock = build()
        simulator.tick(66, clock.instant_at(200.0))  # inside INV_OFFLINE

        target = next(
            e for e in producer.events if e.plant_id == "PLANT_04" and e.inverter_id == "INV_01"
        )
        assert target.status == "OFFLINE"
        assert target.active_power_kw == 0.0
        assert target.availability == 0.0

    def test_underperformance_is_published_as_a_warning(self, build):
        simulator, producer, clock = build()
        simulator.tick(40, clock.instant_at(120.0))

        target = next(
            e for e in producer.events if e.plant_id == "PLANT_03" and e.inverter_id == "INV_02"
        )
        assert target.status == "WARNING"
        assert target.simulator_scenario == "INV_UNDERPERFORMANCE"
        assert target.active_power_kw > 0  # degraded, not down

    def test_every_published_event_carries_simulated_time(self, build):
        simulator, producer, clock = build()
        simulator.tick(50, clock.instant_at(150.0))

        assert all(e.timestamp == "2026-08-21T12:00:00Z" for e in producer.events)


class TestEnergyThroughAGap:
    def test_the_meter_keeps_running_while_telemetry_is_suppressed(self, build):
        """The plant kept generating; we merely stopped hearing about it."""
        simulator, producer, clock = build()
        interval = 3.0

        for tick in range(0, 100):
            simulator.tick(tick, clock.instant_at(tick * interval))

        # PLANT_05 is silent from 235-260s, i.e. ticks 79-86.
        gap_asset = "PLANT_05:INV_01"
        published = [
            e for e in producer.events
            if e.plant_id == "PLANT_05" and e.inverter_id == "INV_01"
        ]
        before = next(e for e in published if e.timestamp <= "2026-08-21T18:47:00Z")
        after = published[-1]

        # Energy continued to accumulate across the silence, so the first event
        # after the gap is higher than the last one before it - not frozen.
        assert after.energy_today_kwh > before.energy_today_kwh
        assert simulator.ledger.current(gap_asset) > 0

    def test_energy_never_decreases_within_a_day(self, build):
        simulator, producer, clock = build()
        for tick in range(60):
            simulator.tick(tick, clock.instant_at(tick * 3.0))

        readings = [
            e.energy_today_kwh for e in producer.events
            if e.plant_id == "PLANT_01" and e.inverter_id == "INV_01"
        ]
        assert readings == sorted(readings)


class TestDayBoundary:
    def test_the_reference_feed_is_written_when_a_day_ends(self, build, tmp_path):
        simulator, _, clock = build()

        simulator.on_day_boundary(clock.instant_at(0.0), overwrite=True)
        assert list(tmp_path.glob("*.csv")) == []  # day 0 has not finished yet

        simulator.on_day_boundary(clock.instant_at(300.0), overwrite=True)
        written = list(tmp_path.glob("daily_reference_*.csv"))

        # The file is for the day that ENDED, not the one starting.
        assert [p.name for p in written] == ["daily_reference_2026-08-21.csv"]

    def test_it_is_not_rewritten_mid_day(self, build, tmp_path):
        simulator, _, clock = build()
        simulator.on_day_boundary(clock.instant_at(0.0), overwrite=True)
        simulator.on_day_boundary(clock.instant_at(150.0), overwrite=True)

        assert list(tmp_path.glob("*.csv")) == []

    def test_a_reference_failure_does_not_stop_the_simulation(self, build, tmp_path, monkeypatch):
        # One lost reference file costs the batch layer a day; stopping the
        # stream over it would cost the whole demo.
        simulator, _, clock = build()
        monkeypatch.setattr(
            "simulators.streaming.simulator.generate_daily_reference",
            lambda *a, **k: (_ for _ in ()).throw(OSError("disk full")),
        )
        simulator.on_day_boundary(clock.instant_at(0.0), overwrite=True)
        simulator.on_day_boundary(clock.instant_at(300.0), overwrite=True)  # must not raise

    def test_the_simulation_day_metric_tracks_the_day(self, build, clock=None):
        simulator, _, clock = build()
        simulator.on_day_boundary(clock.instant_at(0.0), overwrite=True)
        simulator.on_day_boundary(clock.instant_at(300.0), overwrite=True)

        assert simulator.metrics.registry.get_sample_value("solariq_simulation_day") == 1


class TestInvalidEventMode:
    def test_it_is_off_by_default(self, build):
        simulator, producer, clock = build()
        result = simulator.tick(50, clock.instant_at(150.0))

        assert result.quarantined == 0
        assert producer.quarantined == []

    def test_when_enabled_nothing_reaches_the_valid_topic(self, build):
        simulator, producer, clock = build(emit_invalid_events=True)
        result = simulator.tick(50, clock.instant_at(150.0))

        assert result.published == 0
        assert result.quarantined == 35
        assert len(producer.quarantined) == 35
        assert producer.events == []

    def test_quarantined_records_carry_the_reason(self, build):
        simulator, producer, clock = build(emit_invalid_events=True)
        simulator.tick(50, clock.instant_at(150.0))

        record, key = producer.quarantined[0]
        assert record["rejection_reason"] == "NEGATIVE_ACTIVE_POWER"
        assert ":" in key


class TestRunLoop:
    def test_it_stops_after_the_requested_days(self, build):
        simulator, producer, _ = build()
        shutdown = _Shutdown()

        simulator.run(shutdown=shutdown, max_days=0.03, sleep=lambda _s: None)

        # 0.03 of a 300s day = 9s = 3 ticks of 3s.
        assert len(producer.events) == 3 * 35

    def test_a_shutdown_request_ends_the_loop(self, build):
        simulator, producer, _ = build()
        shutdown = _Shutdown()

        ticks = {"n": 0}

        def stop_after_two(_seconds):
            ticks["n"] += 1
            if ticks["n"] >= 2:
                shutdown.requested = True

        simulator.run(shutdown=shutdown, max_days=1.0, sleep=stop_after_two)

        assert 0 < len(producer.events) < 35 * 100

    def test_the_run_is_reproducible(self, build):
        first, producer_a, _ = build()
        first.run(shutdown=_Shutdown(), max_days=0.05, sleep=lambda _s: None)

        second, producer_b, _ = build()
        second.run(shutdown=_Shutdown(), max_days=0.05, sleep=lambda _s: None)

        assert [e.to_json() for e in producer_a.events] == [
            e.to_json() for e in producer_b.events
        ]


class TestShutdownHandling:
    def test_the_flag_starts_clear(self):
        assert _Shutdown().requested is False

    def test_a_signal_sets_the_flag_without_exiting(self):
        # The loop must finish its tick and flush; exiting inside the handler
        # would drop whatever the producer had buffered.
        shutdown = _Shutdown()
        shutdown.request(2, None)

        assert shutdown.requested is True

    def test_a_second_signal_exits(self):
        shutdown = _Shutdown()
        shutdown.request(2, None)

        with pytest.raises(SystemExit):
            shutdown.request(2, None)
