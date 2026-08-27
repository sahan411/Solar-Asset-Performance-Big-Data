"""Tests for the Prometheus metric surface.

The two things worth guarding: label cardinality stays bounded, and
`solariq_last_event_timestamp_seconds` carries REAL time. Both are silent
failures — high cardinality degrades Prometheus slowly, and a simulated-time
staleness metric makes the health alert permanently green.
"""

from __future__ import annotations

import time

import pytest
from prometheus_client import CollectorRegistry

from simulators.common.metrics import SCENARIO_CODES, SimulatorMetrics


@pytest.fixture
def metrics():
    # A private registry per test: the global default would raise on
    # re-registering the same metric names in the second test.
    return SimulatorMetrics(CollectorRegistry())


def value(metrics, name, **labels):
    return metrics.registry.get_sample_value(name, labels or None)


class TestCounters:
    def test_published_events_count_per_plant(self, metrics):
        metrics.record_published("PLANT_01")
        metrics.record_published("PLANT_01")
        metrics.record_published("PLANT_03")

        assert value(metrics, "solariq_events_produced_total", plant_id="PLANT_01") == 2
        assert value(metrics, "solariq_events_produced_total", plant_id="PLANT_03") == 1

    def test_quarantined_events_count_per_reason(self, metrics):
        metrics.record_quarantined("NEGATIVE_ACTIVE_POWER")
        metrics.record_quarantined("NEGATIVE_ACTIVE_POWER")
        metrics.record_quarantined("INVALID_STATUS")

        assert value(metrics, "solariq_events_invalid_total", reason="NEGATIVE_ACTIVE_POWER") == 2
        assert value(metrics, "solariq_events_invalid_total", reason="INVALID_STATUS") == 1

    def test_producer_failures_accumulate(self, metrics):
        metrics.record_producer_failure()
        metrics.record_producer_failure(4)

        assert value(metrics, "solariq_producer_failures_total") == 5

    def test_suppressed_telemetry_counts_per_plant(self, metrics):
        # A telemetry gap must be visible as a metric, not only as an absence.
        metrics.record_suppressed("PLANT_05")
        assert value(metrics, "solariq_telemetry_suppressed_total", plant_id="PLANT_05") == 1

    def test_daily_reference_writes_are_counted(self, metrics):
        metrics.record_daily_reference()
        assert value(metrics, "solariq_daily_reference_written_total") == 1


class TestFreshnessGauge:
    def test_it_records_real_time_not_simulated_time(self, metrics):
        before = time.time()
        metrics.record_published("PLANT_01")
        after = time.time()

        recorded = value(metrics, "solariq_last_event_timestamp_seconds")
        # The no-telemetry alert computes `time() - this`. A simulated timestamp
        # (2026-08-21) would make that hugely negative and the rule would never
        # fire, however dead the producer was.
        assert before <= recorded <= after

    def test_an_explicit_time_can_be_injected(self, metrics):
        metrics.record_published("PLANT_01", now=1_700_000_000.0)
        assert value(metrics, "solariq_last_event_timestamp_seconds") == 1_700_000_000.0

    def test_it_advances_with_each_event(self, metrics):
        metrics.record_published("PLANT_01", now=100.0)
        metrics.record_published("PLANT_01", now=200.0)
        assert value(metrics, "solariq_last_event_timestamp_seconds") == 200.0


class TestScenarioGauge:
    def test_normal_is_seeded_at_construction(self, metrics):
        # A metric never set returns no data at all, which on a dashboard is
        # indistinguishable from the exporter being down.
        assert value(metrics, "solariq_active_simulation_scenario", scenario="NORMAL") == 0

    def test_the_active_scenario_is_set(self, metrics):
        metrics.set_active_scenario("INV_UNDERPERFORMANCE")

        assert value(
            metrics, "solariq_active_simulation_scenario", scenario="INV_UNDERPERFORMANCE"
        ) == SCENARIO_CODES["INV_UNDERPERFORMANCE"]

    def test_switching_scenarios_clears_the_previous_one(self, metrics):
        # Otherwise the dashboard would show two faults running at once.
        metrics.set_active_scenario("INV_UNDERPERFORMANCE")
        metrics.set_active_scenario("INV_OFFLINE")

        assert value(
            metrics, "solariq_active_simulation_scenario", scenario="INV_UNDERPERFORMANCE"
        ) == 0
        assert value(
            metrics, "solariq_active_simulation_scenario", scenario="INV_OFFLINE"
        ) == SCENARIO_CODES["INV_OFFLINE"]

    def test_returning_to_normal_clears_everything(self, metrics):
        metrics.set_active_scenario("TELEMETRY_GAP")
        metrics.set_active_scenario(None)

        for name in ("TELEMETRY_GAP", "INV_OFFLINE", "INV_UNDERPERFORMANCE"):
            assert value(metrics, "solariq_active_simulation_scenario", scenario=name) == 0

    def test_every_scheduled_scenario_has_a_code(self):
        from simulators.streaming.scenarios import ALLOWED_SCENARIOS

        for scenario in ALLOWED_SCENARIOS:
            assert scenario in SCENARIO_CODES, scenario

    def test_normal_is_zero_so_nonzero_means_a_fault(self):
        # Makes `solariq_active_simulation_scenario > 0` the "is anything
        # scripted right now" query.
        assert SCENARIO_CODES["NORMAL"] == 0
        assert all(
            code > 0 for name, code in SCENARIO_CODES.items() if name not in (None, "NORMAL")
        )


class TestCardinality:
    def test_no_metric_is_labelled_by_event_or_inverter(self, metrics):
        # event_id would create one series per event - 3500 a simulated day -
        # and eventually take Prometheus down.
        forbidden = {"event_id", "inverter_id", "timestamp", "asset_key"}
        for metric in metrics.registry.collect():
            for sample in metric.samples:
                assert not (forbidden & set(sample.labels)), (
                    f"{sample.name} is labelled by {set(sample.labels) & forbidden}"
                )

    def test_plant_labels_stay_bounded_by_the_portfolio(self, metrics):
        for plant in ("PLANT_01", "PLANT_02", "PLANT_03", "PLANT_04", "PLANT_05"):
            metrics.record_published(plant)

        series = [
            sample
            for metric in metrics.registry.collect()
            for sample in metric.samples
            if sample.name == "solariq_events_produced_total"
        ]
        assert len(series) == 5


class TestSimulationDay:
    def test_it_tracks_the_current_day(self, metrics):
        metrics.set_simulation_day(3)
        assert value(metrics, "solariq_simulation_day") == 3


class TestServing:
    def test_serving_twice_is_a_no_op(self, metrics, monkeypatch):
        calls = []
        monkeypatch.setattr(
            "simulators.common.metrics.start_http_server",
            lambda port, registry=None: calls.append(port),
        )
        metrics.serve(9101)
        metrics.serve(9101)

        # Binding the same port twice would raise; the guard makes a repeated
        # call harmless.
        assert calls == [9101]
