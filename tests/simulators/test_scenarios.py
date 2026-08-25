"""Tests for the deterministic anomaly schedule.

The demo is only worth rehearsing if it is identical every run, so the exact
transitions are pinned: which scenario is active at which second, which asset it
targets, and what it does to the readings. The most important assertions are the
ones proving INV_OFFLINE and TELEMETRY_GAP behave differently, and that
underperformance leaves irradiance alone.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
import yaml

from simulators.common.config import ConfigError, SimulationSettings
from simulators.common.portfolio import Inverter, load_portfolio
from simulators.common.time import SimulationClock
from simulators.streaming.events import (
    STATUS_OFFLINE,
    STATUS_ONLINE,
    STATUS_WARNING,
    build_event,
    validate_event,
)
from simulators.streaming.generation import EnergyLedger, generate_reading
from simulators.streaming.scenarios import (
    DEFAULT_SCENARIO_PATH,
    SCENARIO_OFFLINE,
    SCENARIO_RECOVERY,
    SCENARIO_TELEMETRY_GAP,
    SCENARIO_UNDERPERFORMANCE,
    ScenarioConfigError,
    ScenarioWindow,
    apply_scenario,
    load_schedule,
    parse_schedule,
)

SEED = 8203
REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA = REPO_ROOT / "contracts/telemetry.schema.json"
SCENARIOS = REPO_ROOT / DEFAULT_SCENARIO_PATH


def settings(day_seconds: float = 300.0) -> SimulationSettings:
    return SimulationSettings(
        day_seconds=day_seconds,
        telemetry_interval_seconds=3.0,
        seed=SEED,
        start_date=date(2026, 8, 21),
        output_dir=Path("/data/daily"),
        portfolio_config_path=Path("simulators/config/portfolio.yaml"),
        emit_invalid_events=False,
    )


@pytest.fixture(scope="module")
def portfolio():
    return load_portfolio(REPO_ROOT / "simulators/config/portfolio.yaml")


@pytest.fixture
def schedule(portfolio):
    return load_schedule(SCENARIOS, day_seconds=300.0, portfolio=portfolio)


@pytest.fixture
def clock():
    return SimulationClock(settings())


@pytest.fixture
def inverter():
    return Inverter(id="INV_01", name="INV_01", rated_power_kw=1000.0, plant_id="PLANT_01")


def minimal_document() -> dict:
    return {
        "reference_day_seconds": 300,
        "windows": [
            {
                "scenario": SCENARIO_UNDERPERFORMANCE,
                "start_second": 90,
                "end_second": 150,
                "plant_id": "PLANT_03",
                "inverter_id": "INV_02",
                "power_factor": 0.45,
            }
        ],
    }


def parsed(document, **kwargs):
    kwargs.setdefault("day_seconds", 300.0)
    with pytest.raises(ScenarioConfigError) as excinfo:
        parse_schedule(document, **kwargs)
    return excinfo.value


class TestTheRealSchedule:
    """The checked-in timeline is what the assessment runs, so it is tested."""

    @pytest.mark.parametrize(
        "second,expected",
        [
            (0, None),
            (89.9, None),
            (90, SCENARIO_UNDERPERFORMANCE),
            (120, SCENARIO_UNDERPERFORMANCE),
            (149.9, SCENARIO_UNDERPERFORMANCE),
            (150, SCENARIO_RECOVERY),
            (189.9, SCENARIO_RECOVERY),
            (190, SCENARIO_OFFLINE),
            (234.9, SCENARIO_OFFLINE),
            (235, SCENARIO_TELEMETRY_GAP),
            (259.9, SCENARIO_TELEMETRY_GAP),
            (260, SCENARIO_RECOVERY),
            (299.9, SCENARIO_RECOVERY),
        ],
    )
    def test_exact_scenario_transitions(self, schedule, second, expected):
        window = schedule.active_window(second)
        assert (window.scenario if window else None) == expected

    def test_the_documented_targets(self, schedule):
        assert schedule.active_window(120).target_label == "PLANT_03:INV_02"
        assert schedule.active_window(200).target_label == "PLANT_04:INV_01"
        # No inverter named: the whole site goes silent.
        assert schedule.active_window(240).target_label == "PLANT_05"

    def test_underperformance_is_the_documented_factor(self, schedule):
        assert schedule.active_window(120).power_factor == 0.45

    def test_every_target_exists_in_the_portfolio(self, portfolio):
        # Renaming a plant must fail here, not silently during the demo.
        load_schedule(SCENARIOS, day_seconds=300.0, portfolio=portfolio)

    def test_all_four_demo_scenarios_are_scheduled(self, schedule):
        scheduled = {window.scenario for window in schedule.windows}
        assert scheduled == {
            SCENARIO_UNDERPERFORMANCE,
            SCENARIO_RECOVERY,
            SCENARIO_OFFLINE,
            SCENARIO_TELEMETRY_GAP,
        }

    def test_the_timeline_is_printable(self, schedule):
        lines = schedule.timeline()
        assert any("NORMAL" in line for line in lines)
        assert any("INV_UNDERPERFORMANCE on PLANT_03:INV_02 at 45% power" in line for line in lines)
        assert any("TELEMETRY_GAP on PLANT_05" in line for line in lines)


class TestTargeting:
    def test_an_inverter_window_hits_only_that_inverter(self, schedule, portfolio):
        plant = portfolio.plant("PLANT_03")
        targeted = [inv for inv in plant.inverters if schedule.window_for(inv, 120)]

        assert [inv.id for inv in targeted] == ["INV_02"]

    def test_a_plant_window_hits_every_inverter_on_it(self, schedule, portfolio):
        plant = portfolio.plant("PLANT_05")
        targeted = [inv for inv in plant.inverters if schedule.window_for(inv, 240)]

        assert len(targeted) == len(plant.inverters) == 10

    def test_other_plants_are_untouched(self, schedule, portfolio):
        for second in (120, 200, 240):
            for inv in portfolio.plant("PLANT_01").inverters:
                assert schedule.window_for(inv, second) is None

    def test_same_inverter_id_on_another_plant_is_not_targeted(self, schedule, portfolio):
        # Every plant has an INV_02; only PLANT_03's is scripted.
        other = portfolio.plant("PLANT_01").inverters[1]
        assert other.id == "INV_02"
        assert schedule.window_for(other, 120) is None


class TestScenarioEffects:
    @pytest.fixture
    def reading(self, clock, inverter):
        return generate_reading(inverter, clock.instant_at(150.0), 50, seed=SEED)

    def test_no_window_passes_the_reading_through(self, inverter, reading):
        outcome = apply_scenario(inverter, reading, None)

        assert outcome.publish is True
        assert outcome.reading == reading
        assert outcome.status == STATUS_ONLINE
        assert outcome.availability == 1.0
        assert outcome.scenario is None

    def test_underperformance_scales_power_only(self, inverter, reading):
        window = ScenarioWindow(SCENARIO_UNDERPERFORMANCE, 90, 150, "PLANT_01", "INV_01", 0.45)
        outcome = apply_scenario(inverter, reading, window)

        assert outcome.reading.active_power_kw == pytest.approx(
            reading.active_power_kw * 0.45
        )
        # The detection signal: output collapses while the resource is unchanged.
        assert outcome.reading.irradiance_wm2 == reading.irradiance_wm2
        assert outcome.reading.module_temp_c == reading.module_temp_c

    def test_underperformance_is_degraded_not_down(self, inverter, reading):
        window = ScenarioWindow(SCENARIO_UNDERPERFORMANCE, 90, 150, "PLANT_01", "INV_01", 0.45)
        outcome = apply_scenario(inverter, reading, window)

        assert outcome.status == STATUS_WARNING
        assert outcome.availability == 1.0  # still available, just producing less
        assert outcome.publish is True
        assert outcome.reading.active_power_kw > 0

    def test_offline_reports_a_zero(self, inverter, reading):
        window = ScenarioWindow(SCENARIO_OFFLINE, 190, 235, "PLANT_01", "INV_01")
        outcome = apply_scenario(inverter, reading, window)

        assert outcome.publish is True  # it publishes — that is the whole point
        assert outcome.reading.active_power_kw == 0.0
        assert outcome.status == STATUS_OFFLINE
        assert outcome.availability == 0.0
        assert outcome.scenario == SCENARIO_OFFLINE

    def test_offline_leaves_the_sun_shining(self, inverter, reading):
        window = ScenarioWindow(SCENARIO_OFFLINE, 190, 235, "PLANT_01", "INV_01")
        outcome = apply_scenario(inverter, reading, window)

        # Zero output under full irradiance is what makes the fault legible.
        assert outcome.reading.irradiance_wm2 == reading.irradiance_wm2
        assert outcome.reading.irradiance_wm2 > 0

    def test_telemetry_gap_publishes_nothing(self, inverter, reading):
        window = ScenarioWindow(SCENARIO_TELEMETRY_GAP, 235, 260, "PLANT_05")
        outcome = apply_scenario(inverter, reading, window)

        assert outcome.publish is False

    def test_a_gap_is_not_an_offline_zero(self, inverter, reading):
        """The distinction the assessment asks for: silence versus a reported zero."""
        gap = apply_scenario(
            inverter, reading, ScenarioWindow(SCENARIO_TELEMETRY_GAP, 235, 260, "PLANT_05")
        )
        offline = apply_scenario(
            inverter, reading, ScenarioWindow(SCENARIO_OFFLINE, 190, 235, "PLANT_01", "INV_01")
        )

        assert gap.publish is False and offline.publish is True
        # The gap's underlying reading is untouched: the plant kept generating,
        # we simply stopped hearing about it.
        assert gap.reading.active_power_kw == reading.active_power_kw
        assert offline.reading.active_power_kw == 0.0

    def test_recovery_restores_normal_readings_with_a_label(self, inverter, reading):
        window = ScenarioWindow(SCENARIO_RECOVERY, 150, 190, "PLANT_03", "INV_02")
        outcome = apply_scenario(inverter, reading, window)

        assert outcome.reading == reading
        assert outcome.status == STATUS_ONLINE
        assert outcome.availability == 1.0
        assert outcome.scenario == SCENARIO_RECOVERY

    def test_a_zero_power_factor_is_allowed(self, inverter, reading):
        # Total loss of output while still reporting ONLINE-with-WARNING.
        window = ScenarioWindow(SCENARIO_UNDERPERFORMANCE, 90, 150, "PLANT_01", "INV_01", 0.0)
        assert apply_scenario(inverter, reading, window).reading.active_power_kw == 0.0


class TestScheduleValidation:
    def test_an_unknown_scenario_is_rejected(self):
        document = minimal_document()
        document["windows"][0]["scenario"] = "SOMETHING_ELSE"

        assert "scenario must be one of" in str(parsed(document))

    def test_overlapping_windows_are_rejected(self):
        document = minimal_document()
        document["windows"].append(
            {
                "scenario": SCENARIO_OFFLINE,
                "start_second": 120,
                "end_second": 200,
                "plant_id": "PLANT_04",
                "inverter_id": "INV_01",
            }
        )

        assert "overlaps" in str(parsed(document))

    def test_touching_windows_are_allowed(self):
        # One ends exactly where the next begins: half-open windows, no overlap.
        document = minimal_document()
        document["windows"].append(
            {
                "scenario": SCENARIO_RECOVERY,
                "start_second": 150,
                "end_second": 190,
                "plant_id": "PLANT_03",
                "inverter_id": "INV_02",
            }
        )

        schedule = parse_schedule(document, day_seconds=300.0)
        assert len(schedule.windows) == 2

    def test_a_window_past_the_day_is_rejected(self):
        document = minimal_document()
        document["windows"][0]["end_second"] = 400

        assert "It would never fire" in str(parsed(document))

    def test_an_inverted_window_is_rejected(self):
        document = minimal_document()
        document["windows"][0]["end_second"] = 50

        assert "must be greater than start_second" in str(parsed(document))

    def test_a_negative_start_is_rejected(self):
        document = minimal_document()
        document["windows"][0]["start_second"] = -1

        assert "must not be negative" in str(parsed(document))

    def test_underperformance_requires_a_power_factor(self):
        document = minimal_document()
        del document["windows"][0]["power_factor"]

        assert "power_factor is required" in str(parsed(document))

    @pytest.mark.parametrize("bad", [1.0, 1.5, -0.1])
    def test_a_meaningless_power_factor_is_rejected(self, bad):
        document = minimal_document()
        document["windows"][0]["power_factor"] = bad

        assert "power_factor must be in [0, 1)" in str(parsed(document))

    def test_power_factor_on_another_scenario_is_rejected(self):
        document = minimal_document()
        document["windows"][0] = {
            "scenario": SCENARIO_OFFLINE,
            "start_second": 90,
            "end_second": 150,
            "plant_id": "PLANT_04",
            "inverter_id": "INV_01",
            "power_factor": 0.45,
        }

        assert "only applies to" in str(parsed(document))

    def test_an_inverter_without_a_plant_is_rejected(self):
        # Inverter ids repeat across plants, so the target would be ambiguous.
        document = minimal_document()
        del document["windows"][0]["plant_id"]

        assert "without a plant_id" in str(parsed(document))

    def test_an_unknown_plant_is_rejected(self, portfolio):
        document = minimal_document()
        document["windows"][0]["plant_id"] = "PLANT_99"

        assert "not in the portfolio" in str(parsed(document, portfolio=portfolio))

    def test_an_unknown_inverter_is_rejected(self, portfolio):
        document = minimal_document()
        document["windows"][0]["inverter_id"] = "INV_99"

        assert "has no inverter" in str(parsed(document, portfolio=portfolio))

    @pytest.mark.parametrize("windows", [None, [], {}, "none"])
    def test_windows_must_be_a_non_empty_list(self, windows):
        assert "non-empty 'windows' list" in str(parsed({"windows": windows}))

    def test_the_document_must_be_a_mapping(self):
        assert "must be a mapping" in str(parsed([]))

    def test_scenarios_stay_within_the_published_contract(self):
        # A scenario label outside the schema enum would fail its own validation.
        import json

        allowed = json.loads(SCHEMA.read_text(encoding="utf-8"))["properties"][
            "simulator_scenario"
        ]["enum"]
        schedule = load_schedule(SCENARIOS, day_seconds=300.0)
        for window in schedule.windows:
            assert window.scenario in allowed


class TestClockScaling:
    def test_a_faster_clock_scales_the_whole_timeline(self, portfolio):
        halved = load_schedule(SCENARIOS, day_seconds=150.0, portfolio=portfolio)

        # 90s of a 300s day is 45s of a 150s day: the same point in the day.
        assert halved.active_window(45).scenario == SCENARIO_UNDERPERFORMANCE
        # A second earlier the demo is still in its baseline stretch.
        assert halved.active_window(44) is None

    def test_a_slower_clock_scales_too(self, portfolio):
        doubled = load_schedule(SCENARIOS, day_seconds=600.0, portfolio=portfolio)
        assert doubled.active_window(180).scenario == SCENARIO_UNDERPERFORMANCE

    def test_scaling_preserves_the_order_and_count(self, portfolio):
        reference = load_schedule(SCENARIOS, day_seconds=300.0, portfolio=portfolio)
        scaled = load_schedule(SCENARIOS, day_seconds=900.0, portfolio=portfolio)

        assert [w.scenario for w in scaled.windows] == [
            w.scenario for w in reference.windows
        ]

    def test_no_window_escapes_the_configured_day(self, portfolio):
        for day_seconds in (60.0, 150.0, 300.0, 1200.0):
            schedule = load_schedule(SCENARIOS, day_seconds=day_seconds, portfolio=portfolio)
            for window in schedule.windows:
                assert 0 <= window.start_second < window.end_second <= day_seconds


class TestLoadingFromDisk:
    def test_a_missing_file_explains_the_consequence(self, tmp_path):
        with pytest.raises(ScenarioConfigError, match="no anomalies at all"):
            load_schedule(tmp_path / "absent.yaml", day_seconds=300.0)

    def test_invalid_yaml_is_reported_as_such(self, tmp_path):
        path = tmp_path / "scenarios.yaml"
        path.write_text("windows: [\n  - scenario:\n", encoding="utf-8")

        with pytest.raises(ScenarioConfigError, match="not valid YAML"):
            load_schedule(path, day_seconds=300.0)

    def test_a_validation_failure_names_the_file(self, tmp_path):
        path = tmp_path / "broken.yaml"
        document = minimal_document()
        document["windows"][0]["power_factor"] = 2.0
        path.write_text(yaml.safe_dump(document), encoding="utf-8")

        with pytest.raises(ScenarioConfigError, match="broken.yaml"):
            load_schedule(path, day_seconds=300.0)

    def test_scenario_config_error_is_a_config_error(self, tmp_path):
        # So a simulator entrypoint catches every startup fault in one place.
        with pytest.raises(ConfigError):
            load_schedule(tmp_path / "absent.yaml", day_seconds=300.0)


class TestAWholeScriptedDay:
    def test_the_run_is_reproducible_and_every_published_event_is_valid(
        self, clock, portfolio, schedule
    ):
        """Replay the demo day twice and assert it is identical, and legal."""

        def run() -> list[tuple]:
            ledger = EnergyLedger()
            emitted = []
            for tick in range(100):
                instant = clock.instant_at(tick * 3.0)
                for asset in portfolio.inverters():
                    reading = generate_reading(asset, instant, tick, seed=SEED)
                    window = schedule.window_for(asset, instant.seconds_into_day)
                    outcome = apply_scenario(asset, reading, window)
                    # The meter advances even through a gap: the plant kept
                    # generating, we just stopped hearing about it.
                    energy = ledger.accumulate(
                        asset.asset_key,
                        instant.day_index,
                        outcome.reading.active_power_kw,
                        clock.tick_simulated_hours,
                    )
                    if not outcome.publish:
                        continue
                    event = build_event(
                        asset,
                        instant,
                        outcome.reading,
                        energy,
                        seed=SEED,
                        tick_index=tick,
                        status=outcome.status,
                        availability=outcome.availability,
                        scenario=outcome.scenario,
                    )
                    validate_event(event, inverter=asset, schema_path=SCHEMA)
                    emitted.append((event.event_id, event.to_json()))
            return emitted

        first, second = run(), run()
        assert first == second, "the scripted day is not reproducible"

        # 100 ticks x 35 inverters, less the silenced site during the gap.
        assert len(first) < 3500
        assert len(first) == 3500 - 10 * len(
            [t for t in range(100) if 235 <= t * 3.0 < 260]
        )

    def test_the_gap_removes_exactly_the_targeted_plant(
        self, clock, portfolio, schedule
    ):
        instant = clock.instant_at(240.0)  # inside the telemetry gap
        published = [
            asset
            for asset in portfolio.inverters()
            if apply_scenario(
                asset,
                generate_reading(asset, instant, 80, seed=SEED),
                schedule.window_for(asset, instant.seconds_into_day),
            ).publish
        ]

        assert len(published) == 25  # 35 total, PLANT_05's 10 silenced
        assert all(asset.plant_id != "PLANT_05" for asset in published)

    def test_underperformance_is_detectable_in_the_scripted_run(
        self, clock, portfolio, schedule
    ):
        # The end-to-end claim: at 120s the target's performance ratio is below
        # Member 2's 0.80 threshold while every healthy asset stays above it.
        instant = clock.instant_at(120.0)
        target = next(
            inv for inv in portfolio.plant("PLANT_03").inverters if inv.id == "INV_02"
        )

        outcome = apply_scenario(
            target,
            generate_reading(target, instant, 40, seed=SEED),
            schedule.window_for(target, instant.seconds_into_day),
        )
        assert outcome.reading.performance_ratio(target.rated_power_kw, 1000.0) < 0.80

        for asset in portfolio.inverters():
            if asset.asset_key == target.asset_key:
                continue
            healthy = apply_scenario(
                asset,
                generate_reading(asset, instant, 40, seed=SEED),
                schedule.window_for(asset, instant.seconds_into_day),
            )
            assert healthy.reading.performance_ratio(asset.rated_power_kw, 1000.0) >= 0.80
