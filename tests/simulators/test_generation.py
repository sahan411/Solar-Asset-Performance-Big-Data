"""Tests for the deterministic solar generation model.

Three groups matter most:

  * shape — the curve is zero at the day's ends and peaks at noon;
  * bounds — power is never negative and never exceeds the inverter rating;
  * integration — the performance ratio a healthy inverter produces stays clear
    of the threshold Member 2 alerts on, across a whole simulated day and every
    asset in the portfolio.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from simulators.common.config import ConfigError, SimulationSettings
from simulators.common.portfolio import Inverter, load_portfolio
from simulators.common.time import SimulationClock
from simulators.streaming.generation import (
    EnergyLedger,
    InverterReading,
    SolarModel,
    deterministic_rng,
    generate_reading,
    solar_shape,
    temperature_factor,
)

SEED = 8203
REPO_ROOT = Path(__file__).resolve().parents[2]

# Member 2's thresholds, from processing/common/config.py. Duplicated here on
# purpose: if he changes them, this suite should fail and force a conversation
# rather than silently letting the simulator drift into false alerts.
UNDERPERFORMANCE_THRESHOLD = 0.80
REFERENCE_IRRADIANCE_WM2 = 1000.0
MIN_IRRADIANCE_WM2 = 150.0


def settings(day_seconds: float = 300.0, interval: float = 3.0) -> SimulationSettings:
    return SimulationSettings(
        day_seconds=day_seconds,
        telemetry_interval_seconds=interval,
        seed=SEED,
        start_date=date(2026, 8, 21),
        output_dir=Path("/data/daily"),
        portfolio_config_path=Path("simulators/config/portfolio.yaml"),
        emit_invalid_events=False,
    )


@pytest.fixture
def clock():
    return SimulationClock(settings())


@pytest.fixture
def inverter():
    return Inverter(id="INV_01", name="INV_01", rated_power_kw=1000.0, plant_id="PLANT_01")


@pytest.fixture(scope="module")
def portfolio():
    return load_portfolio(REPO_ROOT / "simulators/config/portfolio.yaml")


def day_of_readings(clock, inverter, model=None):
    """Every reading one inverter produces across one simulated day."""
    ticks = int(settings().ticks_per_day)
    interval = settings().telemetry_interval_seconds
    return [
        generate_reading(
            inverter, clock.instant_at(tick * interval), tick, seed=SEED, model=model
        )
        for tick in range(ticks)
    ]


class TestSolarShape:
    def test_zero_at_both_ends_of_the_day(self):
        assert solar_shape(0.0) == 0.0
        assert solar_shape(1.0) == pytest.approx(0.0, abs=1e-9)

    def test_peaks_at_solar_noon(self):
        assert solar_shape(0.5) == pytest.approx(1.0)

    def test_symmetric_about_noon(self):
        assert solar_shape(0.25) == pytest.approx(solar_shape(0.75))

    def test_never_negative(self):
        # Floating point can push sin() a hair below zero at the boundaries.
        for step in range(0, 1001):
            assert solar_shape(step / 1000.0) >= 0.0

    def test_rises_monotonically_until_noon(self):
        values = [solar_shape(i / 100.0) for i in range(0, 51)]
        assert values == sorted(values)

    def test_falls_monotonically_after_noon(self):
        values = [solar_shape(i / 100.0) for i in range(50, 101)]
        assert values == sorted(values, reverse=True)

    def test_exponent_sharpens_the_peak(self):
        # A higher exponent means less output away from noon, same value at noon.
        assert solar_shape(0.25, 1.5) < solar_shape(0.25, 1.0)
        assert solar_shape(0.5, 1.5) == pytest.approx(solar_shape(0.5, 1.0))

    def test_full_daylight_reduces_to_the_playbook_formula(self):
        import math

        for step in range(1, 100):
            progress = step / 100.0
            assert solar_shape(progress, 1.5, 1.0) == pytest.approx(
                max(0.0, math.sin(math.pi * progress)) ** 1.5
            )

    def test_a_shorter_daylight_window_is_dark_at_night(self):
        # daylight_fraction=0.5 means sun from 06:00 to 18:00.
        assert solar_shape(0.20, daylight_fraction=0.5) == 0.0  # 04:48
        assert solar_shape(0.25, daylight_fraction=0.5) == 0.0  # 06:00, sunrise
        assert solar_shape(0.75, daylight_fraction=0.5) == 0.0  # 18:00, sunset
        assert solar_shape(0.90, daylight_fraction=0.5) == 0.0  # 21:36

    def test_a_shorter_daylight_window_still_peaks_at_noon(self):
        assert solar_shape(0.5, daylight_fraction=0.5) == pytest.approx(1.0)
        assert solar_shape(0.375, daylight_fraction=0.5) == pytest.approx(
            solar_shape(0.625, daylight_fraction=0.5)
        )


class TestTemperatureFactor:
    def test_no_derate_at_or_below_rated_temperature(self):
        model = SolarModel()
        assert temperature_factor(25.0, model) == 1.0
        assert temperature_factor(10.0, model) == 1.0

    def test_derates_by_the_coefficient_above_rated(self):
        model = SolarModel()
        # 50 C is 25 above STC; 25 * 0.004 = 0.10 lost.
        assert temperature_factor(50.0, model) == pytest.approx(0.90)

    def test_floors_at_the_configured_minimum(self):
        model = SolarModel()
        assert temperature_factor(500.0, model) == model.min_temperature_factor


class TestReadingBounds:
    def test_no_power_at_the_start_and_end_of_the_day(self, clock, inverter):
        assert generate_reading(inverter, clock.instant_at(0.0), 0, seed=SEED).active_power_kw == 0.0
        end = generate_reading(inverter, clock.instant_at(299.999), 99, seed=SEED)
        assert end.active_power_kw == pytest.approx(0.0, abs=0.5)

    def test_power_peaks_around_midday(self, clock, inverter):
        readings = day_of_readings(clock, inverter)
        peak_tick = max(range(len(readings)), key=lambda i: readings[i].active_power_kw)
        # 100 ticks per day, so noon is tick 50. Noise moves it by a tick or two.
        assert 45 <= peak_tick <= 55

    def test_power_is_never_negative(self, clock, inverter):
        assert all(r.active_power_kw >= 0.0 for r in day_of_readings(clock, inverter))

    def test_power_never_exceeds_the_inverter_rating(self, clock, inverter):
        # Upward noise must not manufacture output the hardware cannot produce.
        assert all(
            r.active_power_kw <= inverter.rated_power_kw
            for r in day_of_readings(clock, inverter)
        )

    def test_rating_is_respected_even_with_extreme_noise(self, clock, inverter):
        greedy = SolarModel(power_noise_low=5.0, power_noise_high=5.0)
        readings = day_of_readings(clock, inverter, model=greedy)

        assert max(r.active_power_kw for r in readings) == inverter.rated_power_kw

    def test_irradiance_stays_within_physical_range(self, clock, inverter):
        readings = day_of_readings(clock, inverter)
        assert all(0.0 <= r.irradiance_wm2 <= 1000.0 for r in readings)
        assert max(r.irradiance_wm2 for r in readings) > 950.0

    def test_temperatures_are_plausible(self, clock, inverter):
        readings = day_of_readings(clock, inverter)

        assert all(20.0 <= r.module_temp_c <= 60.0 for r in readings)
        assert all(20.0 <= r.inverter_temp_c <= 55.0 for r in readings)

    def test_modules_run_hotter_than_inverters(self, clock, inverter):
        # Panels sit in direct sun; inverters are housed. Midday, where the
        # difference is largest and the noise cannot invert it.
        midday = generate_reading(inverter, clock.instant_at(150.0), 50, seed=SEED)
        assert midday.module_temp_c > midday.inverter_temp_c

    def test_it_is_hotter_at_noon_than_at_dawn(self, clock, inverter):
        dawn = generate_reading(inverter, clock.instant_at(3.0), 1, seed=SEED)
        noon = generate_reading(inverter, clock.instant_at(150.0), 50, seed=SEED)
        assert noon.module_temp_c > dawn.module_temp_c

    def test_negative_tick_index_is_refused(self, clock, inverter):
        with pytest.raises(ValueError, match="tick_index must not be negative"):
            generate_reading(inverter, clock.instant_at(0.0), -1, seed=SEED)


class TestDeterminism:
    def test_the_same_inputs_produce_the_same_reading(self, clock, inverter):
        instant = clock.instant_at(123.0)
        assert generate_reading(inverter, instant, 41, seed=SEED) == generate_reading(
            inverter, instant, 41, seed=SEED
        )

    def test_a_whole_day_replays_identically(self, clock, inverter):
        assert day_of_readings(clock, inverter) == day_of_readings(clock, inverter)

    def test_a_different_seed_produces_different_values(self, clock, inverter):
        instant = clock.instant_at(150.0)
        assert generate_reading(inverter, instant, 50, seed=SEED) != generate_reading(
            inverter, instant, 50, seed=SEED + 1
        )

    def test_each_asset_has_its_own_stream(self, clock):
        # Same rating and same tick: only the identity differs, so identical
        # values would mean the asset is not part of the derivation.
        left = Inverter(id="INV_01", name="a", rated_power_kw=1000.0, plant_id="PLANT_01")
        right = Inverter(id="INV_02", name="b", rated_power_kw=1000.0, plant_id="PLANT_01")
        instant = clock.instant_at(150.0)

        assert generate_reading(left, instant, 50, seed=SEED) != generate_reading(
            right, instant, 50, seed=SEED
        )

    def test_the_plant_is_part_of_the_identity(self, clock):
        left = Inverter(id="INV_01", name="a", rated_power_kw=1000.0, plant_id="PLANT_01")
        right = Inverter(id="INV_01", name="a", rated_power_kw=1000.0, plant_id="PLANT_02")
        instant = clock.instant_at(150.0)

        assert generate_reading(left, instant, 50, seed=SEED) != generate_reading(
            right, instant, 50, seed=SEED
        )

    def test_a_value_does_not_depend_on_earlier_ticks(self, clock, inverter):
        # The property that makes a telemetry gap or a restart safe: tick 50 is
        # the same whether or not ticks 0-49 were ever generated.
        instant = clock.instant_at(150.0)
        standalone = generate_reading(inverter, instant, 50, seed=SEED)

        for tick in range(50):
            generate_reading(inverter, clock.instant_at(tick * 3.0), tick, seed=SEED)
        after_a_full_run = generate_reading(inverter, instant, 50, seed=SEED)

        assert standalone == after_a_full_run

    def test_the_rng_is_stable_across_processes(self):
        # A golden value, recorded from the implementation rather than derived.
        # Its job is to fail if anyone swaps hashlib for the built-in hash(),
        # which Python randomises per process: that change would leave every
        # other test in this file passing while making the demo unreproducible
        # from one run to the next.
        assert deterministic_rng(SEED, "PLANT_01", "INV_01", 50).random() == pytest.approx(
            0.4505287947723887
        )


class TestPerformanceRatio:
    """The integration contract with Member 2's underperformance rule."""

    def test_ratio_reduces_to_the_temperature_factor(self, clock, inverter):
        # Both irradiance and power derive from the same solar_shape, so it
        # cancels. Without noise the ratio is exactly the temperature factor.
        quiet = SolarModel(power_noise_low=1.0, power_noise_high=1.0)
        reading = generate_reading(inverter, clock.instant_at(150.0), 50, seed=SEED, model=quiet)

        ratio = reading.performance_ratio(inverter.rated_power_kw, REFERENCE_IRRADIANCE_WM2)
        assert ratio == pytest.approx(temperature_factor(reading.module_temp_c, quiet))

    def test_a_healthy_inverter_never_trips_the_alert_threshold(self, clock, inverter):
        # The whole point of the design. A false alert here would flood the
        # dashboard during the demo.
        for reading in day_of_readings(clock, inverter):
            if reading.irradiance_wm2 < MIN_IRRADIANCE_WM2:
                continue  # Member 2's rule ignores readings this dim.
            ratio = reading.performance_ratio(inverter.rated_power_kw, REFERENCE_IRRADIANCE_WM2)
            assert ratio >= UNDERPERFORMANCE_THRESHOLD

    def test_no_asset_in_the_real_portfolio_trips_the_threshold(self, clock, portfolio):
        # Every inverter, every tick of a full simulated day.
        interval = settings().telemetry_interval_seconds
        worst = 1.0

        for asset in portfolio.inverters():
            for tick in range(int(settings().ticks_per_day)):
                reading = generate_reading(
                    asset, clock.instant_at(tick * interval), tick, seed=SEED
                )
                if reading.irradiance_wm2 < MIN_IRRADIANCE_WM2:
                    continue
                worst = min(
                    worst,
                    reading.performance_ratio(asset.rated_power_kw, REFERENCE_IRRADIANCE_WM2),
                )

        assert worst >= UNDERPERFORMANCE_THRESHOLD, f"worst ratio was {worst:.3f}"

    def test_the_scripted_underperformance_is_detectable(self, clock, inverter):
        # Milestone 7 scales power by 0.45 while leaving irradiance normal.
        reading = generate_reading(inverter, clock.instant_at(150.0), 50, seed=SEED)
        degraded = InverterReading(
            active_power_kw=reading.active_power_kw * 0.45,
            irradiance_wm2=reading.irradiance_wm2,
            module_temp_c=reading.module_temp_c,
            inverter_temp_c=reading.inverter_temp_c,
        )

        ratio = degraded.performance_ratio(inverter.rated_power_kw, REFERENCE_IRRADIANCE_WM2)
        assert ratio < UNDERPERFORMANCE_THRESHOLD
        # Comfortably below, not marginally: the rule must catch it every run.
        assert ratio < 0.5

    def test_zero_irradiance_does_not_divide_by_zero(self):
        night = InverterReading(0.0, 0.0, 26.0, 26.0)
        assert night.performance_ratio(1000.0, REFERENCE_IRRADIANCE_WM2) == 0.0

    def test_invalid_denominators_are_refused(self):
        reading = InverterReading(100.0, 500.0, 40.0, 38.0)
        with pytest.raises(ValueError):
            reading.performance_ratio(0.0, REFERENCE_IRRADIANCE_WM2)
        with pytest.raises(ValueError):
            reading.performance_ratio(1000.0, 0.0)


class TestEnergyLedger:
    def test_accumulates_across_ticks(self):
        ledger = EnergyLedger()

        assert ledger.accumulate("PLANT_01:INV_01", 0, 500.0, 0.24) == pytest.approx(120.0)
        assert ledger.accumulate("PLANT_01:INV_01", 0, 500.0, 0.24) == pytest.approx(240.0)

    def test_never_decreases_within_a_day(self, clock, inverter):
        ledger = EnergyLedger()
        totals = []
        for tick, reading in enumerate(day_of_readings(clock, inverter)):
            totals.append(
                ledger.accumulate(
                    inverter.asset_key, 0, reading.active_power_kw, clock.tick_simulated_hours
                )
            )

        assert totals == sorted(totals)
        assert totals[-1] > 0

    def test_resets_at_the_simulated_day_boundary(self):
        ledger = EnergyLedger()
        ledger.accumulate("PLANT_01:INV_01", 0, 500.0, 0.24)

        assert ledger.accumulate("PLANT_01:INV_01", 1, 500.0, 0.24) == pytest.approx(120.0)

    def test_assets_accumulate_independently(self):
        ledger = EnergyLedger()
        ledger.accumulate("PLANT_01:INV_01", 0, 500.0, 0.24)
        ledger.accumulate("PLANT_01:INV_02", 0, 100.0, 0.24)

        assert ledger.current("PLANT_01:INV_01") == pytest.approx(120.0)
        assert ledger.current("PLANT_01:INV_02") == pytest.approx(24.0)

    def test_unknown_asset_reads_as_zero(self):
        assert EnergyLedger().current("PLANT_09:INV_09") == 0.0
        assert EnergyLedger().day_of("PLANT_09:INV_09") is None

    def test_zero_power_still_advances_the_day(self):
        # An OFFLINE inverter contributes nothing but must not be forgotten.
        ledger = EnergyLedger()
        ledger.accumulate("PLANT_01:INV_01", 0, 0.0, 0.24)

        assert ledger.current("PLANT_01:INV_01") == 0.0
        assert ledger.day_of("PLANT_01:INV_01") == 0

    def test_negative_inputs_are_refused(self):
        ledger = EnergyLedger()
        with pytest.raises(ValueError, match="power_kw must not be negative"):
            ledger.accumulate("PLANT_01:INV_01", 0, -1.0, 0.24)
        with pytest.raises(ValueError, match="simulated_hours must not be negative"):
            ledger.accumulate("PLANT_01:INV_01", 0, 1.0, -0.24)

    def equivalent_sun_hours(self, clock, inverter, model=None) -> float:
        ledger = EnergyLedger()
        for reading in day_of_readings(clock, inverter, model=model):
            ledger.accumulate(
                inverter.asset_key, 0, reading.active_power_kw, clock.tick_simulated_hours
            )
        return ledger.current(inverter.asset_key) / inverter.rated_power_kw

    def test_the_default_all_day_arc_yields_about_twelve_sun_hours(self, clock, inverter):
        # The playbook's reference formula keeps the sun up from midnight to
        # midnight, which roughly doubles a real site's yield. Pinned here so the
        # figure is a known, deliberate property rather than a surprise: the
        # daily reference feed is derived from this same model, so expected and
        # actual stay consistent either way.
        hours = self.equivalent_sun_hours(clock, inverter)
        assert 11.0 <= hours <= 13.5, f"{hours:.2f} sun hours"

    def test_a_twelve_hour_daylight_day_yields_a_realistic_figure(self, clock, inverter):
        # Real commercial sites report 4-6 equivalent sun hours. This model omits
        # soiling, wiring and inverter-efficiency losses, so it sits at the top
        # of that range.
        realistic = SolarModel(daylight_fraction=0.5)
        hours = self.equivalent_sun_hours(clock, inverter, model=realistic)
        assert 4.0 <= hours <= 7.0, f"{hours:.2f} sun hours"


class TestModelValidation:
    @pytest.mark.parametrize(
        "kwargs,match",
        [
            ({"clear_sky_peak_wm2": 0}, "clear_sky_peak_wm2"),
            ({"shape_exponent": 0}, "shape_exponent"),
            ({"min_temperature_factor": 0}, "min_temperature_factor"),
            ({"min_temperature_factor": 1.5}, "min_temperature_factor"),
            ({"temp_coefficient_per_c": -0.1}, "temp_coefficient_per_c"),
            ({"power_noise_low": 1.5, "power_noise_high": 0.5}, "power_noise_low"),
            ({"power_noise_low": 0.0}, "power_noise_low"),
        ],
    )
    def test_impossible_coefficients_are_refused(self, kwargs, match):
        with pytest.raises(ConfigError, match=match):
            SolarModel(**kwargs)

    def test_defaults_match_the_playbook_reference_values(self):
        model = SolarModel()

        assert model.clear_sky_peak_wm2 == 1000.0
        assert model.shape_exponent == 1.5
        assert model.temp_coefficient_per_c == 0.004
        assert model.min_temperature_factor == 0.80
        assert (model.power_noise_low, model.power_noise_high) == (0.97, 1.03)

    def test_reference_irradiance_agrees_with_member_2(self):
        # Member 2 divides by REFERENCE_IRRADIANCE_WM2 (default 1000) to get
        # expected power. If the two ever disagree, every performance ratio in
        # the system is wrong by that factor.
        assert SolarModel().clear_sky_peak_wm2 == REFERENCE_IRRADIANCE_WM2
