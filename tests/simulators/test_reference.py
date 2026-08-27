"""Tests for the daily reference feed generator.

Every financial figure the platform reports traces back to this file, so the
tests are weighted towards the two failure modes that would matter: a feed
Member 2's batch layer rejects, and a feed whose expectation does not match what
the simulator actually produces. The second is the dangerous one — it fails
silently, as a confidently wrong revenue report.
"""

from __future__ import annotations

import csv
from datetime import date
from pathlib import Path

import pytest

from simulators.common.config import ConfigError, SimulationSettings
from simulators.common.portfolio import load_portfolio
from simulators.common.time import SimulationClock
from simulators.streaming.generation import EnergyLedger, SolarModel, generate_reading
from simulators.batch.reference import (
    _validate_expectation as validate,
)
from simulators.batch.reference import (
    DEFAULT_PPA_RATE_PER_KWH,
    MAX_EQUIVALENT_SUN_HOURS,
    REFERENCE_COLUMNS,
    SOURCE_VERSION,
    PlantExpectation,
    ReferenceFeedError,
    build_expectations,
    forecast_model,
    forecast_plant,
    generate_daily_reference,
    plant_ppa_rate,
    reference_path,
    write_reference_file,
)

SEED = 8203
REPO_ROOT = Path(__file__).resolve().parents[2]
SIM_DATE = date(2026, 8, 21)

# Member 2's contract, from processing/batch/reference.py. Duplicated so a
# divergence fails here rather than at Airflow run time.
MEMBER_2_COLUMNS = (
    "simulation_date",
    "plant_id",
    "plant_capacity_kw",
    "expected_generation_kwh",
    "expected_peak_power_kw",
    "forecast_irradiance_kwh_m2",
    "ppa_rate_per_kwh",
    "maintenance_flag",
    "source_version",
)
MEMBER_2_MAX_SUN_HOURS = 14.0
MEMBER_2_TRUE_VALUES = {"true", "t", "yes", "y", "1"}
MEMBER_2_FALSE_VALUES = {"false", "f", "no", "n", "0", ""}


def settings(**overrides) -> SimulationSettings:
    base = dict(
        day_seconds=300.0,
        telemetry_interval_seconds=3.0,
        seed=SEED,
        start_date=SIM_DATE,
        output_dir=Path("/data/daily"),
        portfolio_config_path=Path("simulators/config/portfolio.yaml"),
        emit_invalid_events=False,
    )
    base.update(overrides)
    return SimulationSettings(**base)


@pytest.fixture(scope="module")
def portfolio():
    return load_portfolio(REPO_ROOT / "simulators/config/portfolio.yaml")


@pytest.fixture
def clock():
    return SimulationClock(settings())


@pytest.fixture(scope="module")
def expectations(portfolio):
    return build_expectations(
        portfolio,
        SimulationClock(settings()),
        seed=SEED,
        telemetry_interval_seconds=3.0,
    )


def an_expectation(**overrides) -> PlantExpectation:
    base = dict(
        plant_id="PLANT_01",
        plant_capacity_kw=6000.0,
        expected_generation_kwh=60000.0,
        expected_peak_power_kw=5400.0,
        forecast_irradiance_kwh_m2=13.35,
        ppa_rate_per_kwh=0.081,
        maintenance_flag=False,
    )
    base.update(overrides)
    return PlantExpectation(**base)


def read_csv(path: Path) -> list[dict]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


class TestForecastModel:
    def test_it_switches_every_noise_term_off(self):
        quiet = forecast_model()

        assert quiet.module_noise_c == 0.0
        assert quiet.inverter_noise_c == 0.0
        assert quiet.power_noise_low == quiet.power_noise_high == 1.0

    def test_it_inherits_every_other_coefficient(self):
        # So the forecast tracks a change to the generation model automatically
        # rather than quietly drifting away from it.
        custom = SolarModel(daylight_fraction=0.5, shape_exponent=2.0, clear_sky_peak_wm2=900.0)
        quiet = forecast_model(custom)

        assert quiet.daylight_fraction == 0.5
        assert quiet.shape_exponent == 2.0
        assert quiet.clear_sky_peak_wm2 == 900.0

    def test_the_forecast_is_noise_free(self, clock, portfolio):
        # Identical readings regardless of seed, since nothing random remains.
        inverter = portfolio.plant("PLANT_01").inverters[0]
        instant = clock.instant_at(150.0)

        a = generate_reading(inverter, instant, 50, seed=1, model=forecast_model())
        b = generate_reading(inverter, instant, 50, seed=999, model=forecast_model())
        assert a == b


class TestForecastPlant:
    def test_it_produces_a_plausible_expectation(self, clock, portfolio):
        expectation = forecast_plant(
            portfolio.plant("PLANT_01"), clock, seed=SEED, telemetry_interval_seconds=3.0
        )

        assert expectation.plant_id == "PLANT_01"
        assert expectation.plant_capacity_kw == 6000.0
        assert expectation.expected_generation_kwh > 0
        assert expectation.expected_peak_power_kw > 0
        assert expectation.forecast_irradiance_kwh_m2 > 0

    def test_peak_never_exceeds_nameplate_capacity(self, expectations):
        # The temperature derate guarantees it, but Member 2 rejects a feed that
        # breaks this so it is asserted rather than assumed.
        for expectation in expectations:
            assert expectation.expected_peak_power_kw <= expectation.plant_capacity_kw

    def test_it_is_deterministic(self, clock, portfolio):
        plant = portfolio.plant("PLANT_03")
        first = forecast_plant(plant, clock, seed=SEED, telemetry_interval_seconds=3.0)
        second = forecast_plant(plant, clock, seed=SEED, telemetry_interval_seconds=3.0)

        assert first == second

    def test_bigger_plants_expect_more_generation(self, expectations):
        by_id = {e.plant_id: e for e in expectations}
        assert (
            by_id["PLANT_01"].expected_generation_kwh
            > by_id["PLANT_04"].expected_generation_kwh
            > by_id["PLANT_05"].expected_generation_kwh
        )

    def test_a_negative_rate_is_refused(self, clock, portfolio):
        with pytest.raises(ReferenceFeedError, match="must not be negative"):
            forecast_plant(
                portfolio.plant("PLANT_01"),
                clock,
                seed=SEED,
                telemetry_interval_seconds=3.0,
                ppa_rate_per_kwh=-0.1,
            )

    def test_equivalent_sun_hours_stay_under_member_2s_ceiling(self, expectations):
        # The default all-day arc yields ~12.3, against his limit of 14. Real
        # headroom, but not much: if the curve is ever widened further, this
        # fails here instead of at Airflow run time.
        for expectation in expectations:
            assert expectation.equivalent_sun_hours < MEMBER_2_MAX_SUN_HOURS
        assert MAX_EQUIVALENT_SUN_HOURS == MEMBER_2_MAX_SUN_HOURS

    def test_a_wider_daylight_window_would_be_rejected(self, clock, portfolio):
        # Proof the guard works: stretch the arc past the plausible limit and the
        # generator refuses to write a file Member 2 would reject.
        absurd = SolarModel(shape_exponent=0.05)
        with pytest.raises(ReferenceFeedError, match="equivalent full-power hours"):
            forecast_plant(
                portfolio.plant("PLANT_01"),
                clock,
                seed=SEED,
                telemetry_interval_seconds=3.0,
                model=absurd,
            )


class TestForecastMatchesTheSimulator:
    """The point of deriving expectation and measurement from one model."""

    def test_a_healthy_day_lands_close_to_its_own_forecast(self, clock, portfolio):
        plant = portfolio.plant("PLANT_02")
        expectation = forecast_plant(
            plant, clock, seed=SEED, telemetry_interval_seconds=3.0
        )

        # Run the real, noisy simulator over the same day.
        ledger = EnergyLedger()
        for tick in range(100):
            instant = clock.instant_at(tick * 3.0)
            for inverter in plant.inverters:
                reading = generate_reading(inverter, instant, tick, seed=SEED)
                ledger.accumulate(
                    inverter.asset_key,
                    instant.day_index,
                    reading.active_power_kw,
                    clock.tick_simulated_hours,
                )
        actual = sum(ledger.current(inv.asset_key) for inv in plant.inverters)

        performance = actual / expectation.expected_generation_kwh
        # Noise alone must not move a healthy plant far from 100%. A wider gap
        # would mean the reconciliation reports losses nobody caused.
        assert 0.97 <= performance <= 1.03, f"healthy plant reported {performance:.1%}"

    def test_every_plant_reconciles_near_one_hundred_percent(self, clock, portfolio):
        for plant in portfolio.plants:
            expectation = forecast_plant(
                plant, clock, seed=SEED, telemetry_interval_seconds=3.0
            )
            ledger = EnergyLedger()
            for tick in range(100):
                instant = clock.instant_at(tick * 3.0)
                for inverter in plant.inverters:
                    reading = generate_reading(inverter, instant, tick, seed=SEED)
                    ledger.accumulate(
                        inverter.asset_key,
                        instant.day_index,
                        reading.active_power_kw,
                        clock.tick_simulated_hours,
                    )
            actual = sum(ledger.current(inv.asset_key) for inv in plant.inverters)
            ratio = actual / expectation.expected_generation_kwh
            assert 0.95 <= ratio <= 1.05, f"{plant.id} reported {ratio:.1%}"


class TestPpaRates:
    def test_the_portfolio_rate_is_used_when_present(self, portfolio):
        assert plant_ppa_rate(portfolio.plant("PLANT_05")) == 0.112

    def test_the_default_applies_when_a_plant_has_none(self, portfolio):
        from dataclasses import replace

        plain = replace(portfolio.plant("PLANT_01"), ppa_rate_per_kwh=None)
        assert plant_ppa_rate(plain) == DEFAULT_PPA_RATE_PER_KWH

    def test_rates_differ_across_the_portfolio(self, expectations):
        # So lost revenue is not simply proportional to lost energy.
        rates = {e.ppa_rate_per_kwh for e in expectations}
        assert len(rates) > 1

    def test_expected_revenue_is_generation_times_rate(self):
        expectation = an_expectation(expected_generation_kwh=1000.0, ppa_rate_per_kwh=0.1)
        assert expectation.expected_revenue == pytest.approx(100.0)


class TestRowShape:
    def test_columns_match_member_2s_contract_exactly(self):
        assert REFERENCE_COLUMNS == MEMBER_2_COLUMNS

    def test_a_row_carries_every_column_in_order(self):
        row = an_expectation().as_row(SIM_DATE)
        assert tuple(row) == REFERENCE_COLUMNS

    def test_the_date_is_iso(self):
        assert an_expectation().as_row(SIM_DATE)["simulation_date"] == "2026-08-21"

    def test_the_maintenance_flag_is_parseable_by_member_2(self):
        assert an_expectation(maintenance_flag=False).as_row(SIM_DATE)[
            "maintenance_flag"
        ] in MEMBER_2_FALSE_VALUES
        assert an_expectation(maintenance_flag=True).as_row(SIM_DATE)[
            "maintenance_flag"
        ] in MEMBER_2_TRUE_VALUES

    def test_the_source_version_is_stamped(self):
        assert an_expectation().as_row(SIM_DATE)["source_version"] == SOURCE_VERSION


class TestExpectationValidation:
    """Rows Member 2's batch layer would reject must never be written."""

    def test_peak_above_capacity_is_refused(self):
        with pytest.raises(ReferenceFeedError, match="exceeds the plant"):
            validate(an_expectation(expected_peak_power_kw=99_999.0))

    def test_zero_expected_generation_is_refused(self):
        with pytest.raises(ReferenceFeedError, match="expected_generation_kwh"):
            validate(an_expectation(expected_generation_kwh=0.0))

    def test_zero_expected_peak_is_refused(self):
        with pytest.raises(ReferenceFeedError, match="expected_peak_power_kw"):
            validate(an_expectation(expected_peak_power_kw=0.0))

    def test_an_implausible_yield_is_refused(self):
        with pytest.raises(ReferenceFeedError, match="equivalent full-power hours"):
            validate(
                an_expectation(
                    expected_generation_kwh=6000.0 * (MAX_EQUIVALENT_SUN_HOURS + 1)
                )
            )

    def test_a_realistic_expectation_passes(self):
        validate(an_expectation())


class TestFilePath:
    def test_the_filename_matches_member_2s_convention(self):
        assert reference_path("/data/daily", SIM_DATE).name == (
            "daily_reference_2026-08-21.csv"
        )

    def test_the_date_in_the_name_is_the_simulated_day(self):
        assert reference_path("/data/daily", date(2026, 9, 1)).name == (
            "daily_reference_2026-09-01.csv"
        )


class TestWriting:
    def test_it_writes_one_row_per_plant(self, tmp_path, expectations):
        path = write_reference_file(expectations, SIM_DATE, tmp_path)
        rows = read_csv(path)

        assert len(rows) == 5
        assert [r["plant_id"] for r in rows] == [
            "PLANT_01",
            "PLANT_02",
            "PLANT_03",
            "PLANT_04",
            "PLANT_05",
        ]

    def test_the_header_is_the_contract(self, tmp_path, expectations):
        path = write_reference_file(expectations, SIM_DATE, tmp_path)
        with path.open(encoding="utf-8") as handle:
            assert handle.readline().strip() == ",".join(REFERENCE_COLUMNS)

    def test_every_row_carries_the_same_date(self, tmp_path, expectations):
        path = write_reference_file(expectations, SIM_DATE, tmp_path)
        assert {r["simulation_date"] for r in read_csv(path)} == {"2026-08-21"}

    def test_no_blank_rows_on_windows(self, tmp_path, expectations):
        # csv without newline="" writes \r\r\n on Windows and pandas then reads a
        # blank record between every row.
        path = write_reference_file(expectations, SIM_DATE, tmp_path)
        assert b"\r\r\n" not in path.read_bytes()

    def test_it_creates_the_directory(self, tmp_path, expectations):
        target = tmp_path / "nested" / "daily"
        path = write_reference_file(expectations, SIM_DATE, target)
        assert path.exists()

    def test_no_temporary_file_is_left_behind(self, tmp_path, expectations):
        write_reference_file(expectations, SIM_DATE, tmp_path)
        assert list(tmp_path.glob("*.tmp")) == []

    def test_regenerating_a_day_is_refused_by_default(self, tmp_path, expectations):
        write_reference_file(expectations, SIM_DATE, tmp_path)

        with pytest.raises(ReferenceFeedError, match="already exists"):
            write_reference_file(expectations, SIM_DATE, tmp_path)

    def test_overwrite_regenerates_deterministically(self, tmp_path, expectations):
        first = write_reference_file(expectations, SIM_DATE, tmp_path)
        original = first.read_bytes()

        again = write_reference_file(expectations, SIM_DATE, tmp_path, overwrite=True)
        assert again.read_bytes() == original

    def test_an_empty_feed_is_refused(self, tmp_path):
        with pytest.raises(ReferenceFeedError, match="empty reference feed"):
            write_reference_file([], SIM_DATE, tmp_path)

    def test_a_duplicate_plant_row_is_refused(self, tmp_path):
        with pytest.raises(ReferenceFeedError, match="Duplicate row"):
            write_reference_file([an_expectation(), an_expectation()], SIM_DATE, tmp_path)

    def test_different_days_get_different_files(self, tmp_path, expectations):
        first = write_reference_file(expectations, SIM_DATE, tmp_path)
        second = write_reference_file(expectations, date(2026, 8, 22), tmp_path)

        assert first != second
        assert len(list(tmp_path.glob("daily_reference_*.csv"))) == 2


class TestEndToEnd:
    def test_generate_daily_reference_writes_a_loadable_feed(self, tmp_path, portfolio):
        path = generate_daily_reference(
            portfolio, settings(), SIM_DATE, directory=tmp_path
        )
        rows = read_csv(path)

        assert len(rows) == len(portfolio.plants)
        for row in rows:
            assert float(row["expected_generation_kwh"]) > 0
            assert float(row["expected_peak_power_kw"]) <= float(row["plant_capacity_kw"])
            assert float(row["ppa_rate_per_kwh"]) > 0
            assert row["source_version"] == SOURCE_VERSION

    def test_capacities_match_the_portfolio(self, tmp_path, portfolio):
        path = generate_daily_reference(
            portfolio, settings(), SIM_DATE, directory=tmp_path
        )
        written = {r["plant_id"]: float(r["plant_capacity_kw"]) for r in read_csv(path)}

        assert written == {p.id: p.capacity_kw for p in portfolio.plants}

    def test_the_feed_covers_exactly_the_portfolio(self, tmp_path, portfolio):
        # Member 2 rejects a feed with a missing or unknown plant.
        path = generate_daily_reference(
            portfolio, settings(), SIM_DATE, directory=tmp_path
        )
        assert {r["plant_id"] for r in read_csv(path)} == {
            p.id for p in portfolio.plants
        }

    def test_reference_feed_error_is_a_config_error(self, tmp_path, portfolio):
        generate_daily_reference(portfolio, settings(), SIM_DATE, directory=tmp_path)
        with pytest.raises(ConfigError):
            generate_daily_reference(portfolio, settings(), SIM_DATE, directory=tmp_path)

    def test_a_faster_clock_forecasts_the_same_day(self, tmp_path, portfolio):
        # Day length is real-time compression; the simulated day is 24 hours
        # either way, so the expectation should barely move.
        slow = generate_daily_reference(
            portfolio, settings(), SIM_DATE, directory=tmp_path / "slow"
        )
        fast = generate_daily_reference(
            portfolio, settings(day_seconds=150.0), SIM_DATE, directory=tmp_path / "fast"
        )

        slow_rows = {r["plant_id"]: float(r["expected_generation_kwh"]) for r in read_csv(slow)}
        fast_rows = {r["plant_id"]: float(r["expected_generation_kwh"]) for r in read_csv(fast)}
        for plant_id, expected in slow_rows.items():
            assert fast_rows[plant_id] == pytest.approx(expected, rel=0.02)
