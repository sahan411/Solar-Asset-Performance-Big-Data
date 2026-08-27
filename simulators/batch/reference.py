"""Daily reference feed: the batch layer's expectation of a healthy day.

One CSV per simulated day, one row per plant, describing what each plant *should*
generate and what that generation is worth. Airflow reconciles the day's measured
actuals against it to produce lost energy and lost revenue — so every financial
figure the platform reports ultimately traces back to this file.

**Expected generation is derived from the same model the simulator runs**, not
from an independently chosen sun-hours assumption. That is the single most
important decision here. A baseline that does not come from the same source as
the measurement is not a baseline: had the forecast said "5 equivalent sun hours"
while the simulator produced 12.3, every plant would report ~250% performance and
the entire revenue reconciliation would be meaningless. Deriving both from one
model means a shortfall the reconciliation reports is a real shortfall — caused by
the scripted anomalies, and by nothing else.

The forecast runs the model with noise switched off, so it is the smooth ideal
curve while actuals vary a percent or two around it. That is what a forecast is.

Writes are atomic: a temporary file in the same directory, then a rename. Airflow
polls this directory, and a half-written CSV read mid-write would fail the day's
reconciliation with a parse error that looks like corruption.
"""

from __future__ import annotations

import csv
import os
from dataclasses import dataclass, replace
from datetime import date
from pathlib import Path

from simulators.common.config import ConfigError, SimulationSettings
from simulators.common.logging import get_logger
from simulators.common.portfolio import Plant, Portfolio
from simulators.common.time import SimulationClock
from simulators.streaming.generation import SolarModel, generate_reading

log = get_logger("batch-simulator")

# Frozen contract, in file order. Matches REFERENCE_COLUMNS in Member 2's
# processing/batch/reference.py exactly; reordering or renaming one is a
# team-level contract change.
REFERENCE_COLUMNS = (
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

SOURCE_VERSION = "v1"

# Member 2 rejects a feed implying more than this many equivalent full-power
# hours, as a guard against a kWh/MWh unit error inflating revenue. Duplicated
# here so the generator refuses to write a file he would reject.
MAX_EQUIVALENT_SUN_HOURS = 14.0

# Fictional commercial rate, in currency units per kWh. Not a real tariff and not
# presented as one; plants may override it in portfolio.yaml so lost revenue
# differs across the portfolio.
DEFAULT_PPA_RATE_PER_KWH = 0.085

# Suffix for the in-progress file. Kept in the destination directory so the
# rename is within one filesystem and therefore atomic.
TEMP_SUFFIX = ".tmp"


class ReferenceFeedError(ConfigError):
    """Raised when the daily reference feed cannot be generated or written."""


@dataclass(frozen=True)
class PlantExpectation:
    """One plant's forecast for one simulated day."""

    plant_id: str
    plant_capacity_kw: float
    expected_generation_kwh: float
    expected_peak_power_kw: float
    forecast_irradiance_kwh_m2: float
    ppa_rate_per_kwh: float
    maintenance_flag: bool

    @property
    def equivalent_sun_hours(self) -> float:
        return self.expected_generation_kwh / self.plant_capacity_kw

    @property
    def expected_revenue(self) -> float:
        """What a healthy day is worth. The figure lost revenue is measured against."""
        return self.expected_generation_kwh * self.ppa_rate_per_kwh

    def as_row(self, simulation_date: date) -> dict[str, object]:
        """The plant's row, in frozen contract column order."""
        return {
            "simulation_date": simulation_date.isoformat(),
            "plant_id": self.plant_id,
            "plant_capacity_kw": round(self.plant_capacity_kw, 3),
            "expected_generation_kwh": round(self.expected_generation_kwh, 3),
            "expected_peak_power_kw": round(self.expected_peak_power_kw, 3),
            "forecast_irradiance_kwh_m2": round(self.forecast_irradiance_kwh_m2, 4),
            "ppa_rate_per_kwh": round(self.ppa_rate_per_kwh, 4),
            # Lowercase, which is what Member 2's boolean parser expects.
            "maintenance_flag": str(self.maintenance_flag).lower(),
            "source_version": SOURCE_VERSION,
        }


def forecast_model(model: SolarModel | None = None) -> SolarModel:
    """The runtime model with every noise term switched off.

    A forecast is a smooth expectation; the noise is what makes actuals differ
    from it. Every other coefficient — daylight length, the shape exponent, the
    temperature derate — is inherited, so the forecast tracks any change to the
    generation model automatically instead of drifting away from it.
    """
    return replace(
        model or SolarModel(),
        module_noise_c=0.0,
        inverter_noise_c=0.0,
        power_noise_low=1.0,
        power_noise_high=1.0,
    )


def forecast_plant(
    plant: Plant,
    clock: SimulationClock,
    *,
    seed: int,
    telemetry_interval_seconds: float,
    model: SolarModel | None = None,
    ppa_rate_per_kwh: float = DEFAULT_PPA_RATE_PER_KWH,
    maintenance_flag: bool = False,
) -> PlantExpectation:
    """Integrate a noise-free simulated day for one plant.

    Runs the real generation model over a full day rather than applying a
    formula, so the forecast and the telemetry cannot disagree about what a
    healthy day looks like.
    """
    if ppa_rate_per_kwh < 0:
        raise ReferenceFeedError(
            f"plant {plant.id}: ppa_rate_per_kwh must not be negative, got {ppa_rate_per_kwh}."
        )

    quiet = forecast_model(model)
    ticks = int(round(clock.day_seconds / telemetry_interval_seconds))
    if ticks <= 0:
        raise ReferenceFeedError(
            f"A simulated day of {clock.day_seconds}s at {telemetry_interval_seconds}s "
            "per tick yields no ticks to integrate."
        )

    generation_kwh = 0.0
    peak_power_kw = 0.0
    insolation_kwh_m2 = 0.0
    hours_per_tick = clock.tick_simulated_hours

    for tick in range(ticks):
        instant = clock.instant_at(tick * telemetry_interval_seconds)
        plant_power_kw = 0.0
        irradiance_wm2 = 0.0

        for inverter in plant.inverters:
            reading = generate_reading(inverter, instant, tick, seed=seed, model=quiet)
            plant_power_kw += reading.active_power_kw
            # Identical across a plant's inverters in this model; taken from the
            # last one rather than averaged, to avoid implying more precision
            # than the model has.
            irradiance_wm2 = reading.irradiance_wm2

        generation_kwh += plant_power_kw * hours_per_tick
        peak_power_kw = max(peak_power_kw, plant_power_kw)
        # W/m^2 integrated over hours, converted to kWh/m^2.
        insolation_kwh_m2 += (irradiance_wm2 / 1000.0) * hours_per_tick

    expectation = PlantExpectation(
        plant_id=plant.id,
        plant_capacity_kw=plant.capacity_kw,
        expected_generation_kwh=generation_kwh,
        expected_peak_power_kw=peak_power_kw,
        forecast_irradiance_kwh_m2=insolation_kwh_m2,
        ppa_rate_per_kwh=ppa_rate_per_kwh,
        maintenance_flag=maintenance_flag,
    )
    _validate_expectation(expectation)
    return expectation


def _validate_expectation(expectation: PlantExpectation) -> None:
    """Refuse to emit a row Member 2's validator would reject."""
    if expectation.expected_generation_kwh <= 0:
        raise ReferenceFeedError(
            f"plant {expectation.plant_id}: expected_generation_kwh must be greater "
            "than zero. A day with no expected generation makes every performance "
            "ratio undefined."
        )
    if expectation.expected_peak_power_kw <= 0:
        raise ReferenceFeedError(
            f"plant {expectation.plant_id}: expected_peak_power_kw must be greater than zero."
        )
    if expectation.expected_peak_power_kw > expectation.plant_capacity_kw:
        raise ReferenceFeedError(
            f"plant {expectation.plant_id}: expected peak "
            f"{expectation.expected_peak_power_kw:.1f} kW exceeds the plant's "
            f"{expectation.plant_capacity_kw:g} kW nameplate rating."
        )
    if expectation.equivalent_sun_hours > MAX_EQUIVALENT_SUN_HOURS:
        raise ReferenceFeedError(
            f"plant {expectation.plant_id}: expected generation implies "
            f"{expectation.equivalent_sun_hours:.1f} equivalent full-power hours, "
            f"above the {MAX_EQUIVALENT_SUN_HOURS:g} limit Member 2's batch layer "
            "accepts. The feed would be rejected on load."
        )


def plant_ppa_rate(plant: Plant, default: float = DEFAULT_PPA_RATE_PER_KWH) -> float:
    """A plant's commercial rate, falling back to the portfolio default."""
    return plant.ppa_rate_per_kwh if plant.ppa_rate_per_kwh is not None else default


def build_expectations(
    portfolio: Portfolio,
    clock: SimulationClock,
    *,
    seed: int,
    telemetry_interval_seconds: float,
    model: SolarModel | None = None,
    default_ppa_rate: float = DEFAULT_PPA_RATE_PER_KWH,
) -> list[PlantExpectation]:
    """Forecast every plant in the portfolio. One row per plant, no more, no less."""
    return [
        forecast_plant(
            plant,
            clock,
            seed=seed,
            telemetry_interval_seconds=telemetry_interval_seconds,
            model=model,
            ppa_rate_per_kwh=plant_ppa_rate(plant, default_ppa_rate),
        )
        for plant in portfolio.plants
    ]


def reference_path(directory: str | Path, simulation_date: date) -> Path:
    """Conventional filename for a simulated day's feed.

    Matches `reference_path` in Member 2's batch layer exactly; a mismatch here
    means Airflow looks for a file that is never written.
    """
    return Path(directory) / f"daily_reference_{simulation_date.isoformat()}.csv"


def write_reference_file(
    expectations: list[PlantExpectation],
    simulation_date: date,
    directory: str | Path,
    *,
    overwrite: bool = False,
) -> Path:
    """Write one simulated day's feed atomically. Returns the final path.

    Regenerating an existing day is refused unless `overwrite` is set, which the
    demo reset script passes. Silently replacing a day Airflow may already have
    reconciled would make the batch layer's history untraceable.
    """
    if not expectations:
        raise ReferenceFeedError(
            f"Refusing to write an empty reference feed for {simulation_date.isoformat()}: "
            "the batch layer would have no plants to reconcile."
        )

    seen: set[str] = set()
    for expectation in expectations:
        if expectation.plant_id in seen:
            raise ReferenceFeedError(
                f"Duplicate row for plant {expectation.plant_id} on "
                f"{simulation_date.isoformat()}."
            )
        seen.add(expectation.plant_id)

    destination = reference_path(directory, simulation_date)
    if destination.exists() and not overwrite:
        raise ReferenceFeedError(
            f"{destination} already exists. Pass overwrite=True (as demo_reset.sh does) "
            "to regenerate a simulated day."
        )

    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(destination.name + TEMP_SUFFIX)

    try:
        # newline="" is required: without it csv writes \r\r\n on Windows and
        # pandas reads a blank row between every record.
        with temporary.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(REFERENCE_COLUMNS))
            writer.writeheader()
            for expectation in expectations:
                writer.writerow(expectation.as_row(simulation_date))
            handle.flush()
            # Force the bytes to disk before the rename, so a crash cannot leave
            # a correctly-named file with truncated contents.
            os.fsync(handle.fileno())

        # Atomic on both POSIX and Windows, and silently replaces an existing
        # file — which is why the overwrite check above happens first.
        os.replace(temporary, destination)
    except OSError as exc:
        temporary.unlink(missing_ok=True)
        raise ReferenceFeedError(f"Could not write {destination}: {exc}") from exc

    log.info(
        "daily_reference_ready",
        f"Wrote reference feed for {simulation_date.isoformat()} "
        f"({len(expectations)} plants)",
        simulation_date=simulation_date.isoformat(),
        path=str(destination),
        plants=len(expectations),
        expected_generation_kwh=round(
            sum(e.expected_generation_kwh for e in expectations), 3
        ),
        expected_revenue=round(sum(e.expected_revenue for e in expectations), 2),
    )
    return destination


def generate_daily_reference(
    portfolio: Portfolio,
    settings: SimulationSettings,
    simulation_date: date,
    *,
    model: SolarModel | None = None,
    default_ppa_rate: float = DEFAULT_PPA_RATE_PER_KWH,
    overwrite: bool = False,
    directory: str | Path | None = None,
) -> Path:
    """Forecast the portfolio and write the day's reference feed."""
    clock = SimulationClock(settings)
    expectations = build_expectations(
        portfolio,
        clock,
        seed=settings.seed,
        telemetry_interval_seconds=settings.telemetry_interval_seconds,
        model=model,
        default_ppa_rate=default_ppa_rate,
    )
    return write_reference_file(
        expectations,
        simulation_date,
        directory if directory is not None else settings.output_dir,
        overwrite=overwrite,
    )
