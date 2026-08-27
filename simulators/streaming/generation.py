"""Deterministic solar generation model.

Produces physically plausible readings for one inverter at one instant. This is a
**demo approximation, not a production PV model**: a real one would model the DC
array, inverter clipping, soiling, shading and spectral response. Here a single
sine-shaped term drives the whole day and a linear coefficient handles heat.

The model is built around one property that matters more than realism:

    irradiance    = clear_sky_peak * solar_shape
    active_power  = rated_kw * solar_shape * temperature_factor

Both derive from the *same* `solar_shape`, so when the underperformance rule
divides one by the other the shape cancels and the performance ratio reduces to
`temperature_factor` alone — roughly 0.87 to 0.98. Member 2's rule alerts below
0.80, so a healthy plant can never trip it, while the scripted underperformance
(x0.45) lands near 0.39 and is caught every time. Generating irradiance and power
from independent noise would let that ratio wander and fire alerts at random.

Anomalies are deliberately absent here. This module models a healthy inverter;
milestone 7 layers the scripted faults on top of what it returns.
"""

from __future__ import annotations

import hashlib
import math
import random
from dataclasses import dataclass

from simulators.common.config import ConfigError
from simulators.common.portfolio import Inverter
from simulators.common.time import SimulatedInstant


@dataclass(frozen=True)
class SolarModel:
    """Coefficients of the generation model.

    Defaults are the reference values from the project playbook. They are
    parameters rather than literals so tests can isolate one effect at a time —
    setting the noise bounds to 1.0 gives a noise-free curve, for instance.
    """

    # Clear-sky peak irradiance. Also the denominator Member 2's rule uses to
    # turn measured irradiance into a fraction of nameplate capacity, so the two
    # must agree: REFERENCE_IRRADIANCE_WM2 defaults to the same 1000.
    clear_sky_peak_wm2: float = 1000.0

    # Sharpens the midday peak. A plain sine is too broad — real generation ramps
    # faster after sunrise and falls faster before sunset.
    shape_exponent: float = 1.5

    # Fraction of the simulated day the sun is above the horizon, centred on
    # solar noon. 1.0 is the project playbook's reference formula: the arc is
    # stretched across the whole calendar day, so there is never darkness.
    #
    # That is deliberate for the demo — a five-minute assessment run should show
    # generation at every moment rather than spending its first quarter at night
    # — but it is not physical, and it roughly doubles daily yield: a full-day
    # arc integrates to about 12 equivalent sun hours against a real site's 4-6.
    # Setting 0.5 gives a twelve-hour daylight day and a realistic yield.
    #
    # Whatever this is set to, the daily reference feed's expected generation is
    # derived from this same model (milestone 8) rather than from an independent
    # assumption, so expected and actual stay consistent and any shortfall the
    # reconciliation reports is a real one.
    daylight_fraction: float = 1.0

    # Standard Test Conditions. Panels are rated at 25 C and lose roughly 0.4%
    # of their output per degree above it, which is why a hot afternoon can
    # generate less than a cool morning at identical irradiance.
    reference_temp_c: float = 25.0
    temp_coefficient_per_c: float = 0.004
    # Floor, so the derate cannot run away at extreme temperatures.
    min_temperature_factor: float = 0.80

    ambient_base_c: float = 26.0
    ambient_swing_c: float = 5.0
    module_rise_c: float = 20.0
    inverter_rise_c: float = 14.0
    module_noise_c: float = 1.5
    inverter_noise_c: float = 1.0

    power_noise_low: float = 0.97
    power_noise_high: float = 1.03

    def __post_init__(self) -> None:
        if self.clear_sky_peak_wm2 <= 0:
            raise ConfigError("clear_sky_peak_wm2 must be greater than zero.")
        if self.shape_exponent <= 0:
            raise ConfigError("shape_exponent must be greater than zero.")
        if not 0 < self.daylight_fraction <= 1:
            raise ConfigError("daylight_fraction must be in (0, 1].")
        if not 0 < self.min_temperature_factor <= 1:
            raise ConfigError("min_temperature_factor must be in (0, 1].")
        if self.temp_coefficient_per_c < 0:
            raise ConfigError("temp_coefficient_per_c must not be negative.")
        if self.power_noise_low > self.power_noise_high:
            raise ConfigError("power_noise_low must not exceed power_noise_high.")
        if self.power_noise_low <= 0:
            raise ConfigError("power_noise_low must be greater than zero.")


@dataclass(frozen=True)
class InverterReading:
    """The physical measurements one healthy inverter reports at one instant."""

    active_power_kw: float
    irradiance_wm2: float
    module_temp_c: float
    inverter_temp_c: float

    def performance_ratio(self, rated_power_kw: float, reference_irradiance_wm2: float) -> float:
        """Actual output as a fraction of what the measured irradiance implies.

        The quantity Member 2's underperformance rule thresholds on, reproduced
        here so the simulator's own tests can prove healthy readings stay above
        it. Zero irradiance yields 0.0 rather than dividing by zero; the rule
        ignores readings below its minimum irradiance anyway.
        """
        if reference_irradiance_wm2 <= 0 or rated_power_kw <= 0:
            raise ValueError("rated power and reference irradiance must be positive.")
        expected_kw = rated_power_kw * (self.irradiance_wm2 / reference_irradiance_wm2)
        if expected_kw <= 0:
            return 0.0
        return self.active_power_kw / expected_kw


def solar_shape(
    progress: float, exponent: float = 1.5, daylight_fraction: float = 1.0
) -> float:
    """The day's generation arc: 0.0 at sunrise and sunset, 1.0 at solar noon.

    `progress` runs 0.0 to 1.0 across the simulated day. The daylight window is
    centred on solar noon and `sin(pi * ...)` gives the sunrise-to-sunset curve
    in a single term; the exponent sharpens the midday peak. Outside the window
    the result is zero — night.

    At the default `daylight_fraction=1.0` the window is the whole day and this
    reduces exactly to the playbook's `sin(pi * progress) ** exponent`.

    Clamped at zero so floating-point noise at the boundaries can never produce
    negative power.
    """
    sunrise = 0.5 - daylight_fraction / 2.0
    daylight_progress = (progress - sunrise) / daylight_fraction
    if daylight_progress <= 0.0 or daylight_progress >= 1.0:
        return 0.0
    return max(0.0, math.sin(math.pi * daylight_progress)) ** exponent


def temperature_factor(module_temp_c: float, model: SolarModel) -> float:
    """Efficiency multiplier for a module running above its rated temperature."""
    excess_c = max(module_temp_c - model.reference_temp_c, 0.0)
    return max(model.min_temperature_factor, 1.0 - excess_c * model.temp_coefficient_per_c)


def deterministic_rng(
    seed: int, plant_id: str, inverter_id: str, tick_index: int
) -> random.Random:
    """A reproducible random source for one asset at one tick.

    Two decisions here, and the second is the important one:

    * `hashlib` rather than the built-in `hash()`. Python randomises string
      hashing per process (PYTHONHASHSEED), so `hash()` would make every run
      differ — the exact opposite of a reproducible demo.
    * The stream is keyed by *which* asset and *which* tick, not drawn from one
      running sequence. So a value depends only on its own coordinates, never on
      history: a telemetry gap or a mid-demo restart does not shift every
      subsequent number, and tick 50 is identical whether or not tick 49 was
      published.
    """
    key = f"{seed}|{plant_id}|{inverter_id}|{tick_index}".encode()
    digest = hashlib.blake2b(key, digest_size=8).digest()
    return random.Random(int.from_bytes(digest, "big"))


def generate_reading(
    inverter: Inverter,
    instant: SimulatedInstant,
    tick_index: int,
    *,
    seed: int,
    model: SolarModel | None = None,
) -> InverterReading:
    """Model one healthy inverter's measurements at one simulated instant."""
    model = model or SolarModel()
    if tick_index < 0:
        raise ValueError(f"tick_index must not be negative, got {tick_index}.")

    rng = deterministic_rng(seed, inverter.plant_id, inverter.id, tick_index)
    shape = solar_shape(instant.progress, model.shape_exponent, model.daylight_fraction)

    irradiance_wm2 = model.clear_sky_peak_wm2 * shape

    # Draw order is part of the contract: changing it changes every generated
    # number for a given seed, and with it the reproducibility of the demo.
    ambient_c = model.ambient_base_c + model.ambient_swing_c * shape
    module_temp_c = (
        ambient_c
        + model.module_rise_c * shape
        + rng.uniform(-model.module_noise_c, model.module_noise_c)
    )
    inverter_temp_c = (
        ambient_c
        + model.inverter_rise_c * shape
        + rng.uniform(-model.inverter_noise_c, model.inverter_noise_c)
    )
    power_noise = rng.uniform(model.power_noise_low, model.power_noise_high)

    base_power_kw = inverter.rated_power_kw * shape * temperature_factor(module_temp_c, model)
    # Bounded on both sides: never negative, never above the inverter's rating.
    # Noise must not be able to manufacture output the hardware cannot produce.
    active_power_kw = max(0.0, min(inverter.rated_power_kw, base_power_kw * power_noise))

    return InverterReading(
        active_power_kw=active_power_kw,
        irradiance_wm2=irradiance_wm2,
        module_temp_c=module_temp_c,
        inverter_temp_c=inverter_temp_c,
    )


class EnergyLedger:
    """Cumulative generation per inverter, integrated over simulated time.

    `energy_today_kwh` must never decrease within a day — it is a meter reading,
    and the batch layer's daily totals are reconciled against it. Generating it
    randomly could run backwards, which is physically impossible. So it is
    integrated instead: energy += power * simulated hours elapsed.

    Under the default clock one tick advances event time by 0.24 simulated
    hours, and 100 ticks per day integrate to exactly 24. It is a right-hand
    Riemann sum, accurate enough for a demo and trivial to explain.

    State is held per asset key and reset at the simulated-day boundary.
    """

    def __init__(self) -> None:
        self._energy_kwh: dict[str, float] = {}
        self._day_index: dict[str, int] = {}

    def accumulate(
        self, asset_key: str, day_index: int, power_kw: float, simulated_hours: float
    ) -> float:
        """Add one tick's generation and return the running daily total."""
        if power_kw < 0:
            raise ValueError(f"power_kw must not be negative, got {power_kw}.")
        if simulated_hours < 0:
            raise ValueError(f"simulated_hours must not be negative, got {simulated_hours}.")

        # A new simulated day resets the meter. Comparing the day index rather
        # than watching for a rollover means a restart mid-day cannot double-count.
        if self._day_index.get(asset_key) != day_index:
            self._day_index[asset_key] = day_index
            self._energy_kwh[asset_key] = 0.0

        self._energy_kwh[asset_key] += power_kw * simulated_hours
        return self._energy_kwh[asset_key]

    def current(self, asset_key: str) -> float:
        """The running daily total, or 0.0 for an asset that has not reported."""
        return self._energy_kwh.get(asset_key, 0.0)

    def day_of(self, asset_key: str) -> int | None:
        """Which simulated day this asset's meter is currently accumulating."""
        return self._day_index.get(asset_key)
