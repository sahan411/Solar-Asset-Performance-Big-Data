"""The simulated customer base: 30 households spread over 3 grid zones.

Both simulators and the Spark job import this, so they all agree on which
meters and zones exist. The list is built deterministically, so every run
has exactly the same households.
"""

from __future__ import annotations  # Spark image runs Python 3.8

from dataclasses import dataclass

ZONES = ("north", "central", "south")

# Share of households in each zone with rooftop solar panels.
SOLAR_SHARE = {"north": 0.3, "central": 0.4, "south": 0.5}

HOUSEHOLDS_PER_ZONE = 10
READINGS_PER_HOUR = 4  # one reading per simulated 15 minutes


@dataclass(frozen=True)
class Household:
    household_id: str
    meter_id: str
    grid_zone: str
    size_factor: float  # scales consumption: 0.7 = small flat, 1.5 = large house
    solar_kw: float  # rooftop panel capacity, 0 = no panels
    billing_tier: str  # A (low use), B (medium), C (high)
    subsidy: bool  # eligible for the government energy subsidy


def _build() -> list[Household]:
    households = []
    for i in range(len(ZONES) * HOUSEHOLDS_PER_ZONE):
        zone = ZONES[i % len(ZONES)]
        position_in_zone = i // len(ZONES)
        size = round(0.7 + (i * 37 % 9) / 10, 2)  # 0.7 .. 1.5, fixed pattern
        has_solar = position_in_zone < SOLAR_SHARE[zone] * HOUSEHOLDS_PER_ZONE
        households.append(Household(
            household_id=f"H{i + 1:03d}",
            meter_id=f"M{i + 1:03d}",
            grid_zone=zone,
            size_factor=size,
            solar_kw=(1.5 + 0.5 * (i % 3)) if has_solar else 0.0,
            billing_tier="A" if size < 0.95 else "B" if size < 1.25 else "C",
            subsidy=(i % 5 == 0),
        ))
    return households


HOUSEHOLDS = _build()


def expected_readings_per_hour(zone: str) -> int:
    return sum(1 for h in HOUSEHOLDS if h.grid_zone == zone) * READINGS_PER_HOUR
