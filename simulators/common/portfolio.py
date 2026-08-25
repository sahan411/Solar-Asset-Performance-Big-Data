"""Load and validate the simulated solar portfolio.

The portfolio config decides which assets exist, and every telemetry event the
simulator produces carries an identity taken from it. A typo here does not fail
loudly at the source — it produces a stream of events for an inverter that no
database row matches, and the mistake only surfaces three subsystems later as
missing dashboard rows. So this module refuses to return a portfolio it cannot
fully validate.

Member 2's `storage/seed_portfolio.py` parses the same file to seed the database,
and deliberately only *warns* when a plant's inverter ratings disagree with its
nameplate capacity, on the grounds that the file belongs to Member 1. This module
is that owner, so the same condition is an error here. Strict at the source,
tolerant at the consumer.

Validation is split in two:

  * structural rules always apply — ids, names, positive capacities, uniqueness;
  * demo-scale rules (5 plants, 5-10 inverters each) apply when loading the real
    portfolio, and are skipped for the small fixtures used in tests.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from simulators.common.config import ConfigError

# Repository-relative location of the real portfolio. Callers that read
# PORTFOLIO_CONFIG_PATH from the environment should pass an explicit path
# instead of relying on this.
DEFAULT_PORTFOLIO_PATH = Path("simulators/config/portfolio.yaml")

# Master specification section 5.1.
MIN_PLANTS = 5
MIN_INVERTERS_PER_PLANT = 5
MAX_INVERTERS_PER_PLANT = 10

# How far a plant's summed inverter ratings may sit from its nameplate capacity
# before the file is treated as a typo. Matches the tolerance Member 2's seeder
# warns at, so the two sides agree on what "consistent" means.
CAPACITY_TOLERANCE = 0.25


class PortfolioConfigError(ConfigError):
    """Raised when the portfolio config cannot be trusted.

    A ConfigError subclass so a simulator entrypoint can catch every startup
    configuration fault — environment and portfolio file alike — in one place.
    """


@dataclass(frozen=True)
class Inverter:
    id: str
    name: str
    rated_power_kw: float
    plant_id: str

    @property
    def asset_key(self) -> str:
        """Global asset identity, and the Kafka message key for this inverter."""
        return f"{self.plant_id}:{self.id}"


@dataclass(frozen=True)
class Plant:
    id: str
    name: str
    capacity_kw: float
    timezone: str
    inverters: tuple[Inverter, ...]

    @property
    def inverter_capacity_kw(self) -> float:
        return sum(inverter.rated_power_kw for inverter in self.inverters)


@dataclass(frozen=True)
class Portfolio:
    plants: tuple[Plant, ...]

    @property
    def inverter_count(self) -> int:
        return sum(len(plant.inverters) for plant in self.plants)

    @property
    def capacity_kw(self) -> float:
        return sum(plant.capacity_kw for plant in self.plants)

    def plant(self, plant_id: str) -> Plant:
        for plant in self.plants:
            if plant.id == plant_id:
                return plant
        raise KeyError(f"No plant {plant_id!r} in the portfolio.")

    def inverters(self) -> tuple[Inverter, ...]:
        """Every inverter in the portfolio, in config order."""
        return tuple(inv for plant in self.plants for inv in plant.inverters)


def _required_text(raw: Any, field: str, context: str) -> str:
    value = str(raw or "").strip()
    if not value:
        raise PortfolioConfigError(f"{context}: {field} is required and must be non-empty.")
    return value


def _positive_float(raw: Any, field: str, context: str) -> float:
    # bool is an int subclass, and `rated_power_kw: true` would otherwise parse
    # as 1.0 rather than being reported as the mistake it is.
    if isinstance(raw, bool) or raw is None:
        raise PortfolioConfigError(f"{context}: {field} must be a number, got {raw!r}.")
    try:
        value = float(raw)
    except (TypeError, ValueError) as exc:
        raise PortfolioConfigError(
            f"{context}: {field} must be a number, got {raw!r}."
        ) from exc
    if value != value or value in (float("inf"), float("-inf")):
        raise PortfolioConfigError(f"{context}: {field} must be finite, got {raw!r}.")
    if value <= 0:
        raise PortfolioConfigError(f"{context}: {field} must be greater than zero, got {value}.")
    return value


def _parse_inverter(raw: Any, plant_id: str) -> Inverter:
    context = f"plant {plant_id}"
    if not isinstance(raw, dict):
        raise PortfolioConfigError(
            f"{context}: each inverter must be a mapping, got {type(raw).__name__}."
        )

    inverter_id = _required_text(raw.get("id"), "id", context)
    context = f"plant {plant_id}/{inverter_id}"

    return Inverter(
        id=inverter_id,
        # The config carries no inverter display name; the id is a sensible
        # default and keeps the database's NOT NULL name column satisfied
        # without inventing data.
        name=str(raw.get("name") or inverter_id).strip(),
        rated_power_kw=_positive_float(raw.get("rated_power_kw"), "rated_power_kw", context),
        plant_id=plant_id,
    )


def _parse_plant(raw: Any) -> Plant:
    if not isinstance(raw, dict):
        raise PortfolioConfigError(
            f"Each plant must be a mapping, got {type(raw).__name__}."
        )

    plant_id = _required_text(raw.get("id"), "id", "plant")
    context = f"plant {plant_id}"

    raw_inverters = raw.get("inverters")
    if not isinstance(raw_inverters, list) or not raw_inverters:
        raise PortfolioConfigError(f"{context}: at least one inverter is required.")

    inverters = tuple(_parse_inverter(item, plant_id) for item in raw_inverters)

    seen: set[str] = set()
    for inverter in inverters:
        if inverter.id in seen:
            raise PortfolioConfigError(f"{context}: duplicate inverter id {inverter.id!r}.")
        seen.add(inverter.id)

    plant = Plant(
        id=plant_id,
        name=_required_text(raw.get("name"), "name", context),
        capacity_kw=_positive_float(raw.get("capacity_kw"), "capacity_kw", context),
        timezone=str(raw.get("timezone") or "UTC").strip(),
        inverters=inverters,
    )

    drift = abs(plant.inverter_capacity_kw - plant.capacity_kw) / plant.capacity_kw
    if drift > CAPACITY_TOLERANCE:
        raise PortfolioConfigError(
            f"{context}: inverter ratings sum to {plant.inverter_capacity_kw:g} kW but the "
            f"plant is rated {plant.capacity_kw:g} kW, a {drift:.0%} difference "
            f"(tolerance {CAPACITY_TOLERANCE:.0%}). One of the two is a typo."
        )

    return plant


def _validate_demo_scale(portfolio: Portfolio) -> None:
    """Check the portfolio is big enough for the assessment demo."""
    if len(portfolio.plants) < MIN_PLANTS:
        raise PortfolioConfigError(
            f"The demo portfolio needs at least {MIN_PLANTS} plants, found "
            f"{len(portfolio.plants)}."
        )
    for plant in portfolio.plants:
        count = len(plant.inverters)
        if not MIN_INVERTERS_PER_PLANT <= count <= MAX_INVERTERS_PER_PLANT:
            raise PortfolioConfigError(
                f"plant {plant.id}: the specification requires "
                f"{MIN_INVERTERS_PER_PLANT}-{MAX_INVERTERS_PER_PLANT} inverters per plant, "
                f"found {count}."
            )


def parse_portfolio(document: Any, *, require_demo_scale: bool = False) -> Portfolio:
    """Validate an already-loaded YAML document into a Portfolio."""
    if not isinstance(document, dict):
        raise PortfolioConfigError(
            "Portfolio config must be a mapping with a top-level 'plants' key."
        )

    raw_plants = document.get("plants")
    if not isinstance(raw_plants, list) or not raw_plants:
        raise PortfolioConfigError("Portfolio config needs a non-empty 'plants' list.")

    plants = tuple(_parse_plant(item) for item in raw_plants)

    seen: set[str] = set()
    for plant in plants:
        if plant.id in seen:
            raise PortfolioConfigError(f"Duplicate plant id {plant.id!r}.")
        seen.add(plant.id)

    portfolio = Portfolio(plants=plants)
    if require_demo_scale:
        _validate_demo_scale(portfolio)
    return portfolio


def load_portfolio(
    path: str | Path | None = None,
    *,
    require_demo_scale: bool = True,
) -> Portfolio:
    """Read and validate the portfolio config from disk.

    Demo-scale rules are enforced by default: the usual caller is the simulator
    loading the real portfolio, and a short portfolio should fail at startup
    rather than halfway through a demonstration.
    """
    config_path = Path(path) if path is not None else DEFAULT_PORTFOLIO_PATH

    try:
        raw = config_path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise PortfolioConfigError(
            f"Portfolio config not found at {config_path}. This file is owned by "
            f"Member 1 (simulators/config/portfolio.yaml); set PORTFOLIO_CONFIG_PATH "
            f"to point somewhere else."
        ) from exc
    except OSError as exc:
        raise PortfolioConfigError(f"Cannot read portfolio config at {config_path}: {exc}.") from exc

    try:
        document = yaml.safe_load(raw)
    except yaml.YAMLError as exc:
        raise PortfolioConfigError(f"Portfolio config at {config_path} is not valid YAML: {exc}.") from exc

    try:
        return parse_portfolio(document, require_demo_scale=require_demo_scale)
    except PortfolioConfigError as exc:
        # Re-raise with the file named, so the message identifies which config is
        # wrong when several are in play.
        raise PortfolioConfigError(f"{config_path}: {exc}") from exc
