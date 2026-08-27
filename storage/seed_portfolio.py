"""Seed the plant/inverter registry from the shared portfolio configuration.

The portfolio definition (5 plants, 5-10 inverters each) is owned by Member 1 and
lives at `simulators/config/portfolio.yaml`. This module is the single place that
turns it into rows in `plants` and `inverters`, so the simulator and the serving
store can never disagree about which assets exist.

Parsing/validation is deliberately separate from the database write so the rules
can be unit-tested without a running PostgreSQL.

Usage (from the repository root):

    python -m storage.seed_portfolio
    python -m storage.seed_portfolio --config path/to/portfolio.yaml --dry-run
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from processing.common.config import BatchSettings, ConfigError, DatabaseSettings
from processing.common.db import connect, execute_batch
from processing.common.logging import get_logger

log = get_logger("storage-seed")

# A plant whose inverters sum to wildly less/more than its nameplate capacity is
# probably a config typo, but it is Member 1's file to own — so warn, never fail.
_CAPACITY_TOLERANCE = 0.25


@dataclass(frozen=True)
class InverterSpec:
    id: str
    name: str
    rated_power_kw: float


@dataclass(frozen=True)
class PlantSpec:
    id: str
    name: str
    capacity_kw: float
    timezone: str
    inverters: tuple[InverterSpec, ...]

    @property
    def inverter_capacity_kw(self) -> float:
        return sum(inv.rated_power_kw for inv in self.inverters)


@dataclass(frozen=True)
class Portfolio:
    plants: tuple[PlantSpec, ...]

    @property
    def inverter_count(self) -> int:
        return sum(len(plant.inverters) for plant in self.plants)


def _positive_float(value: Any, field: str, context: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"{context}: {field} must be a number, got {value!r}.") from exc
    if number <= 0:
        raise ConfigError(f"{context}: {field} must be greater than zero, got {number}.")
    return number


def _parse_inverter(raw: Any, plant_id: str) -> InverterSpec:
    context = f"plant {plant_id}"
    if not isinstance(raw, dict):
        raise ConfigError(f"{context}: each inverter must be a mapping, got {type(raw).__name__}.")

    inverter_id = str(raw.get("id") or "").strip()
    if not inverter_id:
        raise ConfigError(f"{context}: every inverter needs a non-empty id.")

    return InverterSpec(
        id=inverter_id,
        # The shared config carries no inverter display name; the id is a fine
        # default and keeps `inverters.name` NOT NULL without inventing data.
        name=str(raw.get("name") or inverter_id).strip(),
        rated_power_kw=_positive_float(
            raw.get("rated_power_kw"), "rated_power_kw", f"{context}/{inverter_id}"
        ),
    )


def _parse_plant(raw: Any) -> PlantSpec:
    if not isinstance(raw, dict):
        raise ConfigError(f"Each plant must be a mapping, got {type(raw).__name__}.")

    plant_id = str(raw.get("id") or "").strip()
    if not plant_id:
        raise ConfigError("Every plant needs a non-empty id.")

    raw_inverters = raw.get("inverters") or []
    if not isinstance(raw_inverters, list) or not raw_inverters:
        raise ConfigError(f"plant {plant_id}: at least one inverter is required.")

    inverters = tuple(_parse_inverter(item, plant_id) for item in raw_inverters)

    seen: set[str] = set()
    for inverter in inverters:
        if inverter.id in seen:
            raise ConfigError(f"plant {plant_id}: duplicate inverter id {inverter.id!r}.")
        seen.add(inverter.id)

    return PlantSpec(
        id=plant_id,
        name=str(raw.get("name") or plant_id).strip(),
        capacity_kw=_positive_float(raw.get("capacity_kw"), "capacity_kw", f"plant {plant_id}"),
        timezone=str(raw.get("timezone") or "UTC").strip(),
        inverters=inverters,
    )


def parse_portfolio(document: Any) -> Portfolio:
    """Validate an already-loaded YAML document into a Portfolio."""
    if not isinstance(document, dict):
        raise ConfigError("Portfolio config must be a mapping with a top-level 'plants' key.")

    raw_plants = document.get("plants")
    if not isinstance(raw_plants, list) or not raw_plants:
        raise ConfigError("Portfolio config must define a non-empty 'plants' list.")

    plants = tuple(_parse_plant(item) for item in raw_plants)

    seen: set[str] = set()
    for plant in plants:
        if plant.id in seen:
            raise ConfigError(f"Duplicate plant id {plant.id!r} in portfolio config.")
        seen.add(plant.id)

        # Soft consistency check only: this is Member 1's file to tune.
        ratio = plant.inverter_capacity_kw / plant.capacity_kw
        if abs(ratio - 1.0) > _CAPACITY_TOLERANCE:
            log.warning(
                "portfolio_capacity_mismatch",
                f"Plant {plant.id} inverter ratings sum to "
                f"{plant.inverter_capacity_kw:.0f} kW against a nameplate of "
                f"{plant.capacity_kw:.0f} kW",
                plant_id=plant.id,
                inverter_capacity_kw=plant.inverter_capacity_kw,
                plant_capacity_kw=plant.capacity_kw,
            )

    return Portfolio(plants=plants)


def load_portfolio(path: str | Path) -> Portfolio:
    """Read and validate the shared portfolio configuration file."""
    config_path = Path(path)
    if not config_path.is_file():
        raise ConfigError(
            f"Portfolio config not found at {config_path}. This file is produced by "
            "Member 1 (simulators/config/portfolio.yaml); set PORTFOLIO_CONFIG_PATH "
            "to override the location."
        )
    with config_path.open("r", encoding="utf-8") as handle:
        return parse_portfolio(yaml.safe_load(handle))


# Upserts rather than plain inserts: re-seeding after a demo reset must converge
# on the config's current values instead of failing on the primary key.
_UPSERT_PLANT = """
INSERT INTO plants (id, name, capacity_kw, timezone, active)
VALUES (%s, %s, %s, %s, TRUE)
ON CONFLICT (id) DO UPDATE SET
    name        = EXCLUDED.name,
    capacity_kw = EXCLUDED.capacity_kw,
    timezone    = EXCLUDED.timezone,
    active      = TRUE
"""

_UPSERT_INVERTER = """
INSERT INTO inverters (id, plant_id, name, rated_power_kw, active)
VALUES (%s, %s, %s, %s, TRUE)
ON CONFLICT (plant_id, id) DO UPDATE SET
    name           = EXCLUDED.name,
    rated_power_kw = EXCLUDED.rated_power_kw,
    active         = TRUE
"""


def seed_portfolio(conn, portfolio: Portfolio) -> tuple[int, int]:
    """Upsert the portfolio into `plants` and `inverters`. Returns (plants, inverters)."""
    plant_rows = [
        (plant.id, plant.name, plant.capacity_kw, plant.timezone) for plant in portfolio.plants
    ]
    # Plants first: inverters carry a foreign key onto them.
    execute_batch(conn, _UPSERT_PLANT, plant_rows)

    inverter_rows = [
        (inverter.id, plant.id, inverter.name, inverter.rated_power_kw)
        for plant in portfolio.plants
        for inverter in plant.inverters
    ]
    execute_batch(conn, _UPSERT_INVERTER, inverter_rows)

    return len(plant_rows), len(inverter_rows)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Seed the SolarIQ plant/inverter registry.")
    parser.add_argument("--config", help="Path to portfolio.yaml (defaults to PORTFOLIO_CONFIG_PATH).")
    parser.add_argument(
        "--dry-run", action="store_true", help="Validate the config without writing to the database."
    )
    args = parser.parse_args(argv)

    try:
        config_path = args.config or BatchSettings.from_env().portfolio_config_path
        portfolio = load_portfolio(config_path)

        if args.dry_run:
            log.info(
                "portfolio_validated",
                f"Portfolio config is valid: {len(portfolio.plants)} plants, "
                f"{portfolio.inverter_count} inverters",
                plants=len(portfolio.plants),
                inverters=portfolio.inverter_count,
            )
            return 0

        with connect(DatabaseSettings.from_env().url) as conn:
            plants, inverters = seed_portfolio(conn, portfolio)

        log.info(
            "portfolio_seeded",
            f"Seeded {plants} plants and {inverters} inverters",
            plants=plants,
            inverters=inverters,
        )
    except ConfigError as exc:
        log.error("seed_config_error", str(exc))
        return 2
    except Exception as exc:  # noqa: BLE001 - top-level CLI boundary
        log.exception("seed_failed", f"Portfolio seed failed: {exc}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
