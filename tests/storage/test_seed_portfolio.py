"""Validation rules for the shared portfolio configuration.

These run without a database: parsing and validation are separated from the
upsert precisely so the rules can be checked cheaply.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from processing.common.config import ConfigError
from storage.seed_portfolio import load_portfolio, parse_portfolio

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "portfolio.test.yaml"


def test_loads_valid_portfolio_from_file():
    portfolio = load_portfolio(FIXTURE)

    assert [plant.id for plant in portfolio.plants] == ["PLANT_01", "PLANT_02"]
    assert portfolio.inverter_count == 6

    north = portfolio.plants[0]
    assert north.name == "North Ridge Solar"
    assert north.capacity_kw == 4000
    assert north.timezone == "UTC"
    assert north.inverter_capacity_kw == 4000


def test_inverter_name_defaults_to_its_id_but_explicit_names_win():
    portfolio = load_portfolio(FIXTURE)
    east = portfolio.plants[1]

    # The shared config usually omits inverter names.
    assert east.inverters[0].name == "East Field Inverter A"
    assert east.inverters[1].name == "INV_02"


def test_timezone_defaults_to_utc_when_absent():
    portfolio = parse_portfolio(
        {"plants": [{"id": "P1", "capacity_kw": 10, "inverters": [{"id": "I1", "rated_power_kw": 10}]}]}
    )
    assert portfolio.plants[0].timezone == "UTC"


def test_plant_name_defaults_to_its_id_when_absent():
    portfolio = parse_portfolio(
        {"plants": [{"id": "P1", "capacity_kw": 10, "inverters": [{"id": "I1", "rated_power_kw": 10}]}]}
    )
    assert portfolio.plants[0].name == "P1"


def test_rejects_duplicate_plant_ids():
    document = {
        "plants": [
            {"id": "PLANT_01", "capacity_kw": 100, "inverters": [{"id": "I1", "rated_power_kw": 100}]},
            {"id": "PLANT_01", "capacity_kw": 200, "inverters": [{"id": "I1", "rated_power_kw": 200}]},
        ]
    }
    with pytest.raises(ConfigError, match="Duplicate plant id"):
        parse_portfolio(document)


def test_rejects_duplicate_inverter_ids_within_a_plant():
    document = {
        "plants": [
            {
                "id": "PLANT_01",
                "capacity_kw": 200,
                "inverters": [
                    {"id": "INV_01", "rated_power_kw": 100},
                    {"id": "INV_01", "rated_power_kw": 100},
                ],
            }
        ]
    }
    with pytest.raises(ConfigError, match="duplicate inverter id"):
        parse_portfolio(document)


def test_allows_the_same_inverter_id_on_different_plants():
    """Inverter identity is (plant_id, inverter_id), matching the Kafka key."""
    document = {
        "plants": [
            {"id": "PLANT_01", "capacity_kw": 100, "inverters": [{"id": "INV_01", "rated_power_kw": 100}]},
            {"id": "PLANT_02", "capacity_kw": 100, "inverters": [{"id": "INV_01", "rated_power_kw": 100}]},
        ]
    }
    assert parse_portfolio(document).inverter_count == 2


@pytest.mark.parametrize("bad_capacity", [0, -1, "abc", None])
def test_rejects_non_positive_or_non_numeric_plant_capacity(bad_capacity):
    document = {
        "plants": [
            {"id": "P1", "capacity_kw": bad_capacity, "inverters": [{"id": "I1", "rated_power_kw": 10}]}
        ]
    }
    with pytest.raises(ConfigError, match="capacity_kw"):
        parse_portfolio(document)


@pytest.mark.parametrize("bad_rating", [0, -5, "x", None])
def test_rejects_non_positive_or_non_numeric_inverter_rating(bad_rating):
    document = {
        "plants": [{"id": "P1", "capacity_kw": 10, "inverters": [{"id": "I1", "rated_power_kw": bad_rating}]}]
    }
    with pytest.raises(ConfigError, match="rated_power_kw"):
        parse_portfolio(document)


def test_rejects_plant_without_inverters():
    with pytest.raises(ConfigError, match="at least one inverter"):
        parse_portfolio({"plants": [{"id": "P1", "capacity_kw": 10, "inverters": []}]})


def test_rejects_missing_or_blank_ids():
    with pytest.raises(ConfigError, match="plant needs a non-empty id"):
        parse_portfolio({"plants": [{"capacity_kw": 10, "inverters": [{"id": "I1", "rated_power_kw": 10}]}]})

    with pytest.raises(ConfigError, match="inverter needs a non-empty id"):
        parse_portfolio(
            {"plants": [{"id": "P1", "capacity_kw": 10, "inverters": [{"id": "  ", "rated_power_kw": 10}]}]}
        )


def test_rejects_empty_or_malformed_documents():
    for document in ({}, {"plants": []}, {"plants": "not-a-list"}, []):
        with pytest.raises(ConfigError):
            parse_portfolio(document)


def test_capacity_mismatch_warns_but_does_not_fail(caplog):
    """The portfolio file belongs to Member 1 — flag oddities, never reject them."""
    document = {
        "plants": [
            {"id": "P1", "capacity_kw": 5000, "inverters": [{"id": "I1", "rated_power_kw": 100}]}
        ]
    }
    portfolio = parse_portfolio(document)
    assert portfolio.plants[0].capacity_kw == 5000


def test_missing_config_file_names_the_owning_member():
    with pytest.raises(ConfigError, match="Member 1"):
        load_portfolio("does/not/exist/portfolio.yaml")
