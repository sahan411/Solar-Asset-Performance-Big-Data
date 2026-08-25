"""Tests for the portfolio config loader.

Two things are under test and they are worth keeping separate:

  * the parser rejects malformed config — the reason it exists;
  * the real `simulators/config/portfolio.yaml` satisfies the demo contract, so
    the file the assessment runs against is checked in CI rather than by eye.
"""

from __future__ import annotations

import copy
from pathlib import Path

import pytest
import yaml

from simulators.common.portfolio import (
    CAPACITY_TOLERANCE,
    DEFAULT_PORTFOLIO_PATH,
    MAX_INVERTERS_PER_PLANT,
    MIN_INVERTERS_PER_PLANT,
    MIN_PLANTS,
    PortfolioConfigError,
    load_portfolio,
    parse_portfolio,
)

REPO_ROOT = Path(__file__).resolve().parents[2]


def minimal_document() -> dict:
    """A structurally valid two-plant config, below demo scale on purpose.

    Structural rules and demo-scale rules are enforced independently, so the
    fixture used to test the former must not accidentally satisfy the latter.
    """
    return {
        "plants": [
            {
                "id": "PLANT_01",
                "name": "Test Plant One",
                "capacity_kw": 2000,
                "inverters": [
                    {"id": "INV_01", "rated_power_kw": 1000},
                    {"id": "INV_02", "rated_power_kw": 1000},
                ],
            },
            {
                "id": "PLANT_02",
                "name": "Test Plant Two",
                "capacity_kw": 1500,
                "timezone": "UTC",
                "inverters": [
                    {"id": "INV_01", "name": "Named Inverter", "rated_power_kw": 750},
                    {"id": "INV_02", "rated_power_kw": 750},
                ],
            },
        ]
    }


def document_without(**overrides) -> dict:
    """minimal_document() with the first plant's fields overridden or removed.

    A value of None removes the key, so both "missing" and "wrong" can be tested
    through one helper.
    """
    document = minimal_document()
    plant = document["plants"][0]
    for field, value in overrides.items():
        if value is None:
            plant.pop(field, None)
        else:
            plant[field] = value
    return document


class TestParsing:
    def test_parses_plants_and_inverters(self):
        portfolio = parse_portfolio(minimal_document())

        assert len(portfolio.plants) == 2
        assert portfolio.inverter_count == 4
        assert portfolio.capacity_kw == 3500

        plant = portfolio.plant("PLANT_01")
        assert plant.name == "Test Plant One"
        assert plant.capacity_kw == 2000
        assert plant.inverter_capacity_kw == 2000
        assert [inv.id for inv in plant.inverters] == ["INV_01", "INV_02"]

    def test_timezone_defaults_to_utc(self):
        portfolio = parse_portfolio(document_without(timezone=None))
        assert portfolio.plant("PLANT_01").timezone == "UTC"

    def test_inverter_name_defaults_to_its_id(self):
        portfolio = parse_portfolio(minimal_document())

        # PLANT_01/INV_01 carries no name; PLANT_02/INV_01 does.
        assert portfolio.plant("PLANT_01").inverters[0].name == "INV_01"
        assert portfolio.plant("PLANT_02").inverters[0].name == "Named Inverter"

    def test_asset_key_is_the_kafka_message_key(self):
        portfolio = parse_portfolio(minimal_document())
        assert portfolio.plant("PLANT_02").inverters[1].asset_key == "PLANT_02:INV_02"

    def test_inverters_returns_every_inverter_in_config_order(self):
        portfolio = parse_portfolio(minimal_document())
        assert [inv.asset_key for inv in portfolio.inverters()] == [
            "PLANT_01:INV_01",
            "PLANT_01:INV_02",
            "PLANT_02:INV_01",
            "PLANT_02:INV_02",
        ]

    def test_unknown_plant_id_raises_key_error(self):
        portfolio = parse_portfolio(minimal_document())
        with pytest.raises(KeyError):
            portfolio.plant("PLANT_99")


class TestDuplicateIdentity:
    def test_duplicate_plant_id_fails(self):
        document = minimal_document()
        document["plants"][1]["id"] = "PLANT_01"

        with pytest.raises(PortfolioConfigError, match="Duplicate plant id"):
            parse_portfolio(document)

    def test_duplicate_inverter_id_within_a_plant_fails(self):
        document = minimal_document()
        document["plants"][0]["inverters"][1]["id"] = "INV_01"

        with pytest.raises(PortfolioConfigError, match="duplicate inverter id"):
            parse_portfolio(document)

    def test_same_inverter_id_in_different_plants_is_allowed(self):
        # Identity is (plant_id, inverter_id), so every plant may have an INV_01.
        portfolio = parse_portfolio(minimal_document())
        assert portfolio.plant("PLANT_01").inverters[0].id == "INV_01"
        assert portfolio.plant("PLANT_02").inverters[0].id == "INV_01"


class TestInvalidCapacities:
    @pytest.mark.parametrize("bad", [0, -1, -0.5])
    def test_non_positive_plant_capacity_fails(self, bad):
        with pytest.raises(PortfolioConfigError, match="capacity_kw must be greater than zero"):
            parse_portfolio(document_without(capacity_kw=bad))

    @pytest.mark.parametrize("bad", ["", "abc", None, True, [], {}])
    def test_non_numeric_plant_capacity_fails(self, bad):
        with pytest.raises(PortfolioConfigError, match="capacity_kw must be"):
            parse_portfolio(document_without(capacity_kw=bad))

    def test_non_positive_inverter_rating_fails(self):
        document = minimal_document()
        document["plants"][0]["inverters"][0]["rated_power_kw"] = 0

        with pytest.raises(
            PortfolioConfigError, match="rated_power_kw must be greater than zero"
        ):
            parse_portfolio(document)

    def test_boolean_rating_is_rejected_rather_than_read_as_one(self):
        # bool is a subclass of int, so `rated_power_kw: true` would otherwise
        # silently parse as a 1 kW inverter.
        document = minimal_document()
        document["plants"][0]["inverters"][0]["rated_power_kw"] = True

        with pytest.raises(PortfolioConfigError, match="rated_power_kw must be a number"):
            parse_portfolio(document)

    def test_infinite_capacity_fails(self):
        with pytest.raises(PortfolioConfigError, match="must be finite"):
            parse_portfolio(document_without(capacity_kw=float("inf")))


class TestCapacityConsistency:
    def test_inverter_sum_far_below_nameplate_fails(self):
        document = minimal_document()
        document["plants"][0]["capacity_kw"] = 10_000  # inverters sum to 2000

        with pytest.raises(PortfolioConfigError, match="One of the two is a typo"):
            parse_portfolio(document)

    def test_inverter_sum_far_above_nameplate_fails(self):
        document = minimal_document()
        document["plants"][0]["capacity_kw"] = 200  # inverters sum to 2000

        with pytest.raises(PortfolioConfigError, match="One of the two is a typo"):
            parse_portfolio(document)

    def test_drift_inside_the_tolerance_is_accepted(self):
        document = minimal_document()
        # Inverters sum to 2000; nudge nameplate to just inside the tolerance.
        document["plants"][0]["capacity_kw"] = 2000 * (1 + CAPACITY_TOLERANCE - 0.01)

        portfolio = parse_portfolio(document)
        assert portfolio.plant("PLANT_01").inverter_capacity_kw == 2000


class TestMissingAndMalformedStructure:
    @pytest.mark.parametrize("document", [None, [], "plants", 42])
    def test_document_must_be_a_mapping(self, document):
        with pytest.raises(PortfolioConfigError, match="must be a mapping"):
            parse_portfolio(document)

    @pytest.mark.parametrize("plants", [None, [], {}, "PLANT_01"])
    def test_plants_must_be_a_non_empty_list(self, plants):
        with pytest.raises(PortfolioConfigError, match="non-empty 'plants' list"):
            parse_portfolio({"plants": plants})

    def test_plant_must_be_a_mapping(self):
        with pytest.raises(PortfolioConfigError, match="Each plant must be a mapping"):
            parse_portfolio({"plants": ["PLANT_01"]})

    def test_missing_plant_id_fails(self):
        with pytest.raises(PortfolioConfigError, match="id is required"):
            parse_portfolio(document_without(id=None))

    def test_missing_plant_name_fails(self):
        with pytest.raises(PortfolioConfigError, match="name is required"):
            parse_portfolio(document_without(name=None))

    def test_missing_inverter_id_fails(self):
        document = minimal_document()
        document["plants"][0]["inverters"][0].pop("id")

        with pytest.raises(PortfolioConfigError, match="id is required"):
            parse_portfolio(document)

    @pytest.mark.parametrize("inverters", [None, [], "INV_01", {}])
    def test_plant_without_inverters_fails(self, inverters):
        with pytest.raises(PortfolioConfigError, match="at least one inverter is required"):
            parse_portfolio(document_without(inverters=inverters))

    def test_inverter_must_be_a_mapping(self):
        with pytest.raises(PortfolioConfigError, match="each inverter must be a mapping"):
            parse_portfolio(document_without(inverters=["INV_01"]))


class TestDemoScale:
    def test_too_few_plants_fails_when_demo_scale_is_required(self):
        with pytest.raises(PortfolioConfigError, match=f"at least {MIN_PLANTS} plants"):
            parse_portfolio(minimal_document(), require_demo_scale=True)

    def test_too_few_plants_is_accepted_when_it_is_not(self):
        assert len(parse_portfolio(minimal_document()).plants) == 2

    def test_too_few_inverters_per_plant_fails(self):
        document = {
            "plants": [
                {
                    "id": f"PLANT_{n:02d}",
                    "name": f"Plant {n}",
                    "capacity_kw": 1000,
                    "inverters": [{"id": "INV_01", "rated_power_kw": 1000}],
                }
                for n in range(1, MIN_PLANTS + 1)
            ]
        }

        with pytest.raises(PortfolioConfigError, match="inverters per plant"):
            parse_portfolio(document, require_demo_scale=True)

    def test_too_many_inverters_per_plant_fails(self):
        excess = MAX_INVERTERS_PER_PLANT + 1
        document = {
            "plants": [
                {
                    "id": f"PLANT_{n:02d}",
                    "name": f"Plant {n}",
                    "capacity_kw": 100 * excess,
                    "inverters": [
                        {"id": f"INV_{i:02d}", "rated_power_kw": 100}
                        for i in range(1, excess + 1)
                    ],
                }
                for n in range(1, MIN_PLANTS + 1)
            ]
        }

        with pytest.raises(PortfolioConfigError, match="inverters per plant"):
            parse_portfolio(document, require_demo_scale=True)


class TestLoadingFromDisk:
    def test_loads_a_written_file(self, tmp_path):
        path = tmp_path / "portfolio.yaml"
        path.write_text(yaml.safe_dump(minimal_document()), encoding="utf-8")

        portfolio = load_portfolio(path, require_demo_scale=False)
        assert portfolio.inverter_count == 4

    def test_missing_file_names_the_owner_and_the_override(self, tmp_path):
        with pytest.raises(PortfolioConfigError, match="PORTFOLIO_CONFIG_PATH"):
            load_portfolio(tmp_path / "absent.yaml")

    def test_invalid_yaml_is_reported_as_such(self, tmp_path):
        path = tmp_path / "portfolio.yaml"
        path.write_text("plants: [\n  - id: PLANT_01\n", encoding="utf-8")

        with pytest.raises(PortfolioConfigError, match="not valid YAML"):
            load_portfolio(path, require_demo_scale=False)

    def test_validation_failure_names_the_file(self, tmp_path):
        path = tmp_path / "broken.yaml"
        document = minimal_document()
        document["plants"][1]["id"] = "PLANT_01"
        path.write_text(yaml.safe_dump(document), encoding="utf-8")

        with pytest.raises(PortfolioConfigError, match="broken.yaml"):
            load_portfolio(path, require_demo_scale=False)


class TestTheRealPortfolio:
    """The checked-in portfolio must satisfy the contract the demo depends on."""

    @pytest.fixture(scope="class")
    def portfolio(self):
        return load_portfolio(REPO_ROOT / DEFAULT_PORTFOLIO_PATH)

    def test_it_loads_at_demo_scale(self, portfolio):
        assert len(portfolio.plants) == MIN_PLANTS
        for plant in portfolio.plants:
            assert MIN_INVERTERS_PER_PLANT <= len(plant.inverters) <= MAX_INVERTERS_PER_PLANT

    def test_plant_ids_are_the_expected_sequence(self, portfolio):
        assert [plant.id for plant in portfolio.plants] == [
            "PLANT_01",
            "PLANT_02",
            "PLANT_03",
            "PLANT_04",
            "PLANT_05",
        ]

    def test_every_asset_key_is_unique(self, portfolio):
        keys = [inverter.asset_key for inverter in portfolio.inverters()]
        assert len(keys) == len(set(keys))

    def test_inverter_ratings_sum_exactly_to_nameplate_capacity(self, portfolio):
        # Stricter than the parser's tolerance: the real file is built to balance
        # exactly, so any drift at all means someone edited one side only.
        for plant in portfolio.plants:
            assert plant.inverter_capacity_kw == plant.capacity_kw, plant.id

    def test_demo_anomaly_targets_exist(self, portfolio):
        # docs/data-contracts.md section 7. Removing any of these breaks the
        # assessment timeline, so the failure belongs here and not in the demo.
        assert portfolio.plant("PLANT_03").inverters  # underperformance site
        assert any(inv.id == "INV_02" for inv in portfolio.plant("PLANT_03").inverters)
        assert any(inv.id == "INV_01" for inv in portfolio.plant("PLANT_04").inverters)
        assert portfolio.plant("PLANT_05")  # telemetry-gap site

    def test_plant_names_are_unique(self, portfolio):
        names = [plant.name for plant in portfolio.plants]
        assert len(names) == len(set(names))

    def test_it_is_parsed_from_yaml_not_mutated_in_place(self, portfolio):
        # Guards the frozen dataclasses: a caller must not be able to edit shared
        # asset identity at runtime.
        with pytest.raises(Exception):
            portfolio.plants[0].capacity_kw = 1  # type: ignore[misc]

    def test_reloading_yields_an_equal_portfolio(self, portfolio):
        again = load_portfolio(REPO_ROOT / DEFAULT_PORTFOLIO_PATH)
        assert again == portfolio
        assert copy.deepcopy(portfolio) == portfolio
