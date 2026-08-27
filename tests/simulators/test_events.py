"""Tests for telemetry event construction and source-side validation.

The contract these pin down is the one the rest of the team consumes, so the
tests are written from the consumer's side: does the event match the frozen
schema, is the Kafka key right, and is anything Member 2 would reject stopped
here first.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from simulators.common.config import SimulationSettings
from simulators.common.portfolio import Inverter, load_portfolio
from simulators.common.time import SimulationClock
from simulators.streaming.events import (
    ALLOWED_STATUSES,
    REASON_AVAILABILITY_OUT_OF_RANGE,
    REASON_INVALID_STATUS,
    REASON_IRRADIANCE_OUT_OF_RANGE,
    REASON_NEGATIVE_ACTIVE_POWER,
    REASON_NEGATIVE_ENERGY,
    REASON_POWER_EXCEEDS_RATING,
    REASON_SCHEMA_VIOLATION,
    REASON_STATUS_INCONSISTENT,
    REASON_UNKNOWN_ASSET,
    STATUS_OFFLINE,
    STATUS_ONLINE,
    STATUS_WARNING,
    TELEMETRY_FIELDS,
    EventValidationError,
    build_event,
    corrupt_event,
    deterministic_event_id,
    is_valid,
    load_schema,
    to_quarantine_record,
    validate_event,
    validate_payload,
)
from simulators.streaming.generation import EnergyLedger, InverterReading, generate_reading

SEED = 8203
REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA = REPO_ROOT / "contracts/telemetry.schema.json"

# Member 2's required fields and allowed statuses, from
# processing/streaming/schema.py. Duplicated so a divergence fails here.
MEMBER_2_REQUIRED_FIELDS = ("event_id", "plant_id", "inverter_id", "timestamp")
MEMBER_2_ALLOWED_STATUSES = ("ONLINE", "OFFLINE", "WARNING")
MEMBER_2_MAX_IRRADIANCE = 1500.0


def settings() -> SimulationSettings:
    return SimulationSettings(
        day_seconds=300.0,
        telemetry_interval_seconds=3.0,
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


@pytest.fixture
def event(clock, inverter):
    """A well-formed midday event."""
    instant = clock.instant_at(150.0)
    reading = generate_reading(inverter, instant, 50, seed=SEED)
    return build_event(inverter, instant, reading, 2150.2, seed=SEED, tick_index=50)


def validated(payload, **kwargs):
    """Validate against the repository's schema, returning the raised error."""
    kwargs.setdefault("schema_path", SCHEMA)
    with pytest.raises(EventValidationError) as excinfo:
        validate_payload(payload, **kwargs)
    return excinfo.value


class TestSchemaDocument:
    def test_the_schema_is_itself_valid(self):
        from jsonschema import Draft202012Validator

        Draft202012Validator.check_schema(load_schema(SCHEMA))

    def test_it_requires_exactly_the_contract_fields(self):
        schema = load_schema(SCHEMA)
        assert tuple(schema["required"]) == TELEMETRY_FIELDS
        assert tuple(schema["properties"]) == TELEMETRY_FIELDS

    def test_unknown_fields_are_rejected(self, event):
        payload = event.to_payload()
        payload["rogue_field"] = 1

        assert validated(payload).reason == REASON_SCHEMA_VIOLATION

    def test_statuses_agree_with_member_2(self):
        schema = load_schema(SCHEMA)
        assert tuple(schema["properties"]["status"]["enum"]) == MEMBER_2_ALLOWED_STATUSES
        assert ALLOWED_STATUSES == MEMBER_2_ALLOWED_STATUSES

    def test_irradiance_ceiling_agrees_with_member_2(self):
        # If his MAX_PLAUSIBLE_IRRADIANCE_WM2 moves and this does not, the source
        # would emit readings his validator quarantines.
        schema = load_schema(SCHEMA)
        assert schema["properties"]["irradiance_wm2"]["maximum"] == MEMBER_2_MAX_IRRADIANCE

    def test_every_field_member_2_requires_is_required_here(self):
        schema = load_schema(SCHEMA)
        assert set(MEMBER_2_REQUIRED_FIELDS) <= set(schema["required"])


class TestBuildEvent:
    def test_it_produces_every_contract_field(self, event):
        assert tuple(event.to_payload()) == TELEMETRY_FIELDS

    def test_a_built_event_validates(self, event, inverter):
        validate_event(event, inverter=inverter, schema_path=SCHEMA)

    def test_identity_comes_from_the_inverter(self, event):
        assert event.plant_id == "PLANT_01"
        assert event.inverter_id == "INV_01"

    def test_the_kafka_key_is_plant_colon_inverter(self, event):
        assert event.kafka_key == "PLANT_01:INV_01"

    def test_the_timestamp_is_simulated_event_time(self, event):
        # 150 real seconds into a 300-second day is simulated noon, not now.
        assert event.timestamp == "2026-08-21T12:00:00Z"

    def test_defaults_are_a_healthy_online_inverter(self, event):
        assert event.status == STATUS_ONLINE
        assert event.availability == 1.0
        assert event.simulator_scenario is None

    def test_readings_are_rounded_for_readability(self, event):
        payload = event.to_payload()
        for field in ("active_power_kw", "energy_today_kwh"):
            assert round(payload[field], 3) == payload[field]
        for field in ("irradiance_wm2", "module_temp_c", "inverter_temp_c"):
            assert round(payload[field], 2) == payload[field]

    def test_anomaly_fields_can_be_supplied(self, clock, inverter):
        instant = clock.instant_at(200.0)
        healthy = generate_reading(inverter, instant, 66, seed=SEED)
        # Milestone 7 will zero the power before the event is built; the builder
        # carries what it is given and does not invent the fault itself.
        downed = InverterReading(
            active_power_kw=0.0,
            irradiance_wm2=healthy.irradiance_wm2,
            module_temp_c=healthy.module_temp_c,
            inverter_temp_c=healthy.inverter_temp_c,
        )
        offline = build_event(
            inverter,
            instant,
            downed,
            500.0,
            seed=SEED,
            tick_index=66,
            status=STATUS_OFFLINE,
            availability=0.0,
            scenario="INV_OFFLINE",
        )

        validate_event(offline, inverter=inverter, schema_path=SCHEMA)
        assert offline.simulator_scenario == "INV_OFFLINE"
        assert offline.active_power_kw == 0.0
        # Irradiance is unaffected: the sun still shines on a dead inverter, and
        # that contrast is what makes the fault detectable downstream.
        assert offline.irradiance_wm2 > 0

    def test_json_round_trips(self, event):
        assert json.loads(event.to_json()) == event.to_payload()

    def test_json_is_compact(self, event):
        # 35 events every 3 seconds; whitespace is pure overhead on the wire.
        assert ", " not in event.to_json()


class TestEventIds:
    def test_ids_are_reproducible(self):
        assert deterministic_event_id(SEED, "PLANT_01", "INV_01", 50) == (
            deterministic_event_id(SEED, "PLANT_01", "INV_01", 50)
        )

    def test_ids_are_valid_uuids(self):
        import uuid

        uuid.UUID(deterministic_event_id(SEED, "PLANT_01", "INV_01", 50))

    @pytest.mark.parametrize(
        "args",
        [
            (SEED + 1, "PLANT_01", "INV_01", 50),
            (SEED, "PLANT_02", "INV_01", 50),
            (SEED, "PLANT_01", "INV_02", 50),
            (SEED, "PLANT_01", "INV_01", 51),
        ],
    )
    def test_changing_any_coordinate_changes_the_id(self, args):
        assert deterministic_event_id(*args) != deterministic_event_id(
            SEED, "PLANT_01", "INV_01", 50
        )

    def test_a_full_day_across_the_portfolio_has_no_collisions(self, portfolio):
        # Member 2 de-duplicates on event_id, so a collision would silently
        # discard a real reading.
        ids = [
            deterministic_event_id(SEED, inv.plant_id, inv.id, tick)
            for inv in portfolio.inverters()
            for tick in range(100)
        ]
        assert len(ids) == len(set(ids)) == 3500

    def test_ids_stay_unique_across_simulated_days(self, portfolio):
        # tick_index is global to the run rather than reset each day, so day two
        # cannot reuse day one's coordinates.
        ids = {
            deterministic_event_id(SEED, inv.plant_id, inv.id, tick)
            for inv in portfolio.inverters()
            for tick in range(300)
        }
        assert len(ids) == len(portfolio.inverters()) * 300

    def test_an_explicit_id_overrides_the_derivation(self, clock, inverter):
        instant = clock.instant_at(150.0)
        reading = generate_reading(inverter, instant, 50, seed=SEED)
        event = build_event(
            inverter,
            instant,
            reading,
            0.0,
            seed=SEED,
            tick_index=50,
            event_id="11111111-2222-3333-4444-555555555555",
        )
        assert event.event_id == "11111111-2222-3333-4444-555555555555"


class TestSchemaValidation:
    @pytest.mark.parametrize("field", TELEMETRY_FIELDS)
    def test_every_field_is_required(self, event, field):
        payload = event.to_payload()
        del payload[field]

        assert validated(payload).reason is not None

    def test_negative_power_is_rejected_with_the_shared_reason(self, event):
        payload = event.to_payload()
        payload["active_power_kw"] = -1.0

        assert validated(payload).reason == REASON_NEGATIVE_ACTIVE_POWER

    def test_negative_energy_is_rejected(self, event):
        payload = event.to_payload()
        payload["energy_today_kwh"] = -0.1

        assert validated(payload).reason == REASON_NEGATIVE_ENERGY

    def test_impossible_irradiance_is_rejected(self, event):
        payload = event.to_payload()
        payload["irradiance_wm2"] = 5000.0

        assert validated(payload).reason == REASON_IRRADIANCE_OUT_OF_RANGE

    def test_an_unknown_status_is_rejected(self, event):
        payload = event.to_payload()
        payload["status"] = "DEGRADED"

        assert validated(payload).reason == REASON_INVALID_STATUS

    def test_fractional_availability_is_rejected(self, event):
        # The contract specifies a flag. Member 2 would accept 0.5; the source
        # is stricter so the two endpoints are all that ever appear.
        payload = event.to_payload()
        payload["availability"] = 0.5

        assert validated(payload).reason == REASON_AVAILABILITY_OUT_OF_RANGE

    @pytest.mark.parametrize(
        "timestamp",
        [
            "2026-08-21 12:00:00",
            "2026-08-21T12:00:00",
            "2026-08-21T12:00:00+00:00",
            "2026-08-21T12:00:00.000Z",
            "21-08-2026T12:00:00Z",
            "",
        ],
    )
    def test_non_contract_timestamps_are_rejected(self, event, timestamp):
        payload = event.to_payload()
        payload["timestamp"] = timestamp

        assert validated(payload).reason == REASON_SCHEMA_VIOLATION

    def test_a_non_uuid_event_id_is_rejected(self, event):
        payload = event.to_payload()
        payload["event_id"] = "not-a-uuid"

        assert validated(payload).reason == REASON_SCHEMA_VIOLATION

    def test_a_string_reading_is_rejected(self, event):
        payload = event.to_payload()
        payload["active_power_kw"] = "422.7"

        assert validated(payload).reason == REASON_SCHEMA_VIOLATION

    def test_an_unknown_scenario_label_is_rejected(self, event):
        payload = event.to_payload()
        payload["simulator_scenario"] = "MADE_UP"

        assert validated(payload).reason == REASON_SCHEMA_VIOLATION

    def test_every_problem_is_reported_not_just_the_first(self, event):
        payload = event.to_payload()
        payload["active_power_kw"] = -1.0
        payload["irradiance_wm2"] = -1.0

        assert len(validated(payload).problems) >= 2


class TestSemanticValidation:
    def test_power_above_the_nameplate_rating_is_rejected(self, event, inverter):
        payload = event.to_payload()
        payload["active_power_kw"] = inverter.rated_power_kw + 1

        error = validated(payload, inverter=inverter)
        assert error.reason == REASON_POWER_EXCEEDS_RATING
        assert "1000.0 kW" in str(error)

    def test_power_exactly_at_the_rating_is_allowed(self, event, inverter):
        payload = event.to_payload()
        payload["active_power_kw"] = inverter.rated_power_kw

        validate_payload(payload, inverter=inverter, schema_path=SCHEMA)

    def test_the_rating_check_needs_an_inverter(self, event, inverter):
        # A schema cannot express a per-asset bound, so without the asset this
        # passes. Documented rather than silently surprising.
        payload = event.to_payload()
        payload["active_power_kw"] = inverter.rated_power_kw + 1

        validate_payload(payload, schema_path=SCHEMA)

    def test_an_asset_outside_the_portfolio_is_rejected(self, event, portfolio):
        payload = event.to_payload()
        payload["plant_id"] = "PLANT_99"

        assert validated(payload, portfolio=portfolio).reason == REASON_UNKNOWN_ASSET

    def test_an_unknown_inverter_within_a_real_plant_is_rejected(self, event, portfolio):
        payload = event.to_payload()
        payload["inverter_id"] = "INV_99"

        assert validated(payload, portfolio=portfolio).reason == REASON_UNKNOWN_ASSET

    def test_a_real_asset_passes_the_portfolio_check(self, event, portfolio):
        validate_payload(event.to_payload(), portfolio=portfolio, schema_path=SCHEMA)

    def test_offline_with_output_is_rejected(self, event):
        payload = event.to_payload()
        payload["status"] = STATUS_OFFLINE
        payload["availability"] = 0

        error = validated(payload)
        assert error.reason == REASON_STATUS_INCONSISTENT
        assert "active_power_kw" in str(error)

    def test_offline_while_available_is_rejected(self, event):
        payload = event.to_payload()
        payload["status"] = STATUS_OFFLINE
        payload["active_power_kw"] = 0.0
        payload["availability"] = 1

        assert validated(payload).reason == REASON_STATUS_INCONSISTENT

    def test_a_coherent_offline_event_passes(self, event):
        payload = event.to_payload()
        payload["status"] = STATUS_OFFLINE
        payload["active_power_kw"] = 0.0
        payload["availability"] = 0

        validate_payload(payload, schema_path=SCHEMA)

    def test_online_but_unavailable_is_rejected(self, event):
        payload = event.to_payload()
        payload["availability"] = 0

        assert validated(payload).reason == REASON_STATUS_INCONSISTENT

    def test_warning_keeps_availability_and_output(self, event):
        # Underperformance is degraded, not down: the asset is still available.
        payload = event.to_payload()
        payload["status"] = STATUS_WARNING
        payload["active_power_kw"] = payload["active_power_kw"] * 0.45

        validate_payload(payload, schema_path=SCHEMA)


class TestIsValid:
    def test_true_for_a_good_event(self, event):
        assert is_valid(event.to_payload(), schema_path=SCHEMA)

    def test_false_for_a_bad_event(self, event):
        payload = event.to_payload()
        payload["active_power_kw"] = -1.0
        assert not is_valid(payload, schema_path=SCHEMA)


class TestQuarantineRecords:
    def test_it_carries_the_reason_and_the_original_payload(self, event):
        payload = corrupt_event(event)
        error = validated(payload)
        record = to_quarantine_record(payload, error)

        assert record["rejection_reason"] == REASON_NEGATIVE_ACTIVE_POWER
        assert record["source"] == "streaming-simulator"
        assert record["event_id"] == event.event_id
        assert record["plant_id"] == "PLANT_01"
        assert record["inverter_id"] == "INV_01"
        assert record["event_timestamp_raw"] == event.timestamp
        assert json.loads(record["raw_payload"])["active_power_kw"] < 0

    def test_the_record_is_json_serialisable(self, event):
        payload = corrupt_event(event)
        record = to_quarantine_record(payload, validated(payload))

        assert json.loads(json.dumps(record))["rejection_reason"] == (
            REASON_NEGATIVE_ACTIVE_POWER
        )

    def test_shared_keys_match_member_2s_quarantine_shape(self, event):
        # So one consumer can read both producers of solar.telemetry.invalid.
        payload = corrupt_event(event)
        record = to_quarantine_record(payload, validated(payload))

        for key in (
            "rejection_reason",
            "rejected_at",
            "event_id",
            "plant_id",
            "inverter_id",
            "event_timestamp_raw",
            "raw_payload",
        ):
            assert key in record

    def test_problems_are_preserved_for_diagnosis(self, event):
        payload = corrupt_event(event)
        record = to_quarantine_record(payload, validated(payload))

        assert record["problems"]


class TestCorruptEvent:
    def test_it_produces_something_the_validator_rejects(self, event):
        assert not is_valid(corrupt_event(event), schema_path=SCHEMA)

    def test_it_only_breaks_the_power_field(self, event):
        payload = corrupt_event(event)
        original = event.to_payload()

        assert payload["active_power_kw"] < 0
        assert {k: v for k, v in payload.items() if k != "active_power_kw"} == {
            k: v for k, v in original.items() if k != "active_power_kw"
        }

    def test_it_is_rejected_for_the_expected_reason(self, event):
        assert validated(corrupt_event(event)).reason == REASON_NEGATIVE_ACTIVE_POWER


class TestWholeDayAcrossThePortfolio:
    def test_every_event_a_healthy_run_produces_is_valid(self, clock, portfolio):
        """The real integration check: nothing the simulator emits gets rejected."""
        ledger = EnergyLedger()
        interval = settings().telemetry_interval_seconds
        count = 0

        for tick in range(100):
            instant = clock.instant_at(tick * interval)
            for asset in portfolio.inverters():
                reading = generate_reading(asset, instant, tick, seed=SEED)
                energy = ledger.accumulate(
                    asset.asset_key,
                    instant.day_index,
                    reading.active_power_kw,
                    clock.tick_simulated_hours,
                )
                built = build_event(
                    asset, instant, reading, energy, seed=SEED, tick_index=tick
                )
                validate_event(
                    built, inverter=asset, portfolio=portfolio, schema_path=SCHEMA
                )
                count += 1

        assert count == 3500

    def test_keys_cover_every_asset_exactly_once_per_tick(self, clock, portfolio):
        instant = clock.instant_at(150.0)
        keys = [
            build_event(
                asset,
                instant,
                generate_reading(asset, instant, 50, seed=SEED),
                0.0,
                seed=SEED,
                tick_index=50,
            ).kafka_key
            for asset in portfolio.inverters()
        ]

        assert len(keys) == len(set(keys)) == 35
