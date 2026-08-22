"""Validation, normalisation, quarantine and de-duplication (Milestone 3)."""

from __future__ import annotations

import json

import pytest

from processing.streaming.transforms import parse_telemetry
from processing.streaming.validation import (
    REASON_AVAILABILITY_OUT_OF_RANGE,
    REASON_INVALID_STATUS,
    REASON_INVALID_TIMESTAMP,
    REASON_IRRADIANCE_OUT_OF_RANGE,
    REASON_MISSING_EVENT_ID,
    REASON_MISSING_INVERTER_ID,
    REASON_MISSING_PLANT_ID,
    REASON_NEGATIVE_ACTIVE_POWER,
    REASON_NEGATIVE_ENERGY,
    REASON_NEGATIVE_IRRADIANCE,
    REASON_UNPARSEABLE_JSON,
    deduplicate_events,
    invalid_events,
    normalize_valid_events,
    to_quarantine_records,
    valid_events,
    validate_telemetry,
)
from tests.processing._events import kafka_frame, telemetry_event

pytestmark = pytest.mark.spark


def _validated(spark, payloads):
    return validate_telemetry(parse_telemetry(kafka_frame(spark, payloads)))


def _reason(spark, payload):
    return _validated(spark, [payload]).collect()[0].rejection_reason


def test_a_valid_event_has_no_rejection_reason(spark):
    assert _reason(spark, telemetry_event()) is None


@pytest.mark.parametrize("status", ["ONLINE", "OFFLINE", "WARNING"])
def test_every_contract_status_is_accepted(spark, status):
    assert _reason(spark, telemetry_event(status=status)) is None


def test_corrupt_json_is_rejected_as_unparseable(spark):
    assert _reason(spark, "{not json at all") == REASON_UNPARSEABLE_JSON


def test_missing_identity_fields_are_rejected(spark):
    for field, expected in (
        ("event_id", REASON_MISSING_EVENT_ID),
        ("plant_id", REASON_MISSING_PLANT_ID),
        ("inverter_id", REASON_MISSING_INVERTER_ID),
    ):
        event = telemetry_event()
        del event[field]
        assert _reason(spark, event) == expected


def test_unparseable_timestamp_is_rejected(spark):
    assert _reason(spark, telemetry_event(timestamp="yesterday")) == REASON_INVALID_TIMESTAMP


def test_status_outside_the_contract_is_rejected(spark):
    assert _reason(spark, telemetry_event(status="EXPLODED")) == REASON_INVALID_STATUS
    assert _reason(spark, telemetry_event(status="online")) == REASON_INVALID_STATUS
    assert _reason(spark, telemetry_event(status=None)) == REASON_INVALID_STATUS


def test_physically_impossible_readings_are_rejected(spark):
    assert _reason(spark, telemetry_event(active_power_kw=-1.0)) == REASON_NEGATIVE_ACTIVE_POWER
    assert _reason(spark, telemetry_event(energy_today_kwh=-0.5)) == REASON_NEGATIVE_ENERGY
    assert _reason(spark, telemetry_event(irradiance_wm2=-10.0)) == REASON_NEGATIVE_IRRADIANCE
    assert _reason(spark, telemetry_event(irradiance_wm2=5000.0)) == REASON_IRRADIANCE_OUT_OF_RANGE


def test_availability_outside_zero_to_one_is_rejected(spark):
    assert _reason(spark, telemetry_event(availability=1.5)) == REASON_AVAILABILITY_OUT_OF_RANGE
    assert _reason(spark, telemetry_event(availability=-0.1)) == REASON_AVAILABILITY_OUT_OF_RANGE


def test_zero_power_at_night_is_valid_not_rejected(spark):
    """Zero output is normal after sunset and must never be quarantined."""
    night = telemetry_event(active_power_kw=0.0, irradiance_wm2=0.0, energy_today_kwh=0.0)
    assert _reason(spark, night) is None


def test_offline_inverter_is_valid_data(spark):
    """An offline inverter is a business problem, not a data-quality problem."""
    offline = telemetry_event(status="OFFLINE", active_power_kw=0.0, availability=0.0)
    assert _reason(spark, offline) is None


def test_only_the_first_and_most_fundamental_failure_is_reported(spark):
    """A corrupt record reports corruption, not a cascade of missing fields."""
    assert _reason(spark, "}}}") == REASON_UNPARSEABLE_JSON

    # Missing identity outranks a bad reading on the same event.
    event = telemetry_event(active_power_kw=-5.0)
    del event["plant_id"]
    assert _reason(spark, event) == REASON_MISSING_PLANT_ID


def test_valid_and_invalid_streams_partition_the_input(spark):
    payloads = [
        telemetry_event(event_id="ok-1"),
        "{corrupt",
        telemetry_event(event_id="ok-2"),
        telemetry_event(event_id="bad", active_power_kw=-3.0),
    ]
    validated = _validated(spark, payloads)

    good = valid_events(validated)
    bad = invalid_events(validated)

    assert {r.event_id for r in good.collect()} == {"ok-1", "ok-2"}
    assert bad.count() == 2
    # Nothing is lost: the two sides sum to the input.
    assert good.count() + bad.count() == len(payloads)
    # The reason column does not leak into the clean stream.
    assert "rejection_reason" not in good.columns


class TestNormalisation:
    def test_missing_numerics_become_explicit_zeros(self, spark):
        event = telemetry_event()
        del event["active_power_kw"]
        del event["irradiance_wm2"]

        row = normalize_valid_events(valid_events(_validated(spark, [event]))).collect()[0]

        # Zero, not null: a null would vanish from a SUM and overstate the plant.
        assert row.active_power_kw == 0.0
        assert row.irradiance_wm2 == 0.0

    def test_missing_availability_is_inferred_from_status(self, spark):
        online = telemetry_event()
        del online["availability"]
        offline = telemetry_event(event_id="off", status="OFFLINE", active_power_kw=0.0)
        del offline["availability"]

        rows = {
            r.event_id: r
            for r in normalize_valid_events(
                valid_events(_validated(spark, [online, offline]))
            ).collect()
        }

        assert rows["11111111-1111-4111-8111-111111111111"].availability == 1.0
        assert rows["off"].availability == 0.0

    def test_present_values_are_left_alone(self, spark):
        row = normalize_valid_events(
            valid_events(_validated(spark, [telemetry_event()]))
        ).collect()[0]
        assert row.active_power_kw == pytest.approx(422.7)
        assert row.availability == 1.0


class TestDeduplication:
    def test_repeated_event_ids_are_collapsed(self, spark):
        """The producer retries, so at-least-once duplicates are expected."""
        duplicated = telemetry_event(event_id="dupe")
        validated = _validated(spark, [duplicated, duplicated, telemetry_event(event_id="other")])

        deduped = deduplicate_events(valid_events(validated), "2 minutes")

        assert deduped.count() == 2
        assert {r.event_id for r in deduped.collect()} == {"dupe", "other"}

    def test_duplicates_with_differing_payloads_still_collapse_to_one(self, spark):
        """Identity is event_id alone; a retry may carry a re-read power value."""
        first = telemetry_event(event_id="dupe", active_power_kw=100.0)
        second = telemetry_event(event_id="dupe", active_power_kw=101.0)

        deduped = deduplicate_events(valid_events(_validated(spark, [first, second])), "2 minutes")
        assert deduped.count() == 1

    def test_distinct_events_are_all_retained(self, spark):
        payloads = [telemetry_event(event_id=f"e{i}") for i in range(5)]
        deduped = deduplicate_events(valid_events(_validated(spark, payloads)), "2 minutes")
        assert deduped.count() == 5


class TestQuarantine:
    def test_rejected_record_carries_reason_payload_and_coordinates(self, spark):
        validated = _validated(spark, [telemetry_event(active_power_kw=-9.0)])
        record = to_quarantine_records(invalid_events(validated)).collect()[0]

        payload = json.loads(record.value)

        assert payload["rejection_reason"] == REASON_NEGATIVE_ACTIVE_POWER
        # Attributable to a specific asset...
        assert payload["plant_id"] == "PLANT_01"
        assert payload["inverter_id"] == "INV_01"
        # ...traceable back to the exact source record...
        assert payload["source_topic"] == "solar.telemetry.raw"
        assert payload["source_offset"] == 0
        # ...and the original bytes are preserved for inspection.
        assert json.loads(payload["raw_payload"])["active_power_kw"] == -9.0
        assert payload["rejected_at"] is not None

    def test_quarantine_is_keyed_by_asset_for_partition_locality(self, spark):
        validated = _validated(spark, [telemetry_event(status="NOPE")])
        assert to_quarantine_records(invalid_events(validated)).collect()[0].key == "PLANT_01:INV_01"

    def test_unattributable_record_gets_a_stable_key_not_null(self, spark):
        """A null Kafka key round-robins partitions; a placeholder does not."""
        frame = kafka_frame(spark, [None])
        validated = validate_telemetry(parse_telemetry(frame))
        record = to_quarantine_records(invalid_events(validated)).collect()[0]

        assert record.key == "unattributed"
        assert json.loads(record.value)["rejection_reason"] == REASON_UNPARSEABLE_JSON

    def test_corrupt_record_keeps_its_original_text(self, spark):
        validated = _validated(spark, ["{broken"])
        payload = json.loads(to_quarantine_records(invalid_events(validated)).collect()[0].value)

        assert payload["raw_payload"] == "{broken"
        assert payload["plant_id"] is None
