"""Kafka payload decoding (Milestone 2).

Proves the parse step is faithful and lossless before any business logic runs on
top of it.
"""

from __future__ import annotations

import pytest

from processing.streaming.transforms import parse_telemetry
from tests.processing._events import kafka_frame, telemetry_event, utc

pytestmark = pytest.mark.spark


def test_valid_event_decodes_every_contract_field(spark):
    row = parse_telemetry(kafka_frame(spark, [telemetry_event()])).collect()[0]

    assert row.event_id == "11111111-1111-4111-8111-111111111111"
    assert row.plant_id == "PLANT_01"
    assert row.inverter_id == "INV_01"
    assert row.active_power_kw == pytest.approx(422.7)
    assert row.energy_today_kwh == pytest.approx(2150.2)
    assert row.irradiance_wm2 == pytest.approx(782.4)
    assert row.module_temp_c == pytest.approx(47.3)
    assert row.inverter_temp_c == pytest.approx(51.0)
    assert row.status == "ONLINE"
    assert row.availability == pytest.approx(1.0)
    assert row.simulator_scenario is None
    assert row.payload_parsed is True


def test_iso8601_zulu_timestamp_becomes_a_utc_instant(spark):
    """The contract sends '...Z'; event-time windows depend on this parsing.

    Asserted as a Spark-side UTC string rather than a collected datetime, which
    PySpark would render in the host's local timezone.
    """
    parsed = parse_telemetry(kafka_frame(spark, [telemetry_event()]))
    row = parsed.select(utc("event_time"), "event_timestamp_raw").collect()[0]

    assert row.event_time == "2026-08-21 05:00:00"
    # The original text is retained for diagnostics.
    assert row.event_timestamp_raw == "2026-08-21T05:00:00Z"


def test_timestamp_with_explicit_offset_is_normalised_to_utc(spark):
    """A +05:30 reading must land on the same instant as its UTC equivalent."""
    parsed = parse_telemetry(kafka_frame(spark, [telemetry_event(timestamp="2026-08-21T10:30:00+05:30")]))
    assert parsed.select(utc("event_time")).collect()[0].event_time == "2026-08-21 05:00:00"


def test_kafka_metadata_is_preserved_for_debugging(spark):
    frame = kafka_frame(spark, [telemetry_event()], topic="solar.telemetry.raw", partition=2)
    row = parse_telemetry(frame).collect()[0]

    assert row.kafka_topic == "solar.telemetry.raw"
    assert row.kafka_partition == 2
    assert row.kafka_offset == 0
    assert row.kafka_key == "PLANT_01:INV_01"
    assert row.kafka_timestamp is not None


def test_unparseable_json_is_flagged_not_dropped(spark):
    """A corrupt record must survive to the quarantine step with its bytes."""
    frame = kafka_frame(spark, ["{this is not json"])
    row = parse_telemetry(frame).collect()[0]

    assert row.payload_parsed is False
    assert row.event_id is None
    assert row.plant_id is None
    # The original payload is still available to quarantine and inspect.
    assert row.raw_payload == "{this is not json"


def test_null_kafka_value_is_flagged_not_dropped(spark):
    row = parse_telemetry(kafka_frame(spark, [None])).collect()[0]

    assert row.payload_parsed is False
    assert row.raw_payload is None


def test_valid_json_with_missing_fields_parses_with_nulls(spark):
    """Structurally valid but incomplete: a different failure from corrupt JSON."""
    event = telemetry_event()
    del event["plant_id"]
    del event["active_power_kw"]

    row = parse_telemetry(kafka_frame(spark, [event])).collect()[0]

    assert row.payload_parsed is True
    assert row.plant_id is None
    assert row.active_power_kw is None
    # The rest of the record survived.
    assert row.inverter_id == "INV_01"


def test_malformed_timestamp_nulls_only_event_time(spark):
    """The reason `timestamp` is read as text: one bad date must not void the row.

    Had the field been typed in the JSON schema, Spark would have nulled the
    entire record and we would have lost the plant id needed to report the fault.
    """
    frame = kafka_frame(spark, [telemetry_event(timestamp="not-a-timestamp")])
    row = parse_telemetry(frame).collect()[0]

    assert row.event_time is None
    assert row.event_timestamp_raw == "not-a-timestamp"
    # Everything else is intact and attributable to a specific asset.
    assert row.plant_id == "PLANT_01"
    assert row.inverter_id == "INV_01"
    assert row.active_power_kw == pytest.approx(422.7)


def test_wrong_typed_field_nulls_that_field_only(spark):
    frame = kafka_frame(spark, [telemetry_event(active_power_kw="not-a-number")])
    row = parse_telemetry(frame).collect()[0]

    assert row.active_power_kw is None
    assert row.plant_id == "PLANT_01"


def test_mixed_batch_preserves_order_and_offsets(spark):
    payloads = [
        telemetry_event(event_id="a"),
        "{{ corrupt",
        telemetry_event(event_id="c"),
    ]
    rows = parse_telemetry(kafka_frame(spark, payloads)).orderBy("kafka_offset").collect()

    assert [r.kafka_offset for r in rows] == [0, 1, 2]
    assert [r.payload_parsed for r in rows] == [True, False, True]
    assert [r.event_id for r in rows] == ["a", None, "c"]
