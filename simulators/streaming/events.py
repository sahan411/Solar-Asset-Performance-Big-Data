"""Telemetry event construction and source-side validation.

Kafka is the boundary where this subsystem's output becomes everyone else's
input, so nothing crosses it unchecked. An event is validated in two layers,
because one of them cannot be expressed in a schema:

  * `contracts/telemetry.schema.json` — the shared, frozen structure: field
    presence, types, enums and absolute ranges. It is the canonical contract
    document and it is genuinely executed here, not decorative.
  * semantic checks — the rules that need context a schema does not have: the
    asset must exist in the portfolio, power must not exceed *that inverter's*
    nameplate rating, and status must agree with availability and output.

Rejected events never reach `solar.telemetry.raw`. They are quarantined to
`solar.telemetry.invalid` with a machine-readable reason, following the same
policy Member 2 applies at his end: quarantine, never silent drop.

The rejection reason codes are deliberately the same strings Member 2 uses in
processing/streaming/validation.py, so a dashboard grouping the quarantine topic
by reason works whether the record was rejected at the source or in the stream.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable

from jsonschema import Draft202012Validator

from simulators.common.portfolio import Inverter, Portfolio
from simulators.common.time import SimulatedInstant, to_iso
from simulators.streaming.generation import InverterReading

# Canonical schema, relative to the repository root.
TELEMETRY_SCHEMA_PATH = Path("contracts/telemetry.schema.json")

STATUS_ONLINE = "ONLINE"
STATUS_OFFLINE = "OFFLINE"
STATUS_WARNING = "WARNING"
ALLOWED_STATUSES = (STATUS_ONLINE, STATUS_OFFLINE, STATUS_WARNING)

# Field order of the frozen contract. Payloads are emitted in this order so the
# JSON on the topic is easy to read and diff by eye during a demo.
TELEMETRY_FIELDS = (
    "event_id",
    "plant_id",
    "inverter_id",
    "active_power_kw",
    "energy_today_kwh",
    "irradiance_wm2",
    "module_temp_c",
    "inverter_temp_c",
    "status",
    "availability",
    "timestamp",
    "simulator_scenario",
)

# Shared with processing/streaming/validation.py — same strings, same meanings.
REASON_SCHEMA_VIOLATION = "SCHEMA_VIOLATION"
REASON_INVALID_STATUS = "INVALID_STATUS"
REASON_NEGATIVE_ACTIVE_POWER = "NEGATIVE_ACTIVE_POWER"
REASON_NEGATIVE_ENERGY = "NEGATIVE_ENERGY"
REASON_NEGATIVE_IRRADIANCE = "NEGATIVE_IRRADIANCE"
REASON_IRRADIANCE_OUT_OF_RANGE = "IRRADIANCE_OUT_OF_RANGE"
REASON_AVAILABILITY_OUT_OF_RANGE = "AVAILABILITY_OUT_OF_RANGE"
# Source-side only: the simulator knows the portfolio and the nameplate ratings,
# so it can catch faults the stream has no way to see.
REASON_UNKNOWN_ASSET = "UNKNOWN_ASSET"
REASON_POWER_EXCEEDS_RATING = "POWER_EXCEEDS_RATING"
REASON_STATUS_INCONSISTENT = "STATUS_INCONSISTENT"

# Namespace for deterministic event ids. A fixed UUID, so the derivation is
# stable across machines and Python versions.
EVENT_ID_NAMESPACE = uuid.UUID("6f2a1c4e-3b7d-5e8f-9a0b-1c2d3e4f5a6b")


class EventValidationError(ValueError):
    """Raised when an event must not be published to the valid topic.

    Carries every problem found rather than only the first, so one rejection
    reports everything wrong with the record.
    """

    def __init__(self, reason: str, problems: list[str]) -> None:
        self.reason = reason
        self.problems = list(problems)
        listed = "; ".join(self.problems)
        super().__init__(f"{reason}: {listed}")


@dataclass(frozen=True)
class TelemetryEvent:
    """One inverter's readings at one simulated instant."""

    event_id: str
    plant_id: str
    inverter_id: str
    active_power_kw: float
    energy_today_kwh: float
    irradiance_wm2: float
    module_temp_c: float
    inverter_temp_c: float
    status: str
    availability: float
    timestamp: str
    simulator_scenario: str | None

    @property
    def kafka_key(self) -> str:
        """Partition key. Keeps one inverter's events ordered within a partition."""
        return f"{self.plant_id}:{self.inverter_id}"

    def to_payload(self) -> dict[str, Any]:
        """The event as a plain dict, in frozen contract field order."""
        return {field: getattr(self, field) for field in TELEMETRY_FIELDS}

    def to_json(self) -> str:
        """Compact JSON, as published to Kafka."""
        return json.dumps(self.to_payload(), separators=(",", ":"))


@lru_cache(maxsize=4)
def _validator(schema_path: str) -> Draft202012Validator:
    """Compile the schema once. Called per event, so compiling each time would
    dominate the cost of producing telemetry."""
    schema = json.loads(Path(schema_path).read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema)


def load_schema(path: str | Path | None = None) -> dict[str, Any]:
    """The canonical telemetry schema as a dict."""
    return json.loads(Path(path or TELEMETRY_SCHEMA_PATH).read_text(encoding="utf-8"))


def deterministic_event_id(seed: int, plant_id: str, inverter_id: str, tick_index: int) -> str:
    """A reproducible UUID for one asset at one tick.

    Random UUIDs would make every run produce different ids, so two runs of the
    same seeded simulation could not be compared, and a replay could not be shown
    to be a replay. UUID5 over the same coordinates the generation model uses
    gives ids that are unique within a run and identical across runs.

    Uniqueness holds because `tick_index` is global to the run rather than reset
    each simulated day, so an asset never revisits a coordinate.
    """
    return str(
        uuid.uuid5(EVENT_ID_NAMESPACE, f"{seed}|{plant_id}|{inverter_id}|{tick_index}")
    )


def build_event(
    inverter: Inverter,
    instant: SimulatedInstant,
    reading: InverterReading,
    energy_today_kwh: float,
    *,
    seed: int,
    tick_index: int,
    status: str = STATUS_ONLINE,
    availability: float = 1.0,
    scenario: str | None = None,
    event_id: str | None = None,
) -> TelemetryEvent:
    """Assemble a telemetry event. Does not validate — call `validate_event`.

    Construction and validation are separate so an intentionally malformed event
    can be built for the quarantine test path.
    """
    return TelemetryEvent(
        event_id=event_id
        or deterministic_event_id(seed, inverter.plant_id, inverter.id, tick_index),
        plant_id=inverter.plant_id,
        inverter_id=inverter.id,
        # Rounded at the boundary rather than downstream: the JSON on the topic
        # is what a human reads during a demo, and full float precision makes it
        # unreadable without changing any aggregate meaningfully.
        active_power_kw=round(reading.active_power_kw, 3),
        energy_today_kwh=round(energy_today_kwh, 3),
        irradiance_wm2=round(reading.irradiance_wm2, 2),
        module_temp_c=round(reading.module_temp_c, 2),
        inverter_temp_c=round(reading.inverter_temp_c, 2),
        status=status,
        availability=float(availability),
        timestamp=instant.iso_timestamp,
        simulator_scenario=scenario,
    )


def _semantic_problems(
    payload: dict[str, Any], inverter: Inverter | None
) -> tuple[str, list[str]] | None:
    """Checks that need context beyond the schema. Returns (reason, problems)."""
    status = payload.get("status")
    availability = payload.get("availability")
    power = payload.get("active_power_kw")

    if inverter is not None and isinstance(power, (int, float)):
        if power > inverter.rated_power_kw:
            return (
                REASON_POWER_EXCEEDS_RATING,
                [
                    f"active_power_kw {power} exceeds the nameplate rating of "
                    f"{inverter.rated_power_kw} kW for {inverter.asset_key}"
                ],
            )

    # An OFFLINE inverter that reports output, or availability, is contradicting
    # itself. Downstream this would inflate plant availability while the asset is
    # down, so it is caught here rather than reconciled later.
    if status == STATUS_OFFLINE:
        problems = []
        if availability != 0:
            problems.append(f"status is OFFLINE but availability is {availability}")
        if isinstance(power, (int, float)) and power != 0:
            problems.append(f"status is OFFLINE but active_power_kw is {power}")
        if problems:
            return REASON_STATUS_INCONSISTENT, problems
    elif status in (STATUS_ONLINE, STATUS_WARNING) and availability != 1:
        return (
            REASON_STATUS_INCONSISTENT,
            [f"status is {status} but availability is {availability}"],
        )

    return None


def _schema_reason(problems: Iterable[str], payload: dict[str, Any]) -> str:
    """Map a schema failure onto the most specific shared reason code.

    A dashboard grouping the quarantine topic by reason is far more useful when
    a negative power reading reports NEGATIVE_ACTIVE_POWER rather than a generic
    SCHEMA_VIOLATION, so the common physical faults keep the codes Member 2 uses.
    """
    checks = (
        ("active_power_kw", lambda v: v < 0, REASON_NEGATIVE_ACTIVE_POWER),
        ("energy_today_kwh", lambda v: v < 0, REASON_NEGATIVE_ENERGY),
        ("irradiance_wm2", lambda v: v < 0, REASON_NEGATIVE_IRRADIANCE),
        ("irradiance_wm2", lambda v: v > 1500, REASON_IRRADIANCE_OUT_OF_RANGE),
    )
    for field, is_bad, reason in checks:
        value = payload.get(field)
        if isinstance(value, (int, float)) and not isinstance(value, bool) and is_bad(value):
            return reason

    status = payload.get("status")
    if status is not None and status not in ALLOWED_STATUSES:
        return REASON_INVALID_STATUS

    availability = payload.get("availability")
    if isinstance(availability, (int, float)) and not isinstance(availability, bool):
        if availability not in (0, 1):
            return REASON_AVAILABILITY_OUT_OF_RANGE

    return REASON_SCHEMA_VIOLATION


def validate_payload(
    payload: dict[str, Any],
    *,
    inverter: Inverter | None = None,
    portfolio: Portfolio | None = None,
    schema_path: str | Path | None = None,
) -> None:
    """Raise EventValidationError unless the payload may be published.

    Pass `portfolio` to confirm the asset exists, or `inverter` when the caller
    already holds it — the nameplate rating check needs one of the two.
    """
    if portfolio is not None and inverter is None:
        try:
            plant = portfolio.plant(str(payload.get("plant_id")))
            inverter = next(
                inv for inv in plant.inverters if inv.id == payload.get("inverter_id")
            )
        except (KeyError, StopIteration) as exc:
            raise EventValidationError(
                REASON_UNKNOWN_ASSET,
                [
                    f"{payload.get('plant_id')}:{payload.get('inverter_id')} is not in "
                    "the portfolio configuration"
                ],
            ) from exc

    validator = _validator(str(schema_path or TELEMETRY_SCHEMA_PATH))
    errors = sorted(validator.iter_errors(payload), key=lambda e: list(e.absolute_path))
    if errors:
        problems = [
            f"{'/'.join(str(p) for p in error.absolute_path) or '<root>'}: {error.message}"
            for error in errors
        ]
        raise EventValidationError(_schema_reason(problems, payload), problems)

    semantic = _semantic_problems(payload, inverter)
    if semantic is not None:
        raise EventValidationError(*semantic)


def validate_event(event: TelemetryEvent, **kwargs: Any) -> None:
    """Convenience wrapper over `validate_payload` for a built event."""
    validate_payload(event.to_payload(), **kwargs)


def is_valid(payload: dict[str, Any], **kwargs: Any) -> bool:
    """Whether the payload would be accepted. Prefer `validate_payload` when the
    reason matters — this discards it."""
    try:
        validate_payload(payload, **kwargs)
    except EventValidationError:
        return False
    return True


def to_quarantine_record(
    payload: dict[str, Any], error: EventValidationError
) -> dict[str, Any]:
    """Shape a rejected event for `solar.telemetry.invalid`.

    Field names match Member 2's quarantine records where they overlap, so both
    producers of that topic yield one consistent shape. `source` distinguishes a
    record rejected here, before publication, from one rejected in the stream —
    the Kafka coordinates his records carry do not exist yet at this point.
    """
    return {
        "rejection_reason": error.reason,
        "rejected_at": to_iso(datetime.now(tz=timezone.utc)),
        "source": "streaming-simulator",
        "event_id": payload.get("event_id"),
        "plant_id": payload.get("plant_id"),
        "inverter_id": payload.get("inverter_id"),
        "event_timestamp_raw": payload.get("timestamp"),
        "problems": error.problems,
        "raw_payload": json.dumps(payload, separators=(",", ":"), default=str),
    }


def corrupt_event(event: TelemetryEvent) -> dict[str, Any]:
    """A deliberately invalid payload, for exercising the quarantine path.

    Negative active power: physically impossible, unambiguous, and it maps onto a
    reason code both this module and Member 2's validator recognise. Only reached
    when SIMULATION_EMIT_INVALID_EVENTS is enabled, which is off by default and
    must stay off for the assessment demo.
    """
    payload = event.to_payload()
    payload["active_power_kw"] = -abs(payload["active_power_kw"]) - 1.0
    return payload
