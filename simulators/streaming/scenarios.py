"""Deterministic anomaly scenarios for the assessment demo.

The faults this schedules are the point of the whole demonstration, so none of
them are random. A window fires at a fixed position within every simulated day,
against a named asset, and the schedule lives in `simulators/config/scenarios.yaml`
where it can be read and rehearsed rather than inferred from code.

Two distinctions are load-bearing and easy to blur:

  * **INV_OFFLINE is not TELEMETRY_GAP.** Offline is a *reported* zero — the
    asset publishes `active_power_kw = 0`, `availability = 0`, `status = OFFLINE`
    and is therefore visibly, knowably down. A gap is *silence*: nothing is
    published at all, so the platform cannot distinguish "generating fine" from
    "on fire". The first is an operational alert, the second a pipeline-health
    alert, and the assessment asks to see both.

  * **Irradiance is never touched.** Underperformance scales power while leaving
    the measured resource alone, because that contrast is the entire detection
    signal. Scaling irradiance too would just simulate a cloud, which is not a
    fault and should not alert.

Scenario application is kept separate from generation: `generation.py` models a
healthy inverter, and this module transforms that result. Faults therefore cannot
leak into the physics, and a healthy run is testable on its own.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import yaml

from simulators.common.config import ConfigError
from simulators.common.portfolio import Inverter, Portfolio
from simulators.streaming.events import STATUS_OFFLINE, STATUS_ONLINE, STATUS_WARNING
from simulators.streaming.generation import InverterReading

DEFAULT_SCENARIO_PATH = Path("simulators/config/scenarios.yaml")

SCENARIO_UNDERPERFORMANCE = "INV_UNDERPERFORMANCE"
SCENARIO_OFFLINE = "INV_OFFLINE"
SCENARIO_TELEMETRY_GAP = "TELEMETRY_GAP"
SCENARIO_RECOVERY = "RECOVERY"

# Must stay a subset of the `simulator_scenario` enum in
# contracts/telemetry.schema.json, or a scripted event fails its own validation.
ALLOWED_SCENARIOS = (
    SCENARIO_UNDERPERFORMANCE,
    SCENARIO_OFFLINE,
    SCENARIO_TELEMETRY_GAP,
    SCENARIO_RECOVERY,
)


class ScenarioConfigError(ConfigError):
    """Raised when the anomaly schedule cannot be trusted."""


@dataclass(frozen=True)
class ScenarioWindow:
    """One scripted fault: what happens, when, and to which asset."""

    scenario: str
    start_second: float
    end_second: float
    plant_id: str | None = None
    inverter_id: str | None = None
    # Only meaningful for INV_UNDERPERFORMANCE.
    power_factor: float | None = None

    @property
    def duration_seconds(self) -> float:
        return self.end_second - self.start_second

    def covers(self, seconds_into_day: float) -> bool:
        """Whether this window is active. Half-open, so adjacent windows do not
        both claim the instant where one ends and the next begins."""
        return self.start_second <= seconds_into_day < self.end_second

    def targets(self, inverter: Inverter) -> bool:
        """Whether this window applies to a given asset.

        A window naming only a plant hits every inverter on it — that is how
        TELEMETRY_GAP silences a whole site.
        """
        if self.plant_id is not None and inverter.plant_id != self.plant_id:
            return False
        if self.inverter_id is not None and inverter.id != self.inverter_id:
            return False
        return True

    @property
    def target_label(self) -> str:
        """Human-readable target, for logs and the demo timeline printout."""
        if self.plant_id is None:
            return "portfolio"
        if self.inverter_id is None:
            return self.plant_id
        return f"{self.plant_id}:{self.inverter_id}"


@dataclass(frozen=True)
class AssetOutcome:
    """What to publish for one asset at one tick, after scenarios are applied."""

    publish: bool
    reading: InverterReading
    status: str
    availability: float
    scenario: str | None


@dataclass(frozen=True)
class ScenarioSchedule:
    """The full demo timeline, scaled to the configured clock."""

    windows: tuple[ScenarioWindow, ...]
    day_seconds: float

    def active_window(self, seconds_into_day: float) -> ScenarioWindow | None:
        """The window covering this point of the day, if any.

        Windows are validated non-overlapping, so at most one can match — which
        keeps "what is happening right now?" a question with one answer, both for
        the Prometheus scenario gauge and for anyone watching the demo.
        """
        for window in self.windows:
            if window.covers(seconds_into_day):
                return window
        return None

    def window_for(
        self, inverter: Inverter, seconds_into_day: float
    ) -> ScenarioWindow | None:
        """The active window, but only if it targets this asset."""
        window = self.active_window(seconds_into_day)
        if window is not None and window.targets(inverter):
            return window
        return None

    def timeline(self) -> list[str]:
        """The schedule as printable lines, for demo_start.sh and the logs."""
        lines = []
        cursor = 0.0
        for window in self.windows:
            if window.start_second > cursor:
                lines.append(f"{cursor:>6.0f}-{window.start_second:<6.0f} NORMAL")
            detail = window.target_label
            if window.power_factor is not None:
                detail += f" at {window.power_factor:.0%} power"
            lines.append(
                f"{window.start_second:>6.0f}-{window.end_second:<6.0f} "
                f"{window.scenario} on {detail}"
            )
            cursor = window.end_second
        if cursor < self.day_seconds:
            lines.append(f"{cursor:>6.0f}-{self.day_seconds:<6.0f} NORMAL")
        return lines


def apply_scenario(
    inverter: Inverter, reading: InverterReading, window: ScenarioWindow | None
) -> AssetOutcome:
    """Transform a healthy reading according to the active scenario.

    With no window, or a NORMAL/RECOVERY one, the reading passes through
    untouched — RECOVERY only labels the asset as having returned to health.
    """
    if window is None:
        return AssetOutcome(True, reading, STATUS_ONLINE, 1.0, None)

    if window.scenario == SCENARIO_RECOVERY:
        return AssetOutcome(True, reading, STATUS_ONLINE, 1.0, SCENARIO_RECOVERY)

    if window.scenario == SCENARIO_TELEMETRY_GAP:
        # Nothing is published. The reading is carried along unchanged so the
        # caller can still advance the energy meter: the plant kept generating,
        # we simply stopped hearing about it, and pretending otherwise would
        # make the day's total wrong once telemetry resumes.
        return AssetOutcome(False, reading, STATUS_ONLINE, 1.0, SCENARIO_TELEMETRY_GAP)

    if window.scenario == SCENARIO_OFFLINE:
        return AssetOutcome(
            True,
            # Irradiance and temperatures are untouched: the sun still shines on
            # a dead inverter, and that contrast is what makes the fault legible.
            replace(reading, active_power_kw=0.0),
            STATUS_OFFLINE,
            0.0,
            SCENARIO_OFFLINE,
        )

    if window.scenario == SCENARIO_UNDERPERFORMANCE:
        factor = window.power_factor if window.power_factor is not None else 1.0
        return AssetOutcome(
            True,
            replace(reading, active_power_kw=reading.active_power_kw * factor),
            # Degraded, not down: the asset is still available and still
            # producing, which is exactly what makes this the subtle case.
            STATUS_WARNING,
            1.0,
            SCENARIO_UNDERPERFORMANCE,
        )

    raise ScenarioConfigError(f"Unhandled scenario {window.scenario!r}.")


def _number(raw: Any, field: str, context: str) -> float:
    if isinstance(raw, bool) or raw is None:
        raise ScenarioConfigError(f"{context}: {field} must be a number, got {raw!r}.")
    try:
        value = float(raw)
    except (TypeError, ValueError) as exc:
        raise ScenarioConfigError(
            f"{context}: {field} must be a number, got {raw!r}."
        ) from exc
    if value != value or value in (float("inf"), float("-inf")):
        raise ScenarioConfigError(f"{context}: {field} must be finite, got {raw!r}.")
    return value


def _parse_window(raw: Any, index: int) -> ScenarioWindow:
    context = f"window {index}"
    if not isinstance(raw, dict):
        raise ScenarioConfigError(
            f"{context}: each window must be a mapping, got {type(raw).__name__}."
        )

    scenario = str(raw.get("scenario") or "").strip()
    if scenario not in ALLOWED_SCENARIOS:
        raise ScenarioConfigError(
            f"{context}: scenario must be one of {', '.join(ALLOWED_SCENARIOS)}, "
            f"got {scenario!r}."
        )
    context = f"window {index} ({scenario})"

    start = _number(raw.get("start_second"), "start_second", context)
    end = _number(raw.get("end_second"), "end_second", context)
    if start < 0:
        raise ScenarioConfigError(f"{context}: start_second must not be negative.")
    if end <= start:
        raise ScenarioConfigError(
            f"{context}: end_second ({end}) must be greater than start_second ({start})."
        )

    inverter_id = raw.get("inverter_id")
    plant_id = raw.get("plant_id")
    if inverter_id is not None and plant_id is None:
        raise ScenarioConfigError(
            f"{context}: inverter_id {inverter_id!r} given without a plant_id. "
            "Inverter ids repeat across plants, so the target would be ambiguous."
        )

    power_factor = raw.get("power_factor")
    if scenario == SCENARIO_UNDERPERFORMANCE:
        if power_factor is None:
            raise ScenarioConfigError(f"{context}: power_factor is required.")
        power_factor = _number(power_factor, "power_factor", context)
        if not 0 <= power_factor < 1:
            raise ScenarioConfigError(
                f"{context}: power_factor must be in [0, 1), got {power_factor}. "
                "A factor of 1 or more is not underperformance."
            )
    elif power_factor is not None:
        raise ScenarioConfigError(
            f"{context}: power_factor only applies to {SCENARIO_UNDERPERFORMANCE}."
        )

    return ScenarioWindow(
        scenario=scenario,
        start_second=start,
        end_second=end,
        plant_id=str(plant_id).strip() if plant_id is not None else None,
        inverter_id=str(inverter_id).strip() if inverter_id is not None else None,
        power_factor=power_factor,
    )


def _validate_targets(windows: tuple[ScenarioWindow, ...], portfolio: Portfolio) -> None:
    """Every named asset must exist. A renamed plant should fail at startup."""
    for window in windows:
        if window.plant_id is None:
            continue
        try:
            plant = portfolio.plant(window.plant_id)
        except KeyError as exc:
            raise ScenarioConfigError(
                f"{window.scenario} targets plant {window.plant_id!r}, which is not in "
                "the portfolio configuration."
            ) from exc
        if window.inverter_id is not None and not any(
            inv.id == window.inverter_id for inv in plant.inverters
        ):
            raise ScenarioConfigError(
                f"{window.scenario} targets {window.target_label}, but plant "
                f"{window.plant_id} has no inverter {window.inverter_id!r}."
            )


def _validate_no_overlap(windows: tuple[ScenarioWindow, ...]) -> None:
    """Overlapping windows would make 'what is happening now' ambiguous."""
    ordered = sorted(windows, key=lambda w: w.start_second)
    for earlier, later in zip(ordered, ordered[1:]):
        if later.start_second < earlier.end_second:
            raise ScenarioConfigError(
                f"{earlier.scenario} ({earlier.start_second}-{earlier.end_second}s) "
                f"overlaps {later.scenario} "
                f"({later.start_second}-{later.end_second}s)."
            )


def parse_schedule(
    document: Any,
    *,
    day_seconds: float,
    portfolio: Portfolio | None = None,
) -> ScenarioSchedule:
    """Validate a loaded schedule document and scale it to the configured clock."""
    if not isinstance(document, dict):
        raise ScenarioConfigError(
            "Scenario config must be a mapping with 'windows' and "
            "'reference_day_seconds' keys."
        )
    if day_seconds <= 0:
        raise ScenarioConfigError("day_seconds must be greater than zero.")

    reference = _number(
        document.get("reference_day_seconds", day_seconds),
        "reference_day_seconds",
        "scenario config",
    )
    if reference <= 0:
        raise ScenarioConfigError("reference_day_seconds must be greater than zero.")

    raw_windows = document.get("windows")
    if not isinstance(raw_windows, list) or not raw_windows:
        raise ScenarioConfigError("Scenario config needs a non-empty 'windows' list.")

    windows = tuple(
        _parse_window(raw, index) for index, raw in enumerate(raw_windows, start=1)
    )
    _validate_no_overlap(windows)

    for window in windows:
        if window.end_second > reference:
            raise ScenarioConfigError(
                f"{window.scenario} ends at {window.end_second}s, past the "
                f"{reference}s reference day. It would never fire."
            )

    # Scale to the configured clock, so changing SIMULATION_DAY_SECONDS moves the
    # whole narrative with it instead of pushing anomalies off the end of the day.
    scale = day_seconds / reference
    if scale != 1.0:
        windows = tuple(
            replace(
                window,
                start_second=window.start_second * scale,
                end_second=window.end_second * scale,
            )
            for window in windows
        )

    if portfolio is not None:
        _validate_targets(windows, portfolio)

    return ScenarioSchedule(
        windows=tuple(sorted(windows, key=lambda w: w.start_second)),
        day_seconds=day_seconds,
    )


def load_schedule(
    path: str | Path | None = None,
    *,
    day_seconds: float,
    portfolio: Portfolio | None = None,
) -> ScenarioSchedule:
    """Read and validate the anomaly schedule from disk."""
    config_path = Path(path) if path is not None else DEFAULT_SCENARIO_PATH

    try:
        raw = config_path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise ScenarioConfigError(
            f"Scenario schedule not found at {config_path}. Without it the demo "
            "would run with no anomalies at all."
        ) from exc
    except OSError as exc:
        raise ScenarioConfigError(f"Cannot read {config_path}: {exc}.") from exc

    try:
        document = yaml.safe_load(raw)
    except yaml.YAMLError as exc:
        raise ScenarioConfigError(f"{config_path} is not valid YAML: {exc}.") from exc

    try:
        return parse_schedule(document, day_seconds=day_seconds, portfolio=portfolio)
    except ScenarioConfigError as exc:
        raise ScenarioConfigError(f"{config_path}: {exc}") from exc
