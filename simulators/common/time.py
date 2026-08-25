"""The compressed simulated clock.

One simulated day is 24 hours of *event time* squeezed into `day_seconds` of
real time — 300 real seconds by default, a compression factor of 288. Two clocks
therefore run at once and confusing them is the easiest way to break the
pipeline:

  * real time   — how long the demo has actually been running. Drives the tick
                  loop, the anomaly schedule and log timestamps.
  * event time  — the timestamp written into each telemetry event. Spans a full
                  midnight-to-midnight day per simulated day.

Every telemetry event carries **event time**. This is not cosmetic: Member 2's
Spark job windows on event time and holds an alert for `ALERT_SUSTAIN_SECONDS`
(default 3600 = one simulated hour) of it. Publish real wall-clock timestamps
instead and a simulated day would span five minutes of event time, no window
would ever fill, and no alert would ever fire.

The mapping is pure and total, so a given elapsed time always yields the same
instant — the determinism the assessment demo depends on:

    real elapsed 0 s    -> 2026-08-21T00:00:00Z, progress 0.00
    real elapsed 150 s  -> 2026-08-21T12:00:00Z, progress 0.50
    real elapsed 300 s  -> 2026-08-22T00:00:00Z, progress 0.00 (day 2 begins)
"""

from __future__ import annotations

import time as _time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Callable

from simulators.common.config import SECONDS_PER_SIMULATED_DAY, ConfigError, SimulationSettings


def utc_now() -> datetime:
    """Current real wall-clock time, UTC. Never used for event timestamps."""
    return datetime.now(tz=timezone.utc)


def to_iso(moment: datetime) -> str:
    """Render an instant in the frozen contract's timestamp format.

    Seconds precision with a trailing `Z`, matching the contract example
    (`2026-08-21T05:00:00Z`). Sub-second precision would be noise: at the default
    clock one tick advances event time by more than fourteen simulated minutes.
    """
    if moment.tzinfo is None:
        raise ValueError("Refusing to serialise a naive datetime; event times must be UTC-aware.")
    return moment.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


@dataclass(frozen=True)
class SimulatedInstant:
    """Where the simulation is, in both clocks, at one moment."""

    real_elapsed_seconds: float
    day_index: int
    simulation_date: date
    # 0.0 at simulated midnight, approaching 1.0 at the end of the simulated day.
    progress: float
    # Event time: what goes in the telemetry event's `timestamp` field.
    timestamp: datetime
    # Real seconds per simulated day, carried so the instant can report its own
    # position without the caller needing the settings again.
    day_seconds: float

    @property
    def iso_timestamp(self) -> str:
        return to_iso(self.timestamp)

    @property
    def seconds_into_day(self) -> float:
        """Real seconds elapsed since this simulated day began.

        The anomaly schedule is written against this rather than total elapsed
        time, so the same timeline repeats on every simulated day.
        """
        return self.progress * self.day_seconds


class SimulationClock:
    """Maps real elapsed seconds onto simulated dates and event timestamps.

    Construct it from settings, call `start()` once when the simulation begins,
    then `now()` each tick. `instant_at()` is the pure mapping underneath and is
    what tests should drive — it needs no wall clock at all.
    """

    def __init__(
        self,
        settings: SimulationSettings,
        *,
        monotonic: Callable[[], float] = _time.monotonic,
    ) -> None:
        self._settings = settings
        # Injectable so tests advance time deterministically, and monotonic by
        # default so an NTP correction mid-demo cannot rewind the simulation.
        self._monotonic = monotonic
        self._started_at: float | None = None

    @property
    def day_seconds(self) -> float:
        return self._settings.day_seconds

    @property
    def start_date(self) -> date:
        return self._settings.start_date

    @property
    def compression_factor(self) -> float:
        """Simulated seconds per real second."""
        return self._settings.compression_factor

    @property
    def tick_simulated_hours(self) -> float:
        """Simulated hours advanced by one telemetry tick.

        The integration step for cumulative energy: energy_kwh += power_kw * this.
        """
        return self.simulated_seconds(self._settings.telemetry_interval_seconds) / 3600.0

    def simulated_seconds(self, real_seconds: float) -> float:
        """Convert a real-time duration into simulated (event-time) seconds."""
        return real_seconds * self.compression_factor

    def real_seconds(self, simulated_seconds: float) -> float:
        """Convert a simulated (event-time) duration back into real seconds."""
        return simulated_seconds / self.compression_factor

    def start(self) -> None:
        """Mark the simulation's origin. Idempotent calls are a bug, so they fail."""
        if self._started_at is not None:
            raise ConfigError("SimulationClock.start() called twice; the origin would move.")
        self._started_at = self._monotonic()

    @property
    def started(self) -> bool:
        return self._started_at is not None

    def elapsed(self) -> float:
        """Real seconds since `start()`."""
        if self._started_at is None:
            raise ConfigError("SimulationClock.start() has not been called.")
        return self._monotonic() - self._started_at

    def now(self) -> SimulatedInstant:
        """The current simulated instant."""
        return self.instant_at(self.elapsed())

    def instant_at(self, real_elapsed_seconds: float) -> SimulatedInstant:
        """Map real elapsed seconds onto a simulated instant. Pure and total."""
        if real_elapsed_seconds < 0:
            raise ValueError(
                f"Elapsed time cannot be negative, got {real_elapsed_seconds}. "
                "The simulation has no time before its start."
            )

        day_seconds = self._settings.day_seconds
        day_index = int(real_elapsed_seconds // day_seconds)
        into_day = real_elapsed_seconds - day_index * day_seconds

        # Floating-point division can land `into_day` exactly on day_seconds,
        # which belongs to the next day rather than at progress 1.0.
        if into_day >= day_seconds:
            day_index += 1
            into_day = 0.0

        progress = into_day / day_seconds
        simulation_date = self._settings.start_date + timedelta(days=day_index)
        midnight = datetime.combine(simulation_date, datetime.min.time(), tzinfo=timezone.utc)

        return SimulatedInstant(
            real_elapsed_seconds=real_elapsed_seconds,
            day_index=day_index,
            simulation_date=simulation_date,
            progress=progress,
            timestamp=midnight + timedelta(seconds=progress * SECONDS_PER_SIMULATED_DAY),
            day_seconds=day_seconds,
        )
