"""Tests for the compressed simulated clock.

The clock decides what goes in every event's `timestamp`, so these tests pin the
real-time-to-event-time mapping precisely. The integration risk they guard is
specific: Member 2 windows on event time and sustains alerts for 3600 event-time
seconds. If a simulated day did not span a full 24 hours of event time, no
window would fill and no alert would fire.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from simulators.common.config import ConfigError, SimulationSettings
from simulators.common.time import SimulationClock, to_iso, utc_now


def settings(day_seconds: float = 300.0, interval: float = 3.0) -> SimulationSettings:
    return SimulationSettings(
        day_seconds=day_seconds,
        telemetry_interval_seconds=interval,
        seed=8203,
        start_date=date(2026, 8, 21),
        output_dir=Path("/data/daily"),
        portfolio_config_path=Path("simulators/config/portfolio.yaml"),
        emit_invalid_events=False,
    )


class FakeMonotonic:
    """A hand-cranked clock, so tests never sleep."""

    def __init__(self, start: float = 1000.0) -> None:
        self.value = start

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


@pytest.fixture
def clock():
    return SimulationClock(settings())


class TestIsoFormatting:
    def test_matches_the_contract_format(self):
        moment = datetime(2026, 8, 21, 5, 0, 0, tzinfo=timezone.utc)
        assert to_iso(moment) == "2026-08-21T05:00:00Z"

    def test_sub_second_precision_is_dropped(self):
        moment = datetime(2026, 8, 21, 5, 0, 0, 123456, tzinfo=timezone.utc)
        assert to_iso(moment) == "2026-08-21T05:00:00Z"

    def test_naive_datetimes_are_refused(self):
        # A naive datetime would be serialised as if it were UTC and silently
        # shift every event by the local offset.
        with pytest.raises(ValueError, match="naive datetime"):
            to_iso(datetime(2026, 8, 21, 5, 0, 0))

    def test_non_utc_input_is_converted_not_relabelled(self):
        from datetime import timedelta

        kolkata = timezone(timedelta(hours=5, minutes=30))
        moment = datetime(2026, 8, 21, 10, 30, 0, tzinfo=kolkata)
        assert to_iso(moment) == "2026-08-21T05:00:00Z"

    def test_utc_now_is_timezone_aware(self):
        assert utc_now().tzinfo is not None


class TestTheMapping:
    def test_day_begins_at_simulated_midnight(self, clock):
        instant = clock.instant_at(0.0)

        assert instant.day_index == 0
        assert instant.simulation_date == date(2026, 8, 21)
        assert instant.progress == 0.0
        assert instant.iso_timestamp == "2026-08-21T00:00:00Z"

    def test_halfway_through_the_day_is_simulated_noon(self, clock):
        instant = clock.instant_at(150.0)

        assert instant.progress == 0.5
        assert instant.iso_timestamp == "2026-08-21T12:00:00Z"

    def test_a_simulated_day_spans_a_full_24_hours_of_event_time(self, clock):
        # The property Member 2's event-time windows depend on.
        start = clock.instant_at(0.0).timestamp
        nearly_over = clock.instant_at(299.999).timestamp

        assert (nearly_over - start).total_seconds() == pytest.approx(86_400, rel=1e-4)

    def test_one_real_second_advances_event_time_by_the_compression_factor(self, clock):
        first = clock.instant_at(10.0).timestamp
        second = clock.instant_at(11.0).timestamp

        assert (second - first).total_seconds() == pytest.approx(288.0)

    @pytest.mark.parametrize(
        "elapsed,expected",
        [
            (0.0, "2026-08-21T00:00:00Z"),
            (75.0, "2026-08-21T06:00:00Z"),
            (150.0, "2026-08-21T12:00:00Z"),
            (225.0, "2026-08-21T18:00:00Z"),
            (300.0, "2026-08-22T00:00:00Z"),
            (450.0, "2026-08-22T12:00:00Z"),
        ],
    )
    def test_known_points_on_the_timeline(self, clock, elapsed, expected):
        assert clock.instant_at(elapsed).iso_timestamp == expected

    def test_the_mapping_is_deterministic(self, clock):
        assert clock.instant_at(123.456) == clock.instant_at(123.456)

    def test_negative_elapsed_time_is_refused(self, clock):
        with pytest.raises(ValueError, match="cannot be negative"):
            clock.instant_at(-0.001)


class TestDayBoundaries:
    def test_the_day_rolls_over_exactly_at_the_boundary(self, clock):
        end_of_day = clock.instant_at(299.999)
        start_of_next = clock.instant_at(300.0)

        assert end_of_day.day_index == 0
        assert end_of_day.simulation_date == date(2026, 8, 21)

        assert start_of_next.day_index == 1
        assert start_of_next.simulation_date == date(2026, 8, 22)
        # Not progress 1.0 of the old day: the boundary belongs to the new one,
        # which is what makes the energy reset and the daily file unambiguous.
        assert start_of_next.progress == 0.0

    def test_progress_never_reaches_one(self, clock):
        for elapsed in (299.0, 299.9, 299.999, 299.9999):
            assert 0.0 <= clock.instant_at(elapsed).progress < 1.0

    def test_later_days_keep_advancing_the_calendar(self, clock):
        assert clock.instant_at(300.0 * 5).simulation_date == date(2026, 8, 26)
        assert clock.instant_at(300.0 * 5).day_index == 5

    def test_the_day_boundary_crosses_a_month_end(self):
        # 2026-08-31 -> 2026-09-01, the arithmetic most likely to be wrong.
        month_end = SimulationClock(
            SimulationSettings(
                day_seconds=300.0,
                telemetry_interval_seconds=3.0,
                seed=1,
                start_date=date(2026, 8, 31),
                output_dir=Path("/data/daily"),
                portfolio_config_path=Path("p.yaml"),
                emit_invalid_events=False,
            )
        )
        assert month_end.instant_at(300.0).simulation_date == date(2026, 9, 1)

    def test_seconds_into_day_restarts_each_day(self, clock):
        assert clock.instant_at(60.0).seconds_into_day == pytest.approx(60.0)
        # Same point in the second simulated day.
        assert clock.instant_at(360.0).seconds_into_day == pytest.approx(60.0)


class TestDurationConversion:
    def test_simulated_and_real_durations_round_trip(self, clock):
        assert clock.simulated_seconds(1.0) == 288.0
        assert clock.real_seconds(288.0) == 1.0
        assert clock.real_seconds(clock.simulated_seconds(42.0)) == pytest.approx(42.0)

    def test_tick_advances_energy_by_the_right_number_of_hours(self, clock):
        # 3 real seconds x 288 = 864 simulated seconds = 0.24 simulated hours.
        # This is the multiplier the energy integration in milestone 3 uses.
        assert clock.tick_simulated_hours == pytest.approx(0.24)

    def test_a_full_day_of_ticks_integrates_to_24_hours(self, clock):
        ticks = settings().ticks_per_day
        assert ticks * clock.tick_simulated_hours == pytest.approx(24.0)

    def test_alert_sustain_window_fits_inside_a_simulated_day(self, clock):
        # Member 2 holds an alert for 3600 event-time seconds. Under this clock
        # that must be a small fraction of a real demo, or nothing fires on time.
        assert clock.real_seconds(3600) == pytest.approx(12.5)


class TestRunningTheClock:
    def test_now_tracks_the_injected_monotonic_source(self):
        fake = FakeMonotonic()
        clock = SimulationClock(settings(), monotonic=fake)
        clock.start()

        assert clock.now().progress == 0.0
        fake.advance(150.0)
        assert clock.now().iso_timestamp == "2026-08-21T12:00:00Z"
        fake.advance(150.0)
        assert clock.now().simulation_date == date(2026, 8, 22)

    def test_elapsed_before_start_is_an_error(self, clock):
        with pytest.raises(ConfigError, match="has not been called"):
            clock.elapsed()

    def test_starting_twice_is_an_error(self):
        clock = SimulationClock(settings(), monotonic=FakeMonotonic())
        clock.start()
        # Restarting mid-run would silently rewind event time and duplicate a day.
        with pytest.raises(ConfigError, match="called twice"):
            clock.start()

    def test_started_reports_state(self):
        clock = SimulationClock(settings(), monotonic=FakeMonotonic())
        assert clock.started is False
        clock.start()
        assert clock.started is True


class TestAlternativeClockSpeeds:
    def test_a_real_time_clock_maps_one_to_one(self):
        clock = SimulationClock(settings(day_seconds=86_400.0, interval=3.0))

        assert clock.compression_factor == 1.0
        assert clock.instant_at(3600.0).iso_timestamp == "2026-08-21T01:00:00Z"

    def test_a_faster_demo_still_spans_a_full_day(self):
        clock = SimulationClock(settings(day_seconds=120.0, interval=3.0))

        assert clock.instant_at(60.0).iso_timestamp == "2026-08-21T12:00:00Z"
        assert clock.instant_at(120.0).simulation_date == date(2026, 8, 22)
