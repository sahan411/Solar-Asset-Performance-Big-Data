"""Daily reference feed: validation and idempotent load.

The reference feed is where every financial figure in the platform ultimately
comes from — it carries each plant's expected generation and its commercial rate.
A silently wrong reference feed produces a confident, wrong revenue report, which
is worse than no report at all. So this module is deliberately strict: it
validates the whole file, reports *every* problem it finds rather than the first,
and refuses to load anything unless the feed is entirely sound.

The feed is produced by Member 1's generator (one file per simulated day) and its
schema is frozen in the master specification, section 9.2.

Read with pandas rather than Spark: it is one small file per day, and the Airflow
task that calls this runs in a plain Python process where starting a Spark
session would cost more than the work.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, Sequence

import pandas as pd

from processing.common.db import execute_batch
from processing.common.logging import get_logger

log = get_logger("batch-reference")

# Frozen contract, in file order. Renaming or dropping one is a team-level
# contract change, not a local edit.
REFERENCE_COLUMNS = (
    "simulation_date",
    "plant_id",
    "plant_capacity_kw",
    "expected_generation_kwh",
    "expected_peak_power_kw",
    "forecast_irradiance_kwh_m2",
    "ppa_rate_per_kwh",
    "maintenance_flag",
    "source_version",
)

NUMERIC_COLUMNS = (
    "plant_capacity_kw",
    "expected_generation_kwh",
    "expected_peak_power_kw",
    "forecast_irradiance_kwh_m2",
    "ppa_rate_per_kwh",
)

_TRUE_VALUES = {"true", "t", "yes", "y", "1"}
_FALSE_VALUES = {"false", "f", "no", "n", "0", ""}

# A plant generating more than this many equivalent full-power hours in a day is
# not a solar plant. Catches unit errors (MWh submitted as kWh) that would
# otherwise inflate the whole portfolio's expected revenue.
MAX_EQUIVALENT_SUN_HOURS = 14.0


class ReferenceFeedError(ValueError):
    """Raised when the daily reference feed cannot be trusted.

    Carries every problem found, not just the first, so one Airflow run tells the
    operator everything that needs fixing.
    """

    def __init__(self, problems: Sequence[str], source: str) -> None:
        self.problems = list(problems)
        self.source = source
        listed = "\n  - ".join(self.problems)
        super().__init__(
            f"Daily reference feed at {source} failed validation "
            f"({len(self.problems)} problem(s)):\n  - {listed}"
        )


@dataclass
class ReferenceFeed:
    """A validated daily reference feed, ready to load."""

    simulation_date: date
    rows: list[tuple]
    source: str
    warnings: list[str] = field(default_factory=list)

    @property
    def plant_count(self) -> int:
        return len(self.rows)


def reference_path(directory: str | Path, simulation_date: date) -> Path:
    """Conventional filename for a simulated day's feed."""
    return Path(directory) / f"daily_reference_{simulation_date.isoformat()}.csv"


def _parse_bool(raw: Any, row_label: str, problems: list[str]) -> bool | None:
    text = str(raw).strip().lower() if raw is not None else ""
    if text in _TRUE_VALUES:
        return True
    if text in _FALSE_VALUES or text == "nan":
        return False
    problems.append(f"{row_label}: maintenance_flag {raw!r} is not a boolean")
    return None


def _parse_number(raw: Any, column: str, row_label: str, problems: list[str]) -> float | None:
    try:
        value = float(raw)
    except (TypeError, ValueError):
        problems.append(f"{row_label}: {column} {raw!r} is not a number")
        return None
    if pd.isna(value):
        problems.append(f"{row_label}: {column} is missing")
        return None
    return value


def validate_reference_frame(
    frame: pd.DataFrame,
    expected_plants: Iterable[str],
    simulation_date: date | None = None,
    source: str = "<dataframe>",
) -> ReferenceFeed:
    """Validate a loaded feed against the frozen contract and the asset registry.

    `expected_plants` comes from the `plants` table: the feed must describe the
    portfolio the platform actually operates, no more and no less.
    """
    problems: list[str] = []
    warnings: list[str] = []

    missing_columns = [c for c in REFERENCE_COLUMNS if c not in frame.columns]
    if missing_columns:
        # Nothing further can be checked reliably without the contract columns.
        raise ReferenceFeedError(
            [f"missing required column(s): {', '.join(missing_columns)}"], source
        )

    unexpected = [c for c in frame.columns if c not in REFERENCE_COLUMNS]
    if unexpected:
        # Tolerated, not fatal: an added column is more likely a harmless
        # generator change than corruption, and failing the day's reconciliation
        # over it would be disproportionate.
        warnings.append(f"ignoring unexpected column(s): {', '.join(unexpected)}")

    if frame.empty:
        raise ReferenceFeedError(["the feed contains no rows"], source)

    rows: list[tuple] = []
    seen_plants: set[str] = set()
    observed_dates: set[date] = set()

    for position, record in enumerate(frame.to_dict("records"), start=1):
        plant_id = str(record.get("plant_id") or "").strip()
        row_label = f"row {position}" + (f" ({plant_id})" if plant_id else "")

        if not plant_id:
            problems.append(f"{row_label}: plant_id is missing")
            continue
        if plant_id in seen_plants:
            problems.append(f"{row_label}: duplicate row for plant {plant_id}")
            continue
        seen_plants.add(plant_id)

        row_date = _parse_date(record.get("simulation_date"), row_label, problems)
        if row_date is not None:
            observed_dates.add(row_date)

        values = {
            column: _parse_number(record.get(column), column, row_label, problems)
            for column in NUMERIC_COLUMNS
        }
        maintenance = _parse_bool(record.get("maintenance_flag"), row_label, problems)
        source_version = str(record.get("source_version") or "").strip()
        if not source_version:
            problems.append(f"{row_label}: source_version is missing")

        _check_physical_ranges(values, row_label, problems)

        if None in values.values() or maintenance is None or row_date is None:
            continue

        rows.append(
            (
                row_date,
                plant_id,
                values["plant_capacity_kw"],
                values["expected_generation_kwh"],
                values["expected_peak_power_kw"],
                values["forecast_irradiance_kwh_m2"],
                values["ppa_rate_per_kwh"],
                maintenance,
                source_version,
            )
        )

    _check_portfolio_coverage(seen_plants, expected_plants, problems)
    feed_date = _check_single_date(observed_dates, simulation_date, problems)

    if problems:
        raise ReferenceFeedError(problems, source)

    for warning in warnings:
        log.warning("reference_feed_warning", warning, source=source)

    return ReferenceFeed(
        simulation_date=feed_date, rows=rows, source=source, warnings=warnings
    )


def _parse_date(raw: Any, row_label: str, problems: list[str]) -> date | None:
    text = str(raw).strip() if raw is not None else ""
    if not text:
        problems.append(f"{row_label}: simulation_date is missing")
        return None
    try:
        return datetime.strptime(text[:10], "%Y-%m-%d").date()
    except ValueError:
        problems.append(f"{row_label}: simulation_date {raw!r} is not an ISO date")
        return None


def _check_physical_ranges(
    values: dict[str, float | None], row_label: str, problems: list[str]
) -> None:
    capacity = values.get("plant_capacity_kw")
    generation = values.get("expected_generation_kwh")
    peak = values.get("expected_peak_power_kw")
    irradiance = values.get("forecast_irradiance_kwh_m2")
    rate = values.get("ppa_rate_per_kwh")

    if capacity is not None and capacity <= 0:
        problems.append(f"{row_label}: plant_capacity_kw must be greater than zero")
    if generation is not None and generation <= 0:
        problems.append(f"{row_label}: expected_generation_kwh must be greater than zero")
    if peak is not None and peak <= 0:
        problems.append(f"{row_label}: expected_peak_power_kw must be greater than zero")
    if irradiance is not None and irradiance < 0:
        problems.append(f"{row_label}: forecast_irradiance_kwh_m2 must not be negative")
    if rate is not None and rate < 0:
        problems.append(f"{row_label}: ppa_rate_per_kwh must not be negative")

    # A plant cannot be expected to peak above its own nameplate rating.
    if capacity and peak and peak > capacity:
        problems.append(
            f"{row_label}: expected_peak_power_kw ({peak:g}) exceeds "
            f"plant_capacity_kw ({capacity:g})"
        )

    # Guards against a unit error turning MWh into kWh and inflating revenue.
    if capacity and generation:
        equivalent_hours = generation / capacity
        if equivalent_hours > MAX_EQUIVALENT_SUN_HOURS:
            problems.append(
                f"{row_label}: expected_generation_kwh ({generation:g}) implies "
                f"{equivalent_hours:.1f} equivalent full-power hours, which is not "
                f"physically plausible (limit {MAX_EQUIVALENT_SUN_HOURS:g})"
            )


def _check_portfolio_coverage(
    seen: set[str], expected_plants: Iterable[str], problems: list[str]
) -> None:
    """The feed must describe exactly the portfolio the platform operates.

    A missing plant silently drops it from the day's revenue; an unknown plant
    would violate the foreign key on load. Both are caught here with a clear
    message rather than as a database error later.
    """
    expected = set(expected_plants)
    if not expected:
        return

    missing = sorted(expected - seen)
    unknown = sorted(seen - expected)
    if missing:
        problems.append(f"no reference row for configured plant(s): {', '.join(missing)}")
    if unknown:
        problems.append(f"reference rows for unknown plant(s): {', '.join(unknown)}")


def _check_single_date(
    observed: set[date], expected: date | None, problems: list[str]
) -> date | None:
    if len(observed) > 1:
        problems.append(
            "feed mixes multiple simulation_dates: "
            + ", ".join(d.isoformat() for d in sorted(observed))
        )
        return None
    if not observed:
        return expected

    feed_date = next(iter(observed))
    if expected is not None and feed_date != expected:
        problems.append(
            f"feed is for {feed_date.isoformat()} but {expected.isoformat()} was requested"
        )
    return feed_date


def load_reference_file(
    path: str | Path,
    expected_plants: Iterable[str],
    simulation_date: date | None = None,
) -> ReferenceFeed:
    """Read and validate one daily reference CSV."""
    source = str(path)
    if not Path(path).is_file():
        raise ReferenceFeedError(
            [
                "file not found — it is produced by Member 1's daily generator into "
                "SIMULATION_OUTPUT_DIR"
            ],
            source,
        )

    # Everything is read as text and converted explicitly: letting pandas infer
    # types would quietly coerce a malformed number to NaN, which is exactly the
    # corruption this module exists to catch.
    frame = pd.read_csv(path, dtype=str, keep_default_na=False)
    return validate_reference_frame(frame, expected_plants, simulation_date, source)


_UPSERT_REFERENCE = """
INSERT INTO daily_reference (
    simulation_date, plant_id, plant_capacity_kw, expected_generation_kwh,
    expected_peak_power_kw, forecast_irradiance_kwh_m2, ppa_rate_per_kwh,
    maintenance_flag, source_version, loaded_at
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
ON CONFLICT (simulation_date, plant_id) DO UPDATE SET
    plant_capacity_kw          = EXCLUDED.plant_capacity_kw,
    expected_generation_kwh    = EXCLUDED.expected_generation_kwh,
    expected_peak_power_kw     = EXCLUDED.expected_peak_power_kw,
    forecast_irradiance_kwh_m2 = EXCLUDED.forecast_irradiance_kwh_m2,
    ppa_rate_per_kwh           = EXCLUDED.ppa_rate_per_kwh,
    maintenance_flag           = EXCLUDED.maintenance_flag,
    source_version             = EXCLUDED.source_version,
    loaded_at                  = NOW()
"""


def load_reference_into_db(conn, feed: ReferenceFeed) -> int:
    """Upsert a validated feed, inside the caller's transaction.

    Upsert rather than insert so re-running a day's DAG — after a demo reset, or
    because the generator reissued a corrected feed — converges on the current
    values instead of failing on the primary key.
    """
    count = execute_batch(conn, _UPSERT_REFERENCE, feed.rows)
    log.info(
        "reference_feed_loaded",
        f"Loaded {count} reference row(s) for {feed.simulation_date.isoformat()}",
        simulation_date=feed.simulation_date.isoformat(),
        plants=count,
        source=feed.source,
    )
    return count


def configured_plant_ids(conn) -> list[str]:
    """Active plant ids from the registry, for coverage validation."""
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM plants WHERE active ORDER BY id")
        return [row[0] for row in cur.fetchall()]
