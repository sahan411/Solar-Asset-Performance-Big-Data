"""PostgreSQL sink for the SolarIQ speed layer.

Writes happen once per microbatch through `foreachBatch`: one connection, one
transaction, one batched statement. Opening a connection per row would spend
more time on handshakes than on work and would leave partial state behind on
failure.

IDEMPOTENCY
Every write is an upsert keyed on the metric's window identity. Structured
Streaming guarantees at-least-once delivery to `foreachBatch`, so a batch *will*
occasionally be replayed after a failure. Replaying recomputes identical rows
under identical keys, so the second write overwrites the first instead of
duplicating it.

TIMESTAMPS
Timestamp columns are formatted to UTC strings inside Spark before collection.
PySpark converts TimestampType to a timezone-naive datetime in the *host's*
local zone when collecting to Python, so handing collected datetimes to a
TIMESTAMPTZ column would silently shift every window boundary by the host's
offset. Formatting in Spark (which honours the UTC session timezone) and
appending an explicit "+00" removes the ambiguity entirely.
"""

from __future__ import annotations

from typing import Any, Sequence

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat, date_format, lit

from processing.common.db import connect, execute_batch
from processing.common.logging import get_logger
from processing.streaming.metrics import PLANT_METRIC_COLUMNS, PORTFOLIO_METRIC_COLUMNS

log = get_logger("spark-stream")

PLANT_METRICS_TABLE = "live_plant_metrics"
PLANT_METRICS_KEY = ("plant_id", "window_start", "window_end")

PORTFOLIO_METRICS_TABLE = "live_portfolio_metrics"
PORTFOLIO_METRICS_KEY = ("window_start", "window_end")

# Columns needing the UTC-string treatment described in the module docstring.
TIMESTAMP_COLUMNS = ("window_start", "window_end")

# Postgres parses this unambiguously as an instant, whatever the server's
# timezone setting happens to be.
_UTC_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS"
_UTC_SUFFIX = "+00"


def with_utc_timestamp_strings(df: DataFrame, timestamp_columns: Sequence[str]) -> DataFrame:
    """Render timestamp columns as explicit UTC strings, inside Spark."""
    for column in timestamp_columns:
        df = df.withColumn(
            column, concat(date_format(col(column), _UTC_FORMAT), lit(_UTC_SUFFIX))
        )
    return df


def collect_rows(df: DataFrame, columns: Sequence[str]) -> list[tuple]:
    """Collect a small aggregate DataFrame into value tuples for psycopg2.

    Only ever applied to aggregated output — at most one row per plant per
    microbatch — never to raw telemetry, which stays distributed.
    """
    prepared = with_utc_timestamp_strings(df, TIMESTAMP_COLUMNS).select(*columns)
    return [tuple(row) for row in prepared.collect()]


def build_upsert(
    table: str, columns: Sequence[str], conflict_columns: Sequence[str]
) -> str:
    """Build an INSERT ... ON CONFLICT DO UPDATE statement.

    Table and column names are module constants tied to the migrations, never
    user input, so interpolating them is safe. Values are always passed as bound
    parameters.
    """
    updatable = [c for c in columns if c not in conflict_columns]
    assignments = ", ".join(f"{c} = EXCLUDED.{c}" for c in updatable)
    return (
        f"INSERT INTO {table} ({', '.join(columns)}) "
        f"VALUES ({', '.join(['%s'] * len(columns))}) "
        f"ON CONFLICT ({', '.join(conflict_columns)}) DO UPDATE SET "
        f"{assignments}, updated_at = NOW()"
    )


UPSERT_PLANT_METRICS = build_upsert(
    PLANT_METRICS_TABLE, PLANT_METRIC_COLUMNS, PLANT_METRICS_KEY
)
UPSERT_PORTFOLIO_METRICS = build_upsert(
    PORTFOLIO_METRICS_TABLE, PORTFOLIO_METRIC_COLUMNS, PORTFOLIO_METRICS_KEY
)


def write_live_metrics(
    database_url: str,
    plant_rows: Sequence[Sequence[Any]],
    portfolio_rows: Sequence[Sequence[Any]],
) -> tuple[int, int]:
    """Persist one microbatch's metrics in a single transaction.

    Plant and portfolio rows are written together so the serving layer can never
    read a portfolio total that disagrees with the plant rows it was derived
    from.
    """
    if not plant_rows and not portfolio_rows:
        return 0, 0

    with connect(database_url) as conn:
        plants = execute_batch(conn, UPSERT_PLANT_METRICS, list(plant_rows))
        portfolio = execute_batch(conn, UPSERT_PORTFOLIO_METRICS, list(portfolio_rows))

    return plants, portfolio
