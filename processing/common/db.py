"""PostgreSQL access helpers for the SolarIQ processing subsystem.

Small on purpose. Member 2 owns the schema and writes to it from two places
(the Spark stream sink and the Airflow batch tasks); both want the same thing:
a short-lived connection, one transaction, parameterised batch statements.

There is no ORM here. Migrations are the authoritative schema definition, and an
ORM model layer would duplicate it — the specification explicitly warns against
a second schema owner.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterable, Iterator, Sequence

import psycopg2
import psycopg2.extras

from processing.common.logging import get_logger

log = get_logger("processing-db")


@contextmanager
def connect(database_url: str) -> Iterator[Any]:
    """Open a connection wrapped in a single transaction.

    Commits on clean exit, rolls back on any exception, and always closes. This
    is what makes the stream sink safe to retry: a microbatch either lands
    completely or not at all, so Spark can replay it without leaving half-written
    metric rows behind.
    """
    conn = psycopg2.connect(database_url)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        # Re-raised: swallowing a write failure would let the pipeline silently
        # report healthy while producing no data.
        raise
    finally:
        conn.close()


def execute_batch(
    conn: Any,
    statement: str,
    rows: Sequence[Sequence[Any]],
    page_size: int = 500,
) -> int:
    """Run one parameterised statement over many rows in few round trips.

    Returns the number of rows submitted. `rows` must be a sequence of value
    tuples matching the statement's placeholders — never interpolate values into
    the SQL string.
    """
    if not rows:
        return 0

    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, statement, rows, page_size=page_size)
    return len(rows)


def fetch_all(conn: Any, statement: str, params: Iterable[Any] | None = None) -> list[tuple]:
    """Run a parameterised query and return all rows."""
    with conn.cursor() as cur:
        cur.execute(statement, tuple(params) if params is not None else None)
        return cur.fetchall()


def fetch_one(conn: Any, statement: str, params: Iterable[Any] | None = None) -> tuple | None:
    """Run a parameterised query and return the first row, or None."""
    with conn.cursor() as cur:
        cur.execute(statement, tuple(params) if params is not None else None)
        return cur.fetchone()


def ping(database_url: str) -> bool:
    """Cheap connectivity probe used by health reporting and tooling."""
    try:
        with connect(database_url) as conn:
            fetch_one(conn, "SELECT 1")
        return True
    except psycopg2.Error as exc:
        log.error("database_unreachable", "Could not reach PostgreSQL", error=str(exc))
        return False
