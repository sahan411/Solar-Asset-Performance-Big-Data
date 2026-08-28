"""PostgreSQL access for the SolarIQ serving API.

A small pooled connection layer over psycopg2 — the same driver Member 2 uses
for the stream sink and batch tasks, so the project has one Postgres driver
rather than two. There is no ORM: storage/migrations/ is the single owner of
the schema, and repositories issue parameterized SQL directly against it.

The pool is thread-backed (`ThreadedConnectionPool`) because FastAPI runs sync
route handlers in a worker thread pool; a single-threaded pool would serialize
every request on one connection.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator

import psycopg2
import psycopg2.extras
import psycopg2.pool

from app.logging import get_logger

log = get_logger("api-db")


class Database:
    """Owns a connection pool for one PostgreSQL DSN."""

    def __init__(self, database_url: str, minconn: int = 1, maxconn: int = 10) -> None:
        self._database_url = database_url
        self._pool = psycopg2.pool.ThreadedConnectionPool(minconn, maxconn, database_url)

    @contextmanager
    def connection(self) -> Iterator[Any]:
        """One connection, one transaction: commits on success, rolls back on error."""
        conn = self._pool.getconn()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self._pool.putconn(conn)

    def ping(self) -> bool:
        """Cheap connectivity probe. Never raises."""
        try:
            with self.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
            return True
        except psycopg2.Error as exc:
            log.error("database_unreachable", "Could not reach PostgreSQL", error=str(exc))
            return False

    def key_table_query_ok(self) -> bool:
        """The '/ready' contract requires more than a bare connection: a real query
        against a table the API depends on must also succeed."""
        try:
            with self.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1 FROM plants LIMIT 1")
            return True
        except psycopg2.Error as exc:
            log.error("readiness_query_failed", "Readiness query against plants failed", error=str(exc))
            return False

    def close(self) -> None:
        self._pool.closeall()


@contextmanager
def dict_cursor(conn: Any) -> Iterator[Any]:
    """A cursor that returns rows as dict-like objects, keyed by column name."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        yield cur
