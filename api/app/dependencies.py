"""FastAPI dependencies shared across routers.

Everything here reads from `request.app.state`, which the lifespan handler in
main.py populates once at startup. Routes depend on `get_connection` (a live
psycopg2 connection for the request) rather than the pool itself, so
repository functions never need to know about pooling.
"""

from __future__ import annotations

from typing import Any, Iterator

from fastapi import Request

from app.config import Settings
from app.db import Database


def get_settings(request: Request) -> Settings:
    return request.app.state.settings


def get_database(request: Request) -> Database:
    return request.app.state.database


def get_connection(request: Request) -> Iterator[Any]:
    """One pooled connection for the lifetime of a single request."""
    database: Database = request.app.state.database
    with database.connection() as conn:
        yield conn
