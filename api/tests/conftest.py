"""Test setup for the SolarIQ serving API.

The repository root's pytest.ini puts the repo root on sys.path (for
`processing`/`storage`), but api/app imports itself as the top-level package
`app` (e.g. `from app.config import Settings`) — the layout FastAPI's own
tutorials use. That requires `api/` itself on sys.path too, which this
conftest adds before any test module imports `app.*`.

No real PostgreSQL is used here: route tests override FastAPI dependencies
directly (see api/tests/test_*_router.py), so they never touch app.state or
the lifespan handler. Seeded-database checks live in tests/integration/,
gated by SOLARIQ_TEST_DATABASE_URL like the rest of the project's integration
suite.
"""

from __future__ import annotations

import sys
from pathlib import Path

API_ROOT = Path(__file__).resolve().parents[1]
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))
