"""Migration runner behaviour, and structural checks on the real migration set."""

from __future__ import annotations

from pathlib import Path

import pytest

from processing.common.config import ConfigError
from storage.migrate import (
    MIGRATIONS_DIR,
    discover_migrations,
    pending_migrations,
    _assert_no_drift,
)

# Tables the rest of the team codes against. If one disappears from the schema,
# Member 3's API and the Airflow DAG break — so assert on the frozen contract.
CONTRACT_TABLES = (
    "plants",
    "inverters",
    "live_plant_metrics",
    "live_portfolio_metrics",
    "alerts",
    "daily_reference",
    "daily_plant_summary",
    "pipeline_health",
)


def test_real_migrations_are_discoverable_and_ordered():
    migrations = discover_migrations()

    assert migrations, "expected at least one migration file"
    versions = [m.version for m in migrations]
    assert versions == sorted(versions), "migrations must be returned in version order"
    assert len(set(versions)) == len(versions), "migration versions must be unique"


def test_every_contract_table_is_created_by_a_migration():
    combined = "\n".join(m.sql for m in discover_migrations()).lower()
    for table in CONTRACT_TABLES:
        assert f"create table if not exists {table}" in combined, f"{table} is not created"


def test_migrations_are_rerunnable_by_construction():
    """Every DDL statement must be guarded, so applying twice is harmless."""
    for migration in discover_migrations():
        lowered = migration.sql.lower()
        assert "create table " not in lowered.replace("create table if not exists ", ""), (
            f"{migration.path.name} has an unguarded CREATE TABLE"
        )
        assert "create index " not in lowered.replace("create index if not exists ", "").replace(
            "create unique index if not exists ", ""
        ), f"{migration.path.name} has an unguarded CREATE INDEX"


def test_checksum_changes_when_content_changes(tmp_path: Path):
    (tmp_path / "001_a.sql").write_text("CREATE TABLE IF NOT EXISTS a (id TEXT);", encoding="utf-8")
    first = discover_migrations(tmp_path)[0].checksum

    (tmp_path / "001_a.sql").write_text("CREATE TABLE IF NOT EXISTS a (id INT);", encoding="utf-8")
    second = discover_migrations(tmp_path)[0].checksum

    assert first != second


def test_pending_excludes_already_applied_versions(tmp_path: Path):
    (tmp_path / "001_a.sql").write_text("SELECT 1;", encoding="utf-8")
    (tmp_path / "002_b.sql").write_text("SELECT 2;", encoding="utf-8")
    migrations = discover_migrations(tmp_path)

    applied = {"001": migrations[0].checksum}
    assert [m.version for m in pending_migrations(migrations, applied)] == ["002"]


def test_editing_an_applied_migration_is_rejected(tmp_path: Path):
    (tmp_path / "001_a.sql").write_text("SELECT 1;", encoding="utf-8")
    migrations = discover_migrations(tmp_path)

    # Simulate a database that recorded a different checksum for version 001.
    with pytest.raises(ConfigError, match="contents have changed"):
        _assert_no_drift(migrations, {"001": "a-different-checksum"})


def test_unversioned_filenames_are_rejected(tmp_path: Path):
    (tmp_path / "create_stuff.sql").write_text("SELECT 1;", encoding="utf-8")
    with pytest.raises(ConfigError, match="numeric prefix"):
        discover_migrations(tmp_path)


def test_missing_directory_is_reported_clearly(tmp_path: Path):
    with pytest.raises(ConfigError, match="Migrations directory not found"):
        discover_migrations(tmp_path / "nope")


def test_alerts_schema_enforces_one_active_alert_per_asset():
    """The anti-spam guarantee is a DB constraint, not just stream-job discipline."""
    sql = (MIGRATIONS_DIR / "003_alerts.sql").read_text(encoding="utf-8").lower()
    assert "unique index" in sql
    assert "where status = 'active'" in sql
