"""Forward-only SQL migration runner for the SolarIQ serving schema.

Why plain SQL files rather than Alembic: the schema is small, fixed by the team's
frozen data contract, and read by three subsystems written in different styles.
Explicit DDL is easier to review, easier to defend in a viva, and avoids
introducing an ORM model layer that would compete with these files for ownership
of the schema.

Guarantees:
  * migrations run in filename order, each in its own transaction (PostgreSQL DDL
    is transactional, so a failing migration leaves no partial schema behind);
  * already-applied migrations are skipped, so running this repeatedly is safe
    and it can be wired into container startup;
  * an already-applied migration whose contents have since changed is a hard
    error — silently diverging environments are worse than a failed deploy.

Usage (from the repository root):

    python -m storage.migrate                 # apply pending migrations
    python -m storage.migrate --dry-run       # show what would run
    python -m storage.migrate --status        # show applied/pending
"""

from __future__ import annotations

import argparse
import hashlib
import sys
from dataclasses import dataclass
from pathlib import Path

from processing.common.config import ConfigError, DatabaseSettings
from processing.common.db import connect, fetch_all
from processing.common.logging import get_logger

log = get_logger("storage-migrate")

MIGRATIONS_DIR = Path(__file__).resolve().parent / "migrations"

# Tracking table. Created by the runner itself rather than by a migration,
# because it has to exist before the first migration can be recorded.
_MIGRATIONS_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version    TEXT PRIMARY KEY,
    name       TEXT NOT NULL,
    checksum   TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""


@dataclass(frozen=True)
class Migration:
    """One `NNN_name.sql` file on disk."""

    version: str
    name: str
    path: Path
    sql: str

    @property
    def checksum(self) -> str:
        """Content hash, used to detect edits to already-applied migrations."""
        return hashlib.sha256(self.sql.encode("utf-8")).hexdigest()


def discover_migrations(directory: Path = MIGRATIONS_DIR) -> list[Migration]:
    """Load every `*.sql` migration, ordered by its numeric prefix.

    Filenames must look like `001_core_assets.sql`; the numeric prefix is the
    version and defines the order.
    """
    if not directory.is_dir():
        raise ConfigError(f"Migrations directory not found: {directory}")

    migrations: list[Migration] = []
    for path in sorted(directory.glob("*.sql")):
        version, separator, name = path.stem.partition("_")
        if not separator or not version.isdigit():
            raise ConfigError(
                f"Migration filename {path.name!r} must start with a numeric prefix, "
                "e.g. 001_core_assets.sql"
            )
        migrations.append(
            Migration(
                version=version,
                name=name,
                path=path,
                sql=path.read_text(encoding="utf-8"),
            )
        )

    versions = [m.version for m in migrations]
    duplicates = {v for v in versions if versions.count(v) > 1}
    if duplicates:
        raise ConfigError(f"Duplicate migration version(s): {sorted(duplicates)}")

    return migrations


def _ensure_tracking_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(_MIGRATIONS_TABLE_DDL)


def _applied(conn) -> dict[str, str]:
    """Map of already-applied version -> checksum recorded at apply time."""
    rows = fetch_all(conn, "SELECT version, checksum FROM schema_migrations")
    return {version: checksum for version, checksum in rows}


def _assert_no_drift(migrations: list[Migration], applied: dict[str, str]) -> None:
    """Fail if an applied migration's file has been edited since it ran.

    Editing applied migrations makes a fresh database and an existing one
    disagree about the schema. The fix is always a new migration file.
    """
    drifted = [
        m.path.name
        for m in migrations
        if m.version in applied and applied[m.version] != m.checksum
    ]
    if drifted:
        raise ConfigError(
            "These migrations were already applied but their contents have changed: "
            f"{', '.join(drifted)}. Add a new migration instead of editing an applied one."
        )


def pending_migrations(migrations: list[Migration], applied: dict[str, str]) -> list[Migration]:
    return [m for m in migrations if m.version not in applied]


def _apply(conn, migration: Migration) -> None:
    with conn.cursor() as cur:
        cur.execute(migration.sql)
        cur.execute(
            "INSERT INTO schema_migrations (version, name, checksum) VALUES (%s, %s, %s)",
            (migration.version, migration.name, migration.checksum),
        )
    # Commit per migration so an earlier success is preserved if a later one fails.
    conn.commit()


def run_migrations(
    database_url: str,
    directory: Path = MIGRATIONS_DIR,
    dry_run: bool = False,
) -> list[str]:
    """Apply pending migrations. Returns the versions applied (or that would be)."""
    migrations = discover_migrations(directory)

    with connect(database_url) as conn:
        _ensure_tracking_table(conn)
        conn.commit()

        applied = _applied(conn)
        _assert_no_drift(migrations, applied)
        pending = pending_migrations(migrations, applied)

        if not pending:
            log.info("migrations_up_to_date", "Schema is already up to date", applied=len(applied))
            return []

        if dry_run:
            log.info(
                "migrations_pending",
                f"{len(pending)} migration(s) would be applied",
                versions=[m.version for m in pending],
            )
            return [m.version for m in pending]

        for migration in pending:
            log.info(
                "migration_applying",
                f"Applying {migration.path.name}",
                version=migration.version,
            )
            _apply(conn, migration)

        log.info(
            "migrations_applied",
            f"Applied {len(pending)} migration(s)",
            versions=[m.version for m in pending],
        )
        return [m.version for m in pending]


def print_status(database_url: str, directory: Path = MIGRATIONS_DIR) -> None:
    migrations = discover_migrations(directory)
    with connect(database_url) as conn:
        _ensure_tracking_table(conn)
        conn.commit()
        applied = _applied(conn)

    for migration in migrations:
        state = "applied" if migration.version in applied else "pending"
        print(f"  [{state:>7}] {migration.path.name}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Apply SolarIQ serving-schema migrations.")
    parser.add_argument(
        "--dry-run", action="store_true", help="List pending migrations without applying them."
    )
    parser.add_argument(
        "--status", action="store_true", help="Show applied and pending migrations."
    )
    args = parser.parse_args(argv)

    try:
        database_url = DatabaseSettings.from_env().url
        if args.status:
            print_status(database_url)
        else:
            run_migrations(database_url, dry_run=args.dry_run)
    except ConfigError as exc:
        # Configuration problems are user error: report them plainly, without a
        # traceback that would bury the actual message.
        log.error("migration_config_error", str(exc))
        return 2
    except Exception as exc:  # noqa: BLE001 - top-level CLI boundary
        log.exception("migration_failed", f"Migration run failed: {exc}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
