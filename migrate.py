"""Apply SQL migrations in pipeline/db/migrations in filename order, once each."""

from __future__ import annotations

import logging
from pathlib import Path

import psycopg

log = logging.getLogger(__name__)

MIGRATIONS_DIR = Path(__file__).parent / "migrations"


def migrate(conn: psycopg.Connection, directory: Path = MIGRATIONS_DIR) -> list[str]:
    """Apply pending migrations, each in its own transaction. Returns the names applied."""
    with conn.transaction():
        conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations ("
            " name text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())"
        )
    done = {row[0] for row in conn.execute("SELECT name FROM schema_migrations")}
    applied: list[str] = []
    for path in sorted(directory.glob("*.sql")):
        if path.name in done:
            continue
        with conn.transaction():
            conn.execute(path.read_text(encoding="utf-8"))
            conn.execute("INSERT INTO schema_migrations (name) VALUES (%s)", (path.name,))
        log.info("applied migration %s", path.name)
        applied.append(path.name)
    return applied


if __name__ == "__main__":
    from pipeline.config import Settings

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    url = Settings.from_env().database_url
    if not url:
        raise SystemExit("DATABASE_URL is not set")
    from pipeline.store import connect

    with connect(url) as connection:
        names = migrate(connection)
    print(f"applied {len(names)} migration(s)")
