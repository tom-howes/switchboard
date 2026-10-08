"""Apply migrations/*.sql in filename order, once each.

Deliberately about forty lines rather than Alembic. Migrations here are plain
SQL files that can be read without knowing a migration framework, which matters
for a repo whose point is that the SQL is worth reading.
"""

from __future__ import annotations

import os
import pathlib
import sys

import psycopg
from dotenv import load_dotenv

ROOT = pathlib.Path(__file__).resolve().parent.parent
MIGRATIONS = ROOT / "migrations"

LEDGER = """
CREATE TABLE IF NOT EXISTS schema_migration (
    filename   text PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT now()
)
"""


def apply(url: str) -> list[str]:
    """Apply whatever has not been applied yet. Returns the filenames applied.

    A function rather than only a script so the test suite can build its own
    database from the same files production uses, instead of a second schema
    that drifts.
    """
    applied: list[str] = []
    with psycopg.connect(url, autocommit=False) as conn:
        with conn.cursor() as cur:
            cur.execute(LEDGER)
            cur.execute("SELECT filename FROM schema_migration")
            done = {r[0] for r in cur.fetchall()}
        conn.commit()

        for path in sorted(p for p in MIGRATIONS.glob("*.sql") if p.name not in done):
            # Each migration is one transaction: it lands whole or not at all.
            # Postgres can do this because its DDL is transactional; MySQL's is not.
            with conn.cursor() as cur:
                cur.execute(path.read_text(encoding="utf-8"))
                cur.execute("INSERT INTO schema_migration (filename) VALUES (%s)", (path.name,))
            conn.commit()
            applied.append(path.name)
    return applied


def main() -> int:
    load_dotenv()
    url = os.environ.get("DATABASE_URL", "postgresql://sync:sync@localhost:5433/switchboard")
    try:
        applied = apply(url)
    except Exception as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        return 1
    print("\n".join(f"applied {name}" for name in applied) or "nothing to apply")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
