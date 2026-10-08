"""Apply migrations/*.sql in filename order, once each.

Deliberately about forty lines rather than Alembic. Migrations here are plain
SQL files that can be read without knowing a migration framework, which matters
for a repo whose point is that the SQL is worth reading.
"""

from __future__ import annotations

import pathlib
import sys

import psycopg
from dotenv import load_dotenv

load_dotenv()

ROOT = pathlib.Path(__file__).resolve().parent.parent
MIGRATIONS = ROOT / "migrations"

LEDGER = """
CREATE TABLE IF NOT EXISTS schema_migration (
    filename   text PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT now()
)
"""


def main() -> int:
    import os

    url = os.environ.get("DATABASE_URL", "postgresql://sync:sync@localhost:5433/switchboard")

    with psycopg.connect(url, autocommit=False) as conn:
        with conn.cursor() as cur:
            cur.execute(LEDGER)
            cur.execute("SELECT filename FROM schema_migration")
            done = {r[0] for r in cur.fetchall()}
        conn.commit()

        pending = sorted(p for p in MIGRATIONS.glob("*.sql") if p.name not in done)
        if not pending:
            print("nothing to apply")
            return 0

        for path in pending:
            print(f"applying {path.name} ... ", end="", flush=True)
            try:
                # Each migration is one transaction: it lands whole or not at all.
                with conn.cursor() as cur:
                    cur.execute(path.read_text(encoding="utf-8"))
                    cur.execute(
                        "INSERT INTO schema_migration (filename) VALUES (%s)", (path.name,)
                    )
                conn.commit()
                print("ok")
            except Exception as exc:
                conn.rollback()
                print("FAILED")
                print(f"  {exc}", file=sys.stderr)
                return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
