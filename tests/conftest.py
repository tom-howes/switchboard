"""Shared test setup.

Database tests run against a separate `switchboard_test` database built from
the same migration files as production, so the tests cannot pass against a
schema that has drifted from the real one. If Postgres is not reachable, those
tests skip rather than fail: the mapping tests still need nothing at all.
"""

from __future__ import annotations

import asyncio
import base64
import importlib.util
import os
import pathlib
import selectors
import sys

import psycopg
import pytest
from psycopg import AsyncConnection

# Set before anything reads settings (they are cached on first read). A fresh
# random key per run: no test may depend on a particular key existing.
os.environ.setdefault("CREDENTIALS_KEY", base64.b64encode(os.urandom(32)).decode())
os.environ.setdefault("STRIPE_WEBHOOK_SECRET", "whsec_test_secret")

ROOT = pathlib.Path(__file__).resolve().parent.parent
ADMIN_URL = os.environ.get("DATABASE_URL", "postgresql://sync:sync@localhost:5433/switchboard")
TEST_DB = "switchboard_test"
TEST_URL = ADMIN_URL.rsplit("/", 1)[0] + f"/{TEST_DB}"


def pytest_asyncio_loop_factories(config, item):
    # Same reason as switchboard/__main__.py: psycopg's async mode cannot run
    # on Windows' default ProactorEventLoop.
    if sys.platform == "win32":
        return {"selector": lambda: asyncio.SelectorEventLoop(selectors.SelectSelector())}
    return None


def _load_migrate():
    spec = importlib.util.spec_from_file_location("migrate", ROOT / "scripts" / "migrate.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="session")
def test_database_url() -> str:
    try:
        # CREATE DATABASE cannot run inside a transaction, hence autocommit.
        with psycopg.connect(ADMIN_URL, autocommit=True, connect_timeout=3) as admin:
            exists = admin.execute("SELECT 1 FROM pg_database WHERE datname = %s", (TEST_DB,)).fetchone()
            if not exists:
                admin.execute(f'CREATE DATABASE "{TEST_DB}"')
    except psycopg.OperationalError as exc:
        pytest.skip(f"Postgres not reachable ({exc}); run `docker compose up -d`")
    _load_migrate().apply(TEST_URL)
    return TEST_URL


@pytest.fixture
async def db(test_database_url):
    """An autocommit connection: every statement is its own transaction,
    which is how the worker uses the queue. Tests that need a transaction held
    open say so explicitly."""
    conn = await AsyncConnection.connect(test_database_url, autocommit=True)
    # workspace cascades to everything that hangs off it
    await conn.execute("TRUNCATE workspace, field_mapping CASCADE")
    yield conn
    await conn.close()


@pytest.fixture
async def connection_id(db):
    cur = await db.execute("INSERT INTO workspace (name) VALUES ('test') RETURNING id")
    workspace_id = (await cur.fetchone())[0]
    cur = await db.execute(
        "INSERT INTO connection (workspace_id, provider, credentials) VALUES (%s, 'stripe', 'x') RETURNING id",
        (workspace_id,),
    )
    return (await cur.fetchone())[0]


@pytest.fixture
async def pool(test_database_url, db):
    """The app's own connection pool, pointed at the test database, for tests
    that exercise handlers and routes rather than single queries."""
    from switchboard import db as app_db

    await app_db.open_pool(test_database_url)
    yield app_db.pool()
    await app_db.close_pool()
