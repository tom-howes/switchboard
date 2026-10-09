"""Entrypoint: `python -m switchboard serve` or `python -m switchboard worker`.

Why this file exists rather than just running `uvicorn switchboard.main:app`:
on Windows, asyncio defaults to the ProactorEventLoop, and psycopg's async mode
cannot run on it (it needs the selector-based loop that every other OS uses by
default). Running uvicorn from the command line gives us no clean way to choose
the loop, so we start the server ourselves inside a loop we picked. On Linux
and macOS the selector loop is already the default, so this changes nothing
there.
"""

from __future__ import annotations

import asyncio
import selectors
import sys

import uvicorn

from .config import get_settings


def _loop_factory() -> asyncio.AbstractEventLoop:
    return asyncio.SelectorEventLoop(selectors.SelectSelector())


def run(coro) -> None:
    if sys.platform == "win32":
        asyncio.run(coro, loop_factory=_loop_factory)
    else:
        asyncio.run(coro)


def serve() -> None:
    # loop="none" tells uvicorn not to install its own loop: we already did.
    config = uvicorn.Config("switchboard.main:app", port=get_settings().port, loop="none")
    run(uvicorn.Server(config).serve())


def worker() -> None:
    from .engine.worker import run_forever

    run(run_forever())


async def _connect(provider: str) -> None:
    """Connect a provider for the default workspace and start its backfill.

    Idempotent: re-running it rotates the stored credentials and restarts the
    backfill, which is also how you recover a connection from needs_reauth.
    """
    from . import db
    from .connectors import get_connector
    from .engine import connections, mappings, sync

    connector = get_connector(provider)
    creds = await connector.authorize(code="", state="")
    await db.open_pool()
    try:
        async with db.transaction() as conn:
            cur = await conn.execute("SELECT id FROM workspace WHERE name = 'default'")
            row = await cur.fetchone()
            if row is None:
                cur = await conn.execute("INSERT INTO workspace (name) VALUES ('default') RETURNING id")
                row = await cur.fetchone()
            workspace_id = row["id"]

            cur = await conn.execute(
                "SELECT 1 FROM field_mapping WHERE provider = %s AND status = 'approved'", (provider,)
            )
            if await cur.fetchone() is None:
                await mappings.approve(conn, provider, mappings.seed_rules(provider), approved_by="seed")

            connection_id = await connections.save(conn, workspace_id, provider, creds)
            await sync.start_backfill(conn, connection_id)
    finally:
        await db.close_pool()
    print(f"connected {provider}: {connection_id}")
    print(f"webhook url: {get_settings().public_url}/webhooks/{provider}/{connection_id}")
    print("backfill queued; run `python -m switchboard worker` to process it")


def connect() -> None:
    if len(sys.argv) < 3:
        raise SystemExit("usage: python -m switchboard connect <provider>")
    run(_connect(sys.argv[2]))


COMMANDS = {"serve": serve, "worker": worker, "connect": connect}


if __name__ == "__main__":
    name = sys.argv[1] if len(sys.argv) > 1 else "serve"
    if name not in COMMANDS:
        raise SystemExit(f"usage: python -m switchboard [{'|'.join(COMMANDS)}]")
    COMMANDS[name]()
