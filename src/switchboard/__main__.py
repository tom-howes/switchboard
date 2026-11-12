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
    from .core.models import Capability
    from .engine import connections, mappings, reconcile, sync

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
            has_mapping = await cur.fetchone() is not None
            if not has_mapping and mappings.has_seed(provider):
                await mappings.approve(conn, provider, mappings.seed_rules(provider), approved_by="seed")
                has_mapping = True

            connection_id = await connections.save(conn, workspace_id, provider, creds)
            if has_mapping:
                await sync.start_backfill(conn, connection_id)
            # The first sweep after one interval; each sweep schedules the next.
            await reconcile.schedule(conn, connection_id)
    finally:
        await db.close_pool()
    print(f"connected {provider}: {connection_id}")
    if Capability.WEBHOOKS in connector.capabilities:
        print(f"webhook url: {get_settings().public_url}/webhooks/{provider}/{connection_id}")
    if has_mapping:
        print("backfill queued; run `python -m switchboard worker` to process it")
    else:
        # No hand-written mapping for this provider: draft one first. The
        # backfill starts when it is approved.
        print(f"no mapping yet: run `python -m switchboard propose {provider}`, then approve it")


def connect() -> None:
    if len(sys.argv) < 3:
        raise SystemExit("usage: python -m switchboard connect <provider>")
    run(_connect(sys.argv[2]))


async def _reconcile_now(connection_id: str) -> None:
    from . import db
    from .engine import reconcile
    from datetime import timedelta

    await db.open_pool()
    try:
        async with db.transaction() as conn:
            await reconcile.schedule(conn, connection_id, after=timedelta(0))
    finally:
        await db.close_pool()
    print(f"reconcile queued for {connection_id}; the worker will pick it up")


def reconcile_now() -> None:
    """Run a sweep now instead of waiting for the daily one. If a sweep is
    already in flight this one steps aside, so it is always safe to run."""
    if len(sys.argv) < 3:
        raise SystemExit("usage: python -m switchboard reconcile <connection_id>")
    run(_reconcile_now(sys.argv[2]))


async def _propose(provider: str, sample_file: str | None) -> None:
    import json
    import pathlib

    from . import db
    from .connectors import get_connector
    from .core.mapping import MappingDocument, MappingError, apply_mapping
    from .engine import connections, mappings
    from .llm.proposer import propose

    await db.open_pool()
    try:
        if sample_file:
            loaded = json.loads(pathlib.Path(sample_file).read_text(encoding="utf-8"))
            samples = loaded if isinstance(loaded, list) else [loaded]
        else:
            # A first page straight from the provider, using the stored connection.
            async with db.pool().connection() as conn:
                cur = await conn.execute(
                    "SELECT c.id FROM connection c JOIN workspace w ON w.id = c.workspace_id "
                    "WHERE w.name = 'default' AND c.provider = %s", (provider,))
                row = await cur.fetchone()
                if row is None:
                    raise SystemExit(f"no {provider} connection; run `connect {provider}` or pass --sample FILE")
                connection = await connections.load(conn, row["id"])
            connector = get_connector(provider)
            creds = await connections.fresh_credentials(connection, connector)
            samples = [r.payload for r in (await connector.list_changes(creds, None)).records]

        proposal = await propose(provider, samples)
        async with db.transaction() as conn:
            mapping_id = await mappings.save_proposal(
                conn, provider, proposal.rules, proposed_by="claude", model=proposal.model,
                notes=proposal.notes, sample=proposal.masked_samples)
    finally:
        await db.close_pool()

    print(json.dumps(proposal.rules, indent=2))
    for name, note in proposal.notes["fields"].items():
        print(f"  {name:<14} [{note['confidence']}] {note['reasoning']}")
    if proposal.notes["unmapped"]:
        print(f"  notes: {proposal.notes['unmapped']}")
    # Preview on the REAL samples, locally. They never left this machine; only
    # the masked copies were sent.
    doc = MappingDocument.model_validate(proposal.rules)
    print("\npreview on your actual records:")
    for sample in samples[:3]:
        try:
            print(" ", apply_mapping(sample, doc).person.model_dump())
        except MappingError as exc:
            print("  could not map:", exc)
    print(f"\nproposal saved as {mapping_id} (status: proposed)")
    print(f"approve with: python -m switchboard approve-mapping {mapping_id}")


def propose_mapping() -> None:
    """Have Claude draft a mapping for a provider. Saved as 'proposed' only."""
    args = sys.argv[2:]
    if not args:
        raise SystemExit("usage: python -m switchboard propose <provider> [--sample FILE]")
    sample_file = args[args.index("--sample") + 1] if "--sample" in args else None
    run(_propose(args[0], sample_file))


async def _approve(mapping_id: str) -> None:
    import getpass

    from . import db
    from .engine import mappings, sync

    await db.open_pool()
    try:
        async with db.pool().connection() as conn:
            provider = await mappings.approve_proposal(conn, mapping_id, approved_by=getpass.getuser())
            started = await sync.start_pending_backfills(conn, provider)
    finally:
        await db.close_pool()
    print(f"mapping {mapping_id} is now the approved {provider} mapping")
    if started:
        print(f"backfill started for {started} connection(s) that were waiting for a mapping")


def approve_mapping() -> None:
    if len(sys.argv) < 3:
        raise SystemExit("usage: python -m switchboard approve-mapping <mapping_id>")
    run(_approve(sys.argv[2]))


COMMANDS = {
    "serve": serve,
    "worker": worker,
    "connect": connect,
    "reconcile": reconcile_now,
    "propose": propose_mapping,
    "approve-mapping": approve_mapping,
}


if __name__ == "__main__":
    name = sys.argv[1] if len(sys.argv) > 1 else "serve"
    if name not in COMMANDS:
        raise SystemExit(f"usage: python -m switchboard [{'|'.join(COMMANDS)}]")
    COMMANDS[name]()
