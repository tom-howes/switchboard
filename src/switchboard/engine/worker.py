"""The worker loop: claim a job, run its handler, record the outcome.

Handlers are registered by job kind. The loop knows nothing about Stripe or
HubSpot; it only knows that a job of kind 'backfill' goes to whatever function
registered itself for 'backfill'. Provider knowledge stays in connectors.

Each step uses its own short-lived connection from the pool, and each commits
on its own. That is deliberate: the claim must be committed BEFORE the handler
runs (so other workers see it as taken), and the outcome must be committed
after, without a transaction spanning the network calls in between.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import timedelta
from typing import Awaitable, Callable

from .. import db
from . import connections, queue
from .errors import AuthError, PermanentError
from .queue import Job

log = logging.getLogger("switchboard.worker")

Handler = Callable[[Job], Awaitable[None]]
HANDLERS: dict[str, Handler] = {}

POLL_INTERVAL_SECONDS = 1.0
LEASE = timedelta(minutes=5)
REAP_EVERY_SECONDS = 30.0


def handles(kind: str) -> Callable[[Handler], Handler]:
    """Register a handler: `@handles("backfill")`."""

    def register(fn: Handler) -> Handler:
        HANDLERS[kind] = fn
        return fn

    return register


async def run_once() -> bool:
    """Process at most one job. Returns False if there was nothing due."""
    async with db.pool().connection() as conn:
        job = await queue.claim(conn)
    if job is None:
        return False

    try:
        handler = HANDLERS.get(job.kind)
        if handler is None:
            # Retrying cannot make a handler appear, so this is permanent.
            raise PermanentError(f"no handler registered for job kind {job.kind!r}")
        await handler(job)
    except Exception as exc:
        async with db.pool().connection() as conn:
            outcome = await queue.fail(conn, job, exc)
            if isinstance(exc, AuthError):
                await connections.mark_needs_reauth(conn, job.connection_id)
        log.warning("job %s (%s) attempt %d failed, %s: %s", job.id, job.kind, job.attempts, outcome, exc)
    else:
        async with db.pool().connection() as conn:
            if not await queue.complete(conn, job):
                log.warning("job %s finished after its lease was reclaimed", job.id)
    return True


async def run_forever() -> None:
    """Poll for work. Single process for now (a known gap; the SKIP LOCKED claim
    is already safe for more).

    Polling every second is the simple option. LISTEN/NOTIFY would wake the
    worker the instant a job is enqueued, but it still needs a poll as a
    fallback (a NOTIFY sent while the worker is restarting is lost), so polling
    is the part that has to exist either way.
    """
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    from . import sync  # noqa: F401  importing it registers the handlers

    await db.open_pool()
    loop = asyncio.get_running_loop()
    next_reap = 0.0
    try:
        while True:
            if loop.time() >= next_reap:
                async with db.pool().connection() as conn:
                    counts = await queue.reap_stale(conn, LEASE)
                if any(counts.values()):
                    log.info("reaped stale jobs: %s", counts)
                next_reap = loop.time() + REAP_EVERY_SECONDS

            if not await run_once():
                await asyncio.sleep(POLL_INTERVAL_SECONDS)
    finally:
        await db.close_pool()
