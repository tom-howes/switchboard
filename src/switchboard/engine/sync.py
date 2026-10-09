"""The job handlers: backfill and webhook.

Neither handler knows which provider it is talking to. Both go through the
same three steps, and the provider only appears as "whichever connector the
registry returns":

    load connection + approved mapping  ->  call the connector (no transaction)
                                         ->  write everything in one transaction

The middle step is outside any transaction on purpose (see queue.py). The last
step is one transaction so that a page of records, the cursor that says the
page is done, and the job for the next page land together or not at all.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from uuid import UUID

from psycopg import AsyncConnection

from .. import db
from ..connectors import get_connector
from ..core.models import WebhookEvent
from . import connections, mappings, queue, records
from .queue import Job
from .worker import handles

log = logging.getLogger("switchboard.sync")


def _now() -> datetime:
    return datetime.now(timezone.utc)


async def start_backfill(conn: AsyncConnection, connection_id: UUID) -> UUID:
    """Reset the backfill cursor and enqueue page one, atomically."""
    await conn.execute(
        """
        INSERT INTO sync_cursor (connection_id, kind, cursor, completed_at)
        VALUES (%s, 'backfill', NULL, NULL)
        ON CONFLICT (connection_id, kind)
        DO UPDATE SET cursor = NULL, completed_at = NULL, updated_at = now()
        """,
        (connection_id,),
    )
    return await queue.enqueue(conn, connection_id, "backfill", {"cursor": None})


# Compare-and-set on the cursor. The page is only written if the stored cursor
# is still the one this job was created for. If a previous run of this same job
# already committed (and then the worker died before marking the job done),
# the cursor has moved on, zero rows match, and this run is a no-op instead of
# forking a second chain of page jobs.
ADVANCE_CURSOR_SQL = """
UPDATE sync_cursor
SET cursor       = %(next)s,
    completed_at = CASE WHEN %(next)s::text IS NULL THEN now() END,
    updated_at   = now()
WHERE connection_id = %(connection_id)s
  AND kind = 'backfill'
  AND completed_at IS NULL
  AND cursor IS NOT DISTINCT FROM %(current)s
"""


@handles("backfill")
async def backfill_page(job: Job) -> None:
    """One page per job, never a whole backfill per job.

    Short jobs keep each one well inside the lease, make progress durable page
    by page, and let other connections' work interleave instead of waiting
    behind a 50,000 record backfill.
    """
    current = job.payload.get("cursor")
    async with db.pool().connection() as conn:
        connection = await connections.load(conn, job.connection_id)
        mapping_id, mapping = await mappings.approved(conn, connection.provider)

    connector = get_connector(connection.provider)
    # Taken BEFORE the request. Anything that changed after this moment may or
    # may not be in the response, so claiming the earlier time is the honest
    # version: a webhook for a change made mid-request will carry a later time
    # and correctly win.
    observed_at = _now()
    page = await connector.list_changes(connection.credentials, current)

    async with db.transaction() as conn:
        cur = await conn.execute(
            ADVANCE_CURSOR_SQL, {"next": page.next_cursor, "current": current, "connection_id": job.connection_id}
        )
        if cur.rowcount == 0:
            log.info("backfill page %r for %s already applied; skipping", current, job.connection_id)
            return

        outcomes: dict[str, int] = {}
        for raw in page.records:
            result = await records.store(
                conn, job.connection_id, mapping_id, mapping, raw.payload, observed_at=observed_at, job_id=job.id
            )
            outcomes[result] = outcomes.get(result, 0) + 1

        if page.next_cursor:
            await queue.enqueue(conn, job.connection_id, "backfill", {"cursor": page.next_cursor})

    log.info("backfill %s page %r: %s%s", connection.provider, current, outcomes,
             "" if page.next_cursor else " (complete)")


@handles("webhook")
async def apply_webhook(job: Job) -> None:
    event = WebhookEvent.model_validate(job.payload)
    async with db.pool().connection() as conn:
        connection = await connections.load(conn, job.connection_id)
        mapping_id, mapping = await mappings.approved(conn, connection.provider)

    payload, observed_at, deleted = event.record, event.occurred_at, event.deleted
    if payload is None and not deleted:
        # The provider only told us something changed. Read the current state;
        # it is current as of now, so it is versioned as of now.
        observed_at = _now()
        fetched = await get_connector(connection.provider).fetch_one(connection.credentials, event.external_id)
        if fetched is None:
            deleted = True
        else:
            payload = fetched.payload

    async with db.transaction() as conn:
        if deleted:
            result = await records.tombstone(conn, job.connection_id, event.external_id, observed_at=observed_at)
        else:
            result = await records.store(
                conn, job.connection_id, mapping_id, mapping, payload, observed_at=observed_at, job_id=job.id
            )
    log.info("webhook %s %s %s: %s", connection.provider, event.topic, event.external_id, result)
