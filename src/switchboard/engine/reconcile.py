"""The reconciliation sweep: catch what the webhooks missed.

Webhooks are best effort. Providers give up after a few retries, a deploy can
drop a burst, and an endpoint misconfigured for an hour loses everything sent
in that hour. A sync engine that trusts webhooks alone drifts silently, so
every connection is re-walked once a day and compared with what we hold.

One job kind ('reconcile') with three phases, each a short job:

    start    open a sync_run, reset the cursor, queue page one
    page     walk one page: store it (the observed_at guard means a stale
             page can never overwrite newer webhook data), count what
             actually changed, mark records as seen
    confirm  records we hold that the walk did not return are asked about one
             by one, and only a confirmed 404 becomes a delete

Scheduling is the same trick as backoff: the finished run queues the next
'start' with run_after 24 hours out. No cron, no scheduler process, and a
restart loses nothing because the schedule is a row.

The same full walk is used for every provider, including HubSpot, which could
search by modification date. One code path, and a modified-since search does
not return deleted records, so it could never find what this phase exists to
find.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from uuid import UUID

from psycopg import AsyncConnection
from psycopg.rows import dict_row

from .. import db
from ..connectors import get_connector
from . import connections, mappings, queue, records
from .errors import PermanentError
from .queue import Job
from .sync import ADVANCE_CURSOR_SQL
from .worker import handles

log = logging.getLogger("switchboard.reconcile")

RECONCILE_EVERY = timedelta(hours=24)

# A run still 'running' this long after it started has lost its job chain (a
# page job dead-lettered, say). It is marked failed so the next start is not
# blocked by the one-running-per-connection index forever.
ABANDONED_AFTER = timedelta(hours=6)

# Missing records are confirmed in batches, so one confirm job stays well
# inside the worker lease even when there are hundreds to check.
CONFIRM_BATCH = 50

# The circuit breaker. If more than this share of a connection's live records
# went missing in one walk, the walk is far more likely to be broken (a paging
# bug, a provider returning partial results) than a fifth of the customers to
# have been deleted overnight. The run fails loudly and nothing is checked or
# deleted. The floor stops a five-record test account tripping it over one.
MISSING_ALARM_FRACTION = 0.2
MISSING_ALARM_FLOOR = 10


def _now() -> datetime:
    return datetime.now(timezone.utc)


async def schedule(conn: AsyncConnection, connection_id: UUID, *, after: timedelta = RECONCILE_EVERY) -> UUID:
    return await queue.enqueue(conn, connection_id, "reconcile", {"phase": "start"},
                               delay_seconds=after.total_seconds())


@handles("reconcile")
async def reconcile(job: Job) -> None:
    phase = job.payload.get("phase")
    if phase == "start":
        await _start(job)
    elif phase == "page":
        await _page(job)
    elif phase == "confirm":
        await _confirm(job)
    else:
        raise PermanentError(f"unknown reconcile phase {phase!r}")


async def _start(job: Job) -> None:
    async with db.transaction() as conn:
        await conn.execute(
            """UPDATE sync_run SET status = 'failed', finished_at = now(),
                   error = 'abandoned: did not finish within ' || %s::text
               WHERE connection_id = %s AND status = 'running' AND started_at < now() - %s""",
            (str(ABANDONED_AFTER), job.connection_id, ABANDONED_AFTER),
        )
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """INSERT INTO sync_run (connection_id) VALUES (%s)
                   ON CONFLICT (connection_id) WHERE status = 'running' DO NOTHING
                   RETURNING id""",
                (job.connection_id,),
            )
            run = await cur.fetchone()
        if run is None:
            # A sweep is already in flight. It will schedule the next one when
            # it finishes, so this start simply steps aside.
            log.info("reconcile already running for %s; skipping", job.connection_id)
            return
        await conn.execute(
            """INSERT INTO sync_cursor (connection_id, kind, cursor, completed_at)
               VALUES (%s, 'reconcile', NULL, NULL)
               ON CONFLICT (connection_id, kind)
               DO UPDATE SET cursor = NULL, completed_at = NULL, updated_at = now()""",
            (job.connection_id,),
        )
        await queue.enqueue(conn, job.connection_id, "reconcile",
                            {"phase": "page", "run_id": str(run["id"]), "cursor": None})


async def _running(conn: AsyncConnection, run_id: str) -> dict | None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute("SELECT * FROM sync_run WHERE id = %s AND status = 'running'", (run_id,))
        return await cur.fetchone()


# What the person-facing fields of a set of records currently are. Compared
# before and after a page is stored, so "changed" means a mapped field really
# differed. Comparing raw payloads instead would count every Stripe balance
# update as a missed webhook and overstate the loss rate.
SNAPSHOT_SQL = """
SELECT external_id, primary_email, full_name, given_name, family_name, phone, deleted_at IS NOT NULL AS deleted
FROM provider_record
WHERE connection_id = %s AND external_id = ANY(%s)
"""


async def _snapshot(conn: AsyncConnection, connection_id: UUID, external_ids: list[str]) -> dict[str, tuple]:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(SNAPSHOT_SQL, (connection_id, external_ids))
        return {row.pop("external_id"): tuple(row.values()) for row in await cur.fetchall()}


async def _page(job: Job) -> None:
    run_id, current = job.payload["run_id"], job.payload.get("cursor")
    async with db.pool().connection() as conn:
        if await _running(conn, run_id) is None:
            return  # the run was abandoned or failed; nothing to continue
        connection = await connections.load(conn, job.connection_id)
        mapping_id, mapping = await mappings.approved(conn, connection.provider)

    connector = get_connector(connection.provider)
    creds = await connections.fresh_credentials(connection, connector)
    observed_at = _now()  # before the request, for the same reason as backfill
    page = await connector.list_changes(creds, current)

    async with db.transaction() as conn:
        cur = await conn.execute(ADVANCE_CURSOR_SQL, {"next": page.next_cursor, "current": current,
                                                      "connection_id": job.connection_id, "kind": "reconcile"})
        if cur.rowcount == 0:
            return  # this page was already applied by an earlier run of this job

        ids = [r.external_id for r in page.records]
        before = await _snapshot(conn, job.connection_id, ids)
        for raw in page.records:
            await records.store(conn, job.connection_id, mapping_id, mapping, raw.payload,
                                observed_at=observed_at, job_id=job.id)
        after = await _snapshot(conn, job.connection_id, ids)
        # New records count too: a customer we never heard about is the most
        # basic missed webhook there is.
        changed = sum(1 for i in after if before.get(i) != after[i])

        await conn.execute(
            "UPDATE provider_record SET last_seen_at = now() WHERE connection_id = %s AND external_id = ANY(%s)",
            (job.connection_id, ids),
        )
        await conn.execute(
            "UPDATE sync_run SET records_seen = records_seen + %s, records_changed = records_changed + %s WHERE id = %s",
            (len(ids), changed, run_id),
        )
        next_phase = ({"phase": "page", "run_id": run_id, "cursor": page.next_cursor} if page.next_cursor
                      else {"phase": "confirm", "run_id": run_id})
        await queue.enqueue(conn, job.connection_id, "reconcile", next_phase)


async def _finish(conn: AsyncConnection, run_id: str, connection_id: UUID, *, error: str | None = None) -> None:
    await conn.execute(
        "UPDATE sync_run SET status = %s, finished_at = now(), error = %s WHERE id = %s",
        ("failed" if error else "completed", error, run_id),
    )
    # Even a failed run schedules the next one: whatever went wrong may well
    # be gone in a day, and a sweep that stops itself after one bad night is
    # worse than one that keeps trying.
    await schedule(conn, connection_id)


# Live records this walk did not return. fetched_at guards the race where a
# webhook created a record after the walk had already passed its page: it was
# written after the run started, so it is clearly alive, not missing.
MISSING_SQL = """
SELECT external_id FROM provider_record
WHERE connection_id = %(connection_id)s
  AND deleted_at IS NULL
  AND (last_seen_at IS NULL OR last_seen_at < %(started_at)s)
  AND fetched_at < %(started_at)s
ORDER BY external_id
"""


async def _missing(conn: AsyncConnection, connection_id: UUID, started_at: datetime) -> list[str]:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(MISSING_SQL, {"connection_id": connection_id, "started_at": started_at})
        return [r["external_id"] for r in await cur.fetchall()]


async def _confirm(job: Job) -> None:
    """Ask the provider about each record the walk did not return.

    Only a confirmed absence (fetch_one returns None: a 404, or Stripe's
    deleted stub) becomes a tombstone. Absence from a listing is a hint, not
    proof. A listing can skip records for reasons that have nothing to do with
    the record, and a delete is the one mistake a sync engine cannot quietly
    undo.
    """
    run_id = job.payload["run_id"]
    async with db.pool().connection() as conn:
        run = await _running(conn, run_id)
        if run is None:
            return
        missing = await _missing(conn, job.connection_id, run["started_at"])
        connection = await connections.load(conn, job.connection_id)
        mapping_id, mapping = await mappings.approved(conn, connection.provider)

        if not job.payload.get("breaker_checked"):
            cur = await conn.execute(
                "SELECT count(*) AS live FROM provider_record WHERE connection_id = %s AND deleted_at IS NULL",
                (job.connection_id,),
            )
            live = (await cur.fetchone())["live"]
            if len(missing) > max(MISSING_ALARM_FLOOR, MISSING_ALARM_FRACTION * live):
                async with conn.transaction():
                    await conn.execute("UPDATE sync_run SET records_missing = %s WHERE id = %s",
                                       (len(missing), run_id))
                    await _finish(conn, run_id, job.connection_id, error=(
                        f"circuit breaker: {len(missing)} of {live} live records missing from the walk; "
                        "nothing was deleted. Check the connector before trusting this sweep."))
                log.error("reconcile %s tripped the circuit breaker (%d of %d missing)",
                          job.connection_id, len(missing), live)
                return

    batch, remaining = missing[:CONFIRM_BATCH], missing[CONFIRM_BATCH:]
    connector = get_connector(connection.provider)
    creds = await connections.fresh_credentials(connection, connector)
    observed_at = _now()
    answers = {external_id: await connector.fetch_one(creds, external_id) for external_id in batch}

    async with db.transaction() as conn:
        if await _running(conn, run_id) is None:
            return
        deleted = 0
        for external_id, record in answers.items():
            if record is None:
                await records.tombstone(conn, job.connection_id, external_id, observed_at=observed_at)
                deleted += 1
            else:
                # Still there; the listing just did not return it. Store what
                # we learned and mark it seen so the next batch skips it.
                await records.store(conn, job.connection_id, mapping_id, mapping, record.payload,
                                    observed_at=observed_at, job_id=job.id)
                await conn.execute(
                    "UPDATE provider_record SET last_seen_at = now() WHERE connection_id = %s AND external_id = %s",
                    (job.connection_id, external_id),
                )
        await conn.execute(
            "UPDATE sync_run SET records_missing = records_missing + %s, records_deleted = records_deleted + %s "
            "WHERE id = %s",
            (len(batch), deleted, run_id),
        )
        if remaining:
            await queue.enqueue(conn, job.connection_id, "reconcile",
                                {"phase": "confirm", "run_id": run_id, "breaker_checked": True})
        else:
            await _finish(conn, run_id, job.connection_id)
