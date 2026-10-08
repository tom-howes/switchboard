"""The job queue: a Postgres table and a handful of queries.

Why Postgres rather than Redis/Celery/SQS: the jobs and the data they write
live in the same database, so there is one thing to run, one thing to back up,
and a job can be enqueued in the same transaction as the write that caused it.
The cost is throughput: a table-backed queue is good for thousands of jobs a
minute, not millions. A sync engine for one company's Stripe and HubSpot is
nowhere near that ceiling.

Delivery is AT LEAST ONCE, not exactly once. A worker can finish the work and
crash before recording that it did; the job then runs again. Every handler
must therefore be idempotent (upserts keyed on external_id, webhook dedupe on
provider_event_id). Exactly-once is not something any queue can promise across
a network boundary, so the design does not pretend to.

Two mechanisms work together:

1. The row lock (FOR UPDATE SKIP LOCKED) stops two workers claiming the same
   job in the same instant. It only lasts as long as the claim transaction.
2. The status column ('running' + locked_at) is the claim that outlives the
   transaction. We commit the claim immediately and do the slow HTTP work
   OUTSIDE any transaction, because holding a transaction open across network
   calls pins a pooled connection and holds row locks for seconds at a time.

Because (2) is a lease rather than a lock, a crashed worker would leave a job
'running' forever. `reap_stale` takes those back. And because a reaped job can
be re-claimed while the original worker is still alive and merely slow,
`attempts` doubles as a fencing token: complete() and fail() only touch the
row if attempts still matches what this worker claimed, so a late finisher
cannot overwrite the newer attempt's state.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import timedelta
from typing import Any
from uuid import UUID

from psycopg import AsyncConnection
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from .errors import PermanentError, RetryableError

BACKOFF_BASE_SECONDS = 5.0
BACKOFF_CAP_SECONDS = 15 * 60.0


@dataclass(frozen=True)
class Job:
    id: UUID
    connection_id: UUID
    kind: str
    payload: dict[str, Any]
    attempts: int
    max_attempts: int


def backoff_seconds(attempt: int, *, rng: random.Random | None = None) -> float:
    """Exponential backoff with full jitter.

    Exponential so a provider that is down gets progressively less traffic from
    us rather than a steady hammering. Jittered because without it every job
    that failed during the same outage retries at the same instant when the
    provider comes back, and knocks it over again (the thundering herd).
    "Full" jitter, uniform between zero and the ceiling, is the variant AWS's
    published analysis found spreads load best.

    attempt 1 -> up to 5s, 2 -> up to 10s, 3 -> up to 20s ... capped at 15 min.
    """
    ceiling = min(BACKOFF_CAP_SECONDS, BACKOFF_BASE_SECONDS * 2 ** (attempt - 1))
    return (rng or random).uniform(0, ceiling)


async def enqueue(
    conn: AsyncConnection,
    connection_id: UUID,
    kind: str,
    payload: dict | None = None,
    *,
    delay_seconds: float = 0,
    max_attempts: int = 5,
) -> UUID:
    """Add a job.

    Takes a connection rather than opening its own so the caller can enqueue
    inside the same transaction as the write that caused the job: if that
    write rolls back, the job never existed. That is the transactional outbox
    pattern, and it comes free from keeping the queue in the same database.
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            INSERT INTO sync_job (connection_id, kind, payload, max_attempts, run_after)
            VALUES (%s, %s, %s, %s, now() + make_interval(secs => %s))
            RETURNING id
            """,
            (connection_id, kind, Jsonb(payload or {}), max_attempts, delay_seconds),
        )
        return (await cur.fetchone())["id"]


# The inner SELECT finds the oldest due job and locks it. SKIP LOCKED means a
# second worker running this at the same moment steps over the locked row and
# takes the next one, instead of waiting behind the first worker's lock.
#
# Without SKIP LOCKED (plain FOR UPDATE), worker B blocks until A commits, then
# finds the row no longer pending and comes back EMPTY even though other jobs
# were waiting. With no lock at all, both can read the same id and both
# "claim" it.
#
# attempts is incremented here, at claim time, not at failure time. A job that
# crashes the worker outright never reaches fail(), and must still use up its
# budget, or one poison job would take down every worker forever.
CLAIM_SQL = """
UPDATE sync_job
SET status     = 'running',
    locked_at  = now(),
    attempts   = attempts + 1,
    updated_at = now()
WHERE id = (
    SELECT id
    FROM sync_job
    WHERE status = 'pending'
      AND run_after <= now()
    ORDER BY run_after, created_at
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
RETURNING id, connection_id, kind, payload, attempts, max_attempts
"""


async def claim(conn: AsyncConnection) -> Job | None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(CLAIM_SQL)
        row = await cur.fetchone()
    return Job(**row) if row else None


async def complete(conn: AsyncConnection, job: Job) -> bool:
    """Mark done. False means we lost the lease and the row was left alone."""
    cur = await conn.execute(
        """
        UPDATE sync_job
        SET status = 'done', locked_at = NULL, last_error = NULL, updated_at = now()
        WHERE id = %s AND status = 'running' AND attempts = %s
        """,
        (job.id, job.attempts),
    )
    return cur.rowcount == 1


# One statement, so a job cannot end up 'failed' without its dead letter row or
# the other way round. The ownership guard sits on the UPDATE: if this worker no
# longer owns the job, the CTE returns no rows and nothing is inserted either.
DEAD_LETTER_SQL = """
WITH failed AS (
    UPDATE sync_job
    SET status = 'failed', locked_at = NULL, last_error = %(error)s, updated_at = now()
    WHERE id = %(id)s AND status = 'running' AND attempts = %(attempts)s
    RETURNING id, connection_id, kind, payload
)
INSERT INTO dead_letter (connection_id, job_id, kind, payload, error)
SELECT connection_id, id, kind, payload, %(error)s FROM failed
"""

RETRY_SQL = """
UPDATE sync_job
SET status     = 'pending',
    locked_at  = NULL,
    last_error = %(error)s,
    run_after  = now() + make_interval(secs => %(delay)s),
    updated_at = now()
WHERE id = %(id)s AND status = 'running' AND attempts = %(attempts)s
"""


async def fail(
    conn: AsyncConnection, job: Job, error: BaseException, *, rng: random.Random | None = None
) -> str:
    """Record a failure. Returns 'retrying', 'dead', or 'lost_lease'.

    Backoff is just a future run_after. No timers and no sleeping worker: the
    claim query does not see the job until it is due, and a restart loses
    nothing because the schedule is a column, not process memory.

    Exceptions that are neither Retryable nor Permanent (our own bugs, mostly)
    are retried. That wastes a few attempts on a real bug, which still reaches
    the dead letter table. The alternative, treating unclassified errors as
    permanent, would dead-letter every transient failure we forgot to label.
    """
    params = {"id": job.id, "attempts": job.attempts, "error": f"{type(error).__name__}: {error}"[:2000]}

    if isinstance(error, PermanentError) or job.attempts >= job.max_attempts:
        cur = await conn.execute(DEAD_LETTER_SQL, params)
        return "dead" if cur.rowcount == 1 else "lost_lease"

    delay = backoff_seconds(job.attempts, rng=rng)
    if isinstance(error, RetryableError) and error.retry_after is not None:
        delay = max(delay, error.retry_after)

    cur = await conn.execute(RETRY_SQL, {**params, "delay": delay})
    return "retrying" if cur.rowcount == 1 else "lost_lease"


async def reap_stale(conn: AsyncConnection, lease: timedelta) -> dict[str, int]:
    """Take back jobs whose worker went away without finishing.

    A job still 'running' after `lease` is presumed abandoned. With budget left
    it goes back to pending; without, it is dead-lettered, which is how a job
    that crashes the worker every time eventually stops being retried.

    The lease must be comfortably longer than any single job takes, or slow but
    healthy jobs run twice. That is why a backfill is one job per page rather
    than one job for the whole backfill.
    """
    counts = {"requeued": 0, "dead": 0}
    async with conn.transaction():
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT id, attempts, attempts >= max_attempts AS exhausted
                FROM sync_job
                WHERE status = 'running' AND locked_at < now() - %s
                FOR UPDATE SKIP LOCKED
                """,
                (lease,),
            )
            stale = await cur.fetchall()

        for row in stale:
            params = {"id": row["id"], "attempts": row["attempts"],
                      "error": "lease expired: worker did not finish"}
            if row["exhausted"]:
                await conn.execute(DEAD_LETTER_SQL, params)
                counts["dead"] += 1
            else:
                await conn.execute(RETRY_SQL, {**params, "delay": 0})
                counts["requeued"] += 1
    return counts
