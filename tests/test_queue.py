"""Job queue tests, against a real Postgres.

These cannot be faked with mocks: the behaviour under test is what Postgres
does with row locks, and a mock would only test our assumptions about that.
"""

from __future__ import annotations

import random
from datetime import timedelta

import psycopg
import pytest
from psycopg import AsyncConnection

from switchboard.engine import queue
from switchboard.engine.errors import PermanentError, RetryableError


async def _status(db, job_id) -> str:
    cur = await db.execute("SELECT status FROM sync_job WHERE id = %s", (job_id,))
    return (await cur.fetchone())[0]


async def _make_due(db, job_id) -> None:
    await db.execute("UPDATE sync_job SET run_after = now() WHERE id = %s", (job_id,))


async def _dead_letters(db) -> int:
    cur = await db.execute("SELECT count(*) FROM dead_letter")
    return (await cur.fetchone())[0]


class TestBackoff:
    def test_ceiling_doubles_then_caps(self):
        # rng that always returns the top of the range exposes the ceiling
        top = type("Top", (), {"uniform": staticmethod(lambda lo, hi: hi)})()
        assert [queue.backoff_seconds(n, rng=top) for n in (1, 2, 3)] == [5, 10, 20]
        assert queue.backoff_seconds(30, rng=top) == queue.BACKOFF_CAP_SECONDS

    def test_jitter_spreads_retries(self):
        rng = random.Random(42)
        delays = {round(queue.backoff_seconds(4, rng=rng), 3) for _ in range(20)}
        # twenty jobs failing together must not all retry at the same instant
        assert len(delays) == 20
        assert all(0 <= d <= 40 for d in delays)


class TestClaim:
    async def test_claims_oldest_due_job(self, db, connection_id):
        first = await queue.enqueue(db, connection_id, "backfill")
        await queue.enqueue(db, connection_id, "backfill")
        job = await queue.claim(db)
        assert job.id == first
        assert job.attempts == 1
        assert await _status(db, first) == "running"

    async def test_future_jobs_are_invisible(self, db, connection_id):
        await queue.enqueue(db, connection_id, "backfill", delay_seconds=60)
        assert await queue.claim(db) is None

    async def test_concurrent_workers_never_share_a_job(self, db, connection_id, test_database_url):
        """The SKIP LOCKED test. Worker A holds its claim transaction open;
        worker B must step past A's row, not block on it and not take it."""
        await queue.enqueue(db, connection_id, "backfill")
        await queue.enqueue(db, connection_id, "backfill")

        a = await AsyncConnection.connect(test_database_url)
        b = await AsyncConnection.connect(test_database_url, autocommit=True)
        # If SKIP LOCKED regressed to a plain FOR UPDATE, B would wait on A's
        # lock forever (A is waiting on B in this same test). Fail fast instead.
        await b.execute("SET lock_timeout = '2s'")
        await b.set_autocommit(False)
        try:
            async with a.transaction():
                job_a = await queue.claim(a)
                async with b.transaction():
                    job_b = await queue.claim(b)
                    # a third claim while both are held: nothing left
                    assert await queue.claim(b) is None
            assert job_a and job_b
            assert job_a.id != job_b.id
        finally:
            await a.close()
            await b.close()


class TestFailure:
    async def test_retryable_error_backs_off(self, db, connection_id):
        job_id = await queue.enqueue(db, connection_id, "backfill")
        job = await queue.claim(db)
        # seeded so the jittered delay is a known non-zero value
        assert await queue.fail(db, job, RetryableError("503"), rng=random.Random(1)) == "retrying"
        assert await _status(db, job_id) == "pending"
        cur = await db.execute("SELECT run_after > now(), last_error FROM sync_job WHERE id = %s", (job_id,))
        in_future, last_error = await cur.fetchone()
        assert in_future
        assert "503" in last_error
        assert await queue.claim(db) is None

    async def test_retry_after_is_respected(self, db, connection_id):
        job_id = await queue.enqueue(db, connection_id, "backfill")
        job = await queue.claim(db)
        await queue.fail(db, job, RetryableError("429", retry_after=120))
        cur = await db.execute("SELECT run_after - now() >= interval '119 seconds' FROM sync_job WHERE id = %s", (job_id,))
        assert (await cur.fetchone())[0]
        assert await queue.claim(db) is None

    async def test_permanent_error_dead_letters_immediately(self, db, connection_id):
        job_id = await queue.enqueue(db, connection_id, "backfill", {"page": 3})
        job = await queue.claim(db)
        assert await queue.fail(db, job, PermanentError("401 bad api key")) == "dead"
        assert await _status(db, job_id) == "failed"
        cur = await db.execute("SELECT job_id, payload, error FROM dead_letter")
        dl_job, payload, error = await cur.fetchone()
        assert dl_job == job_id
        assert payload == {"page": 3}  # kept intact so it can be replayed
        assert "401" in error

    async def test_budget_exhaustion_dead_letters(self, db, connection_id):
        job_id = await queue.enqueue(db, connection_id, "backfill", max_attempts=3)
        outcomes = []
        for _ in range(3):
            await _make_due(db, job_id)
            job = await queue.claim(db)
            outcomes.append(await queue.fail(db, job, RetryableError("timeout")))
        assert outcomes == ["retrying", "retrying", "dead"]
        assert await _dead_letters(db) == 1


class TestLease:
    async def test_crashed_worker_job_is_reclaimed(self, db, connection_id):
        job_id = await queue.enqueue(db, connection_id, "backfill")
        await queue.claim(db)  # ...and the worker dies here
        counts = await queue.reap_stale(db, lease=timedelta(0))
        assert counts == {"requeued": 1, "dead": 0}
        assert await _status(db, job_id) == "pending"

    async def test_poison_job_eventually_dead_letters(self, db, connection_id):
        """A job that kills the worker never reaches fail(). Counting attempts
        at claim time is what stops it being retried forever."""
        job_id = await queue.enqueue(db, connection_id, "backfill", max_attempts=2)
        for _ in range(2):
            await queue.claim(db)
            await queue.reap_stale(db, lease=timedelta(0))
        assert await _status(db, job_id) == "failed"
        assert await _dead_letters(db) == 1

    async def test_late_finisher_cannot_clobber_newer_attempt(self, db, connection_id):
        """The fencing token. Worker 1 is slow, its lease expires, worker 2
        takes over. When worker 1 finally finishes it must not mark worker 2's
        attempt as done."""
        job_id = await queue.enqueue(db, connection_id, "backfill")
        slow = await queue.claim(db)
        await queue.reap_stale(db, lease=timedelta(0))
        fresh = await queue.claim(db)
        assert fresh.attempts == slow.attempts + 1

        assert await queue.complete(db, slow) is False
        assert await queue.fail(db, slow, RetryableError("late")) == "lost_lease"
        assert await _status(db, job_id) == "running"
        assert await queue.complete(db, fresh) is True
        assert await _status(db, job_id) == "done"


async def test_schema_rejects_unknown_job_kind(db, connection_id):
    # The CHECK constraint is the last line of defence against a typo'd kind
    # sitting in the queue with no handler.
    with pytest.raises(psycopg.errors.CheckViolation):
        await queue.enqueue(db, connection_id, "not_a_kind")
