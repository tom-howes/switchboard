"""Reconciliation sweep tests: real Postgres, Stripe HTTP mocked."""

from __future__ import annotations

from datetime import timedelta

import httpx
import respx

from switchboard.connectors.stripe import API
from switchboard.engine import queue, reconcile, sync  # noqa: F401  (registers handlers)
from switchboard.engine.reconcile import RECONCILE_EVERY

from test_stripe import customer
from test_sync import drain, mock_customers, stripe  # noqa: F401  (stripe is a fixture)


async def start(pool, connection_id) -> None:
    async with pool.connection() as conn:
        await reconcile.schedule(conn, connection_id, after=timedelta(0))


async def last_run(db) -> dict:
    cur = await db.execute("""SELECT status, records_seen, records_changed, records_missing, records_deleted, error
                              FROM sync_run ORDER BY started_at DESC LIMIT 1""")
    row = await cur.fetchone()
    return dict(zip(["status", "seen", "changed", "missing", "deleted", "error"], row))


async def backfill(pool, connection_id, pages) -> None:
    mock_customers(pages)
    async with pool.connection() as conn:
        await sync.start_backfill(conn, connection_id)
    await drain()


async def names(db) -> dict[str, tuple]:
    cur = await db.execute("SELECT external_id, full_name, deleted_at IS NOT NULL FROM provider_record ORDER BY 1")
    return {r[0]: (r[1], r[2]) for r in await cur.fetchall()}


class TestWalk:
    @respx.mock
    async def test_nothing_missed_means_nothing_changed(self, db, pool, stripe):
        await backfill(pool, stripe, [[customer(1), customer(2)], [customer(3)]])
        respx.reset()
        mock_customers([[customer(1), customer(2)], [customer(3)]])
        await start(pool, stripe)
        await drain()
        assert await last_run(db) == {"status": "completed", "seen": 3, "changed": 0,
                                      "missing": 0, "deleted": 0, "error": None}

    @respx.mock
    async def test_a_missed_update_and_a_missed_creation_are_caught(self, db, pool, stripe):
        await backfill(pool, stripe, [[customer(1), customer(2)]])
        respx.reset()
        # customer 2 was renamed and customer 3 created; no webhook arrived for either
        mock_customers([[customer(1), customer(2, name="Renamed"), customer(3)]])
        await start(pool, stripe)
        await drain()
        run = await last_run(db)
        assert (run["seen"], run["changed"]) == (3, 2)
        assert (await names(db))["cus_2"] == ("Renamed", False)
        assert "cus_3" in await names(db)

    @respx.mock
    async def test_finished_sweep_schedules_the_next_one_a_day_later(self, db, pool, stripe):
        mock_customers([[customer(1)]])
        await start(pool, stripe)
        await drain()
        cur = await db.execute("""SELECT run_after - now() FROM sync_job
                                  WHERE kind = 'reconcile' AND status = 'pending'""")
        [(delay,)] = await cur.fetchall()
        assert RECONCILE_EVERY - timedelta(minutes=1) < delay <= RECONCILE_EVERY

    @respx.mock
    async def test_a_second_start_while_one_is_running_steps_aside(self, db, pool, stripe):
        mock_customers([[customer(1)]])
        await start(pool, stripe)
        await start(pool, stripe)
        await drain()
        cur = await db.execute("SELECT count(*) FROM sync_run")
        assert (await cur.fetchone())[0] == 1

    @respx.mock
    async def test_an_abandoned_run_does_not_block_sweeps_forever(self, db, pool, stripe):
        await db.execute("INSERT INTO sync_run (connection_id, started_at) VALUES (%s, now() - interval '7 hours')",
                         (stripe,))
        mock_customers([[customer(1)]])
        await start(pool, stripe)
        await drain()
        cur = await db.execute("SELECT status, error IS NOT NULL FROM sync_run ORDER BY started_at")
        assert await cur.fetchall() == [("failed", True), ("completed", False)]
