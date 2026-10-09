"""End-to-end sync tests: handlers and the webhook route against a real
Postgres, with Stripe's HTTP API mocked.

These are the tests for the failure modes the engine exists to handle:
crashed backfills, duplicate webhooks, out-of-order events, deletes that
arrive before creates, records the mapping cannot read, revoked keys.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import httpx
import pytest
import respx

from switchboard import connectors
from switchboard.connectors.stripe import API, StripeConnector
from switchboard.core.models import Credentials
from switchboard.engine import connections, mappings, queue, sync, worker
from switchboard.main import app

from test_stripe import NOW, SECRET, customer, sign

T0 = datetime.fromtimestamp(NOW, tz=timezone.utc)


@pytest.fixture
async def stripe(db, pool, monkeypatch):
    """A connected Stripe account with an approved mapping. Returns the connection id."""
    monkeypatch.setitem(
        connectors.FACTORIES, "stripe",
        lambda: StripeConnector(api_key="sk_test_123", webhook_secret=SECRET, clock=lambda: NOW),
    )
    async with pool.connection() as conn:
        cur = await conn.execute("INSERT INTO workspace (name) VALUES ('test') RETURNING id")
        workspace_id = (await cur.fetchone())["id"]
        await mappings.approve(conn, "stripe", mappings.seed_rules("stripe"), approved_by="test")
        return await connections.save(conn, workspace_id, "stripe", Credentials(api_key="sk_test_123"))


async def drain() -> int:
    """Run the worker until the queue is empty. Returns jobs processed."""
    n = 0
    while await worker.run_once():
        n += 1
    return n


async def records(db) -> dict[str, dict]:
    cur = await db.execute(
        "SELECT external_id, primary_email, full_name, phone, deleted_at FROM provider_record ORDER BY external_id"
    )
    return {r[0]: {"email": r[1], "name": r[2], "phone": r[3], "deleted": r[4] is not None}
            for r in await cur.fetchall()}


def mock_customers(pages: list[list[dict]]):
    """Serve `pages` through Stripe-style starting_after pagination."""
    by_cursor = {None: 0}
    for i, page in enumerate(pages[:-1]):
        by_cursor[page[-1]["id"]] = i + 1

    def handler(request: httpx.Request) -> httpx.Response:
        i = by_cursor[request.url.params.get("starting_after")]
        return httpx.Response(200, json={"data": pages[i], "has_more": i < len(pages) - 1})

    return respx.get(f"{API}/customers").mock(side_effect=handler)


class TestBackfill:
    @respx.mock
    async def test_full_backfill_one_job_per_page(self, db, pool, stripe):
        mock_customers([[customer(1), customer(2)], [customer(3)]])
        async with pool.connection() as conn:
            await sync.start_backfill(conn, stripe)

        assert await drain() == 2
        assert set(await records(db)) == {"cus_1", "cus_2", "cus_3"}
        cur = await db.execute("SELECT cursor, completed_at IS NOT NULL FROM sync_cursor")
        assert await cur.fetchone() == (None, True)

    @respx.mock
    async def test_record_without_email_is_dead_lettered_alone(self, db, pool, stripe):
        mock_customers([[customer(1), customer(2, email=None), customer(3)]])
        async with pool.connection() as conn:
            await sync.start_backfill(conn, stripe)
        await drain()

        assert set(await records(db)) == {"cus_1", "cus_3"}
        cur = await db.execute("SELECT kind, payload->'record'->>'id', error FROM dead_letter")
        kind, record_id, error = await cur.fetchone()
        assert (kind, record_id) == ("record", "cus_2")
        assert "primary_email" in error
        cur = await db.execute("SELECT status FROM sync_job")
        assert [r[0] for r in await cur.fetchall()] == ["done"]  # the job itself succeeded

    @respx.mock
    async def test_rerun_of_committed_page_is_a_no_op(self, db, pool, stripe):
        """The worker commits page 1, then dies before marking the job done.
        The reaper hands the job out again. It must not fork a second chain."""
        route = mock_customers([[customer(1)], [customer(2)]])
        async with pool.connection() as conn:
            await sync.start_backfill(conn, stripe)
            job = await queue.claim(conn)
        await sync.backfill_page(job)  # commits page 1 and enqueues page 2...
        # ...and the worker crashes here, before complete()

        async with pool.connection() as conn:
            await queue.reap_stale(conn, lease=timedelta(0))
        assert await drain() == 2  # the re-run of page 1, then page 2

        cur = await db.execute("SELECT count(*) FROM sync_job WHERE payload->>'cursor' = 'cus_1'")
        assert (await cur.fetchone())[0] == 1  # exactly one page-2 job, not two
        assert set(await records(db)) == {"cus_1", "cus_2"}
        assert route.call_count == 3  # page 1 fetched twice, page 2 once

    @respx.mock
    async def test_revoked_key_parks_jobs_until_reconnect(self, db, pool, stripe):
        respx.get(f"{API}/customers").mock(return_value=httpx.Response(401, json={}))
        async with pool.connection() as conn:
            await sync.start_backfill(conn, stripe)
        await drain()

        cur = await db.execute("SELECT status FROM connection")
        assert (await cur.fetchone())[0] == "needs_reauth"
        cur = await db.execute("SELECT status, attempts FROM sync_job")
        assert await cur.fetchone() == ("pending", 0)  # parked, budget refunded
        assert await drain() == 0  # invisible while the connection is broken
        cur = await db.execute("SELECT count(*) FROM dead_letter")
        assert (await cur.fetchone())[0] == 0  # nothing was given up on


class TestWebhooks:
    async def post(self, connection_id, event: dict, *, headers=None):
        body = json.dumps(event).encode()
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            return await client.post(f"/webhooks/stripe/{connection_id}", content=body,
                                     headers=headers if headers is not None else sign(body))

    def event(self, evt_id: str, topic: str, at: int, **fields) -> dict:
        return {"id": evt_id, "type": topic, "created": at, "data": {"object": customer(1, **fields)}}

    async def test_bad_signature_is_rejected_and_nothing_queued(self, db, pool, stripe):
        resp = await self.post(stripe, self.event("evt_1", "customer.updated", NOW), headers={"stripe-signature": "t=1,v1=00"})
        assert resp.status_code == 400
        cur = await db.execute("SELECT count(*) FROM sync_job")
        assert (await cur.fetchone())[0] == 0

    async def test_duplicate_delivery_is_queued_once(self, db, pool, stripe):
        event = self.event("evt_1", "customer.updated", NOW)
        first = await self.post(stripe, event)
        second = await self.post(stripe, event)
        assert first.json() == {"received": 1, "queued": 1}
        assert second.status_code == 200 and second.json() == {"received": 1, "queued": 0}

    async def test_out_of_order_updates_keep_the_newest(self, db, pool, stripe):
        await self.post(stripe, self.event("evt_new", "customer.updated", NOW, name="Newer Name"))
        await self.post(stripe, self.event("evt_old", "customer.updated", NOW - 60, name="Older Name"))
        await drain()
        assert (await records(db))["cus_1"]["name"] == "Newer Name"

    async def test_delete_before_create_does_not_resurrect(self, db, pool, stripe):
        await self.post(stripe, self.event("evt_del", "customer.deleted", NOW))
        await self.post(stripe, self.event("evt_create", "customer.created", NOW - 60))
        await drain()
        assert (await records(db))["cus_1"]["deleted"] is True

    @respx.mock
    async def test_stale_backfill_page_does_not_overwrite_newer_webhook(self, db, pool, stripe, monkeypatch):
        """Backfill starts at T0 and reads the old name. A webhook from T0+30s
        with the new name is applied first. The page must lose."""
        monkeypatch.setattr(sync, "_now", lambda: T0)
        mock_customers([[customer(1, name="Old Name")]])
        await self.post(stripe, self.event("evt_1", "customer.updated", NOW + 30, name="New Name"))
        async with pool.connection() as conn:
            await sync.start_backfill(conn, stripe)
        await drain()
        assert (await records(db))["cus_1"]["name"] == "New Name"

    async def test_irrelevant_event_is_acknowledged_not_queued(self, db, pool, stripe):
        resp = await self.post(stripe, {"id": "evt_inv", "type": "invoice.paid", "created": NOW,
                                        "data": {"object": {"id": "in_1"}}})
        assert resp.status_code == 200 and resp.json() == {"received": 0, "queued": 0}

    async def test_unknown_connection_is_404(self, db, pool, stripe):
        import uuid
        resp = await self.post(uuid.uuid4(), self.event("evt_1", "customer.updated", NOW))
        assert resp.status_code == 404
