"""The generic connector end to end: the shipped providers/acme_crm.json, a
mapping, the real engine and Postgres. HTTP mocked with respx."""

from __future__ import annotations

import json
from datetime import timedelta

import httpx
import pytest
import respx

from switchboard.core.models import Credentials
from switchboard.engine import connections, mappings, reconcile, sync  # noqa: F401  (registers handlers)
from switchboard.main import app

from test_sync import drain

BASE = "https://api.acme-crm.example/v2"
RULES = {
    "external_id": "$.uuid",
    "fields": [
        {"canonical": "primary_email", "source": "$.email_address", "transforms": ["trim", "lower"], "required": True},
        {"canonical": "full_name", "source": "$.display_name", "transforms": ["trim"]},
    ],
}


def person(n: int) -> dict:
    return {"uuid": f"p-{n}", "email_address": f"Person{n}@Acme.example", "display_name": f"Person {n}"}


@pytest.fixture
async def acme(db, pool):
    """An acme_crm connection with no mapping yet, as `connect` leaves it."""
    async with pool.connection() as conn:
        cur = await conn.execute("INSERT INTO workspace (name) VALUES ('test') RETURNING id")
        workspace_id = (await cur.fetchone())["id"]
        return await connections.save(conn, workspace_id, "acme_crm", Credentials(api_key="k"))


async def approve_mapping(pool) -> int:
    async with pool.connection() as conn:
        await mappings.approve(conn, "acme_crm", RULES, approved_by="test")
        return await sync.start_pending_backfills(conn, "acme_crm")


@respx.mock
async def test_backfill_from_a_json_file_and_no_code(db, pool, acme):
    def pages(request: httpx.Request) -> httpx.Response:
        assert request.headers["x-api-key"] == "k"
        assert request.url.params["status"] == "active"  # fixed params from the provider file
        return httpx.Response(200, json={"results": [person(1), person(2)]})  # short page: the walk ends

    respx.get(f"{BASE}/people").mock(side_effect=pages)
    assert await approve_mapping(pool) == 1  # approving started the waiting backfill
    await drain()

    cur = await db.execute("SELECT primary_email, full_name FROM person ORDER BY 1")
    assert await cur.fetchall() == [("person1@acme.example", "Person 1"), ("person2@acme.example", "Person 2")]


async def test_a_connection_that_already_backfilled_is_not_restarted(db, pool, acme):
    assert await approve_mapping(pool) == 1
    async with pool.connection() as conn:
        assert await sync.start_pending_backfills(conn, "acme_crm") == 0


@respx.mock
async def test_generic_connectors_sweep_hourly(db, pool, acme):
    respx.get(f"{BASE}/people").mock(return_value=httpx.Response(200, json={"results": [person(1)]}))
    await approve_mapping(pool)
    async with pool.connection() as conn:
        await reconcile.schedule(conn, acme, after=timedelta(0))
    await drain()
    cur = await db.execute("SELECT run_after - now() FROM sync_job WHERE kind = 'reconcile' AND status = 'pending'")
    [(delay,)] = await cur.fetchall()
    assert timedelta(minutes=59) < delay <= timedelta(hours=1)


async def test_webhooks_to_a_generic_connector_are_turned_away(db, pool, acme):
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(f"/webhooks/acme_crm/{acme}", content=json.dumps({"x": 1}))
    assert resp.status_code == 404 and "does not accept webhooks" in resp.text
