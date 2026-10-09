"""Application entrypoint."""

from __future__ import annotations

from contextlib import asynccontextmanager
from uuid import UUID

from fastapi import FastAPI, HTTPException, Request

from .connectors import get_connector
from .db import close_pool, fetch_one, open_pool, transaction
from .engine import queue


@asynccontextmanager
async def lifespan(app: FastAPI):
    await open_pool()
    yield
    await close_pool()


app = FastAPI(
    title="Switchboard",
    description="A provider-agnostic sync engine.",
    version="0.1.0",
    lifespan=lifespan,
)


@app.get("/health")
async def health() -> dict:
    """Liveness plus a real database round trip.

    A health check that does not touch its dependencies reports healthy right
    up until someone tries to use the thing.
    """
    row = await fetch_one("SELECT count(*) AS n FROM schema_migration")
    return {"status": "ok", "migrations_applied": row["n"] if row else 0}


@app.post("/webhooks/{provider}/{connection_id}")
async def receive_webhook(provider: str, connection_id: UUID, request: Request) -> dict:
    """Verify, dedupe, enqueue, return. No sync work happens in the request.

    Providers time out a webhook in seconds and retry on anything but a 2xx,
    so the handler's only job is to get the event durably into the queue and
    answer quickly. The worker does the rest.

    The connection id is in the URL because a webhook has to be attributed to
    a workspace before anything else can happen, and Stripe's payload does not
    say which of our customers it belongs to.
    """
    raw_body = await request.body()  # bytes, before any JSON parsing: see verify_webhook
    row = await fetch_one("SELECT provider FROM connection WHERE id = %s", (connection_id,))
    if row is None or row["provider"] != provider:
        raise HTTPException(status_code=404, detail="unknown connection")

    connector = get_connector(provider)
    headers = {k.lower(): v for k, v in request.headers.items()}
    if not connector.verify_webhook(raw_body, headers):
        raise HTTPException(status_code=400, detail="invalid signature")

    events = connector.parse_webhook(raw_body, headers)
    queued = 0
    async with transaction() as conn:
        for event in events:
            # The unique constraint is the dedupe. A redelivery inserts nothing,
            # returns no row, and is acknowledged with a 200 like any other:
            # answering a duplicate with an error would only cause another one.
            cur = await conn.execute(
                """
                INSERT INTO webhook_event (connection_id, provider_event_id, topic)
                VALUES (%s, %s, %s)
                ON CONFLICT (connection_id, provider_event_id) DO NOTHING
                RETURNING id
                """,
                (connection_id, event.event_id, event.topic),
            )
            if await cur.fetchone():
                await queue.enqueue(conn, connection_id, "webhook", event.model_dump(mode="json"))
                queued += 1
    return {"received": len(events), "queued": queued}
