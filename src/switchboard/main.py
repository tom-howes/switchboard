"""Application entrypoint."""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from .db import close_pool, fetch_one, open_pool


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
