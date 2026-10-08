"""Database access.

Raw SQL on purpose. The interesting parts of this project are the queries:
the SKIP LOCKED claim, the guarded upserts, the reconciliation sweep. An ORM
would hide exactly the code worth reading.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from .config import get_settings

_pool: AsyncConnectionPool | None = None


async def open_pool() -> AsyncConnectionPool:
    global _pool
    if _pool is None:
        _pool = AsyncConnectionPool(
            get_settings().database_url,
            min_size=1,
            max_size=10,
            kwargs={"row_factory": dict_row},
            open=False,
        )
        await _pool.open(wait=True)
    return _pool


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


def pool() -> AsyncConnectionPool:
    if _pool is None:
        raise RuntimeError("pool not open; call open_pool() during startup")
    return _pool


@asynccontextmanager
async def transaction() -> AsyncIterator[Any]:
    """A connection inside a transaction.

    Used where several writes must land together, e.g. inserting a
    provider_record and its person_link.
    """
    async with pool().connection() as conn:
        async with conn.transaction():
            yield conn


async def fetch_all(sql: str, params: tuple | dict | None = None) -> list[dict]:
    async with pool().connection() as conn:
        cur = await conn.execute(sql, params)
        return await cur.fetchall()


async def fetch_one(sql: str, params: tuple | dict | None = None) -> dict | None:
    async with pool().connection() as conn:
        cur = await conn.execute(sql, params)
        return await cur.fetchone()


async def execute(sql: str, params: tuple | dict | None = None) -> int:
    async with pool().connection() as conn:
        cur = await conn.execute(sql, params)
        return cur.rowcount
