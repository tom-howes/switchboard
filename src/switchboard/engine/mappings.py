"""Storing and loading mapping documents.

A mapping is versioned and never edited in place. Approving a new one
supersedes the old one, so "which mapping produced this row?" always has an
answer: provider_record.mapping_id points at an immutable version.
"""

from __future__ import annotations

import json
import pathlib
from uuid import UUID

from psycopg import AsyncConnection
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from ..core.mapping import MappingDocument
from .errors import PermanentError

SEED_DIR = pathlib.Path(__file__).resolve().parents[3] / "mappings"


async def approved(conn: AsyncConnection, provider: str) -> tuple[UUID, MappingDocument]:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT id, rules FROM field_mapping WHERE provider = %s AND status = 'approved'",
            (provider,),
        )
        row = await cur.fetchone()
    if row is None:
        # Permanent rather than retryable: time will not approve a mapping.
        # The job is dead-lettered and can be replayed once one exists.
        raise PermanentError(f"no approved mapping for {provider!r}")
    return row["id"], MappingDocument.model_validate(row["rules"])


async def approve(
    conn: AsyncConnection, provider: str, rules: dict, *, approved_by: str, proposed_by: str = "human",
    model: str | None = None,
) -> UUID:
    """Validate, supersede the current approved version, insert the new one.

    Validation happens first and outside the database: a mapping that does not
    parse is rejected here, by a person looking at it, rather than discovered
    mid-backfill.
    """
    MappingDocument.model_validate(rules)
    async with conn.transaction():
        await conn.execute(
            "UPDATE field_mapping SET status = 'superseded' WHERE provider = %s AND status = 'approved'",
            (provider,),
        )
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                INSERT INTO field_mapping (provider, version, rules, status, proposed_by, model, approved_at, approved_by)
                SELECT %s, coalesce(max(version), 0) + 1, %s, 'approved', %s, %s, now(), %s
                FROM field_mapping WHERE provider = %s
                RETURNING id
                """,
                (provider, Jsonb(rules), proposed_by, model, approved_by, provider),
            )
            return (await cur.fetchone())["id"]


def seed_rules(provider: str) -> dict:
    return json.loads((SEED_DIR / f"{provider}.json").read_text(encoding="utf-8"))
