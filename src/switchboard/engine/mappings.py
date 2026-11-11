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


async def save_proposal(conn: AsyncConnection, provider: str, rules: dict, *, proposed_by: str,
                        model: str | None, notes: dict | None, sample: list[dict] | None) -> UUID:
    """Store a drafted mapping as 'proposed'. It does nothing until approved.

    `sample` must be the MASKED sample the proposal was drafted from: it is
    kept so the reviewer can see exactly what the model saw, which is also why
    it must never hold real customer data.
    """
    MappingDocument.model_validate(rules)
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            INSERT INTO field_mapping (provider, version, rules, status, proposed_by, model, notes, sample_payload)
            SELECT %s, coalesce(max(version), 0) + 1, %s, 'proposed', %s, %s, %s, %s
            FROM field_mapping WHERE provider = %s
            RETURNING id
            """,
            (provider, Jsonb(rules), proposed_by, model, Jsonb(notes) if notes else None,
             Jsonb(sample) if sample is not None else None, provider),
        )
        return (await cur.fetchone())["id"]


async def approve_proposal(conn: AsyncConnection, mapping_id: UUID, *, approved_by: str) -> str:
    """Promote a proposal to the live mapping. Returns its provider.

    Supersede-then-approve in one transaction, so there is never a moment with
    two approved mappings (the partial unique index would reject it anyway)
    or with none.
    """
    async with conn.transaction():
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT provider, rules FROM field_mapping WHERE id = %s AND status = 'proposed' FOR UPDATE",
                (mapping_id,),
            )
            row = await cur.fetchone()
        if row is None:
            raise PermanentError(f"mapping {mapping_id} is not a pending proposal")
        MappingDocument.model_validate(row["rules"])
        await conn.execute(
            "UPDATE field_mapping SET status = 'superseded' WHERE provider = %s AND status = 'approved'",
            (row["provider"],),
        )
        await conn.execute(
            "UPDATE field_mapping SET status = 'approved', approved_at = now(), approved_by = %s WHERE id = %s",
            (approved_by, mapping_id),
        )
    return row["provider"]
