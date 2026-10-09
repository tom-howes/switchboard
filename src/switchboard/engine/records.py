"""Writing provider_record rows: the guarded upsert and the tombstone.

Both writes are guarded on observed_at, the moment the incoming version was
known to be true. That is what makes the system safe against the three ways
data arrives out of order:

1. Webhooks are not delivered in order. customer.updated (12:00:05) can land
   after customer.updated (12:00:09).
2. A backfill page fetched at 12:00:00 can be written after a webhook from
   12:00:03 has already been applied.
3. Any job can run twice (the queue is at-least-once).

In every case the older observation hits the WHERE clause and does nothing.
The guard lives in SQL, not in a read-compare-write in Python, because two
workers can both read, both compare, and both write.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from psycopg import AsyncConnection
from psycopg.types.json import Jsonb

from ..core.mapping import MappingDocument, MappingError, apply_mapping

UPSERT_SQL = """
INSERT INTO provider_record (
    connection_id, external_id, raw,
    primary_email, full_name, given_name, family_name, phone,
    mapping_id, source_created_at, source_updated_at, observed_at
)
VALUES (
    %(connection_id)s, %(external_id)s, %(raw)s,
    %(primary_email)s, %(full_name)s, %(given_name)s, %(family_name)s, %(phone)s,
    %(mapping_id)s, %(source_created_at)s, %(source_updated_at)s, %(observed_at)s
)
ON CONFLICT (connection_id, external_id) DO UPDATE SET
    raw               = EXCLUDED.raw,
    primary_email     = EXCLUDED.primary_email,
    full_name         = EXCLUDED.full_name,
    given_name        = EXCLUDED.given_name,
    family_name       = EXCLUDED.family_name,
    phone             = EXCLUDED.phone,
    mapping_id        = EXCLUDED.mapping_id,
    source_created_at = EXCLUDED.source_created_at,
    source_updated_at = EXCLUDED.source_updated_at,
    observed_at       = EXCLUDED.observed_at,
    fetched_at        = now(),
    normalized_at     = now(),
    deleted_at        = NULL
WHERE EXCLUDED.observed_at >= provider_record.observed_at
RETURNING id
"""

# The tombstone is an upsert too. If the record is not here yet, we still
# write a deleted row, so a "created" event that arrives after the "deleted"
# one loses the observed_at comparison instead of resurrecting the customer.
# On conflict, `raw` is left alone: the last known state stays for the audit.
TOMBSTONE_SQL = """
INSERT INTO provider_record (connection_id, external_id, raw, observed_at, deleted_at)
VALUES (%(connection_id)s, %(external_id)s, '{}'::jsonb, %(observed_at)s, %(observed_at)s)
ON CONFLICT (connection_id, external_id) DO UPDATE SET
    deleted_at  = EXCLUDED.deleted_at,
    observed_at = EXCLUDED.observed_at,
    fetched_at  = now()
WHERE EXCLUDED.observed_at >= provider_record.observed_at
RETURNING id
"""


async def store(
    conn: AsyncConnection,
    connection_id: UUID,
    mapping_id: UUID,
    mapping: MappingDocument,
    payload: dict,
    *,
    observed_at: datetime,
    job_id: UUID | None = None,
) -> str:
    """Map and upsert one record. Returns 'stored', 'stale' or 'dead_letter'.

    A record the mapping cannot read is dead-lettered on its own rather than
    failing the job: one Stripe customer with no email must not stop the other
    ninety-nine on the page. Those rows are also useful output in themselves
    ("23 customers have no email address").
    """
    try:
        mapped = apply_mapping(payload, mapping)
    except MappingError as exc:
        await conn.execute(
            "INSERT INTO dead_letter (connection_id, job_id, kind, payload, error) VALUES (%s, %s, 'record', %s, %s)",
            (connection_id, job_id, Jsonb({"mapping_id": str(mapping_id), "record": payload}), str(exc)),
        )
        return "dead_letter"

    cur = await conn.execute(
        UPSERT_SQL,
        {
            "connection_id": connection_id,
            "external_id": mapped.external_id,
            "raw": Jsonb(payload),
            **mapped.person.model_dump(),
            "mapping_id": mapping_id,
            "source_created_at": mapped.source_created_at,
            "source_updated_at": mapped.source_updated_at,
            # A provider timestamp, where there is one, is a better version
            # than our clock. Stripe has none, so it falls back.
            "observed_at": mapped.source_updated_at or observed_at,
        },
    )
    return "stored" if await cur.fetchone() else "stale"


async def tombstone(conn: AsyncConnection, connection_id: UUID, external_id: str, *, observed_at: datetime) -> str:
    cur = await conn.execute(
        TOMBSTONE_SQL, {"connection_id": connection_id, "external_id": external_id, "observed_at": observed_at}
    )
    return "deleted" if await cur.fetchone() else "stale"
