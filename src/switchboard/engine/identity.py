"""Identity resolution: which provider records are the same human, what the
person record says, and where the providers disagree.

Rules, each chosen to be explainable after the fact:

- Matching is exact email only. No fuzzy matching: a false merge (two people
  combined) is far more damaging than a missed one, and much harder to undo.
- The person record is RECOMPUTED from all its live records every time, never
  patched incrementally. Recomputing is idempotent, so running it twice, or
  after a record is deleted, always lands in the same place.
- Which value wins is fixed per field (config/field_precedence.json), not
  "most recent". Stripe has no updated timestamp, so its "most recent" would
  really mean "most recently fetched" and every backfill would make Stripe win.
- Disagreements are written to field_conflict rather than silently dropped.

This module is split into pure functions (comparison, precedence, conflict
detection: no database, unit tested directly) and the SQL that applies them.
"""

from __future__ import annotations

import json
import pathlib
import re
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from typing import Callable
from uuid import UUID

from psycopg import AsyncConnection
from psycopg.rows import dict_row

PRECEDENCE_FILE = pathlib.Path(__file__).resolve().parents[3] / "config" / "field_precedence.json"

PERSON_FIELDS = ("full_name", "given_name", "family_name", "phone")


@dataclass(frozen=True)
class LinkedRecord:
    """One live provider record attached to a person, as the resolver sees it."""

    provider: str
    observed_at: datetime
    full_name: str | None
    given_name: str | None
    family_name: str | None
    phone: str | None


@dataclass(frozen=True)
class Conflict:
    field: str
    winning_provider: str
    winning_value: str
    winning_observed_at: datetime
    losing_provider: str
    losing_value: str
    losing_observed_at: datetime


@lru_cache
def precedence() -> dict[str, list[str]]:
    rules = json.loads(PRECEDENCE_FILE.read_text(encoding="utf-8"))
    return {k: v for k, v in rules.items() if not k.startswith("_")}


def rank(field: str, provider: str) -> int:
    """Lower is better. Providers not named in the config rank after all named
    ones, so a new connector never silently outranks an established source."""
    order = precedence().get(field, [])
    return order.index(provider) if provider in order else len(order)


# --------------------------------------------------------------------------
# comparison: what counts as "the same value"
# --------------------------------------------------------------------------


def display_name(r: LinkedRecord) -> str | None:
    """The name as this provider would show it. Used for COMPARISON only.

    Joining HubSpot's first and last name here is not inference: the result
    is never stored, only compared. The person record keeps full_name and the
    split names separately, exactly as the providers supplied them.
    """
    if r.full_name:
        return r.full_name
    parts = [p for p in (r.given_name, r.family_name) if p]
    return " ".join(parts) or None


def name_key(value: str) -> str:
    # "tom  howes" and "Tom Howes" are the same name typed differently.
    return " ".join(value.lower().split())


def phone_key(value: str) -> str:
    """Last ten digits. Stripe keeps the country code (18573519582) and
    HubSpot often does not (8573519582); this treats those as equal.

    Deliberately US-centric, and a known gap: two international numbers that
    share their last ten digits would compare equal. The fix is the
    phonenumbers library with a default region, at the cost of a dependency.
    """
    digits = re.sub(r"\D", "", value)
    return digits[-10:]


# field -> (how to read it off a record, how to normalise it for comparison)
COMPARED: dict[str, tuple[Callable[[LinkedRecord], str | None], Callable[[str], str]]] = {
    "name": (display_name, name_key),
    "phone": (lambda r: r.phone, phone_key),
}


# --------------------------------------------------------------------------
# the pure core
# --------------------------------------------------------------------------


def _latest_per_provider(records: list[LinkedRecord]) -> dict[str, LinkedRecord]:
    """Two Stripe customers can share an email (Stripe allows it, and it
    happens constantly). Within one provider the most recent observation
    stands for that provider; cross-provider precedence then applies."""
    latest: dict[str, LinkedRecord] = {}
    for r in records:
        if r.provider not in latest or r.observed_at > latest[r.provider].observed_at:
            latest[r.provider] = r
    return latest


def merge(records: list[LinkedRecord]) -> dict[str, str | None]:
    """The person's field values: for each field, the highest-precedence
    provider that has a value. A provider with nothing for a field falls
    through to the next, so a blank never beats a real value."""
    by_provider = _latest_per_provider(records)
    result: dict[str, str | None] = {}
    for field in PERSON_FIELDS:
        candidates = [r for r in by_provider.values() if getattr(r, field)]
        candidates.sort(key=lambda r: rank(field, r.provider))
        result[field] = getattr(candidates[0], field) if candidates else None
    return result


def detect_conflicts(records: list[LinkedRecord]) -> list[Conflict]:
    """Every (field, losing provider) pair where a provider's value differs
    from the winning provider's. Providers with no value do not conflict:
    missing is not the same as different."""
    by_provider = _latest_per_provider(records)
    conflicts = []
    for field, (read, normalise) in COMPARED.items():
        present = [(r, read(r)) for r in by_provider.values()]
        present = [(r, v) for r, v in present if v]
        if len(present) < 2:
            continue
        present.sort(key=lambda pair: rank(field, pair[0].provider))
        winner, winning_value = present[0]
        for loser, losing_value in present[1:]:
            if normalise(losing_value) != normalise(winning_value):
                conflicts.append(Conflict(
                    field=field,
                    winning_provider=winner.provider, winning_value=winning_value,
                    winning_observed_at=winner.observed_at,
                    losing_provider=loser.provider, losing_value=losing_value,
                    losing_observed_at=loser.observed_at,
                ))
    return conflicts



# --------------------------------------------------------------------------
# applying it: the SQL
# --------------------------------------------------------------------------

# ON CONFLICT ... DO UPDATE rather than DO NOTHING, on purpose. DO UPDATE
# takes a row lock on the person even when the row already exists. So when a
# Stripe record and a HubSpot record with the same email are saved at the
# same moment, the second transaction waits here until the first commits,
# and its recompute then sees both records. With DO NOTHING each would
# recompute from only its own record, and a disagreement between them would
# go undetected.
CLAIM_PERSON_SQL = """
INSERT INTO person (workspace_id, primary_email)
SELECT c.workspace_id, %(email)s FROM connection c WHERE c.id = %(connection_id)s
ON CONFLICT (workspace_id, primary_email) DO UPDATE SET updated_at = now()
RETURNING id
"""

LINK_SQL = """
INSERT INTO person_link (person_id, provider_record_id, match_rule)
VALUES (%(person_id)s, %(record_id)s, 'email_exact')
ON CONFLICT (provider_record_id)
DO UPDATE SET person_id = EXCLUDED.person_id, match_rule = EXCLUDED.match_rule, matched_at = now()
"""

LIVE_RECORDS_SQL = """
SELECT c.provider, pr.observed_at, pr.full_name, pr.given_name, pr.family_name, pr.phone
FROM provider_record pr
JOIN connection c ON c.id = pr.connection_id
WHERE pr.person_id = %s AND pr.deleted_at IS NULL
"""


async def _one(conn: AsyncConnection, sql: str, params) -> dict | None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(sql, params)
        return await cur.fetchone()


async def resolve(conn: AsyncConnection, record_id: UUID) -> None:
    """Attach one provider record to its person and recompute that person.

    Called inside the same transaction that saved the record, so a record and
    its person are never out of step. If the record's email changed, it moves
    to the person with the new email, and the person it left is recomputed
    too (or deleted, if nothing points at it any more).
    """
    row = await _one(conn, "SELECT connection_id, primary_email, person_id, deleted_at "
                           "FROM provider_record WHERE id = %s", (record_id,))
    previous = row["person_id"]

    if row["deleted_at"] is not None or not row["primary_email"]:
        # A tombstone keeps its link for the audit trail but stops counting.
        if previous:
            await recompute(conn, previous)
        return

    person_id = (await _one(conn, CLAIM_PERSON_SQL, {"email": row["primary_email"],
                                                     "connection_id": row["connection_id"]}))["id"]
    await conn.execute(LINK_SQL, {"person_id": person_id, "record_id": record_id})
    await conn.execute("UPDATE provider_record SET person_id = %s WHERE id = %s", (person_id, record_id))
    await recompute(conn, person_id)

    if previous and previous != person_id:
        await recompute(conn, previous)


async def recompute(conn: AsyncConnection, person_id: UUID) -> None:
    """Rebuild one person's fields from its live records."""
    # Lock first, so two transactions recomputing the same person take turns.
    if await _one(conn, "SELECT id FROM person WHERE id = %s FOR UPDATE", (person_id,)) is None:
        return

    if (await _one(conn, "SELECT count(*) AS n FROM person_link WHERE person_id = %s", (person_id,)))["n"] == 0:
        # Every record moved away (an email change). A person exists only
        # because some record points at it, so this one goes.
        await conn.execute("DELETE FROM person WHERE id = %s", (person_id,))
        return

    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(LIVE_RECORDS_SQL, (person_id,))
        linked = [LinkedRecord(**r) for r in await cur.fetchall()]

    await conn.execute(
        """UPDATE person SET full_name = %(full_name)s, given_name = %(given_name)s,
               family_name = %(family_name)s, phone = %(phone)s, updated_at = now()
           WHERE id = %(person_id)s""",
        {**merge(linked), "person_id": person_id},
    )
