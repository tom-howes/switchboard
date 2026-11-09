"""Identity resolution against a real Postgres: records from two providers
landing on one person, email changes, deletes, and the concurrent-save race."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from switchboard import db as app_db
from switchboard.core.mapping import MappingDocument
from switchboard.core.models import Credentials
from switchboard.engine import connections, mappings, records

T = datetime(2026, 11, 1, tzinfo=timezone.utc)
STRIPE = MappingDocument.model_validate(mappings.seed_rules("stripe"))
HUBSPOT = MappingDocument.model_validate(mappings.seed_rules("hubspot"))


@pytest.fixture
async def workspace(db, pool):
    """One workspace with a Stripe and a HubSpot connection and approved mappings."""
    async with pool.connection() as conn:
        cur = await conn.execute("INSERT INTO workspace (name) VALUES ('test') RETURNING id")
        ws = (await cur.fetchone())["id"]
        ids = {}
        for provider in ("stripe", "hubspot"):
            mapping_id = await mappings.approve(conn, provider, mappings.seed_rules(provider), approved_by="test")
            connection_id = await connections.save(conn, ws, provider, Credentials(api_key="k"))
            ids[provider] = (connection_id, mapping_id)
        return ids


def stripe_customer(cid: str, email: str, name: str | None = "Tom Howes", phone: str | None = "+1 857 351 9582") -> dict:
    return {"id": cid, "email": email, "name": name, "phone": phone, "created": 1728000000}


def hubspot_contact(cid: str, email: str, first="Tom", last="Howes", phone="857-351-9582", at: datetime = T) -> dict:
    return {"id": cid, "createdAt": "2026-01-01T00:00:00Z", "updatedAt": at.isoformat(),
            "properties": {"email": email, "firstname": first, "lastname": last, "phone": phone}}


async def save(workspace, provider: str, payload: dict, at: datetime = T) -> str:
    connection_id, mapping_id = workspace[provider]
    async with app_db.transaction() as conn:
        return await records.store(conn, connection_id, mapping_id, STRIPE if provider == "stripe" else HUBSPOT,
                                   payload, observed_at=at)


async def delete(workspace, provider: str, external_id: str, at: datetime) -> None:
    async with app_db.transaction() as conn:
        await records.tombstone(conn, workspace[provider][0], external_id, observed_at=at)


async def people(db) -> list[tuple]:
    cur = await db.execute("SELECT primary_email, full_name, given_name, family_name, phone FROM person ORDER BY 1")
    return await cur.fetchall()


async def links(db) -> list[tuple]:
    cur = await db.execute("""SELECT pr.external_id, p.primary_email, pl.match_rule FROM person_link pl
                              JOIN provider_record pr ON pr.id = pl.provider_record_id
                              JOIN person p ON p.id = pl.person_id ORDER BY 1""")
    return await cur.fetchall()


class TestLinking:
    async def test_same_email_in_both_systems_is_one_person(self, db, workspace):
        await save(workspace, "stripe", stripe_customer("cus_1", "tom@example.com"))
        await save(workspace, "hubspot", hubspot_contact("501", "TOM@example.com"))  # mapping lowercases
        assert await people(db) == [("tom@example.com", "Tom Howes", "Tom", "Howes", "8573519582")]
        assert await links(db) == [("501", "tom@example.com", "email_exact"),
                                   ("cus_1", "tom@example.com", "email_exact")]

    async def test_different_emails_are_different_people(self, db, workspace):
        await save(workspace, "stripe", stripe_customer("cus_1", "a@example.com"))
        await save(workspace, "hubspot", hubspot_contact("501", "b@example.com"))
        assert [p[0] for p in await people(db)] == ["a@example.com", "b@example.com"]

    async def test_each_field_comes_from_its_owner(self, db, workspace):
        # Stripe owns full_name; HubSpot owns the split names and phone.
        await save(workspace, "stripe", stripe_customer("cus_1", "tom@example.com", phone="+1 617 555 0100"))
        await save(workspace, "hubspot", hubspot_contact("501", "tom@example.com"))
        [(_, full, given, family, phone)] = await people(db)
        assert (full, given, family, phone) == ("Tom Howes", "Tom", "Howes", "8573519582")

    async def test_email_change_moves_the_record_and_removes_the_empty_person(self, db, workspace):
        await save(workspace, "stripe", stripe_customer("cus_1", "old@example.com"))
        await save(workspace, "stripe", stripe_customer("cus_1", "new@example.com"), at=T + timedelta(hours=1))
        assert [p[0] for p in await people(db)] == ["new@example.com"]
        assert await links(db) == [("cus_1", "new@example.com", "email_exact")]

    async def test_deleted_record_stops_counting(self, db, workspace):
        await save(workspace, "stripe", stripe_customer("cus_1", "tom@example.com", phone="+1 617 555 0100"))
        await save(workspace, "hubspot", hubspot_contact("501", "tom@example.com", phone=None))
        await delete(workspace, "stripe", "cus_1", at=T + timedelta(hours=1))
        [(_, full, _, _, phone)] = await people(db)
        assert (full, phone) == (None, None)  # Stripe's values no longer contribute
        assert len(await links(db)) == 2      # but the link stays, for the audit trail

    async def test_concurrent_saves_from_both_systems_still_produce_one_person(self, db, workspace):
        await asyncio.gather(
            save(workspace, "stripe", stripe_customer("cus_1", "tom@example.com")),
            save(workspace, "hubspot", hubspot_contact("501", "tom@example.com")),
        )
        assert await people(db) == [("tom@example.com", "Tom Howes", "Tom", "Howes", "8573519582")]

async def conflicts(db, open_only: bool = True) -> list[tuple]:
    cur = await db.execute(
        "SELECT field, winning_provider, winning_value, losing_provider, losing_value, resolution FROM field_conflict"
        + (" WHERE resolved_at IS NULL" if open_only else "") + " ORDER BY field"
    )
    return await cur.fetchall()


class TestConflicts:
    async def test_name_disagreement_is_recorded_with_hubspot_winning(self, db, workspace):
        await save(workspace, "stripe", stripe_customer("cus_1", "tom@example.com", name="Thomas Howes"))
        await save(workspace, "hubspot", hubspot_contact("501", "tom@example.com"))
        assert await conflicts(db) == [("name", "hubspot", "Tom Howes", "stripe", "Thomas Howes", None)]

    async def test_country_code_is_not_a_conflict(self, db, workspace):
        await save(workspace, "stripe", stripe_customer("cus_1", "tom@example.com", phone="+1 857 351 9582"))
        await save(workspace, "hubspot", hubspot_contact("501", "tom@example.com", phone="857-351-9582"))
        assert await conflicts(db) == []

    async def test_repeated_saves_update_one_conflict_not_many(self, db, workspace):
        await save(workspace, "stripe", stripe_customer("cus_1", "tom@example.com", phone="+1 617 555 0100"))
        for i in range(3):
            await save(workspace, "hubspot", hubspot_contact("501", "tom@example.com", at=T + timedelta(minutes=i)))
        assert [c[0] for c in await conflicts(db)] == ["phone"]
        cur = await db.execute("SELECT count(*) FROM field_conflict")
        assert (await cur.fetchone())[0] == 1

    async def test_fixing_the_value_closes_the_conflict(self, db, workspace):
        await save(workspace, "stripe", stripe_customer("cus_1", "tom@example.com", phone="+1 617 555 0100"))
        await save(workspace, "hubspot", hubspot_contact("501", "tom@example.com"))
        await save(workspace, "hubspot", hubspot_contact("501", "tom@example.com", phone="617-555-0100",
                                                         at=T + timedelta(hours=1)))
        assert await conflicts(db) == []
        closed = await conflicts(db, open_only=False)
        assert [(c[0], c[5]) for c in closed] == [("phone", "converged")]  # kept as history

    async def test_deleting_the_disagreeing_record_closes_the_conflict(self, db, workspace):
        await save(workspace, "stripe", stripe_customer("cus_1", "tom@example.com", name="Thomas Howes"))
        await save(workspace, "hubspot", hubspot_contact("501", "tom@example.com"))
        await delete(workspace, "stripe", "cus_1", at=T + timedelta(hours=1))
        assert await conflicts(db) == []

    async def test_concurrent_saves_for_an_existing_person_still_detect_the_conflict(self, db, workspace):
        """The race the person row lock exists for.

        The person already exists (from an earlier Stripe record with no name).
        A new Stripe record and a HubSpot record for the same email are then
        saved at the same moment. Without the lock, each transaction recomputes
        while the other's record is still uncommitted, so neither sees both
        names and the disagreement is never recorded. Repeated, because a race
        test that passes once proves little.
        """
        for i in range(10):
            email = f"tom{i}@example.com"
            await save(workspace, "stripe", stripe_customer(f"cus_old{i}", email, name=None, phone=None))
            await asyncio.gather(
                save(workspace, "stripe", stripe_customer(f"cus_new{i}", email, name="Thomas Howes", phone=None)),
                save(workspace, "hubspot", hubspot_contact(f"50{i}", email, phone=None)),
            )
        cur = await db.execute("SELECT count(*) FROM field_conflict WHERE field = 'name' AND resolved_at IS NULL")
        assert (await cur.fetchone())[0] == 10
