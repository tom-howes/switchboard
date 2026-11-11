"""Mapping proposer tests. The Anthropic client is faked: these test what we
send and what we do with the answer, not the model."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from switchboard.llm.proposer import (
    ProposalError,
    ProposalOutput,
    ProposedField,
    ProposedTimestamp,
    propose,
)

from test_hubspot import contact


class FakeMessages:
    def __init__(self, replies):
        self.replies = list(replies)
        self.calls = []

    async def parse(self, **kwargs):
        self.calls.append(kwargs)
        reply = self.replies.pop(0)
        if isinstance(reply, str):  # a bare stop_reason, e.g. "refusal"
            return SimpleNamespace(stop_reason=reply, parsed_output=None)
        return SimpleNamespace(stop_reason="end_turn", parsed_output=reply)


class FakeClient:
    def __init__(self, *replies):
        self.beta = SimpleNamespace(messages=FakeMessages(replies))

    @property
    def calls(self):
        return self.beta.messages.calls


def field(canonical, source, transforms=(), required=False):
    return ProposedField(canonical=canonical, source=[source] if isinstance(source, str) else source,
                         transforms=list(transforms), required=required,
                         reasoning=f"{source} looks like {canonical}", confidence="high")


GOOD = ProposalOutput(
    external_id="$.id",
    source_created_at=ProposedTimestamp(source="$.createdAt", format="iso8601"),
    source_updated_at=ProposedTimestamp(source="$.updatedAt", format="iso8601"),
    fields=[
        field("primary_email", "$.properties.email", ["trim", "lower"], required=True),
        field("given_name", "$.properties.firstname", ["trim"]),
        field("family_name", "$.properties.lastname", ["trim"]),
    ],
    unmapped_notes="no full name field",
)

# Plausible but wrong: email at the top level, where this provider has none
WRONG_PATH = GOOD.model_copy(update={"fields": [field("primary_email", "$.email", ["trim", "lower"], required=True)]})

SAMPLES = [contact(1), contact(2)]


async def test_a_good_proposal_comes_back_with_its_reasoning():
    client = FakeClient(GOOD)
    proposal = await propose("acme", SAMPLES, client=client)
    assert proposal.rules["fields"][0] == {"canonical": "primary_email", "source": "$.properties.email",
                                           "transforms": ["trim", "lower"], "required": True}
    assert proposal.notes["fields"]["given_name"]["confidence"] == "high"
    assert proposal.notes["unmapped"] == "no full name field"
    assert len(client.calls) == 1


async def test_only_masked_values_are_sent():
    client = FakeClient(GOOD)
    proposal = await propose("acme", SAMPLES, client=client)
    sent = json.dumps(client.calls[0]["messages"])
    for real in ("user1@example.com", "Ada", "Lovelace", "857-351-9582"):
        assert real not in sent
    assert "properties" in sent and "firstname" in sent  # the shape still goes
    assert proposal.masked_samples[0]["properties"]["email"] == "user@example.com"


async def test_request_uses_opus_high_effort_and_fallbacks():
    client = FakeClient(GOOD)
    await propose("acme", SAMPLES, client=client)
    call = client.calls[0]
    assert call["model"] == "claude-opus-5-5"
    assert call["output_config"] == {"effort": "high"}
    assert call["fallbacks"] == "default"
    assert call["output_format"] is ProposalOutput


async def test_a_failed_check_is_retried_once_with_the_errors():
    client = FakeClient(WRONG_PATH, GOOD)
    proposal = await propose("acme", SAMPLES, client=client)
    assert len(client.calls) == 2
    retry_prompt = client.calls[1]["messages"][0]["content"]
    assert "failed validation" in retry_prompt and "primary_email" in retry_prompt
    assert proposal.rules["fields"][0]["source"] == "$.properties.email"


async def test_two_failures_give_up_and_save_nothing():
    client = FakeClient(WRONG_PATH, WRONG_PATH)
    with pytest.raises(ProposalError, match="after one retry"):
        await propose("acme", SAMPLES, client=client)
    assert len(client.calls) == 2  # never a third attempt


async def test_a_refusal_is_reported_not_retried():
    client = FakeClient("refusal")
    with pytest.raises(ProposalError, match="declined"):
        await propose("acme", SAMPLES, client=client)
    assert len(client.calls) == 1


async def test_no_samples_means_no_api_call():
    client = FakeClient()
    with pytest.raises(ProposalError):
        await propose("acme", [], client=client)
    assert client.calls == []


class TestStoringProposals:
    """Against Postgres: a proposal is inert until approved, and approving it
    swaps the live mapping atomically."""

    async def test_a_proposal_is_not_live_until_approved(self, db, pool):
        from switchboard.engine import mappings
        from switchboard.engine.errors import PermanentError

        proposal = await propose("acme", SAMPLES, client=FakeClient(GOOD))
        async with pool.connection() as conn:
            mapping_id = await mappings.save_proposal(
                conn, "acme", proposal.rules, proposed_by="claude", model=proposal.model,
                notes=proposal.notes, sample=proposal.masked_samples)
            with pytest.raises(PermanentError, match="no approved mapping"):
                await mappings.approved(conn, "acme")

            await mappings.approve_proposal(conn, mapping_id, approved_by="tom")
            live_id, doc = await mappings.approved(conn, "acme")
        assert live_id == mapping_id
        assert doc.fields[0].source == "$.properties.email"
        cur = await db.execute("SELECT sample_payload::text, notes IS NOT NULL FROM field_mapping")
        stored_sample, has_notes = await cur.fetchone()
        assert "Lovelace" not in stored_sample and has_notes  # only the masked copy is kept

    async def test_approving_supersedes_the_previous_mapping(self, db, pool):
        from switchboard.engine import mappings

        async with pool.connection() as conn:
            await mappings.approve(conn, "hubspot", mappings.seed_rules("hubspot"), approved_by="seed")
            proposal = await propose("hubspot", SAMPLES, client=FakeClient(GOOD))
            new_id = await mappings.save_proposal(conn, "hubspot", proposal.rules, proposed_by="claude",
                                                  model=proposal.model, notes=proposal.notes,
                                                  sample=proposal.masked_samples)
            await mappings.approve_proposal(conn, new_id, approved_by="tom")
        cur = await db.execute("SELECT version, status FROM field_mapping WHERE provider = 'hubspot' ORDER BY 1")
        assert await cur.fetchall() == [(1, "superseded"), (2, "approved")]
