"""Masking tests: no real value survives, every format does."""

from __future__ import annotations

import json

from switchboard.core.mapping import MappingDocument, apply_mapping
from switchboard.engine import mappings
from switchboard.llm.masking import mask

from test_hubspot import contact
from test_stripe import customer

# The test fixtures' real values. user1@ rather than the full address, because
# the placeholder is itself an example.com address.
SECRETS = ["user1@", "User 1", "Ada", "Lovelace", "857-351-9582", "cus_1"]


def test_no_real_value_survives():
    masked = json.dumps([mask(customer(1, phone="857-351-9582")), mask(contact(1))])
    for secret in SECRETS:
        assert secret not in masked


def test_keys_and_nesting_are_kept():
    masked = mask(contact(1))
    assert set(masked["properties"]) == {"email", "firstname", "lastname", "phone"}


def test_formats_are_kept():
    assert mask("Tom@Example.com") == "user@example.com"
    assert mask("cus_QxYz123") == "cus_xxxxxxxx"            # object prefix kept
    assert mask("+1 (857) 351-9582") == "+5 (555) 555-5555"  # punctuation kept
    assert mask("2026-10-08T12:00:00.123Z") == "2026-01-01T00:00:00.000Z"
    assert mask("2026-10-08T12:00:00Z") == "2026-01-01T00:00:00Z"
    assert mask(1_728_000_000) == 1_700_000_000              # epoch seconds stays seconds
    assert mask(1_728_000_000_000) == 1_700_000_000_000      # epoch millis stays millis
    assert mask(True) is True and mask(None) is None


def test_long_lists_are_cut_to_three():
    assert len(mask({"items": list(range(100))})["items"]) == 3


def test_seed_mappings_still_work_on_masked_payloads():
    # The proposer dry-runs a proposal on masked samples, so masking must not
    # break the formats a mapping depends on.
    for provider, payload in (("stripe", customer(1)), ("hubspot", contact(1))):
        doc = MappingDocument.model_validate(mappings.seed_rules(provider))
        mapped = apply_mapping(mask(payload), doc)
        assert mapped.person.primary_email == "user@example.com"
