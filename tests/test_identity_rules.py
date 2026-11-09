"""The identity rules on their own: precedence, comparison, conflict detection.
Pure functions, so no database needed."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from switchboard.engine.identity import LinkedRecord, detect_conflicts, merge, phone_key, rank

T = datetime(2026, 11, 1, tzinfo=timezone.utc)


def stripe(**f) -> LinkedRecord:
    return LinkedRecord(provider="stripe", observed_at=f.pop("at", T), full_name=f.get("full_name"),
                        given_name=None, family_name=None, phone=f.get("phone"))


def hubspot(**f) -> LinkedRecord:
    return LinkedRecord(provider="hubspot", observed_at=f.pop("at", T), full_name=None,
                        given_name=f.get("given"), family_name=f.get("family"), phone=f.get("phone"))


class TestMerge:
    def test_each_field_comes_from_its_owner(self):
        person = merge([stripe(full_name="Tom Howes", phone="18573519582"),
                        hubspot(given="Tom", family="Howes", phone="8573519582")])
        assert person == {"full_name": "Tom Howes", "given_name": "Tom",
                          "family_name": "Howes", "phone": "8573519582"}  # HubSpot owns phone

    def test_a_blank_never_beats_a_value(self):
        # HubSpot owns phone, but has none, so Stripe's fills the gap
        person = merge([stripe(phone="18573519582"), hubspot(given="Tom")])
        assert person["phone"] == "18573519582"

    def test_duplicate_records_in_one_provider_use_the_latest(self):
        person = merge([stripe(full_name="Old Name", at=T), stripe(full_name="New Name", at=T + timedelta(days=1))])
        assert person["full_name"] == "New Name"

    def test_unlisted_providers_rank_last(self):
        assert rank("phone", "salesforce") > rank("phone", "stripe")


class TestNameConflicts:
    def test_different_names_conflict_with_hubspot_winning(self):
        [c] = detect_conflicts([stripe(full_name="Thomas Howes"), hubspot(given="Tom", family="Howes")])
        assert (c.field, c.winning_provider, c.winning_value, c.losing_value) == \
               ("name", "hubspot", "Tom Howes", "Thomas Howes")

    def test_case_and_spacing_are_not_conflicts(self):
        assert detect_conflicts([stripe(full_name="tom  HOWES"), hubspot(given="Tom", family="Howes")]) == []

    def test_missing_is_not_different(self):
        assert detect_conflicts([stripe(full_name="Tom Howes"), hubspot()]) == []


class TestPhoneConflicts:
    def test_country_code_alone_is_not_a_conflict(self):
        assert phone_key("18573519582") == phone_key("857-351-9582")
        assert detect_conflicts([stripe(phone="18573519582"), hubspot(phone="8573519582")]) == []

    def test_a_different_number_is(self):
        [c] = detect_conflicts([stripe(phone="18573519582"), hubspot(phone="6175550100")])
        assert (c.field, c.winning_provider, c.losing_provider) == ("phone", "hubspot", "stripe")
