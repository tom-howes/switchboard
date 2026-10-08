"""Mapping engine tests.

These need no database and no network, which is deliberate: the mapping layer is
where provider differences get absorbed, so it is the part that most needs to be
cheap to test.
"""

from datetime import timezone

import pytest
from pydantic import ValidationError

from switchboard.core.mapping import (
    MappingDocument,
    MappingError,
    apply_mapping,
    extract,
)

# A Stripe customer: one `name` string, epoch-seconds timestamps,
# flat payload, and a deliberately messy email.
STRIPE_CUSTOMER = {
    "id": "cus_QxYz123",
    "object": "customer",
    "email": "  Tom@Example.COM ",
    "name": "Tom Howes",
    "phone": "+1 (857) 351-9582",
    "created": 1728000000,
}

STRIPE_MAPPING = MappingDocument(
    external_id="$.id",
    source_created_at={"source": "$.created", "format": "epoch_seconds"},
    fields=[
        {"canonical": "primary_email", "source": "$.email",
         "transforms": ["trim", "lower"], "required": True},
        {"canonical": "full_name", "source": "$.name", "transforms": ["trim"]},
        {"canonical": "phone", "source": "$.phone", "transforms": ["digits"]},
    ],
)

# A HubSpot contact: names split in two, everything nested under `properties`,
# ISO 8601 timestamps.
HUBSPOT_CONTACT = {
    "id": "501",
    "properties": {
        "email": "tom@example.com",
        "firstname": "Tom",
        "lastname": "Howes",
        "phone": "857-351-9582",
    },
    "updatedAt": "2026-10-08T12:00:00Z",
}

HUBSPOT_MAPPING = MappingDocument(
    external_id="$.id",
    source_updated_at={"source": "$.updatedAt", "format": "iso8601"},
    fields=[
        {"canonical": "primary_email", "source": "$.properties.email",
         "transforms": ["trim", "lower"], "required": True},
        {"canonical": "given_name", "source": "$.properties.firstname", "transforms": ["trim"]},
        {"canonical": "family_name", "source": "$.properties.lastname", "transforms": ["trim"]},
        {"canonical": "phone", "source": "$.properties.phone", "transforms": ["digits"]},
    ],
)


class TestExtract:
    def test_nested_path(self):
        assert extract(HUBSPOT_CONTACT, "$.properties.firstname") == "Tom"

    def test_leading_dollar_is_optional(self):
        assert extract(STRIPE_CUSTOMER, "id") == "cus_QxYz123"

    def test_array_index(self):
        payload = {"data": [{"email": "a@b.com"}, {"email": "c@d.com"}]}
        assert extract(payload, "$.data[1].email") == "c@d.com"

    def test_missing_path_is_none_not_an_error(self):
        # A provider omitting an optional field is normal. `required` is where
        # absence becomes a failure, not extraction.
        assert extract(STRIPE_CUSTOMER, "$.properties.nothing.here") is None

    def test_index_past_end_is_none(self):
        assert extract({"data": []}, "$.data[3].email") is None


class TestApplyMapping:
    def test_stripe_shape(self):
        result = apply_mapping(STRIPE_CUSTOMER, STRIPE_MAPPING)

        assert result.external_id == "cus_QxYz123"
        assert result.person.primary_email == "tom@example.com"  # trimmed and lowered
        assert result.person.full_name == "Tom Howes"
        assert result.person.phone == "18573519582"  # digits only

        # Stripe gives one name string. We do NOT guess at a split.
        assert result.person.given_name is None
        assert result.person.family_name is None

        assert result.source_created_at is not None
        assert result.source_created_at.tzinfo == timezone.utc
        assert result.source_created_at.year == 2024

    def test_hubspot_shape(self):
        result = apply_mapping(HUBSPOT_CONTACT, HUBSPOT_MAPPING)

        assert result.external_id == "501"
        assert result.person.primary_email == "tom@example.com"
        assert result.person.given_name == "Tom"
        assert result.person.family_name == "Howes"

        # HubSpot gives the parts. We do NOT concatenate into full_name, because
        # joining on a space is wrong for a great many real names.
        assert result.person.full_name is None

        assert result.source_updated_at is not None
        assert result.source_updated_at.hour == 12

    def test_both_providers_reach_the_same_identity(self):
        """The whole point: two different payload shapes, one match key."""
        stripe = apply_mapping(STRIPE_CUSTOMER, STRIPE_MAPPING)
        hubspot = apply_mapping(HUBSPOT_CONTACT, HUBSPOT_MAPPING)
        assert stripe.person.primary_email == hubspot.person.primary_email

    def test_phone_does_not_normalise_to_the_same_value(self):
        """Documents a real limitation rather than hiding it.

        Stripe carries the country code and HubSpot does not, so `digits` alone
        leaves 18573519582 against 8573519582 for the same physical phone. This
        is exactly why email is the identity match key and phone is not: phone
        numbers need a parsing library and an assumed default region before they
        can be compared, and guessing the region is how you merge two different
        people.
        """
        stripe = apply_mapping(STRIPE_CUSTOMER, STRIPE_MAPPING)
        hubspot = apply_mapping(HUBSPOT_CONTACT, HUBSPOT_MAPPING)

        assert stripe.person.phone != hubspot.person.phone
        assert stripe.person.phone.endswith(hubspot.person.phone)

    def test_multi_source_join(self):
        doc = MappingDocument(
            external_id="$.id",
            fields=[
                {"canonical": "primary_email", "source": "$.email", "required": True},
                {"canonical": "full_name",
                 "source": ["$.properties.firstname", "$.properties.lastname"],
                 "transforms": ["trim"], "join": " "},
            ],
        )
        payload = {"id": "1", "email": "a@b.com",
                   "properties": {"firstname": "Ada", "lastname": "Lovelace"}}
        assert apply_mapping(payload, doc).person.full_name == "Ada Lovelace"

    def test_multi_source_skips_missing_parts(self):
        doc = MappingDocument(
            external_id="$.id",
            fields=[
                {"canonical": "primary_email", "source": "$.email", "required": True},
                {"canonical": "full_name", "source": ["$.first", "$.last"], "join": " "},
            ],
        )
        # No trailing space when only one half is present.
        assert apply_mapping({"id": "1", "email": "a@b.com", "first": "Ada"}, doc).person.full_name == "Ada"


class TestFailureModes:
    def test_missing_required_field_raises(self):
        with pytest.raises(MappingError, match="required field"):
            apply_mapping({"id": "1"}, STRIPE_MAPPING)

    def test_missing_external_id_raises(self):
        with pytest.raises(MappingError, match="external_id"):
            apply_mapping({"email": "a@b.com"}, STRIPE_MAPPING)

    def test_no_email_raises_even_when_not_marked_required(self):
        doc = MappingDocument(
            external_id="$.id",
            fields=[{"canonical": "full_name", "source": "$.name"}],
        )
        with pytest.raises(MappingError, match="primary_email"):
            apply_mapping({"id": "1", "name": "Tom"}, doc)

    def test_object_where_a_scalar_was_expected_raises(self):
        doc = MappingDocument(
            external_id="$.id",
            fields=[{"canonical": "primary_email", "source": "$.email", "required": True}],
        )
        with pytest.raises(MappingError, match="expected a scalar"):
            apply_mapping({"id": "1", "email": {"value": "a@b.com"}}, doc)

    def test_unparseable_timestamp_raises(self):
        doc = MappingDocument(
            external_id="$.id",
            source_created_at={"source": "$.created", "format": "epoch_seconds"},
            fields=[{"canonical": "primary_email", "source": "$.email", "required": True}],
        )
        with pytest.raises(MappingError, match="could not read"):
            apply_mapping({"id": "1", "email": "a@b.com", "created": "not-a-date"}, doc)


class TestDocumentValidation:
    """A bad mapping should be rejected when a human approves it,
    not at 3am during a backfill."""

    def test_unknown_transform_rejected(self):
        with pytest.raises(ValidationError, match="unknown transforms"):
            MappingDocument(
                external_id="$.id",
                fields=[{"canonical": "primary_email", "source": "$.email",
                         "transforms": ["obliterate"]}],
            )

    def test_unknown_canonical_field_rejected(self):
        with pytest.raises(ValidationError, match="unknown canonical field"):
            MappingDocument(
                external_id="$.id",
                fields=[{"canonical": "favourite_colour", "source": "$.colour"}],
            )

    def test_two_rules_writing_the_same_field_rejected(self):
        with pytest.raises(ValidationError, match="both write"):
            MappingDocument(
                external_id="$.id",
                fields=[
                    {"canonical": "primary_email", "source": "$.email"},
                    {"canonical": "primary_email", "source": "$.contact_email"},
                ],
            )
