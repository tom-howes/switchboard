"""HubSpot connector tests (everything except OAuth refresh, which has its own
file). HTTP mocked with respx; no network, no database."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json

import httpx
import pytest
import respx

from switchboard.connectors.hubspot import API, HubSpotConnector
from switchboard.core.mapping import MappingDocument, apply_mapping
from switchboard.core.models import Credentials
from switchboard.engine import mappings

SECRET = "hubspot_client_secret"
NOW = 1_728_000_000
URL = "https://switchboard.example.com/webhooks/hubspot/abc"
CREDS = Credentials(access_token="at_1", refresh_token="rt_1")


def connector() -> HubSpotConnector:
    return HubSpotConnector(client_id="cid", client_secret=SECRET, clock=lambda: NOW)


def contact(n: int, **props) -> dict:
    return {"id": str(n), "createdAt": "2026-01-01T00:00:00.000Z", "updatedAt": "2026-10-08T12:00:00.123Z",
            "archived": False,
            "properties": {"email": f"user{n}@example.com", "firstname": "Ada", "lastname": "Lovelace",
                           "phone": "857-351-9582", **props}}


def sign(body: bytes, *, url: str = URL, timestamp_ms: int = NOW * 1000, secret: str = SECRET) -> dict[str, str]:
    message = b"POST" + url.encode() + body + str(timestamp_ms).encode()
    sig = base64.b64encode(hmac.new(secret.encode(), message, hashlib.sha256).digest()).decode()
    return {"x-hubspot-signature-v3": sig, "x-hubspot-request-timestamp": str(timestamp_ms)}


class TestReading:
    @respx.mock
    async def test_pagination_uses_opaque_after_token(self):
        def page(request: httpx.Request) -> httpx.Response:
            # always ask for the properties, or email silently goes missing
            assert request.url.params["properties"] == "email,firstname,lastname,phone"
            if request.url.params.get("after") is None:
                return httpx.Response(200, json={"results": [contact(1)], "paging": {"next": {"after": "MjA="}}})
            assert request.url.params["after"] == "MjA="  # the token, not the last id
            return httpx.Response(200, json={"results": [contact(2)]})

        route = respx.get(f"{API}/crm/v3/objects/contacts").mock(side_effect=page)
        first = await connector().list_changes(CREDS, None)
        assert first.next_cursor == "MjA="
        last = await connector().list_changes(CREDS, first.next_cursor)
        assert [r.external_id for r in last.records] == ["2"] and last.next_cursor is None
        assert route.calls[0].request.headers["authorization"] == "Bearer at_1"

    @respx.mock
    async def test_fetch_one_archived_contact_is_none(self):
        respx.get(f"{API}/crm/v3/objects/contacts/9").mock(return_value=httpx.Response(404, json={}))
        assert await connector().fetch_one(CREDS, "9") is None

    def test_seed_mapping_reads_a_real_shaped_contact(self):
        doc = MappingDocument.model_validate(mappings.seed_rules("hubspot"))
        mapped = apply_mapping(contact(1, email=" Ada@Example.com "), doc)
        assert mapped.person.primary_email == "ada@example.com"
        assert (mapped.person.given_name, mapped.person.family_name) == ("Ada", "Lovelace")
        assert mapped.person.full_name is None  # never inferred from the parts
        assert mapped.source_updated_at.microsecond == 123000  # milliseconds survive


class TestWebhookSignature:
    BODY = json.dumps([{"eventId": 1, "subscriptionType": "contact.propertyChange",
                        "objectId": 501, "occurredAt": NOW * 1000}]).encode()

    def test_valid(self):
        assert connector().verify_webhook(self.BODY, sign(self.BODY), URL)

    def test_signed_for_a_different_url(self):
        # the same signed body replayed at another endpoint
        assert not connector().verify_webhook(self.BODY, sign(self.BODY), URL.replace("abc", "xyz"))

    def test_tampered_body(self):
        assert not connector().verify_webhook(self.BODY + b" ", sign(self.BODY), URL)

    def test_expired_timestamp(self):
        old = (NOW - 301) * 1000
        assert not connector().verify_webhook(self.BODY, sign(self.BODY, timestamp_ms=old), URL)

    def test_missing_headers(self):
        assert not connector().verify_webhook(self.BODY, {}, URL)


class TestWebhookParsing:
    def test_batch_becomes_one_event_each_without_records(self):
        body = json.dumps([
            {"eventId": 11, "subscriptionType": "contact.propertyChange", "objectId": 501,
             "propertyName": "firstname", "propertyValue": "Ada", "occurredAt": NOW * 1000},
            {"eventId": 12, "subscriptionType": "contact.deletion", "objectId": 502, "occurredAt": NOW * 1000},
            {"eventId": 13, "subscriptionType": "deal.creation", "objectId": 9, "occurredAt": NOW * 1000},
        ]).encode()
        events = connector().parse_webhook(body, {})
        assert [(e.event_id, e.external_id, e.deleted) for e in events] == [("11", "501", False), ("12", "502", True)]
        assert all(e.record is None for e in events)  # HubSpot never sends the record

    def test_merge_is_not_treated_as_a_record_change(self):
        body = json.dumps([{"eventId": 1, "subscriptionType": "contact.merge", "objectId": 1,
                            "occurredAt": NOW * 1000}]).encode()
        assert connector().parse_webhook(body, {}) == []
