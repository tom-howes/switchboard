"""Stripe connector tests. HTTP is mocked with respx; no network, no database."""

from __future__ import annotations

import hashlib
import hmac
import json

import httpx
import pytest
import respx

from switchboard.connectors.stripe import API, StripeConnector
from switchboard.core.models import Credentials
from switchboard.engine.errors import AuthError, PermanentError, RetryableError

SECRET = "whsec_test"
NOW = 1_728_000_000
CREDS = Credentials(api_key="sk_test_123")


def connector() -> StripeConnector:
    return StripeConnector(api_key="sk_test_123", webhook_secret=SECRET, clock=lambda: NOW)


def customer(n: int, **extra) -> dict:
    return {"id": f"cus_{n}", "object": "customer", "email": f"user{n}@example.com",
            "name": f"User {n}", "phone": None, "created": NOW - n, **extra}


def sign(body: bytes, *, secret: str = SECRET, timestamp: int = NOW) -> dict[str, str]:
    sig = hmac.new(secret.encode(), f"{timestamp}.".encode() + body, hashlib.sha256).hexdigest()
    return {"stripe-signature": f"t={timestamp},v1={sig}"}


class TestPagination:
    @respx.mock
    async def test_walks_pages_with_starting_after(self):
        def page(request: httpx.Request) -> httpx.Response:
            after = request.url.params.get("starting_after")
            if after is None:
                return httpx.Response(200, json={"data": [customer(1), customer(2)], "has_more": True})
            assert after == "cus_2"
            return httpx.Response(200, json={"data": [customer(3)], "has_more": False})

        route = respx.get(f"{API}/customers").mock(side_effect=page)
        first = await connector().list_changes(CREDS, None)
        assert [r.external_id for r in first.records] == ["cus_1", "cus_2"]
        assert first.next_cursor == "cus_2"

        last = await connector().list_changes(CREDS, first.next_cursor)
        assert last.next_cursor is None
        # every request carries the key and the pinned API version
        sent = route.calls[0].request
        assert sent.headers["authorization"] == "Bearer sk_test_123"
        assert sent.headers["stripe-version"] == "2024-06-20"

    @respx.mock
    async def test_empty_account(self):
        respx.get(f"{API}/customers").mock(return_value=httpx.Response(200, json={"data": [], "has_more": False}))
        page = await connector().list_changes(CREDS, None)
        assert page.records == [] and page.next_cursor is None


class TestErrorClassification:
    @respx.mock
    @pytest.mark.parametrize(
        "status, headers, expected",
        [
            (429, {"retry-after": "7"}, RetryableError),
            (500, {}, RetryableError),
            (503, {}, RetryableError),
            (401, {}, AuthError),
            (403, {}, AuthError),
            (400, {}, PermanentError),
        ],
    )
    async def test_status_codes(self, status, headers, expected):
        respx.get(f"{API}/customers").mock(return_value=httpx.Response(status, headers=headers, json={}))
        with pytest.raises(expected) as info:
            await connector().list_changes(CREDS, None)
        if status == 429:
            assert info.value.retry_after == 7

    @respx.mock
    async def test_network_failure_is_retryable(self):
        respx.get(f"{API}/customers").mock(side_effect=httpx.ConnectTimeout("slow"))
        with pytest.raises(RetryableError):
            await connector().list_changes(CREDS, None)

    @respx.mock
    async def test_fetch_one_missing_or_deleted_is_none(self):
        respx.get(f"{API}/customers/cus_gone").mock(return_value=httpx.Response(404, json={}))
        respx.get(f"{API}/customers/cus_del").mock(
            return_value=httpx.Response(200, json={"id": "cus_del", "deleted": True})
        )
        assert await connector().fetch_one(CREDS, "cus_gone") is None
        assert await connector().fetch_one(CREDS, "cus_del") is None


class TestWebhookSignature:
    BODY = json.dumps({"id": "evt_1", "type": "customer.updated", "created": NOW,
                       "data": {"object": customer(1)}}).encode()

    def test_valid_signature(self):
        assert connector().verify_webhook(self.BODY, sign(self.BODY))

    def test_wrong_secret(self):
        assert not connector().verify_webhook(self.BODY, sign(self.BODY, secret="whsec_other"))

    def test_body_modified_after_signing(self):
        # Including a re-serialised body: same JSON, different bytes.
        reserialised = json.dumps(json.loads(self.BODY), indent=2).encode()
        assert not connector().verify_webhook(reserialised, sign(self.BODY))

    def test_replay_outside_window(self):
        old = NOW - 301
        assert not connector().verify_webhook(self.BODY, sign(self.BODY, timestamp=old))

    def test_secret_rotation_accepts_either_signature(self):
        good = sign(self.BODY)["stripe-signature"].split(",v1=")[1]
        header = {"stripe-signature": f"t={NOW},v1={'0' * 64},v1={good}"}
        assert connector().verify_webhook(self.BODY, header)

    @pytest.mark.parametrize("header", ["", "garbage", "t=notanumber,v1=abc", f"t={NOW}"])
    def test_malformed_headers(self, header):
        assert not connector().verify_webhook(self.BODY, {"stripe-signature": header})

    def test_no_secret_configured_rejects_everything(self):
        bare = StripeConnector(api_key="k", webhook_secret="", clock=lambda: NOW)
        assert not bare.verify_webhook(self.BODY, sign(self.BODY))


class TestWebhookParsing:
    def _parse(self, event: dict):
        return connector().parse_webhook(json.dumps(event).encode(), {})

    def test_customer_update_carries_full_record(self):
        [event] = self._parse({"id": "evt_1", "type": "customer.updated", "created": NOW,
                               "data": {"object": customer(1)}})
        assert event.external_id == "cus_1"
        assert event.record["email"] == "user1@example.com"
        assert event.occurred_at.timestamp() == NOW

    def test_delete_has_no_record(self):
        [event] = self._parse({"id": "evt_2", "type": "customer.deleted", "created": NOW,
                               "data": {"object": customer(1)}})
        assert event.deleted and event.record is None

    @pytest.mark.parametrize("topic", ["invoice.paid", "customer.subscription.updated", "customer.source.created"])
    def test_irrelevant_events_ignored(self, topic):
        assert self._parse({"id": "evt_3", "type": topic, "created": NOW, "data": {"object": {"id": "x"}}}) == []
