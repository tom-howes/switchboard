"""Stripe: customers, read-only.

What is genuinely Stripe-specific, and therefore lives here rather than in the
engine:

- Auth is a static secret key. Nothing to refresh, nothing to exchange.
- Pagination is a cursor: `starting_after=<last id on this page>`.
- Webhooks are signed with HMAC-SHA256 over "<timestamp>.<raw body>", and the
  payload includes the full customer object.
- Customers have `created` but NO updated timestamp. That single fact is why
  the engine orders writes on observed_at rather than trusting the source.

What is NOT here: how a customer becomes a Person. That is mappings/stripe.json.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import time
from datetime import datetime, timezone
from typing import Callable

import httpx

from ..core.models import Capability, Credentials, Page, Person, RawRecord, WebhookEvent, WriteResult
from ..engine.errors import NotFoundError, PermanentError
from ..engine.http import TIMEOUT, request

API = "https://api.stripe.com/v1"

# Pinned, so the payload shape the mapping was written against cannot change
# underneath it when the account's default API version is upgraded.
STRIPE_VERSION = "2024-06-20"

PAGE_SIZE = 100  # Stripe's maximum

# Stripe's own recommendation. Bounds how long a captured request stays
# replayable; the webhook_event table catches replays inside the window.
SIGNATURE_TOLERANCE_SECONDS = 300

CUSTOMER_EVENTS = {"customer.created", "customer.updated", "customer.deleted"}


class StripeConnector:
    id = "stripe"
    capabilities = frozenset({Capability.READ, Capability.WEBHOOKS})

    def __init__(self, *, api_key: str, webhook_secret: str, clock: Callable[[], float] = time.time):
        self._api_key = api_key
        self._webhook_secret = webhook_secret
        self._clock = clock  # injectable so tests can check the replay window

    # ---- credentials -------------------------------------------------

    async def authorize(self, code: str, state: str) -> Credentials:
        if not self._api_key:
            raise PermanentError("STRIPE_API_KEY is not set")
        return Credentials(api_key=self._api_key)

    async def refresh(self, creds: Credentials) -> Credentials:
        return creds  # secret keys do not expire

    def _headers(self, creds: Credentials) -> dict[str, str]:
        return {"Authorization": f"Bearer {creds.api_key}", "Stripe-Version": STRIPE_VERSION}

    # ---- reading -----------------------------------------------------

    async def list_changes(self, creds: Credentials, cursor: str | None) -> Page:
        """One page of customers.

        "Changes" is generous: Stripe's list endpoint cannot filter by update
        time, so this is a full walk, used for backfill and reconciliation.
        Incremental updates arrive by webhook.
        """
        params: dict[str, str | int] = {"limit": PAGE_SIZE}
        if cursor:
            params["starting_after"] = cursor
        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            resp = await request(client, "GET", f"{API}/customers", provider=self.id,
                                 params=params, headers=self._headers(creds))
        body = resp.json()
        records = [RawRecord(external_id=c["id"], payload=c) for c in body["data"]]
        next_cursor = records[-1].external_id if body.get("has_more") and records else None
        return Page(records=records, next_cursor=next_cursor)

    async def fetch_one(self, creds: Credentials, external_id: str) -> RawRecord | None:
        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            try:
                resp = await request(client, "GET", f"{API}/customers/{external_id}",
                                     provider=self.id, headers=self._headers(creds))
            except NotFoundError:
                return None
        body = resp.json()
        # Stripe answers a deleted customer with a stub, not a 404.
        if body.get("deleted"):
            return None
        return RawRecord(external_id=body["id"], payload=body)

    # ---- webhooks ----------------------------------------------------

    def verify_webhook(self, raw_body: bytes, headers: dict[str, str]) -> bool:
        """Stripe-Signature: t=1728000000,v1=<hex>[,v1=<hex>...]

        More than one v1 appears while a webhook secret is being rolled, so
        any match is accepted. compare_digest rather than == because == stops
        at the first differing byte, and the time that takes leaks how much of
        a forged signature was right.
        """
        if not self._webhook_secret:
            return False
        pairs = [part.split("=", 1) for part in headers.get("stripe-signature", "").split(",") if "=" in part]
        timestamp = next((v for k, v in pairs if k == "t"), None)
        signatures = [v for k, v in pairs if k == "v1"]
        if not timestamp or not signatures:
            return False
        try:
            if abs(self._clock() - int(timestamp)) > SIGNATURE_TOLERANCE_SECONDS:
                return False
        except ValueError:
            return False
        signed = timestamp.encode() + b"." + raw_body
        expected = hmac.new(self._webhook_secret.encode(), signed, hashlib.sha256).hexdigest()
        return any(hmac.compare_digest(expected, sig) for sig in signatures)

    def parse_webhook(self, raw_body: bytes, headers: dict[str, str]) -> list[WebhookEvent]:
        event = json.loads(raw_body)
        # Exact match, not startswith("customer."): customer.subscription.*
        # and customer.source.* carry a subscription or a card, not a customer.
        if event.get("type") not in CUSTOMER_EVENTS:
            return []
        customer = event["data"]["object"]
        deleted = event["type"] == "customer.deleted"
        return [
            WebhookEvent(
                event_id=event["id"],
                topic=event["type"],
                external_id=customer["id"],
                occurred_at=datetime.fromtimestamp(event["created"], tz=timezone.utc),
                deleted=deleted,
                record=None if deleted else customer,
            )
        ]

    # ---- writing -----------------------------------------------------

    async def upsert(self, creds: Credentials, person: Person, external_id: str | None) -> WriteResult:
        # The write path is HubSpot only (see the design notes on D2).
        raise PermanentError("stripe connector is read-only")
