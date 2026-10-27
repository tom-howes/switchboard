"""HubSpot: contacts, read (and later the narrow write path).

Where HubSpot disagrees with Stripe, which is the point of having both:

- Auth is OAuth 2.0. Access tokens expire after 30 minutes and are renewed
  with a refresh token, so `refresh` is real work here, not a no-op.
- Pagination is an opaque `after` token from `paging.next.after`, not the id
  of the last record.
- Records nest everything under `properties`, names arrive split into
  firstname/lastname, and timestamps are ISO 8601 with milliseconds.
- You get only the properties you ask for. Omit `properties=` and a contact
  comes back without an email, which looks exactly like a contact with none.
- Webhooks arrive in batches of up to 100 events, carry only the object id
  (never the record), and are signed over method + URL + body + timestamp.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from datetime import datetime, timezone
from typing import Callable

import httpx

from ..core.models import Capability, Credentials, Page, Person, RawRecord, WebhookEvent, WriteResult
from ..engine.errors import AuthError, NotFoundError, PermanentError
from ..engine.http import TIMEOUT, request

API = "https://api.hubapi.com"
TOKEN_URL = f"{API}/oauth/v1/token"

PAGE_SIZE = 100  # HubSpot's maximum for the CRM list endpoints

# Must cover every property any approved mapping reads. If a mapping starts
# reading a new property, it has to be added here too: a known coupling, and
# the generic REST connector will derive this list from the mapping instead.
CONTACT_PROPERTIES = ["email", "firstname", "lastname", "phone"]

# HubSpot rejects v3 signatures older than five minutes, and so do we.
SIGNATURE_TOLERANCE_MS = 5 * 60 * 1000

# Creation and restore are treated like a property change: something about
# this contact is different, go and read it. Merges are deliberately ignored
# for now; a merge is an identity event (two HubSpot ids become one) and
# belongs with identity resolution, not with record sync.
CHANGE_EVENTS = {"contact.creation", "contact.propertyChange", "contact.restore"}
DELETE_EVENTS = {"contact.deletion", "contact.privacyDeletion"}


class HubSpotConnector:
    id = "hubspot"
    capabilities = frozenset({Capability.READ, Capability.WEBHOOKS, Capability.WRITE})

    def __init__(self, *, client_id: str, client_secret: str, clock: Callable[[], float] = time.time):
        self._client_id = client_id
        self._client_secret = client_secret
        self._clock = clock

    # ---- credentials -------------------------------------------------

    async def authorize(self, code: str, state: str) -> Credentials:
        # Exchanging the authorization code is the same token request as a
        # refresh with a different grant_type. Comes after refresh().
        raise NotImplementedError("authorization code exchange not written yet")

    async def refresh(self, creds: Credentials) -> Credentials:
        """Trade the refresh token for a new access token.

        Only the HOW lives here. WHEN to refresh, and making sure two workers
        do not both do it, is connections.fresh_credentials, because that
        problem is the same for every OAuth provider.
        """
        if not creds.refresh_token:
            # Nothing to trade in. Only a human reconnecting can fix this, so
            # fail as an auth problem without bothering HubSpot.
            raise AuthError("hubspot: no refresh token stored; reconnect the account")

        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            try:
                # `data=` sends a form body. OAuth token endpoints expect
                # application/x-www-form-urlencoded, not JSON.
                resp = await request(client, "POST", TOKEN_URL, provider=self.id, data={
                    "grant_type": "refresh_token",
                    "client_id": self._client_id,
                    "client_secret": self._client_secret,
                    "refresh_token": creds.refresh_token,
                })
            except PermanentError as exc:
                # On most endpoints a 400 means we sent a bad request. On the
                # token endpoint it means the refresh token was revoked or is
                # unknown, which is a reconnect, not a bug. RetryableError
                # (5xx, timeouts) is not a PermanentError and passes through.
                raise AuthError(f"hubspot refresh rejected: {exc}") from exc

        body = resp.json()
        # model_copy keeps any provider extras (hub_id) that a fresh
        # Credentials(...) would drop.
        return creds.model_copy(update={
            "access_token": body["access_token"],
            # Keep a rotated refresh token if one comes back. Holding on to the
            # old one would make the NEXT refresh fail.
            "refresh_token": body.get("refresh_token") or creds.refresh_token,
            # expires_in is relative ("1800 seconds from now"); store an
            # absolute time so is_expired() does not need to know when we asked.
            "expires_at": datetime.fromtimestamp(self._clock() + body["expires_in"], tz=timezone.utc),
        })

    def _headers(self, creds: Credentials) -> dict[str, str]:
        return {"Authorization": f"Bearer {creds.access_token}"}

    # ---- reading -----------------------------------------------------

    async def list_changes(self, creds: Credentials, cursor: str | None) -> Page:
        params: dict[str, str | int] = {"limit": PAGE_SIZE, "properties": ",".join(CONTACT_PROPERTIES)}
        if cursor:
            params["after"] = cursor
        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            resp = await request(client, "GET", f"{API}/crm/v3/objects/contacts", provider=self.id,
                                 params=params, headers=self._headers(creds))
        body = resp.json()
        records = [RawRecord(external_id=c["id"], payload=c) for c in body.get("results", [])]
        next_cursor = body.get("paging", {}).get("next", {}).get("after")
        return Page(records=records, next_cursor=next_cursor)

    async def fetch_one(self, creds: Credentials, external_id: str) -> RawRecord | None:
        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            try:
                resp = await request(client, "GET", f"{API}/crm/v3/objects/contacts/{external_id}",
                                     provider=self.id, headers=self._headers(creds),
                                     params={"properties": ",".join(CONTACT_PROPERTIES)})
            except NotFoundError:
                return None  # deleted (archived) contacts 404 by default
        body = resp.json()
        return RawRecord(external_id=body["id"], payload=body)

    # ---- webhooks ----------------------------------------------------

    def verify_webhook(self, raw_body: bytes, headers: dict[str, str], url: str) -> bool:
        """X-HubSpot-Signature-v3: base64(HMAC-SHA256(client_secret,
        method + url + body + timestamp)).

        The URL is part of what is signed, which is why the Protocol had to
        grow a `url` argument when this connector arrived. HubSpot's docs also
        describe URL-decoding a short list of characters in the URI before
        signing; our webhook URLs contain none of them, so this compares the
        URL as-is. A known simplification, written down rather than hidden.
        """
        if not self._client_secret:
            return False
        signature = headers.get("x-hubspot-signature-v3", "")
        timestamp = headers.get("x-hubspot-request-timestamp", "")
        if not signature or not timestamp:
            return False
        try:
            if abs(self._clock() * 1000 - int(timestamp)) > SIGNATURE_TOLERANCE_MS:
                return False
        except ValueError:
            return False
        message = b"POST" + url.encode() + raw_body + timestamp.encode()
        expected = base64.b64encode(hmac.new(self._client_secret.encode(), message, hashlib.sha256).digest()).decode()
        return hmac.compare_digest(expected, signature)

    def parse_webhook(self, raw_body: bytes, headers: dict[str, str]) -> list[WebhookEvent]:
        events = []
        for event in json.loads(raw_body):
            topic = event.get("subscriptionType", "")
            if topic not in CHANGE_EVENTS | DELETE_EVENTS:
                continue
            events.append(
                WebhookEvent(
                    event_id=str(event["eventId"]),
                    topic=topic,
                    external_id=str(event["objectId"]),
                    occurred_at=datetime.fromtimestamp(event["occurredAt"] / 1000, tz=timezone.utc),
                    deleted=topic in DELETE_EVENTS,
                    record=None,  # HubSpot never sends the record; the engine fetches it
                )
            )
        return events

    # ---- writing -----------------------------------------------------

    async def upsert(self, creds: Credentials, person: Person, external_id: str | None) -> WriteResult:
        raise PermanentError("write path not built yet (step 11)")
