"""The connector contract.

Everything hard lives behind this line, in the engine: retries, backoff, rate
limiting, backfill, cursors, reconciliation, dead-lettering, identity
resolution. A connector is only responsible for the things that are genuinely
provider-specific.

Note what is NOT here: `normalize`. Mapping a payload onto the canonical shape
is configuration, not code (see field_mapping), so the engine applies an
approved mapping document rather than calling into the connector. That is the
decision that lets a new provider be a day of work instead of a week, and it is
what the generic REST connector is built on.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from .models import Capability, Credentials, Page, Person, RawRecord, WebhookEvent, WriteResult


@runtime_checkable
class Connector(Protocol):
    #: stable identifier, also the key used in field_mapping.provider
    id: str

    #: what this connector can actually do. Stripe is read-only here;
    #: HubSpot additionally supports write. The engine checks before queueing.
    capabilities: frozenset[Capability]

    # ---- credentials -------------------------------------------------

    async def authorize(self, code: str, state: str) -> Credentials:
        """Exchange an authorization code for credentials.

        Providers using a static key return them directly and ignore the args.
        """
        ...

    async def refresh(self, creds: Credentials) -> Credentials:
        """Renew expiring credentials.

        No-op for providers whose credentials do not expire. The engine always
        calls it and does not care which kind it is dealing with.
        """
        ...

    # ---- reading -----------------------------------------------------

    async def list_changes(self, creds: Credentials, cursor: str | None) -> Page:
        """One page of records changed since `cursor`.

        The connector owns its pagination style. The engine only persists the
        returned cursor, so a crash resumes mid-backfill rather than restarting.
        """
        ...

    # ---- webhooks ----------------------------------------------------

    def verify_webhook(self, raw_body: bytes, headers: dict[str, str]) -> bool:
        """Verify a webhook signature.

        Takes the RAW body, not a parsed object. Every JSON parser normalises
        whitespace and key order somewhere, and a signature computed over
        re-serialised JSON will not match.
        """
        ...

    def parse_webhook(self, raw_body: bytes, headers: dict[str, str]) -> list[WebhookEvent]:
        """Turn one delivery into the record-level events it carries.

        A list, because HubSpot batches up to 100 events into one POST while
        Stripe sends one per request. Events the engine does not care about
        (invoices, subscriptions) come back as an empty list: acknowledged so
        the provider stops redelivering, but not queued.

        Only called after verify_webhook has passed. Parsing an unverified body
        means running a JSON parser over attacker-controlled input for nothing.
        """
        ...

    async def fetch_one(self, creds: Credentials, external_id: str) -> RawRecord | None:
        """The current state of one record, or None if it no longer exists.

        Needed because not every provider puts the record in its webhook:
        HubSpot sends only the object id and which property changed, so the
        engine has to come back and read it.
        """
        ...

    # ---- writing (only if Capability.WRITE) --------------------------

    async def upsert(self, creds: Credentials, person: Person, external_id: str | None) -> WriteResult:
        """Push a canonical person back to the provider.

        Implementations must be safe to call twice with the same input: either
        through a provider-supplied idempotency key, or by looking the record up
        before creating it.
        """
        ...
