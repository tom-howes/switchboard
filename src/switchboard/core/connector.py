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

from .models import Capability, Credentials, Page, Person, WriteResult


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

    def webhook_event_id(self, raw_body: bytes, headers: dict[str, str]) -> str:
        """The provider's own id for this delivery, used for replay dedupe."""
        ...

    # ---- writing (only if Capability.WRITE) --------------------------

    async def upsert(self, creds: Credentials, person: Person, external_id: str | None) -> WriteResult:
        """Push a canonical person back to the provider.

        Implementations must be safe to call twice with the same input: either
        through a provider-supplied idempotency key, or by looking the record up
        before creating it.
        """
        ...
