"""The canonical shapes every connector resolves to."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field


class Capability(StrEnum):
    READ = "read"
    WEBHOOKS = "webhooks"
    WRITE = "write"


class Person(BaseModel):
    """The canonical person.

    full_name is carried alongside given_name/family_name rather than derived
    from them, because the two providers disagree and neither direction is safe:
    Stripe gives one `name` string, HubSpot gives firstname and lastname
    separately. "Maria del Carmen Garcia Lopez" does not split on whitespace and
    "Dr. James Wilson Jr." does not either. We populate what we are given and
    never infer the other.
    """

    model_config = ConfigDict(str_strip_whitespace=True)

    primary_email: str
    full_name: str | None = None
    given_name: str | None = None
    family_name: str | None = None
    phone: str | None = None


class RawRecord(BaseModel):
    """One record as the provider returned it, before any mapping is applied."""

    external_id: str
    payload: dict
    source_created_at: datetime | None = None
    source_updated_at: datetime | None = None


class Page(BaseModel):
    """One page of changes.

    next_cursor is opaque: each provider paginates differently and the engine
    never inspects it, it only persists it so a crashed backfill resumes.
    """

    records: list[RawRecord] = Field(default_factory=list)
    next_cursor: str | None = None


class Credentials(BaseModel):
    """Whatever a provider needs to make a call.

    Deliberately loose. Stripe needs an API key and never expires; HubSpot needs
    an access token, a refresh token and an expiry. The engine only cares
    whether `expires_at` has passed.
    """

    model_config = ConfigDict(extra="allow")

    access_token: str | None = None
    refresh_token: str | None = None
    api_key: str | None = None
    expires_at: datetime | None = None

    def is_expired(self, *, skew_seconds: int = 60) -> bool:
        if self.expires_at is None:
            return False
        from datetime import timedelta, timezone

        return datetime.now(timezone.utc) >= self.expires_at - timedelta(seconds=skew_seconds)


class WriteResult(BaseModel):
    """Outcome of pushing a canonical record back to a provider."""

    external_id: str
    created: bool
    # Hash of what we sent. The webhook that our own write provokes carries the
    # same values; matching it here is how the inbound event is recognised as an
    # echo instead of being applied as new information.
    echo_fingerprint: str
