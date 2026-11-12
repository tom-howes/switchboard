"""The generic REST connector: a new provider as a JSON file, no new code.

A provider file (providers/<id>.json) says where the records are, how to
authenticate, how to page, and how to fetch one record. The field mapping is
separate, and usually drafted by the mapping proposer. Between them, a REST
API that returns JSON lists can be connected without writing a connector,
which is the answer to "could you connect this to our EHR?".

Deliberate limits, each written down rather than discovered:

- Static credentials only (bearer token, API key header, HTTP Basic). OAuth
  providers still need a coded connector, as HubSpot has.
- Cursor and page-number pagination. Link headers and offset/limit are not
  supported.
- No webhooks. Generic connectors rely on the reconciliation sweep, run as
  often as the provider file says (hourly by default here, not daily).
- fetch_one is required. The sweep deletes a record only after the provider
  confirms it is gone, and without a single-record lookup it could confirm
  nothing; a config that cannot confirm is rejected rather than trusted.

The config is validated when it is loaded, so a typo in a provider file is an
error at startup, not a confusing failure in the middle of a backfill.
"""

from __future__ import annotations

import json
import os
import pathlib
from datetime import timedelta
from typing import Literal

import httpx
from pydantic import BaseModel, Field, field_validator

from ..core.mapping import extract
from ..core.models import Capability, Credentials, Page, Person, RawRecord, WebhookEvent, WriteResult
from ..engine.errors import AuthError, NotFoundError, PermanentError
from ..engine.http import TIMEOUT, request

PROVIDERS_DIR = pathlib.Path(__file__).resolve().parents[3] / "providers"


class AuthConfig(BaseModel):
    style: Literal["bearer", "header", "basic"]
    header: str | None = None  # the header name, for style "header"
    #: which environment variables hold the secret(s), read once at connect
    #: time and then stored encrypted like any other credentials
    env: dict[str, str]

    @field_validator("env")
    @classmethod
    def _keys_match_style(cls, v: dict[str, str], info) -> dict[str, str]:
        style = info.data.get("style")
        needed = {"username", "password"} if style == "basic" else {"api_key"}
        if set(v) != needed:
            raise ValueError(f"auth style {style!r} needs env keys {sorted(needed)}, got {sorted(v)}")
        return v


class CursorPagination(BaseModel):
    style: Literal["cursor"]
    param: str          # query parameter the cursor is sent back in
    next: str           # path to the next cursor in the response body


class PagePagination(BaseModel):
    style: Literal["page"]
    param: str = "page"
    start: int = 1
    #: if set, a page shorter than this ends the walk without an extra request
    page_size: int | None = None
    size_param: str | None = None


class ListConfig(BaseModel):
    path: str
    records: str = "$"  # where the list of records sits in the response
    id: str = "$.id"    # where each record's id sits, relative to the record
    params: dict[str, str | int] = Field(default_factory=dict)


class FetchOneConfig(BaseModel):
    path: str           # must contain {id}
    record: str = "$"   # where the record sits in the response

    @field_validator("path")
    @classmethod
    def _has_id(cls, v: str) -> str:
        if "{id}" not in v:
            raise ValueError("fetch_one.path must contain {id}")
        return v


class GenericConfig(BaseModel):
    id: str
    base_url: str
    auth: AuthConfig
    list: ListConfig
    fetch_one: FetchOneConfig
    pagination: CursorPagination | PagePagination = Field(discriminator="style")
    reconcile_every_hours: float = 1.0

    @field_validator("base_url")
    @classmethod
    def _https(cls, v: str) -> str:
        if not v.startswith(("https://", "http://localhost", "http://127.0.0.1")):
            raise ValueError("base_url must be https (plain http is allowed only for localhost)")
        return v.rstrip("/")


def load_configs(directory: pathlib.Path | None = None) -> dict[str, GenericConfig]:
    directory = directory or PROVIDERS_DIR
    if not directory.exists():
        return {}
    configs = {}
    for path in sorted(directory.glob("*.json")):
        config = GenericConfig.model_validate(json.loads(path.read_text(encoding="utf-8")))
        if config.id != path.stem:
            raise ValueError(f"{path.name}: id {config.id!r} must match the file name")
        configs[config.id] = config
    return configs


class GenericRestConnector:
    capabilities = frozenset({Capability.READ})

    def __init__(self, config: GenericConfig):
        self.config = config
        self.id = config.id
        # The sweep reads this to decide when to run next (see reconcile.py).
        self.reconcile_every = timedelta(hours=config.reconcile_every_hours)

    # ---- credentials -------------------------------------------------

    async def authorize(self, code: str, state: str) -> Credentials:
        values = {}
        for key, env_name in self.config.auth.env.items():
            value = os.environ.get(env_name)
            if not value:
                raise PermanentError(f"{self.id}: environment variable {env_name} is not set")
            values[key] = value
        return Credentials(**values)

    async def refresh(self, creds: Credentials) -> Credentials:
        return creds  # static credentials do not expire

    def _auth(self, creds: Credentials) -> dict:
        style = self.config.auth.style
        if style == "bearer":
            return {"headers": {"Authorization": f"Bearer {creds.api_key}"}}
        if style == "header":
            return {"headers": {self.config.auth.header: creds.api_key}}
        extra = creds.model_extra or {}
        if not extra.get("username") or not extra.get("password"):
            raise AuthError(f"{self.id}: basic auth credentials are incomplete")
        return {"auth": (extra["username"], extra["password"])}

    # ---- reading -----------------------------------------------------

    async def list_changes(self, creds: Credentials, cursor: str | None) -> Page:
        """One page. The cursor is opaque to the engine either way: a real
        cursor token, or the page number as a string."""
        pagination = self.config.pagination
        params = dict(self.config.list.params)
        if isinstance(pagination, CursorPagination):
            if cursor:
                params[pagination.param] = cursor
        else:
            page_number = int(cursor) if cursor else pagination.start
            params[pagination.param] = page_number
            if pagination.page_size and pagination.size_param:
                params[pagination.size_param] = pagination.page_size

        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            resp = await request(client, "GET", self.config.base_url + self.config.list.path,
                                 provider=self.id, params=params, **self._auth(creds))
        body = resp.json()
        items = extract(body, self.config.list.records) or []
        if not isinstance(items, list):
            raise PermanentError(f"{self.id}: {self.config.list.records} is not a list in the response")

        records = []
        for item in items:
            external_id = extract(item, self.config.list.id)
            if external_id is None:
                raise PermanentError(f"{self.id}: a record has nothing at {self.config.list.id}")
            records.append(RawRecord(external_id=str(external_id), payload=item))

        if isinstance(pagination, CursorPagination):
            next_cursor = extract(body, pagination.next)
            return Page(records=records, next_cursor=str(next_cursor) if next_cursor else None)
        short_page = pagination.page_size is not None and len(records) < pagination.page_size
        next_cursor = None if not records or short_page else str(page_number + 1)
        return Page(records=records, next_cursor=next_cursor)

    async def fetch_one(self, creds: Credentials, external_id: str) -> RawRecord | None:
        url = self.config.base_url + self.config.fetch_one.path.format(id=external_id)
        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            try:
                resp = await request(client, "GET", url, provider=self.id, **self._auth(creds))
            except NotFoundError:
                return None
        record = extract(resp.json(), self.config.fetch_one.record)
        return RawRecord(external_id=external_id, payload=record) if record else None

    # ---- no webhooks, no writes --------------------------------------

    def verify_webhook(self, raw_body: bytes, headers: dict[str, str], url: str = "") -> bool:
        return False

    def parse_webhook(self, raw_body: bytes, headers: dict[str, str]) -> list[WebhookEvent]:
        return []

    async def upsert(self, creds: Credentials, person: Person, external_id: str | None) -> WriteResult:
        raise PermanentError(f"{self.id}: generic connectors are read-only")
