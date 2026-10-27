"""The spec for OAuth refresh. Written before the implementation, on purpose.

Two pieces, and they are deliberately separate:

  HubSpotConnector.refresh(creds)        HOW to refresh: one HTTP call, HubSpot
                                         specifics. No database.
  connections.fresh_credentials(...)     WHEN to refresh, and making sure only
                                         one worker does it. No HubSpot.

Run just these with:  .venv/Scripts/python.exe -m pytest tests/test_oauth_refresh.py -v
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs

import httpx
import pytest
import respx

from switchboard.connectors.hubspot import TOKEN_URL, HubSpotConnector
from switchboard.core.models import Credentials
from switchboard.engine import connections
from switchboard.engine.errors import AuthError, RetryableError

NOW = 1_728_000_000
EXPIRED = datetime.fromtimestamp(NOW - 3600, tz=timezone.utc)


def connector() -> HubSpotConnector:
    # The clock is injected so expires_at is predictable. Use self._clock(),
    # not time.time(), when you compute it.
    return HubSpotConnector(client_id="cid", client_secret="csecret", clock=lambda: NOW)


def stale(**extra) -> Credentials:
    return Credentials(access_token="at_old", refresh_token="rt_1", expires_at=EXPIRED, **extra)


# --------------------------------------------------------------------------
# Part 1: the connector. One HTTP request, translated into Credentials.
# --------------------------------------------------------------------------


class TestConnectorRefresh:
    @respx.mock
    async def test_sends_the_refresh_grant(self):
        route = respx.post(TOKEN_URL).mock(return_value=httpx.Response(
            200, json={"access_token": "at_new", "refresh_token": "rt_1", "expires_in": 1800}))
        await connector().refresh(stale())

        sent = route.calls[0].request
        # OAuth token endpoints take a FORM body, not JSON.
        assert sent.headers["content-type"].startswith("application/x-www-form-urlencoded")
        form = {k: v[0] for k, v in parse_qs(sent.content.decode()).items()}
        assert form == {"grant_type": "refresh_token", "client_id": "cid",
                        "client_secret": "csecret", "refresh_token": "rt_1"}

    @respx.mock
    async def test_returns_new_token_and_expiry(self):
        respx.post(TOKEN_URL).mock(return_value=httpx.Response(
            200, json={"access_token": "at_new", "refresh_token": "rt_1", "expires_in": 1800}))
        fresh = await connector().refresh(stale())
        assert fresh.access_token == "at_new"
        # expires_in is RELATIVE (seconds from now); we store an absolute time
        assert fresh.expires_at == datetime.fromtimestamp(NOW + 1800, tz=timezone.utc)

    @respx.mock
    async def test_a_rotated_refresh_token_is_kept(self):
        # Never assume the refresh token stays the same. If the provider sends
        # a new one and we keep the old, the NEXT refresh fails.
        respx.post(TOKEN_URL).mock(return_value=httpx.Response(
            200, json={"access_token": "at_new", "refresh_token": "rt_2", "expires_in": 1800}))
        assert (await connector().refresh(stale())).refresh_token == "rt_2"

    @respx.mock
    async def test_missing_refresh_token_in_response_keeps_the_old_one(self):
        respx.post(TOKEN_URL).mock(return_value=httpx.Response(
            200, json={"access_token": "at_new", "expires_in": 1800}))
        assert (await connector().refresh(stale())).refresh_token == "rt_1"

    @respx.mock
    async def test_provider_specific_extras_survive(self):
        # Credentials allows extra fields (e.g. hub_id). Do not drop them.
        respx.post(TOKEN_URL).mock(return_value=httpx.Response(
            200, json={"access_token": "at_new", "refresh_token": "rt_1", "expires_in": 1800}))
        fresh = await connector().refresh(stale(hub_id=42))
        assert fresh.model_extra == {"hub_id": 42}

    @respx.mock
    async def test_rejected_refresh_token_is_an_auth_error(self):
        # A revoked or invalid refresh token comes back as a 400, which the
        # shared http module would call a plain PermanentError. For the token
        # endpoint specifically, it means "a human must reconnect".
        respx.post(TOKEN_URL).mock(return_value=httpx.Response(
            400, json={"status": "BAD_REFRESH_TOKEN", "message": "missing or unknown refresh token"}))
        with pytest.raises(AuthError):
            await connector().refresh(stale())

    @respx.mock
    async def test_server_error_is_still_retryable(self):
        respx.post(TOKEN_URL).mock(return_value=httpx.Response(503))
        with pytest.raises(RetryableError):
            await connector().refresh(stale())

    @respx.mock
    async def test_no_refresh_token_stored_fails_without_calling_hubspot(self):
        route = respx.post(TOKEN_URL)
        with pytest.raises(AuthError):
            await connector().refresh(Credentials(access_token="at_old", expires_at=EXPIRED))
        assert not route.called


# --------------------------------------------------------------------------
# Part 2: the engine. When to refresh, persisting the result, and the race.
# --------------------------------------------------------------------------


class FakeConnector:
    """Stands in for any OAuth provider. Counts refreshes; can be made slow or
    made to fail."""

    id = "hubspot"

    def __init__(self, *, delay: float = 0.0, error: Exception | None = None):
        self.calls = 0
        self.delay = delay
        self.error = error

    async def refresh(self, creds: Credentials) -> Credentials:
        self.calls += 1
        await asyncio.sleep(self.delay)  # widens the race window
        if self.error:
            raise self.error
        return creds.model_copy(update={
            "access_token": f"at_fresh_{self.calls}",
            "expires_at": datetime.now(timezone.utc) + timedelta(minutes=30),
        })


@pytest.fixture
async def oauth_connection(db, pool):
    """A HubSpot connection whose stored access token has already expired."""
    async with pool.connection() as conn:
        cur = await conn.execute("INSERT INTO workspace (name) VALUES ('test') RETURNING id")
        workspace_id = (await cur.fetchone())["id"]
        connection_id = await connections.save(conn, workspace_id, "hubspot", stale())
    async with pool.connection() as conn:
        return await connections.load(conn, connection_id)


async def reload(pool, connection):
    async with pool.connection() as conn:
        return (await connections.load(conn, connection.id)).credentials


class TestFreshCredentials:
    async def test_unexpired_credentials_never_call_the_provider(self, pool, oauth_connection):
        fake = FakeConnector()
        live = Credentials(access_token="at_ok", refresh_token="rt_1",
                           expires_at=datetime.now(timezone.utc) + timedelta(minutes=10))
        connection = connections.Connection(**{**oauth_connection.__dict__, "credentials": live})
        assert (await connections.fresh_credentials(connection, fake)).access_token == "at_ok"
        assert fake.calls == 0

    async def test_expired_credentials_are_refreshed_and_saved(self, pool, oauth_connection):
        fake = FakeConnector()
        fresh = await connections.fresh_credentials(oauth_connection, fake)
        assert fresh.access_token == "at_fresh_1"
        # Persisted, and still encrypted (load() decrypts, so this round-trips).
        assert (await reload(pool, oauth_connection)).access_token == "at_fresh_1"

    async def test_concurrent_refreshes_happen_exactly_once(self, pool, oauth_connection):
        """THE test. Two workers pick up jobs for the same connection at the
        same moment, and both see an expired token in memory. Only one may
        call the provider. The other must wait, then use the token the first
        one saved.

        Why it matters beyond wasted calls: if the provider rotates refresh
        tokens, the second refresh is sent with a token the first one just
        invalidated. It fails, the connection is flagged needs_reauth, and a
        perfectly healthy integration goes down for a race condition.
        """
        fake = FakeConnector(delay=0.3)
        a, b = await asyncio.gather(
            connections.fresh_credentials(oauth_connection, fake),
            connections.fresh_credentials(oauth_connection, fake),
        )
        assert fake.calls == 1
        assert a.access_token == b.access_token == "at_fresh_1"

    async def test_an_out_of_date_copy_uses_the_saved_token(self, pool, oauth_connection):
        """This worker loaded the connection a while ago. Since then another
        worker refreshed it. The in-memory copy says expired; the database
        says fresh. Believe the database."""
        await connections.fresh_credentials(oauth_connection, FakeConnector())  # someone else refreshes
        fake = FakeConnector()
        result = await connections.fresh_credentials(oauth_connection, fake)  # old copy, still "expired"
        assert fake.calls == 0
        assert result.access_token == "at_fresh_1"

    async def test_a_failed_refresh_changes_nothing(self, pool, oauth_connection):
        fake = FakeConnector(error=AuthError("refresh token revoked"))
        with pytest.raises(AuthError):
            await connections.fresh_credentials(oauth_connection, fake)
        assert (await reload(pool, oauth_connection)).access_token == "at_old"
