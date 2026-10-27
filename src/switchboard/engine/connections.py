"""Reading and writing `connection` rows, with credentials sealed at rest.

The only code that ever sees plaintext credentials is this module and the
connector it hands them to. Nothing else selects the `credentials` column.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from uuid import UUID

from psycopg import AsyncConnection
from psycopg.rows import dict_row

from .. import db
from ..config import get_settings
from ..core.connector import Connector
from ..core.models import Credentials
from . import crypto
from .errors import PermanentError


@lru_cache
def credentials_key() -> bytes:
    return crypto.load_key(get_settings().credentials_key)


@dataclass(frozen=True)
class Connection:
    id: UUID
    workspace_id: UUID
    provider: str
    status: str
    credentials: Credentials


async def save(conn: AsyncConnection, workspace_id: UUID, provider: str, creds: Credentials) -> UUID:
    """Create the connection, or replace its credentials if it exists.

    Reconnecting is the same operation as connecting, and it also clears
    needs_reauth, which is what releases any jobs parked behind it.
    """
    blob = crypto.seal_credentials(creds, credentials_key(), workspace_id, provider)
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            INSERT INTO connection (workspace_id, provider, credentials)
            VALUES (%s, %s, %s)
            ON CONFLICT (workspace_id, provider)
            DO UPDATE SET credentials = EXCLUDED.credentials, status = 'active', updated_at = now()
            RETURNING id
            """,
            (workspace_id, provider, blob),
        )
        return (await cur.fetchone())["id"]


async def load(conn: AsyncConnection, connection_id: UUID) -> Connection:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT id, workspace_id, provider, status, credentials FROM connection WHERE id = %s",
            (connection_id,),
        )
        row = await cur.fetchone()
    if row is None:
        raise PermanentError(f"connection {connection_id} does not exist")
    creds = crypto.open_credentials(row["credentials"], credentials_key(), row["workspace_id"], row["provider"])
    return Connection(row["id"], row["workspace_id"], row["provider"], row["status"], creds)


async def mark_needs_reauth(conn: AsyncConnection, connection_id: UUID) -> None:
    await conn.execute(
        "UPDATE connection SET status = 'needs_reauth', updated_at = now() WHERE id = %s AND status = 'active'",
        (connection_id,),
    )


async def fresh_credentials(connection: Connection, connector: Connector) -> Credentials:
    """Credentials that are safe to use right now, refreshing them if needed.

    Every handler calls this before talking to a provider. For Stripe it
    always takes the first branch (an API key has no expiry).
    """
    if not connection.credentials.is_expired():
        return connection.credentials

    # Two workers can reach this line at the same moment for the same
    # connection. FOR UPDATE makes the second one wait on the row lock until
    # the first commits, so exactly one of them calls the provider.
    #
    # This holds a transaction open across a network call, which queue.py
    # says never to do. The exception is deliberate: here the lock IS the
    # feature (it serialises refreshers), the call is one short request, and
    # it happens roughly once per 30 minutes per connection. At larger scale
    # a Postgres advisory lock keyed on the connection id would do the same
    # job without tying the lock to a row other queries also touch.
    async with db.transaction() as conn:
        cur = await conn.execute(
            "SELECT credentials FROM connection WHERE id = %s FOR UPDATE", (connection.id,)
        )
        row = await cur.fetchone()
        if row is None:
            raise PermanentError(f"connection {connection.id} does not exist")

        # Re-read and check again now that we hold the lock. The copy in
        # `connection` was loaded before we waited; if another worker
        # refreshed in the meantime, the database has the answer and we must
        # not refresh a second time (with a rotated refresh token, that second
        # call would fail and wrongly flag the connection needs_reauth).
        key = credentials_key()
        current = crypto.open_credentials(row["credentials"], key, connection.workspace_id, connection.provider)
        if not current.is_expired():
            return current

        # If refresh raises, the transaction rolls back and the stored
        # credentials are untouched.
        refreshed = await connector.refresh(current)
        await conn.execute(
            "UPDATE connection SET credentials = %s, updated_at = now() WHERE id = %s",
            (crypto.seal_credentials(refreshed, key, connection.workspace_id, connection.provider), connection.id),
        )
        return refreshed
