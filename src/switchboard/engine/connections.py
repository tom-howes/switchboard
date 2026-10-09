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

from ..config import get_settings
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
