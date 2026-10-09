"""Encryption at rest for provider credentials.

The `connection.credentials` column holds API keys and OAuth refresh tokens:
a database dump without this layer is a set of working keys to every
customer's Stripe and HubSpot. With it, a dump is useless without the key,
which lives in the environment, not the database.

Choices, and what each buys:

- AES-256-GCM, via the `cryptography` package. GCM is AUTHENTICATED
  encryption: besides hiding the plaintext it detects any change to the
  ciphertext, so a tampered blob fails loudly instead of decrypting to
  garbage. The alternative, AES-CBC plus a separate HMAC, is the same
  guarantee with more ways to get the composition wrong. The standard library
  has no AES at all, and hand-rolling crypto is the one place "write it
  yourself so you can explain it" is the wrong rule.

- A fresh random 96-bit nonce per encryption, stored alongside the
  ciphertext. GCM's one catastrophic failure mode is reusing a nonce with the
  same key, which leaks the authentication key. Random 96-bit nonces are safe
  for around 2^32 encryptions per key, and this table will see thousands.

- Associated data (AAD) binding each blob to its row: workspace id plus
  provider. AAD is authenticated but not encrypted. Without it, someone with
  write access to the table could copy workspace A's Stripe blob into
  workspace B's row and it would decrypt fine. With it, decryption fails.

- A "v1:" prefix on every blob. There is no key rotation yet (a known gap),
  but the prefix is what makes adding it possible without a flag day: v2 blobs
  name a new key, v1 blobs keep decrypting with the old one until re-encrypted.
"""

from __future__ import annotations

import base64
import os
from uuid import UUID

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from ..core.models import Credentials

VERSION = "v1"
NONCE_BYTES = 12


class CredentialsError(Exception):
    """A blob could not be decrypted: wrong key, wrong row, or tampered."""


def load_key(encoded: str) -> bytes:
    """Decode and check the key once, at startup, rather than discovering a
    malformed key on the first sync job hours later."""
    if not encoded:
        raise CredentialsError("CREDENTIALS_KEY is not set; see .env.example")
    try:
        key = base64.b64decode(encoded, validate=True)
    except ValueError as exc:
        raise CredentialsError("CREDENTIALS_KEY is not valid base64") from exc
    if len(key) != 32:
        raise CredentialsError(f"CREDENTIALS_KEY must decode to 32 bytes, got {len(key)}")
    return key


def associated_data(workspace_id: UUID, provider: str) -> bytes:
    return f"connection:{workspace_id}:{provider}".encode()


def encrypt(plaintext: bytes, key: bytes, aad: bytes) -> str:
    nonce = os.urandom(NONCE_BYTES)
    sealed = AESGCM(key).encrypt(nonce, plaintext, aad)  # ciphertext with the 16-byte tag appended
    return f"{VERSION}:{base64.b64encode(nonce + sealed).decode()}"


def decrypt(blob: str, key: bytes, aad: bytes) -> bytes:
    version, _, body = blob.partition(":")
    if version != VERSION or not body:
        raise CredentialsError(f"unrecognised credentials format {version!r}")
    raw = base64.b64decode(body)
    nonce, sealed = raw[:NONCE_BYTES], raw[NONCE_BYTES:]
    try:
        return AESGCM(key).decrypt(nonce, sealed, aad)
    except InvalidTag as exc:
        # Deliberately vague: wrong key, wrong row and tampering look identical
        # from here, and the message should not help anyone tell them apart.
        raise CredentialsError("credentials failed authentication") from exc


def seal_credentials(creds: Credentials, key: bytes, workspace_id: UUID, provider: str) -> str:
    return encrypt(creds.model_dump_json().encode(), key, associated_data(workspace_id, provider))


def open_credentials(blob: str, key: bytes, workspace_id: UUID, provider: str) -> Credentials:
    return Credentials.model_validate_json(decrypt(blob, key, associated_data(workspace_id, provider)))
