"""Credentials encryption tests. No database needed."""

from __future__ import annotations

import base64
import os
import uuid

import pytest

from switchboard.core.models import Credentials
from switchboard.engine import crypto

KEY = os.urandom(32)
WS_A, WS_B = uuid.uuid4(), uuid.uuid4()


def test_round_trip():
    creds = Credentials(api_key="sk_test_abc")
    blob = crypto.seal_credentials(creds, KEY, WS_A, "stripe")
    assert "sk_test_abc" not in blob
    assert crypto.open_credentials(blob, KEY, WS_A, "stripe").api_key == "sk_test_abc"


def test_same_plaintext_encrypts_differently():
    # fresh nonce every time; identical ciphertexts would reveal identical keys
    creds = Credentials(api_key="sk_test_abc")
    assert crypto.seal_credentials(creds, KEY, WS_A, "stripe") != crypto.seal_credentials(creds, KEY, WS_A, "stripe")


def test_blob_copied_to_another_row_fails():
    blob = crypto.seal_credentials(Credentials(api_key="k"), KEY, WS_A, "stripe")
    with pytest.raises(crypto.CredentialsError):
        crypto.open_credentials(blob, KEY, WS_B, "stripe")
    with pytest.raises(crypto.CredentialsError):
        crypto.open_credentials(blob, KEY, WS_A, "hubspot")


def test_tampering_is_detected():
    blob = crypto.seal_credentials(Credentials(api_key="k"), KEY, WS_A, "stripe")
    raw = bytearray(base64.b64decode(blob.split(":", 1)[1]))
    raw[-1] ^= 0x01  # flip one bit of the auth tag
    tampered = "v1:" + base64.b64encode(bytes(raw)).decode()
    with pytest.raises(crypto.CredentialsError):
        crypto.open_credentials(tampered, KEY, WS_A, "stripe")


def test_wrong_key_fails():
    blob = crypto.seal_credentials(Credentials(api_key="k"), KEY, WS_A, "stripe")
    with pytest.raises(crypto.CredentialsError):
        crypto.open_credentials(blob, os.urandom(32), WS_A, "stripe")


@pytest.mark.parametrize("bad", ["", "not base64!!", base64.b64encode(b"short").decode()])
def test_bad_keys_rejected_at_startup(bad):
    with pytest.raises(crypto.CredentialsError):
        crypto.load_key(bad)


def test_extra_provider_fields_survive():
    # Credentials allows extras: HubSpot's hub_id must round-trip untouched
    creds = Credentials(access_token="a", refresh_token="r", hub_id=12345)
    out = crypto.open_credentials(crypto.seal_credentials(creds, KEY, WS_A, "hubspot"), KEY, WS_A, "hubspot")
    assert out.model_extra == {"hub_id": 12345}
