"""Mask provider payloads before any of them leave the system.

The mapping proposer sends Claude examples of a provider's records. It sends
their SHAPE, never their contents: every value is replaced by a placeholder of
the same type and format, so `"Maria@acme.com"` becomes `"user@example.com"`
and an epoch-seconds timestamp becomes a different epoch-seconds timestamp.

Why shape survives and content does not: a mapping is decided almost entirely
by field names and value formats (is this ISO 8601 or epoch milliseconds? is
this one name string or two?). The actual names and emails add nothing to that
decision, and sending them would make a third-party API a processor of
customers' personal data, which then has to be covered in a DPA.

This is a best-effort masker for structured payloads, not a general PII
scrubber: free text in an unexpected field is replaced wholesale with "text"
rather than inspected, which is the safe direction to be wrong in.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

EMAIL = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
PREFIXED_ID = re.compile(r"^([a-z]{2,8})_[A-Za-z0-9]+$")  # cus_QxYz123, evt_1Abc
PHONE_LIKE = re.compile(r"^[\d\s+\-().]{7,}$")
DIGITS = re.compile(r"^\d+$")

# Lists are cut down: three elements show the shape, a hundred show nothing more.
MAX_LIST_ITEMS = 3


def mask(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: mask(v) for key, v in value.items()}  # keys are schema, kept as-is
    if isinstance(value, list):
        return [mask(v) for v in value[:MAX_LIST_ITEMS]]
    if value is None or isinstance(value, bool):
        return value  # checked before int: bool is a subclass of int in Python
    if isinstance(value, int):
        return _mask_int(value)
    if isinstance(value, float):
        return 0.0
    if isinstance(value, str):
        return _mask_str(value)
    return None


def _mask_int(value: int) -> int:
    # Keep the magnitude that tells a timestamp's unit apart. 1728000000 is
    # epoch seconds and 1728000000000 epoch milliseconds; the mapping has to
    # say which, so the placeholder must stay in the same range.
    if 10**9 <= value < 10**10:
        return 1_700_000_000
    if 10**12 <= value < 10**13:
        return 1_700_000_000_000
    return 0


def _mask_str(value: str) -> str:
    if value == "":
        return ""
    if EMAIL.match(value):
        return "user@example.com"
    timestamp = _mask_iso_timestamp(value)
    if timestamp:
        return timestamp
    prefixed = PREFIXED_ID.match(value)
    if prefixed:
        return f"{prefixed.group(1)}_xxxxxxxx"  # the prefix says what kind of object it is
    if DIGITS.match(value):
        return "123"
    if PHONE_LIKE.match(value):
        return re.sub(r"\d", "5", value)  # "+1 (857) 351-9582" -> "+5 (555) 555-5555"
    return "text"


def _mask_iso_timestamp(value: str) -> str | None:
    """A fixed date in the same format: same separators, same precision, same
    timezone style, so Claude (and the dry run) can still tell what it is."""
    if len(value) < 10 or not value[:4].isdigit() or value[4] != "-":
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if "T" not in value and " " not in value:
        return "2026-01-01"
    placeholder = datetime(2026, 1, 1, tzinfo=parsed.tzinfo or None)
    timespec = "milliseconds" if "." in value else "seconds"
    text = placeholder.isoformat(timespec=timespec)
    if value.endswith("Z"):
        text = placeholder.astimezone(timezone.utc).isoformat(timespec=timespec).replace("+00:00", "Z")
    return text
