"""The mapping engine: turn a provider payload into a canonical record, using a
mapping document rather than provider-specific code.

This is the piece the extensibility story rests on. Because a mapping is data,
adding a provider means writing (or having an LLM draft) a document like:

    {
      "external_id": "$.id",
      "source_created_at": {"source": "$.created", "format": "epoch_seconds"},
      "fields": [
        {"canonical": "primary_email", "source": "$.email",
         "transforms": ["trim", "lower"], "required": true}
      ]
    }

Two deliberate limits:

1. The path syntax is a small documented subset, not JSONPath. Supporting the
   full spec would mean a dependency and a surface area nobody here needs.
2. A mapping document is validated when it is written, not when it runs. A bad
   mapping should be rejected at approval time by a human looking at it, rather
   than at 3am in the middle of a backfill.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator

from .models import Person

CANONICAL_FIELDS = {"primary_email", "full_name", "given_name", "family_name", "phone"}

TRANSFORMS: dict[str, Any] = {
    "trim": lambda v: v.strip(),
    "lower": lambda v: v.lower(),
    "upper": lambda v: v.upper(),
    "title": lambda v: v.title(),
    # phone numbers arrive as "+1 (857) 351-9582", "857-351-9582", "8573519582"
    "digits": lambda v: re.sub(r"\D", "", v),
    "nullify_empty": lambda v: v or None,
}

_SEGMENT = re.compile(r"([^.\[\]]+)|\[(\d+)\]")


class MappingError(Exception):
    """A mapping could not be applied to a payload."""


# --------------------------------------------------------------------------
# the document
# --------------------------------------------------------------------------


class FieldRule(BaseModel):
    canonical: str
    #: one path, or several to be joined (e.g. firstname + lastname)
    source: str | list[str]
    transforms: list[str] = Field(default_factory=list)
    join: str = " "
    required: bool = False

    @field_validator("canonical")
    @classmethod
    def _known_field(cls, v: str) -> str:
        if v not in CANONICAL_FIELDS:
            raise ValueError(f"unknown canonical field {v!r}; known: {sorted(CANONICAL_FIELDS)}")
        return v

    @field_validator("transforms")
    @classmethod
    def _known_transforms(cls, v: list[str]) -> list[str]:
        unknown = [t for t in v if t not in TRANSFORMS]
        if unknown:
            raise ValueError(f"unknown transforms {unknown}; known: {sorted(TRANSFORMS)}")
        return v


class TimestampRule(BaseModel):
    source: str
    #: Stripe sends epoch seconds, HubSpot sends epoch milliseconds or ISO 8601.
    #: Encoding this per provider is cheaper than guessing from the value.
    format: Literal["iso8601", "epoch_seconds", "epoch_millis"] = "iso8601"


class MappingDocument(BaseModel):
    external_id: str
    source_created_at: TimestampRule | None = None
    source_updated_at: TimestampRule | None = None
    fields: list[FieldRule] = Field(default_factory=list)

    @field_validator("fields")
    @classmethod
    def _no_duplicate_targets(cls, v: list[FieldRule]) -> list[FieldRule]:
        seen: set[str] = set()
        for rule in v:
            if rule.canonical in seen:
                raise ValueError(f"two rules both write {rule.canonical!r}")
            seen.add(rule.canonical)
        return v


class MappedRecord(BaseModel):
    external_id: str
    person: Person
    source_created_at: datetime | None = None
    source_updated_at: datetime | None = None


# --------------------------------------------------------------------------
# extraction
# --------------------------------------------------------------------------


def extract(payload: dict, path: str) -> Any:
    """Read a value out of a payload.

    Supports `$.a.b`, `a.b`, and `$.a[0].b`. Returns None for a path that is not
    present, rather than raising: a provider legitimately omitting an optional
    field is normal, and `required` is where absence becomes an error.
    """
    if path.startswith("$."):
        path = path[2:]
    elif path.startswith("$"):
        path = path[1:]

    current: Any = payload
    for name, index in _SEGMENT.findall(path):
        if current is None:
            return None
        if index:
            if not isinstance(current, list):
                return None
            i = int(index)
            current = current[i] if i < len(current) else None
        else:
            if not isinstance(current, dict):
                return None
            current = current.get(name)
    return current


def _stringify(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        # a bool landing in a text field is a mapping bug, not a value to coerce
        raise MappingError(f"expected a scalar, got boolean {value!r}")
    if isinstance(value, (dict, list)):
        raise MappingError(f"expected a scalar, got {type(value).__name__}")
    return str(value)


def apply_transforms(value: str | None, transforms: list[str]) -> str | None:
    for name in transforms:
        if value is None:
            return None
        value = TRANSFORMS[name](value)
    return value


def coerce_datetime(value: Any, fmt: str) -> datetime | None:
    if value is None or value == "":
        return None
    try:
        if fmt == "epoch_seconds":
            return datetime.fromtimestamp(int(value), tz=timezone.utc)
        if fmt == "epoch_millis":
            return datetime.fromtimestamp(int(value) / 1000, tz=timezone.utc)
        text = str(value).replace("Z", "+00:00")
        parsed = datetime.fromisoformat(text)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError, OSError) as exc:
        raise MappingError(f"could not read {value!r} as {fmt}: {exc}") from exc


# --------------------------------------------------------------------------
# application
# --------------------------------------------------------------------------


def apply_mapping(payload: dict, doc: MappingDocument) -> MappedRecord:
    """Apply a mapping document to one provider payload."""
    external_id = _stringify(extract(payload, doc.external_id))
    if not external_id:
        raise MappingError(f"external_id path {doc.external_id!r} resolved to nothing")

    values: dict[str, str | None] = {}
    for rule in doc.fields:
        paths = [rule.source] if isinstance(rule.source, str) else rule.source
        parts = [apply_transforms(_stringify(extract(payload, p)), rule.transforms) for p in paths]
        present = [p for p in parts if p]
        value = rule.join.join(present) if present else None

        if rule.required and not value:
            raise MappingError(
                f"required field {rule.canonical!r} resolved to nothing from {rule.source!r}"
            )
        values[rule.canonical] = value

    if not values.get("primary_email"):
        # The canonical key. Without it there is nothing to resolve identity on,
        # so this is a hard failure rather than a partial write.
        raise MappingError("mapping produced no primary_email")

    return MappedRecord(
        external_id=external_id,
        person=Person(**values),
        source_created_at=(
            coerce_datetime(extract(payload, doc.source_created_at.source), doc.source_created_at.format)
            if doc.source_created_at
            else None
        ),
        source_updated_at=(
            coerce_datetime(extract(payload, doc.source_updated_at.source), doc.source_updated_at.format)
            if doc.source_updated_at
            else None
        ),
    )
