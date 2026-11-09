"""Identity resolution: which provider records are the same human, what the
person record says, and where the providers disagree.

Rules, each chosen to be explainable after the fact:

- Matching is exact email only. No fuzzy matching: a false merge (two people
  combined) is far more damaging than a missed one, and much harder to undo.
- The person record is RECOMPUTED from all its live records every time, never
  patched incrementally. Recomputing is idempotent, so running it twice, or
  after a record is deleted, always lands in the same place.
- Which value wins is fixed per field (config/field_precedence.json), not
  "most recent". Stripe has no updated timestamp, so its "most recent" would
  really mean "most recently fetched" and every backfill would make Stripe win.
- Disagreements are written to field_conflict rather than silently dropped.

This module is split into pure functions (comparison, precedence, conflict
detection: no database, unit tested directly) and the SQL that applies them.
"""

from __future__ import annotations

import json
import pathlib
import re
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from typing import Callable

PRECEDENCE_FILE = pathlib.Path(__file__).resolve().parents[3] / "config" / "field_precedence.json"

PERSON_FIELDS = ("full_name", "given_name", "family_name", "phone")


@dataclass(frozen=True)
class LinkedRecord:
    """One live provider record attached to a person, as the resolver sees it."""

    provider: str
    observed_at: datetime
    full_name: str | None
    given_name: str | None
    family_name: str | None
    phone: str | None


@dataclass(frozen=True)
class Conflict:
    field: str
    winning_provider: str
    winning_value: str
    winning_observed_at: datetime
    losing_provider: str
    losing_value: str
    losing_observed_at: datetime


@lru_cache
def precedence() -> dict[str, list[str]]:
    rules = json.loads(PRECEDENCE_FILE.read_text(encoding="utf-8"))
    return {k: v for k, v in rules.items() if not k.startswith("_")}


def rank(field: str, provider: str) -> int:
    """Lower is better. Providers not named in the config rank after all named
    ones, so a new connector never silently outranks an established source."""
    order = precedence().get(field, [])
    return order.index(provider) if provider in order else len(order)


# --------------------------------------------------------------------------
# comparison: what counts as "the same value"
# --------------------------------------------------------------------------


def display_name(r: LinkedRecord) -> str | None:
    """The name as this provider would show it. Used for COMPARISON only.

    Joining HubSpot's first and last name here is not inference: the result
    is never stored, only compared. The person record keeps full_name and the
    split names separately, exactly as the providers supplied them.
    """
    if r.full_name:
        return r.full_name
    parts = [p for p in (r.given_name, r.family_name) if p]
    return " ".join(parts) or None


def name_key(value: str) -> str:
    # "tom  howes" and "Tom Howes" are the same name typed differently.
    return " ".join(value.lower().split())


def phone_key(value: str) -> str:
    """Last ten digits. Stripe keeps the country code (18573519582) and
    HubSpot often does not (8573519582); this treats those as equal.

    Deliberately US-centric, and a known gap: two international numbers that
    share their last ten digits would compare equal. The fix is the
    phonenumbers library with a default region, at the cost of a dependency.
    """
    digits = re.sub(r"\D", "", value)
    return digits[-10:]


# field -> (how to read it off a record, how to normalise it for comparison)
COMPARED: dict[str, tuple[Callable[[LinkedRecord], str | None], Callable[[str], str]]] = {
    "name": (display_name, name_key),
    "phone": (lambda r: r.phone, phone_key),
}


# --------------------------------------------------------------------------
# the pure core
# --------------------------------------------------------------------------


def _latest_per_provider(records: list[LinkedRecord]) -> dict[str, LinkedRecord]:
    """Two Stripe customers can share an email (Stripe allows it, and it
    happens constantly). Within one provider the most recent observation
    stands for that provider; cross-provider precedence then applies."""
    latest: dict[str, LinkedRecord] = {}
    for r in records:
        if r.provider not in latest or r.observed_at > latest[r.provider].observed_at:
            latest[r.provider] = r
    return latest


def merge(records: list[LinkedRecord]) -> dict[str, str | None]:
    """The person's field values: for each field, the highest-precedence
    provider that has a value. A provider with nothing for a field falls
    through to the next, so a blank never beats a real value."""
    by_provider = _latest_per_provider(records)
    result: dict[str, str | None] = {}
    for field in PERSON_FIELDS:
        candidates = [r for r in by_provider.values() if getattr(r, field)]
        candidates.sort(key=lambda r: rank(field, r.provider))
        result[field] = getattr(candidates[0], field) if candidates else None
    return result


def detect_conflicts(records: list[LinkedRecord]) -> list[Conflict]:
    """Every (field, losing provider) pair where a provider's value differs
    from the winning provider's. Providers with no value do not conflict:
    missing is not the same as different."""
    by_provider = _latest_per_provider(records)
    conflicts = []
    for field, (read, normalise) in COMPARED.items():
        present = [(r, read(r)) for r in by_provider.values()]
        present = [(r, v) for r, v in present if v]
        if len(present) < 2:
            continue
        present.sort(key=lambda pair: rank(field, pair[0].provider))
        winner, winning_value = present[0]
        for loser, losing_value in present[1:]:
            if normalise(losing_value) != normalise(winning_value):
                conflicts.append(Conflict(
                    field=field,
                    winning_provider=winner.provider, winning_value=winning_value,
                    winning_observed_at=winner.observed_at,
                    losing_provider=loser.provider, losing_value=losing_value,
                    losing_observed_at=loser.observed_at,
                ))
    return conflicts
