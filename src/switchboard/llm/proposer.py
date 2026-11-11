"""Draft a mapping document for a new provider with Claude.

Claude proposes; a human approves; the engine applies. Claude is never in the
sync path: a proposal is stored with status 'proposed' and does nothing until
someone approves it. That keeps an LLM's mistake a review comment rather than
a corrupted customer record, and keeps syncing fast, cheap and deterministic.

How a proposal is checked before anyone sees it:

1. Structured outputs constrain the reply to a schema. Canonical field names,
   transforms and timestamp formats are enums, so an invented field or
   transform cannot even be generated.
2. The result is validated as a real MappingDocument (duplicate targets,
   unknown transforms) and dry-run against the masked samples, which catches
   paths that resolve to nothing and a primary_email that is never found.
3. If that fails, the errors go back to Claude once for a corrected version.
   If the retry fails too, nothing is saved and the errors are reported.

Only masked samples (see masking.py) are ever sent.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Literal

from pydantic import BaseModel, ValidationError

from ..config import get_settings
from ..core.mapping import MappingDocument, MappingError, apply_mapping
from .masking import mask

SAMPLE_SIZE = 3

CanonicalField = Literal["primary_email", "full_name", "given_name", "family_name", "phone"]
Transform = Literal["trim", "lower", "upper", "title", "digits", "nullify_empty"]


class ProposedTimestamp(BaseModel):
    source: str
    format: Literal["iso8601", "epoch_seconds", "epoch_millis"]


class ProposedField(BaseModel):
    canonical: CanonicalField
    source: list[str]
    transforms: list[Transform]
    required: bool
    reasoning: str
    confidence: Literal["high", "medium", "low"]


class ProposalOutput(BaseModel):
    """The schema Claude must answer in. Close to MappingDocument but stricter
    (enums instead of free strings) and with reasoning attached per field."""

    external_id: str
    source_created_at: ProposedTimestamp | None
    source_updated_at: ProposedTimestamp | None
    fields: list[ProposedField]
    unmapped_notes: str


@dataclass(frozen=True)
class Proposal:
    rules: dict
    notes: dict
    model: str
    masked_samples: list[dict]


class ProposalError(Exception):
    """Claude could not produce a mapping that passes validation."""


SYSTEM_PROMPT = """You write mapping documents for Switchboard, a sync engine that turns records from a \
provider's API into a canonical Person.

Canonical fields:
- primary_email: the person's email address. Always required; it is the identity key.
- full_name: a single name string, only if the provider supplies one.
- given_name / family_name: only if the provider supplies the name already split.
- phone: a phone number.

Rules:
- Map only what the provider actually has. Never build full_name from given and family names, and never \
split a full name into parts: names do not convert safely in either direction.
- primary_email must have required: true and the transforms ["trim", "lower"].
- phone should use ["digits", "nullify_empty"]. Text fields usually use ["trim", "nullify_empty"].
- Paths use $.a.b for nesting and $.a[0].b for array elements.
- source is a list of paths. Use one path normally; several paths are joined with a space, which is only \
appropriate when the provider splits one value across fields.
- external_id is the provider's stable unique id for the record.
- source_updated_at is the field that changes whenever the record is edited. Use null if there is no such \
field; never substitute a created date.
- Timestamps: say whether they are iso8601, epoch_seconds (10 digits) or epoch_millis (13 digits).

The sample values are masked placeholders. Reason from the field names, the nesting and the value formats, \
not from the values themselves. In reasoning, say briefly why each source was chosen; in unmapped_notes, \
mention anything ambiguous or any canonical field you could not find."""


def to_rules(out: ProposalOutput) -> dict:
    return {
        "external_id": out.external_id,
        "source_created_at": out.source_created_at.model_dump() if out.source_created_at else None,
        "source_updated_at": out.source_updated_at.model_dump() if out.source_updated_at else None,
        "fields": [
            {"canonical": f.canonical, "source": f.source[0] if len(f.source) == 1 else f.source,
             "transforms": list(f.transforms), "required": f.required}
            for f in out.fields
        ],
    }


def check(rules: dict, masked_samples: list[dict]) -> list[str]:
    """Everything wrong with a proposal, or an empty list if it is usable."""
    try:
        doc = MappingDocument.model_validate(rules)
    except ValidationError as exc:
        return [f"{'.'.join(map(str, e['loc']))}: {e['msg']}" for e in exc.errors()]
    errors = []
    if not any(f.canonical == "primary_email" for f in doc.fields):
        errors.append("no rule maps primary_email")
    for i, sample in enumerate(masked_samples):
        try:
            apply_mapping(sample, doc)
        except MappingError as exc:
            errors.append(f"sample {i}: {exc}")
    return errors


def _user_message(provider: str, masked: list[dict], previous: dict | None, errors: list[str]) -> str:
    text = f"Provider: {provider}\n\nMasked sample records:\n{json.dumps(masked, indent=2)}"
    if previous is not None:
        text += (
            f"\n\nYour previous mapping was:\n{json.dumps(previous, indent=2)}\n\n"
            "It failed validation:\n- " + "\n- ".join(errors) + "\n\nReturn a corrected mapping."
        )
    return text


async def _ask(client, provider: str, masked: list[dict], previous: dict | None, errors: list[str]) -> ProposalOutput:
    response = await client.beta.messages.parse(
        model=get_settings().mapping_model,
        max_tokens=16000,
        # If a safety classifier declines, the API retries on a fallback model
        # inside the same call instead of failing the proposal outright.
        betas=["server-side-fallback-2026-07-01"],
        fallbacks="default",
        # Opus 5.5 defaults to medium effort. A proposal is rare and reviewed
        # by a human, so the extra thoroughness is worth its few cents.
        output_config={"effort": "high"},
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": _user_message(provider, masked, previous, errors)}],
        output_format=ProposalOutput,
    )
    if response.stop_reason == "refusal":
        raise ProposalError("the model declined to produce a mapping")
    if response.stop_reason == "max_tokens" or response.parsed_output is None:
        raise ProposalError(f"no usable proposal (stop_reason={response.stop_reason})")
    return response.parsed_output


async def propose(provider: str, samples: list[dict], *, client=None) -> Proposal:
    """Draft, check, retry once, and return a proposal ready to store."""
    if not samples:
        raise ProposalError("no sample records to propose from")
    if client is None:
        from anthropic import AsyncAnthropic

        # With no key configured, the SDK falls back to its own credential
        # chain (e.g. an `ant auth login` profile).
        client = AsyncAnthropic(api_key=get_settings().anthropic_api_key or None)

    masked = [mask(s) for s in samples[:SAMPLE_SIZE]]
    previous, errors = None, []
    for _attempt in range(2):
        out = await _ask(client, provider, masked, previous, errors)
        rules = to_rules(out)
        errors = check(rules, masked)
        if not errors:
            notes = {
                "fields": {f.canonical: {"reasoning": f.reasoning, "confidence": f.confidence} for f in out.fields},
                "unmapped": out.unmapped_notes,
            }
            return Proposal(rules=rules, notes=notes, model=get_settings().mapping_model, masked_samples=masked)
        previous = rules
    raise ProposalError("proposal failed validation after one retry:\n- " + "\n- ".join(errors))
