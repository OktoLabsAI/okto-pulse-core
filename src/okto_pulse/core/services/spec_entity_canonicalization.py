"""Leaf module for deterministic Spec requirement canonicalization.

Depends only on domain contracts so it can be imported by BOTH ``services.main``
(``SpecService.create_spec``/``update_spec``) AND
``services.spec_structured_entities`` without creating an import cycle — the
latter already imports ``SpecService`` from ``main``, so the canonicalization
helpers must live in a module that depends on neither.

Allocates identities only for new authored objects, preserving current stored
identity and rejecting incompatible persisted collections.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from typing import Any

from okto_pulse.core.domain.criterion_verification import criterion_verification_fields
from okto_pulse.core.domain.requirement_verification import requirement_verification_fields

# Prefixes mirror the structured ids already used elsewhere.
_ID_PREFIX_BY_ENTITY = {
    "functional_requirement": "fr_",
    "technical_requirement": "tr_",
    "acceptance_criterion": "ac_",
}

SPEC_REQUIREMENT_FIELDS: tuple[tuple[str, str], ...] = (
    ("functional_requirements", "functional_requirement"),
    ("technical_requirements", "technical_requirement"),
    ("acceptance_criteria", "acceptance_criterion"),
)


class DuplicateSpecChildIdError(ValueError):
    """Raised when canonicalization input already contains two children sharing an id.

    Fail-closed: the caller's explicit ids are never renamed. Error code:
    ``duplicate_spec_child_id``.
    """


class UnsupportedSpecChildTypeError(ValueError):
    """Raised when a caller asks the closed canonicalizer for another type."""


def spec_child_text(item: Any) -> str:
    """Normalized text of a spec child (dict or legacy string)."""
    if isinstance(item, dict):
        return str(item.get("text") or item.get("title") or item.get("description") or "")
    return str(item)


def spec_child_id(item: Any) -> str | None:
    """Structured id of a spec child, or ``None`` for legacy strings / id-less dicts."""
    if isinstance(item, dict):
        raw = item.get("id")
        return str(raw) if raw not in (None, "") else None
    return None


def _stable_child_id(entity_type: str, text: str, used_ids: set[str]) -> str:
    """Deterministic, UNIQUE id for a child that has no id yet.

    Base is ``<prefix><md5(entity_type:text)[:8]>``. If the base is already taken — a hash collision with a different
    text, OR a duplicate text without an id — a deterministic integer suffix
    ``_N`` (N = 1, 2, ...) is appended until a free id is found. NEVER returns an
    id already present in ``used_ids``.
    """
    try:
        prefix = _ID_PREFIX_BY_ENTITY[entity_type]
    except KeyError as exc:
        raise UnsupportedSpecChildTypeError(
            f"unsupported_spec_child_type: {entity_type!r}"
        ) from exc
    digest = hashlib.md5(f"{entity_type}:{text}".encode("utf-8")).hexdigest()[:8]
    base = f"{prefix}{digest}"
    if base not in used_ids:
        return base
    n = 1
    while f"{base}_{n}" in used_ids:
        n += 1
    return f"{base}_{n}"


def validate_stored_spec_children(items: list | None) -> None:
    """Refuse incompatible persisted requirements without assigning identity."""
    if items is None:
        return
    if not isinstance(items, list):
        raise ValueError("incompatible_spec_requirement: expected a collection")
    for item in items:
        if (
            not isinstance(item, dict)
            or not isinstance(item.get("id"), str)
            or not item["id"].strip()
            or not isinstance(item.get("text"), str)
            or not item["text"].strip()
        ):
            raise ValueError(
                "incompatible_spec_requirement: stored children require id and text"
            )


def canonicalize_spec_children(
    entity_type: str,
    items: list | None,
    existing_items: list | None = None,
) -> list[dict] | None:
    """Allocate IDs for newly authored objects and preserve explicit identities.

    Stored collections must already be current. An omitted ID creates a new
    identity; matching text never imports an existing identity. Explicit ID
    collisions fail closed, and metadata is preserved and domain-validated.
    """
    if entity_type not in _ID_PREFIX_BY_ENTITY:
        raise UnsupportedSpecChildTypeError(
            f"unsupported_spec_child_type: {entity_type!r}"
        )
    if items is None:
        return None

    validate_stored_spec_children(existing_items)

    reserved_ids = {spec_child_id(item) for item in existing_items or []}

    used_ids: set[str] = set()
    out: list[dict] = []
    for item in items:
        if not isinstance(item, dict) or not isinstance(item.get("text"), str) or not item["text"].strip():
            raise ValueError("spec_requirement_object_required: expected an object with text")
        if "id" in item and (not isinstance(item["id"], str) or not item["id"].strip()):
            raise ValueError("spec_requirement_id_invalid")
        text = item["text"]
        explicit_id = spec_child_id(item)

        if explicit_id is not None:
            if explicit_id in used_ids:
                raise DuplicateSpecChildIdError(
                    f"duplicate_spec_child_id: {explicit_id!r} appears more than "
                    f"once in {entity_type} input"
                )
            child_id = explicit_id
        else:
            # Missing ID means new authorship, never a lookup by matching text.
            child_id = _stable_child_id(entity_type, text, used_ids | reserved_ids)

        used_ids.add(child_id)
        child = dict(item)
        child["id"] = child_id
        child["text"] = text
        child.setdefault("status", "active")
        if entity_type == "acceptance_criterion":
            child.update(criterion_verification_fields(child))
        else:
            child.update(requirement_verification_fields(child))
        out.append(child)
    return out


def canonicalize_spec_requirement_fields(
    fields: Mapping[str, list | None],
    *,
    existing_fields: Mapping[str, list | None] | None = None,
) -> dict[str, list[dict] | None]:
    """Canonicalize the present FR/TR/AC fields as one closed collection.

    Only keys present in ``fields`` are returned. Callers that need to validate
    the final state across all three collections pass all three. Explicit IDs
    must be unique across the complete Spec requirement namespace; otherwise
    anchors would be ambiguous even when the duplicate occurred in a different
    field.
    """

    allowed = dict(SPEC_REQUIREMENT_FIELDS)
    unknown = set(fields) - set(allowed)
    if unknown:
        raise UnsupportedSpecChildTypeError(
            "unsupported_spec_requirement_field: "
            + ", ".join(sorted(str(item) for item in unknown))
        )
    existing = existing_fields or {}
    canonical: dict[str, list[dict] | None] = {}
    owner_by_id: dict[str, str] = {}
    for field_name, entity_type in SPEC_REQUIREMENT_FIELDS:
        if field_name not in fields:
            continue
        value = canonicalize_spec_children(
            entity_type,
            fields[field_name],
            existing_items=existing.get(field_name),
        )
        canonical[field_name] = value
        for child in value or []:
            child_id = str(child["id"])
            previous = owner_by_id.get(child_id)
            if previous is not None:
                raise DuplicateSpecChildIdError(
                    "duplicate_spec_child_id: "
                    f"{child_id!r} appears in both {previous} and {field_name}"
                )
            owner_by_id[child_id] = field_name
    return canonical


__all__ = [
    "DuplicateSpecChildIdError",
    "SPEC_REQUIREMENT_FIELDS",
    "UnsupportedSpecChildTypeError",
    "canonicalize_spec_children",
    "canonicalize_spec_requirement_fields",
    "spec_child_id",
    "spec_child_text",
    "validate_stored_spec_children",
]
