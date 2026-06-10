"""Leaf module for spec FR/AC canonicalization (#4, Phase 1).

Self-contained (stdlib only) so it can be imported by BOTH ``services.main``
(``SpecService.create_spec``/``update_spec``) AND
``services.spec_structured_entities`` without creating an import cycle — the
latter already imports ``SpecService`` from ``main``, so the canonicalization
helpers must live in a module that depends on neither.

Spec ``9d66847f`` (writes structured-only FR/AC, Phase 1). The canonicalization
ONLY adds/normalizes id + shape and PRESERVES the text, so text-based
``linked_requirements``/``linked_criteria`` keep resolving (no breaking change).
"""

from __future__ import annotations

import hashlib
from collections import defaultdict, deque
from typing import Any

# Prefixes mirror the structured ids already used elsewhere (``fr_...``/``ac_...``).
_ID_PREFIX_BY_ENTITY = {
    "functional_requirement": "fr_",
    "acceptance_criterion": "ac_",
}


class DuplicateSpecChildIdError(ValueError):
    """Raised when canonicalization input already contains two children sharing an id.

    Fail-closed: the caller's explicit ids are never renamed. Error code:
    ``duplicate_spec_child_id``.
    """


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

    Base is ``<prefix><md5(entity_type:text)[:8]>`` (deterministic -> idempotent
    migrator). If the base is already taken — a hash collision with a different
    text, OR a duplicate text without an id — a deterministic integer suffix
    ``_N`` (N = 1, 2, ...) is appended until a free id is found. NEVER returns an
    id already present in ``used_ids``.
    """
    prefix = _ID_PREFIX_BY_ENTITY.get(entity_type, "se_")
    digest = hashlib.md5(f"{entity_type}:{text}".encode("utf-8")).hexdigest()[:8]
    base = f"{prefix}{digest}"
    if base not in used_ids:
        return base
    n = 1
    while f"{base}_{n}" in used_ids:
        n += 1
    return f"{base}_{n}"


def canonicalize_fr_ac(
    entity_type: str,
    items: list | None,
    existing_items: list | None = None,
) -> list[dict] | None:
    """Canonicalize a FR/AC list to structured dicts with unique, stable ids.

    String or dict inputs become ``{"id", "text", "status", **extra}`` dicts
    while PRESERVING the text. Id-preservation across a whole-list update: a dict
    carrying an id keeps it; an item without an id reuses the next existing id
    for the same text (ordered queue); otherwise a stable hash id is generated.
    ``None`` in -> ``None`` out (no change).

    Shape: every emitted item has ``id``, ``text`` and ``status`` (default
    ``"active"``, preserved when present), plus any extra fields from a dict
    input — compatible with ``StructuredSpecEntityService``.

    Raises :class:`DuplicateSpecChildIdError` if the input already contains two
    items with the same explicit id (fail-closed; ids never renamed).
    """
    if items is None:
        return None

    # text -> ordered queue of existing ids (only items that already carry one).
    existing_ids_by_text: dict[str, deque] = defaultdict(deque)
    for ex in existing_items or []:
        ex_id = spec_child_id(ex)
        if ex_id:
            existing_ids_by_text[spec_child_text(ex)].append(ex_id)

    used_ids: set[str] = set()
    out: list[dict] = []
    for item in items:
        text = spec_child_text(item)
        explicit_id = spec_child_id(item)

        if explicit_id is not None:
            if explicit_id in used_ids:
                raise DuplicateSpecChildIdError(
                    f"duplicate_spec_child_id: {explicit_id!r} appears more than "
                    f"once in {entity_type} input"
                )
            child_id = explicit_id
        else:
            child_id = None
            queue = existing_ids_by_text.get(text)
            if queue:
                # Skip existing ids already consumed earlier in this output.
                while queue and queue[0] in used_ids:
                    queue.popleft()
                if queue:
                    child_id = queue.popleft()
            if child_id is None:
                child_id = _stable_child_id(entity_type, text, used_ids)

        used_ids.add(child_id)
        if isinstance(item, dict):
            child = dict(item)
            child["id"] = child_id
            child["text"] = text
            child.setdefault("status", "active")
        else:
            child = {"id": child_id, "text": text, "status": "active"}
        out.append(child)
    return out
