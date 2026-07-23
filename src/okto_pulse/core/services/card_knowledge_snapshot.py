"""Canonical card knowledge-base snapshot construction and comparison.

Both explicit MCP copy and board-driven spec auto-propagation converge here so
refresh, identity and lineage semantics cannot drift between the two paths.
"""

from __future__ import annotations

import copy
from collections.abc import Mapping
from typing import Any

from okto_pulse.core.domain.knowledge_governance import (
    KnowledgeGovernanceInvalidMetadata,
    knowledge_governance_semantically_equal,
)


CARD_KNOWLEDGE_LINEAGE_FIELDS: tuple[str, ...] = (
    "source_kb_id",
    "root_source_kb_id",
    "immediate_parent_kb_id",
)

_CARD_KNOWLEDGE_COMPARISON_FIELDS: tuple[str, ...] = (
    "id",
    "title",
    "description",
    "content",
    "mime_type",
    "source",
    "author_id",
    *CARD_KNOWLEDGE_LINEAGE_FIELDS,
)


def _value(item: Any, key: str) -> Any:
    if isinstance(item, Mapping):
        return item.get(key)
    return getattr(item, key, None)


def build_card_knowledge_snapshot(
    source_item: Any,
    *,
    source_entity_type: str,
    source_entity_id: str,
    actor_id: str,
    existing: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the canonical card snapshot, preserving durable target identity.

    A refresh owns the mutable content and governance projection, while the
    target keeps the identity, provenance string, original author and all
    lineage fields established by the first copy. Missing lineage on a legacy
    snapshot is backfilled from the current source rather than left ambiguous.
    """

    source_kb_id = str(_value(source_item, "id"))
    source = (
        f"copied_from_{source_entity_type}:{source_entity_id}:{source_kb_id}"
    )
    root_source_kb_id = (
        _value(source_item, "root_source_kb_id")
        or _value(source_item, "source_kb_id")
        or source_kb_id
    )
    payload: dict[str, Any] = {
        "id": f"cardkb_{source_kb_id}",
        "title": _value(source_item, "title"),
        "description": _value(source_item, "description"),
        "content": _value(source_item, "content"),
        "mime_type": _value(source_item, "mime_type") or "text/markdown",
        "source": source,
        "source_kb_id": source_kb_id,
        "root_source_kb_id": str(root_source_kb_id),
        "immediate_parent_kb_id": source_kb_id,
        "author_id": actor_id,
    }
    governance_metadata = _value(source_item, "governance_metadata")
    if governance_metadata is not None:
        payload["governance_metadata"] = copy.deepcopy(governance_metadata)

    if existing is not None:
        for key in ("id", "source", "author_id", *CARD_KNOWLEDGE_LINEAGE_FIELDS):
            current = existing.get(key)
            if current not in (None, ""):
                payload[key] = copy.deepcopy(current)

    return payload


def card_knowledge_snapshots_equivalent(
    current: Any,
    candidate: Mapping[str, Any],
) -> bool:
    """Return semantic equality for a persisted and freshly built snapshot."""

    if not isinstance(current, Mapping):
        return False
    if any(current.get(key) != candidate.get(key) for key in _CARD_KNOWLEDGE_COMPARISON_FIELDS):
        return False

    current_metadata = current.get("governance_metadata")
    candidate_metadata = candidate.get("governance_metadata")
    try:
        return knowledge_governance_semantically_equal(
            current_metadata,
            candidate_metadata,
        )
    except KnowledgeGovernanceInvalidMetadata:
        # Historical partial/unknown JSON stays readable. New writes are
        # validated upstream, but comparisons must tolerate old snapshots.
        return current_metadata == candidate_metadata


__all__ = [
    "CARD_KNOWLEDGE_LINEAGE_FIELDS",
    "build_card_knowledge_snapshot",
    "card_knowledge_snapshots_equivalent",
]
