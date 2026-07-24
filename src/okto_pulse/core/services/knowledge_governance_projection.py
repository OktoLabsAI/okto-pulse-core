"""Canonical read projection for Knowledge Base governance metadata.

The storage field remains ``governance_metadata``.  Public read surfaces expose
the same tolerant, additive ``governance`` envelope regardless of whether the
source value is an ORM row or a copied dictionary snapshot.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from okto_pulse.core.domain.knowledge_governance import (
    project_knowledge_governance,
)
from okto_pulse.core.domain.knowledge_fingerprint import (
    resolve_knowledge_content_sha256,
)


_KNOWLEDGE_BASE_METADATA_FIELDS = (
    "ideation_id",
    "refinement_id",
    "spec_id",
    "card_id",
    "story_id",
    "source",
    "source_type",
    "source_id",
    "source_title",
    "source_version",
    "source_kb_id",
    "root_source_kb_id",
    "immediate_parent_kb_id",
    "knowledge_assignment",
)


def _resource_value(resource: Any, field: str) -> Any:
    if isinstance(resource, Mapping):
        return resource.get(field)
    return getattr(resource, field, None)


def _json_value(value: Any) -> Any:
    isoformat = getattr(value, "isoformat", None)
    return isoformat() if callable(isoformat) else value


def project_knowledge_governance_from_resource(resource: Any) -> dict[str, Any]:
    """Return the canonical tolerant projection for one KB-shaped value."""

    raw = (
        resource.get("governance_metadata")
        if isinstance(resource, Mapping)
        else getattr(resource, "governance_metadata", None)
    )
    return project_knowledge_governance(raw).as_dict()


def with_knowledge_governance(
    payload: Mapping[str, Any], resource: Any
) -> dict[str, Any]:
    """Add the canonical envelope without changing any baseline payload field."""

    return {
        **payload,
        "governance": project_knowledge_governance_from_resource(resource),
    }


def serialize_knowledge_base(
    resource: Any,
    *,
    include_content: bool = True,
) -> dict[str, Any]:
    """Return the canonical public projection for every KB-shaped resource."""

    title = _resource_value(resource, "title")
    if title is None:
        title = _resource_value(resource, "name")
    mime_type = _resource_value(resource, "mime_type")
    if mime_type is None:
        mime_type = _resource_value(resource, "content_type")

    data: dict[str, Any] = {
        "id": _resource_value(resource, "id"),
        "title": title,
        "description": _resource_value(resource, "description"),
        "mime_type": mime_type or "text/markdown",
    }
    for field in _KNOWLEDGE_BASE_METADATA_FIELDS:
        value = _resource_value(resource, field)
        if value is not None:
            data[field] = _json_value(value)
    if include_content:
        data["content"] = _resource_value(resource, "content")
    for field in ("created_by", "created_at", "updated_at"):
        value = _resource_value(resource, field)
        if value is not None:
            data[field] = _json_value(value)

    # Historical rows may not have persisted this field. Resolve it lazily so
    # every list/get/context projection has the same immutable fingerprint.
    data["content_hash"] = resolve_knowledge_content_sha256(resource)
    return with_knowledge_governance(data, resource)


__all__ = [
    "project_knowledge_governance_from_resource",
    "serialize_knowledge_base",
    "with_knowledge_governance",
]
