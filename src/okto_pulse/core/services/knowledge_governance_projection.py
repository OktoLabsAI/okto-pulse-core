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
