"""Explicit authority policy for Resource Gate evaluation contexts.

Resource lineage records what is present.  This module decides which of those
records can block a lifecycle transition.  Keeping the two concerns separate
preserves historical/grandfathered lineage while preventing advisory Knowledge
Base material from becoming a mandatory completion or coverage obligation.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final, Literal, Mapping

from okto_pulse.core.services.resource_gate_contracts import (
    RESOURCE_TYPES,
    ResourceType,
)

ResourceGateAuthority = Literal["blocking", "advisory"]
ResourceGateAuthorityContext = Literal[
    "entity_completion",
    "spec_validation",
    "spec_done",
]

_AUTHORITY_MATRIX: Final[
    Mapping[
        ResourceGateAuthorityContext,
        Mapping[ResourceType, ResourceGateAuthority],
    ]
] = {
    context: {
        "architecture": "blocking",
        "mockup": "blocking",
        "knowledge_base": "advisory",
    }
    for context in ("entity_completion", "spec_validation", "spec_done")
}


@dataclass(frozen=True)
class ResourceGateAuthorityPolicy:
    """Immutable authority projection for one lifecycle context."""

    context: ResourceGateAuthorityContext
    authorities: Mapping[ResourceType, ResourceGateAuthority]

    def authority_for(self, resource_type: str) -> ResourceGateAuthority:
        try:
            return self.authorities[resource_type]  # type: ignore[index]
        except KeyError as exc:
            raise ValueError(
                f"Unknown Resource Gate resource type '{resource_type}'."
            ) from exc

    def is_blocking(self, resource_type: str) -> bool:
        return self.authority_for(resource_type) == "blocking"

    def to_dict(self) -> dict[str, object]:
        blocking = [
            resource_type
            for resource_type in RESOURCE_TYPES
            if self.is_blocking(resource_type)
        ]
        advisory = [
            resource_type
            for resource_type in RESOURCE_TYPES
            if not self.is_blocking(resource_type)
        ]
        return {
            "version": 1,
            "context": self.context,
            "blocking_resource_types": blocking,
            "advisory_resource_types": advisory,
        }


def resource_gate_authority_policy(
    context: ResourceGateAuthorityContext | str,
) -> ResourceGateAuthorityPolicy:
    """Return the fail-closed, versioned authority policy for ``context``."""

    try:
        authorities = _AUTHORITY_MATRIX[context]  # type: ignore[index]
    except KeyError as exc:
        supported = ", ".join(_AUTHORITY_MATRIX)
        raise ValueError(
            f"Unknown Resource Gate authority context '{context}'. "
            f"Expected one of: {supported}."
        ) from exc
    return ResourceGateAuthorityPolicy(
        context=context,  # type: ignore[arg-type]
        authorities=authorities,
    )


__all__ = [
    "ResourceGateAuthority",
    "ResourceGateAuthorityContext",
    "ResourceGateAuthorityPolicy",
    "resource_gate_authority_policy",
]
