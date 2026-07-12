"""NodeSubtypeRegistry port (spec MKG-E-S1, contract api_18a4858b).

Declarative subtyping over the CLOSED physical ontology: a semantic type
declares ``kind_of`` against exactly one of the 11 physical node types.
Own port by design (decision_5fa223267adf — never coupled to the physical
schema-migration port); the relational table lives in the Community
adapter. Declarations are DATA, not schema — they never bump
SCHEMA_VERSION (D6).

The validation rules are PURE and live here (core): normalized non-empty
name, uniqueness per (node_type, kind_of), node_type within NODE_TYPES,
and no case-insensitive collision with the physical type names (a subtype
named 'decision' would masquerade as the physical Decision).
"""

from __future__ import annotations

import unicodedata
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from okto_pulse.core.kg.schema_contract import NODE_TYPES

__all__ = [
    "NodeSubtypeRegistry",
    "SubtypeDeclaration",
    "SubtypeRegistryError",
    "normalize_kind_of",
    "register_node_subtype_registry",
    "require_node_subtype_registry",
    "reset_node_subtype_registry_for_tests",
    "resolve_node_subtype_registry",
    "validate_subtype_declaration",
]

_PHYSICAL_TYPE_NAMES_FOLDED = frozenset(t.casefold() for t in NODE_TYPES)


class SubtypeRegistryError(Exception):
    """Structured registry failure.

    ``failure_reason`` is a stable code: ``kg_subtype_invalid`` for rule
    violations, ``kg_subtype_registry_unavailable`` when the port is not
    composed (fail-closed — a kind_of-bearing commit must never proceed
    unvalidated).
    """

    def __init__(
        self,
        failure_reason: str,
        *,
        node_type: str | None = None,
        kind_of: str | None = None,
        remediation: str | None = None,
    ) -> None:
        self.failure_reason = failure_reason
        self.node_type = node_type
        self.kind_of = kind_of
        self.remediation = remediation
        detail = " ".join(
            part
            for part in (
                f"node_type={node_type}" if node_type else "",
                f"kind_of={kind_of}" if kind_of else "",
            )
            if part
        )
        super().__init__(f"{failure_reason}{(' [' + detail + ']') if detail else ''}")


@dataclass(frozen=True)
class SubtypeDeclaration:
    """One declared subtype: ``kind_of`` under a physical ``node_type``."""

    node_type: str
    kind_of: str
    description: str | None = None
    created_by: str | None = None
    created_at: str | None = None


def normalize_kind_of(value: str | None) -> str:
    """Canonical NFKC-casefold-strip normalization for comparisons
    (same recipe as node_identity.normalize_text — the two rules must
    never drift)."""

    return unicodedata.normalize("NFKC", value or "").casefold().strip()


def validate_subtype_declaration(
    declaration: SubtypeDeclaration,
    existing: tuple[SubtypeDeclaration, ...] = (),
) -> None:
    """Pure validation (TR3) — raises ``SubtypeRegistryError`` with the
    stable code ``kg_subtype_invalid`` on any rule violation.

    * kind_of must be non-empty after normalization;
    * node_type must be one of the 11 physical types;
    * kind_of must not collide (case-insensitive) with a physical type
      name;
    * (node_type, kind_of) must be unique among ``existing``.
    """

    normalized = normalize_kind_of(declaration.kind_of)
    if not normalized:
        raise SubtypeRegistryError(
            "kg_subtype_invalid",
            node_type=declaration.node_type,
            kind_of=declaration.kind_of,
            remediation="kind_of must be a non-empty name.",
        )
    if declaration.node_type not in NODE_TYPES:
        raise SubtypeRegistryError(
            "kg_subtype_invalid",
            node_type=declaration.node_type,
            kind_of=declaration.kind_of,
            remediation=(
                f"node_type must be one of the physical types: "
                f"{', '.join(NODE_TYPES)}."
            ),
        )
    if normalized in _PHYSICAL_TYPE_NAMES_FOLDED:
        raise SubtypeRegistryError(
            "kg_subtype_invalid",
            node_type=declaration.node_type,
            kind_of=declaration.kind_of,
            remediation=(
                "kind_of must not collide with a physical node type name."
            ),
        )
    for prior in existing:
        if (
            prior.node_type == declaration.node_type
            and normalize_kind_of(prior.kind_of) == normalized
        ):
            raise SubtypeRegistryError(
                "kg_subtype_invalid",
                node_type=declaration.node_type,
                kind_of=declaration.kind_of,
                remediation="Subtype already declared for this node_type.",
            )


@runtime_checkable
class NodeSubtypeRegistry(Protocol):
    """Declarative subtype registry (global vocabulary of the install)."""

    async def declare(self, declaration: SubtypeDeclaration) -> SubtypeDeclaration:
        """Validate (pure rules above) and persist the declaration.
        Raises :class:`SubtypeRegistryError` on violation or storage
        failure."""
        ...

    async def get(self, node_type: str, kind_of: str) -> SubtypeDeclaration | None:
        ...

    async def list_all(self) -> tuple[SubtypeDeclaration, ...]:
        """Deterministic order (node_type, kind_of)."""
        ...


_node_subtype_registry: NodeSubtypeRegistry | None = None


def register_node_subtype_registry(registry: NodeSubtypeRegistry) -> None:
    global _node_subtype_registry
    _node_subtype_registry = registry


def resolve_node_subtype_registry() -> NodeSubtypeRegistry | None:
    return _node_subtype_registry


def require_node_subtype_registry() -> NodeSubtypeRegistry:
    if _node_subtype_registry is None:
        raise SubtypeRegistryError(
            "kg_subtype_registry_unavailable",
            remediation=(
                "Register a NodeSubtypeRegistry adapter (community: "
                "sqlalchemy_kg_subtype_registry) before declaring or "
                "committing kind_of-bearing candidates."
            ),
        )
    return _node_subtype_registry


def reset_node_subtype_registry_for_tests() -> None:
    global _node_subtype_registry
    _node_subtype_registry = None
