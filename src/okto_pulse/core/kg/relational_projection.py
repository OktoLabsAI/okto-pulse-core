"""Exact identity helpers for relationally-owned SK-A graph projections.

The relational database remains the source of truth.  These references mark
derived, rebuildable nodes and deliberately use a closed grammar so active-set
cleanup never relies on a broad ``STARTS WITH`` ownership guess.
"""

from __future__ import annotations

from dataclasses import dataclass
import re


RELATIONAL_PROJECTION_NAMESPACE_RDL = "rdl"
RELATIONAL_PROJECTION_OWNER_TYPE = "refinement"
RELATIONAL_PROJECTION_SYSTEM_ACTOR_PREFIX = "system:"

_RDL_REF_PATTERN = re.compile(
    r"^refinement:(?P<owner_id>[^:]+):rdl:(?P<ledger_id>[^:]+):"
    r"(?:(?P<decision>decision)|alternative:(?P<alternative_hash>[0-9a-f]{64}))$"
)
_RDL_BELONGS_TO_RULE_PATTERN = re.compile(
    r"^belongs_to/relational_rdl_(?P<kind>decision|alternative)"
    r"@v?(?P<version>[0-9]+\.[0-9]+(?:\.[0-9]+)?)$"
)


@dataclass(frozen=True)
class RelationalProjectionIdentity:
    """Parsed identity of one relationally-owned graph node."""

    owner_type: str
    owner_id: str
    namespace: str
    ledger_id: str
    node_type: str
    alternative_hash: str | None = None


def parse_relational_projection_ref(
    source_artifact_ref: str,
) -> RelationalProjectionIdentity | None:
    """Parse the closed SK-A RDL reference grammar, returning ``None`` otherwise."""

    match = _RDL_REF_PATTERN.fullmatch(str(source_artifact_ref or "").strip())
    if match is None:
        return None
    alternative_hash = match.group("alternative_hash")
    return RelationalProjectionIdentity(
        owner_type=RELATIONAL_PROJECTION_OWNER_TYPE,
        owner_id=match.group("owner_id"),
        namespace=RELATIONAL_PROJECTION_NAMESPACE_RDL,
        ledger_id=match.group("ledger_id"),
        node_type="Alternative" if alternative_hash else "Decision",
        alternative_hash=alternative_hash,
    )


def is_relational_projection_node(
    *,
    node_type: str,
    source_artifact_ref: str,
    created_by_agent: str,
    owner_type: str | None = None,
    owner_id: str | None = None,
    namespace: str | None = None,
) -> bool:
    """Return whether exact identity and system provenance establish ownership."""

    if not str(created_by_agent or "").startswith(
        RELATIONAL_PROJECTION_SYSTEM_ACTOR_PREFIX
    ):
        return False
    identity = parse_relational_projection_ref(source_artifact_ref)
    if identity is None or identity.node_type != str(node_type or ""):
        return False
    return (
        (owner_type is None or identity.owner_type == owner_type)
        and (owner_id is None or identity.owner_id == owner_id)
        and (namespace is None or identity.namespace == namespace)
    )


def relational_projection_rule_node_type(rule_id: str) -> str | None:
    """Return the exact node type owned by a versioned SK-A RDL edge rule."""

    match = _RDL_BELONGS_TO_RULE_PATTERN.fullmatch(str(rule_id or "").strip())
    if match is None:
        return None
    return "Decision" if match.group("kind") == "decision" else "Alternative"


__all__ = [
    "RELATIONAL_PROJECTION_NAMESPACE_RDL",
    "RELATIONAL_PROJECTION_OWNER_TYPE",
    "RELATIONAL_PROJECTION_SYSTEM_ACTOR_PREFIX",
    "RelationalProjectionIdentity",
    "is_relational_projection_node",
    "parse_relational_projection_ref",
    "relational_projection_rule_node_type",
]
