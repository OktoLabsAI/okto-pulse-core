"""Exact identity helpers for relationally-owned SK-A graph projections.

The relational database remains the source of truth.  These references mark
derived, rebuildable nodes and deliberately use a closed grammar so active-set
cleanup never relies on a broad ``STARTS WITH`` ownership guess.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import re


RELATIONAL_PROJECTION_NAMESPACE_RDL = "rdl"
RELATIONAL_PROJECTION_OWNER_TYPE = "refinement"
RELATIONAL_PROJECTION_SYSTEM_ACTOR_PREFIX = "system:"
RELATIONAL_PROJECTION_RULE_VERSION = "v2.0"

_RDL_REF_PATTERN = re.compile(
    r"^refinement:(?P<owner_id>[^:]+):rdl:(?P<ledger_id>[^:]+):"
    r"(?:(?P<decision>decision)|alternative:(?P<alternative_hash>[0-9a-f]{64}))$"
)
_RDL_BELONGS_TO_RULE_PATTERN = re.compile(
    r"^belongs_to/relational_rdl_(?P<kind>decision|alternative)"
    r"@v?(?P<version>[0-9]+\.[0-9]+(?:\.[0-9]+)?)$"
)
_RDL_RELATES_TO_RULE_PATTERN = re.compile(
    r"^relates_to/relational_rdl_alternative"
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


def relational_projection_candidate_id(source_artifact_ref: str) -> str:
    """Return the closed deterministic candidate id for one projection ref."""

    value = str(source_artifact_ref or "")
    return f"relproj_{hashlib.sha256(value.encode('utf-8')).hexdigest()[:32]}"


def relational_projection_edge_id(
    edge_type: str,
    from_candidate_id: str,
    to_candidate_id: str,
) -> str:
    """Return the closed deterministic edge id used by the RDL projector."""

    identity = f"{edge_type}:{from_candidate_id}:{to_candidate_id}"
    return f"relproj_edge_{hashlib.sha256(identity.encode('utf-8')).hexdigest()[:32]}"


def relational_projection_belongs_to_rule(node_type: str) -> str:
    """Return the sole current ownership rule for an RDL node type."""

    normalized = str(node_type or "")
    if normalized not in {"Decision", "Alternative"}:
        raise ValueError("relational_projection_rule_node_type_invalid")
    return (
        f"belongs_to/relational_rdl_{normalized.casefold()}"
        f"@{RELATIONAL_PROJECTION_RULE_VERSION}"
    )


def relational_projection_alternative_relation_rule() -> str:
    """Return the sole current Decision-to-Alternative relation rule."""

    return f"relates_to/relational_rdl_alternative@{RELATIONAL_PROJECTION_RULE_VERSION}"


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


def is_relational_projection_alternative_relation_rule(rule_id: str) -> bool:
    """Return whether ``rule_id`` is the exact versioned RDL relation rule."""

    return (
        _RDL_RELATES_TO_RULE_PATTERN.fullmatch(str(rule_id or "").strip()) is not None
    )


__all__ = [
    "RELATIONAL_PROJECTION_NAMESPACE_RDL",
    "RELATIONAL_PROJECTION_OWNER_TYPE",
    "RELATIONAL_PROJECTION_RULE_VERSION",
    "RELATIONAL_PROJECTION_SYSTEM_ACTOR_PREFIX",
    "RelationalProjectionIdentity",
    "is_relational_projection_alternative_relation_rule",
    "is_relational_projection_node",
    "parse_relational_projection_ref",
    "relational_projection_candidate_id",
    "relational_projection_belongs_to_rule",
    "relational_projection_alternative_relation_rule",
    "relational_projection_edge_id",
    "relational_projection_rule_node_type",
]
