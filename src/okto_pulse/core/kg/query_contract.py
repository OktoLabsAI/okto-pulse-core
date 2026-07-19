"""Canonical, runtime-neutral contract for public KG query surfaces.

The contract intentionally contains no graph backend or Community imports.  It
is consumed by Pydantic wire models, MCP validation, schema introspection and
resource documentation tests so those surfaces cannot silently invent their
own vocabulary.
"""

from __future__ import annotations

import re
import uuid
from enum import Enum
from typing import Literal, TypeAlias

from okto_pulse.core.kg.schema_contract import (
    NODE_TYPES,
    relationship_endpoint_pairs,
    stable_rel_type_entries,
)


KG_QUERY_CONTRACT_VERSION = "1.0"

GRAPH_LAYER_CANONICAL = "canonical"
GRAPH_LAYER_WORKING = "working"
GRAPH_LAYER_ALL = "all"
GRAPH_LAYER_VALUES: tuple[str, ...] = (
    GRAPH_LAYER_CANONICAL,
    GRAPH_LAYER_WORKING,
    GRAPH_LAYER_ALL,
)
GraphLayer: TypeAlias = Literal["canonical", "working", "all"]

RELATED_CONTEXT_DIRECTIONS: tuple[str, ...] = (
    "both",
    "incoming",
    "outgoing",
)
RELATED_CONTEXT_DEPTHS: tuple[int, ...] = (1, 2)
RelatedContextDirection: TypeAlias = Literal["both", "incoming", "outgoing"]

TYPED_ARTIFACT_KINDS: tuple[str, ...] = ("spec", "card")
TypedArtifactKind: TypeAlias = Literal["spec", "card"]

SIMILARITY_MIN = 0.0
SIMILARITY_MAX = 1.0

# Raw Cypher is deliberately a small read-only subset.  Tokens that mutate the
# graph remain a separate unsafe category; an otherwise safe but unsupported
# root operation receives ``unsupported_operation``.
CYPHER_SUPPORTED_ROOT_OPERATIONS: tuple[str, ...] = (
    "MATCH",
    "OPTIONAL",
    "UNWIND",
    "WITH",
    "RETURN",
)
CYPHER_SUPPORTED_CLAUSES: tuple[str, ...] = (
    "MATCH",
    "WHERE",
    "RETURN",
    "WITH",
    "ORDER",
    "BY",
    "LIMIT",
    "UNWIND",
    "OPTIONAL",
    "UNION",
    "AS",
    "AND",
    "OR",
    "NOT",
    "IN",
    "IS",
    "NULL",
    "TRUE",
    "FALSE",
    "CONTAINS",
    "STARTS",
    "ENDS",
    "DISTINCT",
    "COUNT",
    "COLLECT",
    "SUM",
    "AVG",
    "MIN",
    "MAX",
    "CASE",
    "WHEN",
    "THEN",
    "ELSE",
    "END",
    "DESC",
    "ASC",
)


class CognitiveOutcomeType(str, Enum):
    """Closed terminal outcome vocabulary shared by ledger and MCP."""

    RELATION_CREATED = "relation_created"
    CANDIDATE_CREATED = "candidate_created"
    FORMAL_DECISION_PROMOTED = "formal_decision_promoted"
    EXISTING_DECISION_LINKED = "existing_decision_linked"
    CONTRADICTION_DISMISSED = "contradiction_dismissed"
    NO_ACTION_REQUIRED = "no_action_required"


COGNITIVE_OUTCOME_TYPES: tuple[str, ...] = tuple(
    outcome.value for outcome in CognitiveOutcomeType
)


def _enum_name(value: str) -> str:
    snake = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", value)
    snake = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", snake).upper()
    return snake.replace("-", "_")


KGNodeType = Enum(
    "KGNodeType",
    {_enum_name(value): value for value in NODE_TYPES},
    type=str,
)


def edge_type_values() -> tuple[str, ...]:
    """Return relationship names in canonical schema declaration order."""

    return tuple(dict.fromkeys(entry["name"] for entry in stable_rel_type_entries()))


KGEdgeType = Enum(
    "KGEdgeType",
    {_enum_name(value): value for value in edge_type_values()},
    type=str,
)


def validate_related_context_artifact_ref(value: str) -> str | None:
    """Validate a public related-context anchor without guessing its type.

    Historical non-UUID refs remain readable.  New UUID references must carry
    the canonical ``spec:`` or ``card:`` discriminator.
    """

    artifact_ref = (value or "").strip()
    if not artifact_ref:
        return "artifact_id is required. Use spec:<uuid> or card:<uuid>."
    if ":" in artifact_ref:
        prefix, raw_id = artifact_ref.split(":", 1)
        if prefix not in TYPED_ARTIFACT_KINDS or not raw_id:
            return (
                "artifact_id must use a typed reference: spec:<uuid> or "
                "card:<uuid>. Do not pass raw UUIDs."
            )
        return None
    try:
        uuid.UUID(artifact_ref)
    except (TypeError, ValueError):
        return None
    return (
        "artifact_id is ambiguous as a raw UUID. Use spec:<uuid> or "
        "card:<uuid> so KG queries do not infer relational type."
    )


def query_contract_document() -> dict:
    """Serializable corpus exposed by ``kg_schema_info`` and resources."""

    edge_types = edge_type_values()
    return {
        "version": KG_QUERY_CONTRACT_VERSION,
        "typed_artifact_kinds": list(TYPED_ARTIFACT_KINDS),
        "node_types": list(NODE_TYPES),
        "edge_types": list(edge_types),
        "edge_endpoints": {
            edge_type: [
                {"from": source, "to": target}
                for source, target in relationship_endpoint_pairs(edge_type)
            ]
            for edge_type in edge_types
        },
        "graph_layers": list(GRAPH_LAYER_VALUES),
        "related_context": {
            "directions": list(RELATED_CONTEXT_DIRECTIONS),
            "max_depths": list(RELATED_CONTEXT_DEPTHS),
        },
        "similarity": {"minimum": SIMILARITY_MIN, "maximum": SIMILARITY_MAX},
        "cognitive_outcome_types": list(COGNITIVE_OUTCOME_TYPES),
        "cypher_read_subset": {
            "root_operations": list(CYPHER_SUPPORTED_ROOT_OPERATIONS),
            "clauses": list(CYPHER_SUPPORTED_CLAUSES),
            "unsupported_error": "unsupported_operation",
            "write_error": "unsafe_cypher",
        },
    }


__all__ = [
    "COGNITIVE_OUTCOME_TYPES",
    "CYPHER_SUPPORTED_CLAUSES",
    "CYPHER_SUPPORTED_ROOT_OPERATIONS",
    "CognitiveOutcomeType",
    "GRAPH_LAYER_ALL",
    "GRAPH_LAYER_CANONICAL",
    "GRAPH_LAYER_VALUES",
    "GRAPH_LAYER_WORKING",
    "GraphLayer",
    "KGEdgeType",
    "KGNodeType",
    "KG_QUERY_CONTRACT_VERSION",
    "RELATED_CONTEXT_DEPTHS",
    "RELATED_CONTEXT_DIRECTIONS",
    "RelatedContextDirection",
    "SIMILARITY_MAX",
    "SIMILARITY_MIN",
    "TYPED_ARTIFACT_KINDS",
    "TypedArtifactKind",
    "edge_type_values",
    "query_contract_document",
    "validate_related_context_artifact_ref",
]
