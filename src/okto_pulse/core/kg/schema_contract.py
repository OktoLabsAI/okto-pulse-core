"""Runtime-agnostic Knowledge Graph schema contract.

This module intentionally contains only schema metadata and pure helpers. It
must not import the embedded graph runtime or any Community adapter.
"""

from __future__ import annotations

from typing import Any

from okto_pulse.core.kg.cognitive_policy import (
    COGNITIVE_PROVENANCE_NODE_TYPES,
    LEARNING_RELATES_TO_TARGETS,
)

SCHEMA_VERSION = "0.3.8"


# Provenance metadata required on every rel (KG Pipeline v2 - spec c48a5c33).
# `layer` is a closed enum validated by the worker/agent and the layer_isolation
# business rule. `legacy` is reserved for rels migrated from v0.1.0 where no
# layer attribution is available; agents must treat legacy edges as lower-trust
# than fresh `deterministic` emissions.
EDGE_LAYERS: tuple[str, ...] = ("deterministic", "cognitive", "fallback", "legacy")

# Added in v0.2.0. Names match the add_edge_candidate payload contract so the
# worker can pass attrs through without remapping.
EDGE_METADATA_COLUMNS: tuple[tuple[str, str], ...] = (
    ("layer", "STRING"),
    ("rule_id", "STRING"),
    ("created_by", "STRING"),
    ("fallback_reason", "STRING"),
)

# 11 node types listed in the MVP Fase 0 spec (FR-N nodes).
NODE_TYPES: tuple[str, ...] = (
    "Decision",
    "Criterion",
    "Constraint",
    "Assumption",
    "Requirement",
    "Entity",
    "APIContract",
    "TestScenario",
    "Bug",
    "Learning",
    "Alternative",
)

# Node types that get HNSW vector indexes for semantic search.
# Entity/Criterion/Constraint join Decision and Learning because they carry
# semantic content that the primary tier queries.
VECTOR_INDEX_TYPES: tuple[str, ...] = (
    "Decision",
    "Criterion",
    "Constraint",
    "Requirement",
    "Entity",
    "APIContract",
    "TestScenario",
    "Bug",
    "Learning",
)

# 10 rel types. supersedes and contradicts are the two core semantic relations
# the primary tier walks variable-length paths on; the rest encode provenance,
# context, co-reference, and quality feedback.
REL_TYPES: tuple[tuple[str, str, str], ...] = (
    ("supersedes", "Decision", "Decision"),
    ("contradicts", "Decision", "Decision"),
    ("derives_from", "Decision", "Requirement"),
    ("relates_to", "Decision", "Alternative"),
    ("mentions", "Decision", "Entity"),
    ("depends_on", "Decision", "Decision"),
    ("violates", "Bug", "Constraint"),
    ("implements", "APIContract", "Requirement"),
    ("tests", "TestScenario", "Criterion"),
    ("validates", "Learning", "Bug"),
)


def _cognitive_relates_to_endpoint_pairs() -> tuple[tuple[str, str], ...]:
    """Return cognitive-artifact -> taxonomy pairs for the reused relates_to rel."""
    return tuple(
        (source_type, target_type)
        for source_type in COGNITIVE_PROVENANCE_NODE_TYPES
        for target_type in LEARNING_RELATES_TO_TARGETS
    )


MULTI_REL_TYPES: tuple[tuple[str, tuple[tuple[str, str], ...]], ...] = (
    ("implements", (("APIContract", "Constraint"),)),
    ("relates_to", _cognitive_relates_to_endpoint_pairs()),
    (
        "belongs_to",
        (
            ("Entity", "Entity"),
            ("Entity", "Bug"),
            ("Requirement", "Entity"),
            ("Constraint", "Entity"),
            ("Criterion", "Entity"),
            ("TestScenario", "Entity"),
            ("APIContract", "Entity"),
            ("Decision", "Entity"),
            ("Bug", "Entity"),
            ("Alternative", "Entity"),
            ("Assumption", "Entity"),
            ("Learning", "Entity"),
        ),
    ),
    ("originates_from", (("Bug", "Entity"),)),
    ("covered_by", (("Bug", "Entity"), ("Bug", "TestScenario"))),
)


def stable_rel_type_entries() -> list[dict[str, Any]]:
    """Return one schema-catalog entry per stable relationship name."""

    entries: list[dict[str, Any]] = [
        {"name": rel_name, "from": from_type, "to": to_type}
        for rel_name, from_type, to_type in REL_TYPES
    ]
    entries.extend(
        {
            "name": rel_name,
            "from": "multiple",
            "to": "multiple",
            "multi": True,
            "pairs": [
                {"from": from_type, "to": to_type}
                for from_type, to_type in pairs
            ],
        }
        for rel_name, pairs in MULTI_REL_TYPES
    )
    return entries


def relationship_endpoint_pairs(edge_type: str) -> tuple[tuple[str, str], ...]:
    """Return every concrete endpoint pair accepted by a relationship name."""
    pairs: list[tuple[str, str]] = [
        (from_type, to_type)
        for rel_name, from_type, to_type in REL_TYPES
        if rel_name == edge_type
    ]
    for rel_name, endpoint_pairs in MULTI_REL_TYPES:
        if rel_name == edge_type:
            pairs.extend(endpoint_pairs)

    deduped: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for pair in pairs:
        if pair in seen:
            continue
        seen.add(pair)
        deduped.append(pair)
    return tuple(deduped)


def resolve_relationship_endpoint_pair(
    edge_type: str,
    *,
    from_type: str | None = None,
    to_type: str | None = None,
) -> tuple[str, str]:
    """Resolve concrete endpoint labels for a relationship insert."""
    pairs = relationship_endpoint_pairs(edge_type)
    if not pairs:
        raise ValueError(f"unknown edge_type: {edge_type}")

    if from_type or to_type:
        if not from_type or not to_type:
            raise ValueError(
                f"edge_type '{edge_type}' requires both from_type and to_type "
                f"hints; got {from_type!r}/{to_type!r}"
            )
        if (from_type, to_type) not in pairs:
            raise ValueError(
                f"edge_type '{edge_type}' does not accept pair "
                f"({from_type}, {to_type}); valid pairs: {pairs}"
            )
        return from_type, to_type

    if len(pairs) == 1:
        return pairs[0]

    raise ValueError(
        f"edge_type '{edge_type}' is ambiguous and requires explicit "
        f"from_type/to_type hints; valid pairs: {pairs}"
    )


STABLE_NODE_PROPERTIES: tuple[str, ...] = (
    "id",
    "title",
    "content",
    "context",
    "justification",
    "source_artifact_ref",
    "graph_layer",
    "maturity_status",
    "source_session_id",
    "created_at",
    "created_by_agent",
    "source_confidence",
    "relevance_score",
    "query_hits",
    "last_queried_at",
    "last_recomputed_at",
    "priority_boost",
    "superseded_by",
    "superseded_at",
    "revocation_reason",
    "human_curated",
    "generation",
)

RELEVANCE_COLUMNS: tuple[tuple[str, str], ...] = (
    ("relevance_score", "DOUBLE"),
    ("query_hits", "INT64"),
    ("last_queried_at", "STRING"),
)

PRIORITY_BOOST_COLUMNS: tuple[tuple[str, str], ...] = (
    ("priority_boost", "DOUBLE"),
)

HUMAN_CURATED_COLUMNS: tuple[tuple[str, str], ...] = (
    ("human_curated", "BOOLEAN"),
)

LAST_RECOMPUTED_COLUMNS: tuple[tuple[str, str], ...] = (
    ("last_recomputed_at", "STRING"),
)

KG_LAYER_COLUMNS: tuple[tuple[str, str], ...] = (
    ("graph_layer", "STRING"),
    ("maturity_status", "STRING"),
)

GENERATION_COLUMNS: tuple[tuple[str, str], ...] = (
    # Spec MKG-A-S1 (FR3): supersedence generation for deterministic node
    # identity. NULL (legacy nodes) reads as 0; SUPERSEDE mints old+1.
    ("generation", "INT64"),
)

LEGACY_NODE_COLUMNS: tuple[str, ...] = (
    "validation_status",
    "corroboration_count",
)


def vector_index_name(node_type: str) -> str:
    """Canonical HNSW index name per node type."""
    return f"{node_type.lower()}_embedding_idx"


__all__ = [
    "EDGE_LAYERS",
    "EDGE_METADATA_COLUMNS",
    "GENERATION_COLUMNS",
    "HUMAN_CURATED_COLUMNS",
    "KG_LAYER_COLUMNS",
    "LAST_RECOMPUTED_COLUMNS",
    "LEGACY_NODE_COLUMNS",
    "MULTI_REL_TYPES",
    "NODE_TYPES",
    "PRIORITY_BOOST_COLUMNS",
    "REL_TYPES",
    "RELEVANCE_COLUMNS",
    "SCHEMA_VERSION",
    "STABLE_NODE_PROPERTIES",
    "VECTOR_INDEX_TYPES",
    "relationship_endpoint_pairs",
    "resolve_relationship_endpoint_pair",
    "stable_rel_type_entries",
    "vector_index_name",
]
