"""Pure rebuild-ingestion contracts shared by core and edition adapters.

The SQLite implementation lives in the Community adapter. Core keeps only the
deterministic source filters and receipt helpers needed by the rebuild service
and boundary tests.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any


# Source artifact types that get deterministic rebuild ingestion.
# Decision rows are intentionally excluded here: they are materialized through
# the owning spec payload. task/test/bug are card-derived source types and are
# mapped to the worker's legacy ``card`` artifact_type in ConsolidationQueue.
_DETERMINISTIC_SOURCE_ARTIFACT_TYPES: frozenset[str] = frozenset(
    {
        "story",
        "ideation",
        "refinement",
        "spec",
        "sprint",
        "task",
        "test",
        "bug",
        "card",
        # Path B amendment (spec 7ea1e4be). MUST be enqueued for materialization,
        # otherwise it is counted in expected_by_layer but never materialized.
        "amendment_hotfix_revision",
        # Code Traceability rows are already persisted by the edition adapter.
        # Rebuild ingestion consumes only these relational records; it never
        # opens a repository, filesystem, Git checkout, or provider API.
        "code_investigation_receipt",
        "code_evidence",
        "implementation_target",
    }
)
_CARD_SOURCE_ARTIFACT_TYPES: frozenset[str] = frozenset(
    {
        "task",
        "test",
        "bug",
        "card",
    }
)

DETERMINISTIC_SOURCE_ARTIFACT_TYPES = _DETERMINISTIC_SOURCE_ARTIFACT_TYPES
CARD_SOURCE_ARTIFACT_TYPES = _CARD_SOURCE_ARTIFACT_TYPES

# Only rebuild ingestion consumes this dependency rank. Ordinary event-driven
# queue writes retain their existing priority/timestamp fairness. Values leave
# gaps so future governed source kinds can be inserted without reordering the
# established parent-before-child contract.
_REBUILD_SOURCE_DEPENDENCY_RANK: dict[str, int] = {
    "board": 0,
    "story": 0,
    "ideation": 10,
    "refinement": 20,
    "spec": 30,
    "sprint": 40,
    "task": 50,
    "test": 50,
    "bug": 50,
    "card": 50,
    "amendment_hotfix_revision": 60,
    "code_investigation_receipt": 60,
    "code_evidence": 70,
    "implementation_target": 80,
}
REBUILD_SOURCE_DEPENDENCY_RANK = _REBUILD_SOURCE_DEPENDENCY_RANK


def _queue_artifact_type(source_artifact_type: str) -> str:
    return (
        "card"
        if source_artifact_type in _CARD_SOURCE_ARTIFACT_TYPES
        else source_artifact_type
    )


def queue_artifact_type(source_artifact_type: str) -> str:
    """Public facade for rebuild queue artifact-type mapping."""

    return _queue_artifact_type(source_artifact_type)


def rebuild_source_order_key(source: Mapping[str, Any]) -> tuple[int, str, str]:
    """Return a stable parent-before-child key for one rebuild source.

    The key is intentionally not part of the normal queue priority contract.
    Edition adapters use it only while assigning timestamps to rows created or
    reset by one explicit rebuild batch.
    """

    artifact_type = str(source.get("artifact_type", ""))
    artifact_id = str(source.get("id", ""))
    return (
        _REBUILD_SOURCE_DEPENDENCY_RANK.get(artifact_type, 100),
        _queue_artifact_type(artifact_type),
        artifact_id,
    )


def _expected_layers_from_sources(
    sources: Sequence[Mapping[str, Any]] | None,
) -> dict[str, int]:
    """Return the per-graph-layer source partition presence expected to materialize."""

    out: dict[str, int] = {}
    for row in sources or ():
        layer = (
            row.get("graph_layer")
            if hasattr(row, "get")
            else getattr(row, "graph_layer", None)
        )
        key = str(layer or "unclassified")
        out[key] = out.get(key, 0) + 1
    return out


def expected_layers_from_sources(
    sources: Sequence[Mapping[str, Any]] | None,
) -> dict[str, int]:
    """Public facade for expected graph-layer partition counts."""

    return _expected_layers_from_sources(sources)


__all__ = [
    "CARD_SOURCE_ARTIFACT_TYPES",
    "DETERMINISTIC_SOURCE_ARTIFACT_TYPES",
    "REBUILD_SOURCE_DEPENDENCY_RANK",
    "expected_layers_from_sources",
    "rebuild_source_order_key",
    "queue_artifact_type",
    "_CARD_SOURCE_ARTIFACT_TYPES",
    "_DETERMINISTIC_SOURCE_ARTIFACT_TYPES",
    "_expected_layers_from_sources",
    "_queue_artifact_type",
]
