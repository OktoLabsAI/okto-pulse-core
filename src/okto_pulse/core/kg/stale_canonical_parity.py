"""R2-IMP4 — read-only stale-canonical parity diagnostics.

Surfaces, READ-ONLY, where a canonical deterministic board-graph node is stale
because its SQL source regressed below canonical eligibility (the read-only view
of the R2-IMP1 reconciler's detection — it NEVER demotes/reconciles/syncs here),
and whether the Global Discovery digest for that node is also stale (reusing the
R1-IMP2 digest-vs-board parity detector — no digest repair here).

Distinct health category from canonical_debt_open / cognitive_consolidation_pending
/ DLQ / canonical_partition_integrity (R7) — it never masks those higher-severity
blockers. The Global Discovery evaluation degrades to ``not_evaluated`` (safe,
transparent) if R1 digest metadata is unreadable — never a false healthy.
"""

from __future__ import annotations

import logging
from typing import Any

from okto_pulse.core.kg.canonical_stale_reconciler import (
    COGNITIVE_NODE_TYPES,
    _owning_source_id,
)
from okto_pulse.core.kg.blocking_io import run_blocking_graph_io
from okto_pulse.core.kg.source_maturity import (
    GRAPH_LAYER_CANONICAL,
    GRAPH_LAYER_WORKING,
    MATURITY_WORKING_STALE,
    classify_source_for_kg,
)
from okto_pulse.core.ports.runtime_workers import BlockingExecutionPort

logger = logging.getLogger("okto_pulse.kg.stale_canonical_parity")

HEALTH_ISSUE_CODE = "stale_canonical_parity"
_DETERMINISTIC_SCAN_TYPES: tuple[str, ...] = (
    "Decision", "Requirement", "Criterion", "Constraint", "APIContract",
    "TestScenario", "Bug", "Entity",
)
_RECOMMENDED_ACTION = (
    "Re-mature the source (return it to a canonical-eligible status) or let the "
    "R2 reconciler demote the stale canonical node; this is a read-only diagnostic."
)
GD_EVALUATED = "evaluated"
GD_NOT_EVALUATED = "not_evaluated"


def _source_index(board_id: str) -> dict[str, Any]:
    from okto_pulse.core.kg.interfaces import get_kg_registry

    out: dict[str, Any] = {}
    reader = get_kg_registry().require_board_source_reader()
    for row in reader.fetch(board_id):
        aid = str(row.get("id") or "")
        if not aid:
            continue
        out[aid] = classify_source_for_kg(
            artifact_type=row.get("artifact_type"),
            artifact_status=row.get("source_artifact_status") or row.get("status"),
            content_hash=row.get("content_hash"),
            updated_at=row.get("updated_at"),
            has_minimal_evidence=bool(row.get("has_minimal_evidence", True)),
        )
    return out


def detect_board_graph_stale(board_id: str) -> list[dict[str, Any]]:
    """READ-ONLY: canonical DETERMINISTIC board-graph nodes whose source is no
    longer canonical-eligible. Never mutates. Cognitive nodes are excluded (kept
    in the R7 canonical_partition_integrity category). Degrades to ``[]`` if the
    board graph is unreadable."""
    from okto_pulse.core.kg.interfaces import get_kg_registry

    source_by_id = _source_index(board_id)
    out: list[dict[str, Any]] = []
    try:
        cypher = get_kg_registry().cypher_executor
        for ntype in _DETERMINISTIC_SCAN_TYPES:
            try:
                result = cypher.execute_read_only(
                    board_id,
                    f"MATCH (n:{ntype}) WHERE n.graph_layer = $c "
                    f"RETURN n.id, n.source_artifact_ref, n.created_by_agent",
                    {"c": GRAPH_LAYER_CANONICAL},
                    max_rows=10000,
                )
            except Exception:
                continue
            for row in result.get("rows", []):
                node_id = str(row[0])
                ref = str(row[1] or "")
                writer = str(row[2] or "")
                # Exclude cognitive-origin nodes (R7 territory, kept distinct).
                if ntype in COGNITIVE_NODE_TYPES:
                    continue
                from okto_pulse.core.kg.connectivity_guard import (
                    WriterClass,
                    classify_writer_path,
                )
                if classify_writer_path(writer) == WriterClass.COGNITIVE:
                    continue
                src_id = _owning_source_id(ref)
                if src_id is None:
                    continue
                cls = source_by_id.get(src_id)
                if cls is not None and cls.graph_layer == GRAPH_LAYER_CANONICAL:
                    continue  # source still canonical -> not stale
                out.append({
                    "node_id": node_id,
                    "node_type": ntype,
                    "source_artifact_ref": ref,
                    "owning_source_id": src_id,
                    "board_graph_stale": True,
                    "expected_graph_layer": (
                        cls.graph_layer if cls else GRAPH_LAYER_WORKING
                    ) or GRAPH_LAYER_WORKING,
                    "expected_maturity_status": (
                        cls.maturity_status if cls else MATURITY_WORKING_STALE
                    ),
                    "current_source_status": cls.artifact_status if cls else "",
                    "recommended_action": _RECOMMENDED_ACTION,
                })
    except Exception as exc:
        logger.warning(
            "kg.stale_canonical_parity.board_read_failed board=%s err=%s",
            board_id, exc,
        )
        return []
    return out


async def list_stale_canonical_parity(
    db: object,
    *,
    board_id: str,
    limit: int = 50,
    offset: int = 0,
    blocking_execution: BlockingExecutionPort | None = None,
) -> dict[str, Any]:
    """Read-only unified stale-canonical parity drilldown (MCP/REST).

    Each item is a canonical deterministic node whose source regressed
    (``board_graph_stale``), annotated with ``global_discovery_stale_digest``
    (does its DecisionDigest also diverge, via the R1 parity detector). The GD
    evaluation degrades to ``not_evaluated`` (never false healthy) if R1 digest
    metadata is unreadable. NEVER mutates / demotes / reconciles / syncs.
    """
    bounded_limit = max(1, min(int(limit), 200))
    bounded_offset = max(0, int(offset))

    board_stale = await run_blocking_graph_io(
        lambda: detect_board_graph_stale(board_id),
        task_name="core.kg.stale_canonical_parity.board_read",
        blocking_execution=blocking_execution,
    )

    # Reuse the R1-IMP2 digest-vs-board detector (no digest repair here).
    gd_evaluation = GD_EVALUATED
    stale_digest_node_ids: set[str] = set()
    try:
        from okto_pulse.core.kg.global_discovery.layer_parity import (
            detect_digest_layer_mismatches,
        )
        mismatches = await detect_digest_layer_mismatches(
            db,
            board_id=board_id,
            blocking_execution=blocking_execution,
        )
        stale_digest_node_ids = {str(m.get("original_node_id") or "") for m in mismatches}
    except Exception as exc:  # never claim healthy on an unreadable GD layer
        gd_evaluation = GD_NOT_EVALUATED
        logger.warning(
            "kg.stale_canonical_parity.gd_not_evaluated board=%s err=%s",
            board_id, exc,
        )

    for item in board_stale:
        if gd_evaluation == GD_NOT_EVALUATED:
            item["global_discovery_stale_digest"] = None  # unknown, not False
        else:
            item["global_discovery_stale_digest"] = (
                item["node_id"] in stale_digest_node_ids
            )

    board_stale.sort(key=lambda i: (i["node_type"], i["node_id"]))
    page = board_stale[bounded_offset:bounded_offset + bounded_limit]
    return {
        "board_id": board_id,
        "items": page,
        "count": len(board_stale),
        "total": len(board_stale),
        "health_issue_code": HEALTH_ISSUE_CODE,
        # AC5/AC13: this is a READ-ONLY diagnostic. The literal contract flag makes
        # the no-mutation guarantee explicit to MCP/UI consumers (no agent-facing
        # mutation tool may demote/clear stale here; the R2 reconciler is the only
        # internal demotion path, driven by a maturity/status event or sweep).
        "mutation_allowed": False,
        "global_discovery_evaluation": gd_evaluation,
        "global_discovery_stale_digest_count": len(stale_digest_node_ids),
        "limit": bounded_limit,
        "offset": bounded_offset,
    }


__all__ = [
    "HEALTH_ISSUE_CODE",
    "GD_EVALUATED",
    "GD_NOT_EVALUATED",
    "detect_board_graph_stale",
    "list_stale_canonical_parity",
]
