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
from collections.abc import Mapping
from typing import Any

from okto_pulse.core.kg.canonical_stale_reconciler import (
    COGNITIVE_NODE_TYPES,
    SourceIdentity,
    _build_source_classification_map,
    _source_identity_from_ref,
)
from okto_pulse.core.kg.blocking_io import run_blocking_graph_io
from okto_pulse.core.kg.source_maturity import (
    GRAPH_LAYER_CANONICAL,
    GRAPH_LAYER_WORKING,
    MATURITY_WORKING_STALE,
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


def _source_index(board_id: str) -> dict[SourceIdentity, Any]:
    """Reuse the reconciler's completeness-qualified, type-safe census."""
    from okto_pulse.core.kg.interfaces import SourceUnavailableError

    source_by_identity, complete, cause = _build_source_classification_map(board_id)
    if not complete:
        raise SourceUnavailableError(
            "stale parity source snapshot is incomplete "
            f"(board_id={board_id}, cause={cause})",
            cause_type=str(cause or "unknown"),
        )
    return source_by_identity


def detect_board_graph_stale(board_id: str) -> list[dict[str, Any]]:
    """READ-ONLY: canonical DETERMINISTIC board-graph nodes whose source is no
    longer canonical-eligible. Never mutates. Cognitive nodes are excluded (kept
    in the R7 canonical_partition_integrity category).

    Read failures propagate so callers can distinguish an evaluated empty result
    from an unavailable probe.  Returning ``[]`` after a per-label failure would
    be a false healthy result for governed-takedown verification.
    """
    from okto_pulse.core.kg.interfaces import get_kg_registry

    # Source census failures must propagate. Returning [] here would turn an
    # unavailable/incomplete durable source into a false healthy diagnosis.
    source_by_identity = _source_index(board_id)
    out: list[dict[str, Any]] = []
    cypher = get_kg_registry().cypher_executor
    for ntype in _DETERMINISTIC_SCAN_TYPES:
        result = cypher.execute_read_only(
            board_id,
            f"MATCH (n:{ntype}) WHERE n.graph_layer = $c "
            f"RETURN n.id, n.source_artifact_ref, n.created_by_agent",
            {"c": GRAPH_LAYER_CANONICAL},
            max_rows=10000,
        )
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
            source_identity = _source_identity_from_ref(ref)
            if source_identity is None:
                continue
            cls = source_by_identity.get(source_identity)
            if cls is not None and cls.graph_layer == GRAPH_LAYER_CANONICAL:
                continue  # source still canonical -> not stale
            out.append({
                "node_id": node_id,
                "node_type": ntype,
                "source_artifact_ref": ref,
                "owning_source_id": source_identity[1],
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
    gd_status = "available"
    gd_evaluation_reason = "ok"
    stale_digest_node_ids: set[str] = set()
    try:
        from okto_pulse.core.kg.global_discovery.layer_parity import (
            detect_digest_layer_mismatches,
        )
        detected = await detect_digest_layer_mismatches(
            db,
            board_id=board_id,
            blocking_execution=blocking_execution,
        )
        if not isinstance(detected, Mapping):
            raise RuntimeError("digest_layer_parity_result_invalid")
        gd_evaluation = str(
            detected.get("evaluation") or GD_NOT_EVALUATED
        )
        gd_status = str(detected.get("status") or "unavailable")
        gd_evaluation_reason = str(
            detected.get("reason") or "digest_layer_parity_unavailable"
        )
        raw_mismatches = detected.get("items")
        if not isinstance(raw_mismatches, list):
            raise RuntimeError("digest_layer_parity_result_invalid")
        mismatches = raw_mismatches

        if gd_evaluation == GD_EVALUATED and gd_status == "available":
            stale_digest_node_ids = {
                str(m.get("original_node_id") or "") for m in mismatches
            }
        else:
            gd_evaluation = GD_NOT_EVALUATED
            gd_status = "unavailable"
            logger.warning(
                "kg.stale_canonical_parity.gd_not_evaluated board=%s reason=%s",
                board_id,
                gd_evaluation_reason,
            )
    except Exception as exc:  # never claim healthy on an unreadable GD layer
        gd_evaluation = GD_NOT_EVALUATED
        gd_status = "unavailable"
        gd_evaluation_reason = type(exc).__name__
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
        "global_discovery_status": gd_status,
        "global_discovery_evaluation": gd_evaluation,
        "global_discovery_evaluation_reason": gd_evaluation_reason,
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
