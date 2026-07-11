"""R1-IMP1 — shared ``expected_digest_layer`` resolver (publication layer).

The authoritative publication layer of a ``DecisionDigest`` is NOT the raw board
node ``graph_layer``. For a *canonical* ``Learning`` it is downgraded to
``working`` when R7 says the canonical evidence is incomplete (working-only Bug
evidence, an open ``canonical_debt`` or an active ``cognitive_pending`` hold).
This module is the SINGLE place that publication rule lives, so the Global
Discovery upsert path (``_apply_event``), the parity reconciler
(``_reconcile_board_digest_layers``) and KG Health (R1-IMP2) all agree on what
``DecisionDigest.graph_layer`` SHOULD be.

It is PURE: the caller fetches the (board-scoped) debt/pending overlay once and
passes the per-artifact reason in. ``raw_graph_layer`` must already be coalesced
to ``legacy_unknown`` for a missing layer — this resolver NEVER defaults a
missing layer to ``canonical`` (fail-closed; FR5). R2 (stale canonical demotion
of the source) and R7 partition integrity are out of scope here.
"""

from __future__ import annotations

from typing import Any


from okto_pulse.core.kg.canonical_partition_integrity import (
    evaluate_canonical_learning_publication,
)
from okto_pulse.core.kg.source_maturity import (
    GRAPH_LAYER_CANONICAL,
    GRAPH_LAYER_WORKING,
)

LEARNING_NODE_TYPE = "Learning"
DIGEST_LAYER_MISMATCH_CODE = "digest_vs_board_layer_mismatch"


def resolve_expected_digest_layer(
    *,
    node_type: str,
    raw_graph_layer: str,
    source_artifact_ref: str | None = None,
    canonical_bug_count: int = 0,
    relates_to_endpoints: tuple[tuple[str, str | None], ...] = (),
    overlay_exclusion_reason: str | None = None,
) -> tuple[str, str | None]:
    """Return ``(expected_publication_layer, exclusion_reason_or_None)``.

    Only a *canonical* ``Learning`` is subject to the R7 publication carve-out
    (reuses :func:`evaluate_canonical_learning_publication`); every other node —
    and a non-canonical Learning — publishes at its own (already fail-closed)
    ``raw_graph_layer``. ``exclusion_reason`` is non-None only when a canonical
    Learning was downgraded to ``working`` (so the caller can emit the metric).

    ``relates_to_endpoints`` (S-KG-02) is the non-bug taxonomy evidence
    ``(endpoint_node_type, endpoint_layer)`` the caller collected from the board
    graph; it is threaded to the publication authority so a non-bug canonical
    Learning publishes at ``canonical`` ONLY with a resolved source + a canonical
    ``relates_to`` to an S-KG-01 taxonomy endpoint (else downgraded to ``working``).
    """
    if node_type == LEARNING_NODE_TYPE and raw_graph_layer == GRAPH_LAYER_CANONICAL:
        publishable, reason = evaluate_canonical_learning_publication(
            source_artifact_ref=source_artifact_ref or "",
            canonical_bug_count=int(canonical_bug_count or 0),
            relates_to_endpoints=relates_to_endpoints,
            overlay_exclusion_reason=overlay_exclusion_reason,
        )
        if not publishable:
            return (GRAPH_LAYER_WORKING, reason)
        return (GRAPH_LAYER_CANONICAL, None)
    return (raw_graph_layer, None)


# ---------------------------------------------------------------------------
# R1-IMP2 — digest_vs_board_layer_mismatch detection (READ-ONLY).
#
# A mismatch is a published ``DecisionDigest.graph_layer`` that diverges from the
# ``expected_digest_layer`` recomputed from the CURRENT board graph (via the same
# resolver + the R1-IMP1 board-layer reader). The R1-IMP1 reconciler corrects
# these on each drain, so steady-state is ~0; KG Health + the drilldown surface
# any transient/parity-debt divergence. This module never mutates anything.
# ---------------------------------------------------------------------------


async def detect_digest_layer_mismatches(
    db: object, *, board_id: str
) -> list[dict[str, Any]]:
    """List digests whose published layer != expected_digest_layer.

    Read-only. Degrades to ``[]`` if the global or board graph is unreadable
    (Health/drilldown must never crash). A digest whose source node has vanished
    is NOT a layer mismatch (that is prune territory) and is skipped.
    """
    from okto_pulse.core.kg.canonical_partition_integrity import (
        pending_or_debt_exclusions,
    )
    from okto_pulse.core.application.processors.global_outbox import (
        GlobalOutboxProcessor,
    )
    from okto_pulse.core.kg.interfaces import get_kg_registry
    from okto_pulse.core.kg.rebuild_audit import normalize_cognitive_artifact_id

    try:
        global_runtime = get_kg_registry().require_global_discovery_runtime()
    except Exception:
        return []
    try:
        digests: list[dict[str, Any]] = []
        res = global_runtime.execute(
            "MATCH (d:DecisionDigest) WHERE d.board_id = $bid "
            "RETURN d.id, d.original_node_id, d.node_type, "
            "coalesce(d.graph_layer, 'legacy_unknown')",
            {"bid": board_id},
        )
        while res.has_next():
            row = res.get_next()
            oid = str(row[1]) if row[1] is not None else ""
            if not oid:
                continue
            digests.append({
                "digest_id": str(row[0]),
                "original_node_id": oid,
                "node_type": str(row[2] or ""),
                "actual_layer": str(row[3] or "legacy_unknown"),
            })
    except Exception:
        return []
    if not digests:
        return []

    # Reuse the R1-IMP1 board-layer reader (no IMP1 change).
    board_meta = GlobalOutboxProcessor._read_board_layer_meta(
        board_id, {d["original_node_id"]: d["node_type"] for d in digests}
    )
    if board_meta is None:
        return []

    needs_overlay = any(
        m.get("node_type") == "Learning" and m.get("graph_layer") == "canonical"
        for m in board_meta.values()
    )
    overlay = (
        await pending_or_debt_exclusions(db, board_id=board_id)
        if needs_overlay else {}
    )

    mismatches: list[dict[str, Any]] = []
    for d in digests:
        meta = board_meta.get(d["original_node_id"])
        if meta is None:
            continue
        artifact_id = normalize_cognitive_artifact_id(meta.get("source_artifact_ref") or "")
        expected, _reason = resolve_expected_digest_layer(
            node_type=meta["node_type"],
            raw_graph_layer=meta["graph_layer"],
            source_artifact_ref=meta.get("source_artifact_ref") or "",
            canonical_bug_count=int(meta.get("canonical_bug_count") or 0),
            relates_to_endpoints=tuple(meta.get("relates_to_endpoints") or ()),
            overlay_exclusion_reason=overlay.get(artifact_id),
        )
        if expected != d["actual_layer"]:
            mismatches.append({
                "board_id": board_id,
                "digest_id": d["digest_id"],
                "original_node_id": d["original_node_id"],
                "node_type": meta["node_type"],
                "expected_layer": expected,
                "actual_layer": d["actual_layer"],
                "source_artifact_ref": meta.get("source_artifact_ref") or "",
            })
    return mismatches


async def list_digest_layer_mismatches(
    db: object, *, board_id: str, limit: int = 50, offset: int = 0
) -> dict[str, Any]:
    """Drilldown read model (MCP/REST). Emits one bounded
    ``kg_discovery_digest_layer_mismatch_total`` sample per observed mismatch
    (over the UNFILTERED set, mirroring the R7 OR1 pattern)."""
    from okto_pulse.core.kg.global_discovery.metrics import emit_digest_layer_mismatch

    bounded_limit = max(1, min(int(limit), 200))
    bounded_offset = max(0, int(offset))
    mismatches = await detect_digest_layer_mismatches(db, board_id=board_id)
    for m in mismatches:
        emit_digest_layer_mismatch(
            board_id=board_id,
            expected_layer=m["expected_layer"],
            actual_layer=m["actual_layer"],
        )
    mismatches.sort(key=lambda m: (m["node_type"], m["original_node_id"]))
    page = mismatches[bounded_offset:bounded_offset + bounded_limit]
    return {
        "board_id": board_id,
        "items": page,
        "count": len(mismatches),
        "health_issue_code": DIGEST_LAYER_MISMATCH_CODE,
        "total": len(mismatches),
        "limit": bounded_limit,
        "offset": bounded_offset,
    }


__all__ = [
    "resolve_expected_digest_layer",
    "detect_digest_layer_mismatches",
    "list_digest_layer_mismatches",
    "LEARNING_NODE_TYPE",
    "DIGEST_LAYER_MISMATCH_CODE",
]
