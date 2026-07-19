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

from okto_pulse.core.kg.blocking_io import run_blocking_graph_io
from okto_pulse.core.kg.canonical_partition_integrity import (
    evaluate_canonical_learning_publication,
)
from okto_pulse.core.kg.source_maturity import (
    GRAPH_LAYER_CANONICAL,
    GRAPH_LAYER_WORKING,
)
from okto_pulse.core.ports.runtime_workers import BlockingExecutionPort

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


def _read_authoritative_board_layer_meta(
    board_id: str,
    original_node_ids: set[str],
) -> tuple[dict[str, dict[str, Any]] | None, str | None]:
    """Resolve source metadata by enumerating every digestable board label.

    A global ``DecisionDigest.node_type`` is cache data and may itself be stale
    or corrupt.  It therefore cannot select the board label used by parity.
    The shared R1 source-inventory helper enumerates every supported source type;
    its authoritative mapping then drives the detailed board-metadata reader,
    retaining the Learning completeness inputs implemented there.

    Missing source ids are intentionally absent from the result (the outbox
    pruner owns those).  An id present under more than one digestable board label
    has no unique source authority, so the probe degrades to unavailable instead
    of manufacturing a deterministic-but-wrong answer from tuple ordering.
    """
    from okto_pulse.core.application.processors.global_outbox import (
        GlobalOutboxProcessor,
    )

    try:
        all_source_types = (
            GlobalOutboxProcessor._read_board_digestable_node_types(board_id)
        )
    except Exception as exc:
        reason = (
            "board_source_type_ambiguous"
            if "outbox.source_identity_ambiguous" in str(exc)
            else "board_layer_meta_unavailable"
        )
        return None, reason
    if all_source_types is None:
        return None, "board_layer_meta_unavailable"

    # Missing source ids stay absent: they belong to stale-digest pruning, not
    # layer-parity reporting.
    source_types = {
        node_id: all_source_types[node_id]
        for node_id in original_node_ids
        if node_id in all_source_types
    }
    detailed_meta = GlobalOutboxProcessor._read_board_layer_meta(
        board_id,
        source_types,
    )
    if detailed_meta is None:
        return None, "board_layer_meta_unavailable"
    authoritative = {
        node_id: {
            **meta,
            # The board inventory is authoritative even if an adapter
            # accidentally echoes a different detailed-metadata value.
            "node_type": source_types[node_id],
        }
        for node_id, meta in detailed_meta.items()
        if node_id in source_types
    }
    return authoritative, None


def collect_digest_layer_mismatch_inputs(board_id: str) -> dict[str, Any]:
    """Collect the synchronous graph inputs used by the parity evaluator.

    Embedded graph reads may block on storage locks, so KG Health dispatches
    this function through its bounded probe worker. The public detector below
    keeps the same semantics by calling it directly before its async SQL
    overlay step.
    """
    from okto_pulse.core.kg.interfaces import get_kg_registry

    try:
        global_runtime = get_kg_registry().require_global_discovery_runtime()
    except Exception:
        return {
            "status": "unavailable",
            "reason": "global_discovery_runtime_unavailable",
            "digests": [],
            "board_meta": {},
            "needs_overlay": False,
        }
    try:
        digests: list[dict[str, Any]] = []
        res = global_runtime.execute(
            "MATCH (d:DecisionDigest) WHERE d.board_id = $bid "
            "RETURN d.id, d.original_node_id, "
            "coalesce(d.graph_layer, 'legacy_unknown')",
            {"bid": board_id},
        )
        for row in res.rows:
            oid = str(row[1]) if row[1] is not None else ""
            if not oid:
                continue
            digests.append({
                "board_id": board_id,
                "digest_id": str(row[0]),
                "original_node_id": oid,
                # Filled only from the authoritative board lookup below.  Keep
                # the field for compatibility with existing snapshot callers.
                "node_type": "",
                "actual_layer": str(row[2] or "legacy_unknown"),
            })
    except Exception:
        return {
            "status": "unavailable",
            "reason": "global_discovery_read_failed",
            "digests": [],
            "board_meta": {},
            "needs_overlay": False,
        }
    if not digests:
        return {
            "status": "available",
            "reason": "no_digests",
            "digests": [],
            "board_meta": {},
            "needs_overlay": False,
        }

    board_meta, board_meta_error = _read_authoritative_board_layer_meta(
        board_id,
        {d["original_node_id"] for d in digests},
    )
    if board_meta is None:
        return {
            "status": "unavailable",
            "reason": board_meta_error or "board_layer_meta_unavailable",
            "digests": [],
            "board_meta": {},
            "needs_overlay": False,
        }

    for digest in digests:
        meta = board_meta.get(digest["original_node_id"])
        if meta is not None:
            digest["node_type"] = str(meta["node_type"])

    needs_overlay = any(
        m.get("node_type") == "Learning" and m.get("graph_layer") == "canonical"
        for m in board_meta.values()
    )
    return {
        "status": "available",
        "reason": "ok",
        "digests": digests,
        "board_meta": board_meta,
        "needs_overlay": needs_overlay,
    }


def evaluate_digest_layer_mismatch_inputs(
    inputs: dict[str, Any],
    *,
    overlay: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    """Purely evaluate collected graph inputs against an optional SQL overlay."""
    from okto_pulse.core.kg.rebuild_audit import normalize_cognitive_artifact_id

    if inputs.get("status") != "available":
        return []
    digests = list(inputs.get("digests") or [])
    board_meta = dict(inputs.get("board_meta") or {})
    effective_overlay = overlay or {}

    mismatches: list[dict[str, Any]] = []
    for d in digests:
        meta = board_meta.get(d["original_node_id"])
        if meta is None:
            continue
        artifact_id = normalize_cognitive_artifact_id(
            meta.get("source_artifact_ref") or ""
        )
        expected, _reason = resolve_expected_digest_layer(
            node_type=meta["node_type"],
            raw_graph_layer=meta["graph_layer"],
            source_artifact_ref=meta.get("source_artifact_ref") or "",
            canonical_bug_count=int(meta.get("canonical_bug_count") or 0),
            relates_to_endpoints=tuple(meta.get("relates_to_endpoints") or ()),
            overlay_exclusion_reason=effective_overlay.get(artifact_id),
        )
        if expected != d["actual_layer"]:
            mismatches.append({
                "board_id": str(d.get("board_id") or ""),
                "digest_id": d["digest_id"],
                "original_node_id": d["original_node_id"],
                "node_type": meta["node_type"],
                "expected_layer": expected,
                "actual_layer": d["actual_layer"],
                "source_artifact_ref": meta.get("source_artifact_ref") or "",
            })
    return mismatches


async def detect_digest_layer_mismatches(
    db: object,
    *,
    board_id: str,
    blocking_execution: BlockingExecutionPort | None = None,
) -> list[dict[str, Any]]:
    """List digests whose published layer != expected_digest_layer.

    Read-only. Degrades to ``[]`` if the global or board graph is unreadable
    (Health/drilldown must never crash). A digest whose source node has vanished
    is NOT a layer mismatch (that is prune territory) and is skipped.
    """
    from okto_pulse.core.kg.canonical_partition_integrity import (
        pending_or_debt_exclusions,
    )

    inputs = await run_blocking_graph_io(
        lambda: collect_digest_layer_mismatch_inputs(board_id),
        task_name="core.kg.digest_layer_parity.graph_read",
        blocking_execution=blocking_execution,
    )
    if inputs.get("status") != "available":
        return []
    needs_overlay = bool(inputs.get("needs_overlay"))
    overlay = (
        await pending_or_debt_exclusions(db, board_id=board_id)
        if needs_overlay else {}
    )
    return evaluate_digest_layer_mismatch_inputs(inputs, overlay=overlay)


async def list_digest_layer_mismatches(
    db: object,
    *,
    board_id: str,
    limit: int = 50,
    offset: int = 0,
    blocking_execution: BlockingExecutionPort | None = None,
) -> dict[str, Any]:
    """Drilldown read model (MCP/REST). Emits one bounded
    ``kg_discovery_digest_layer_mismatch_total`` sample per observed mismatch
    (over the UNFILTERED set, mirroring the R7 OR1 pattern)."""
    from okto_pulse.core.kg.global_discovery.metrics import emit_digest_layer_mismatch

    bounded_limit = max(1, min(int(limit), 200))
    bounded_offset = max(0, int(offset))
    mismatches = await detect_digest_layer_mismatches(
        db,
        board_id=board_id,
        blocking_execution=blocking_execution,
    )
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
    "collect_digest_layer_mismatch_inputs",
    "detect_digest_layer_mismatches",
    "evaluate_digest_layer_mismatch_inputs",
    "list_digest_layer_mismatches",
    "LEARNING_NODE_TYPE",
    "DIGEST_LAYER_MISMATCH_CODE",
]
