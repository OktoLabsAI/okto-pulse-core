"""R7 IMP4 — unified canonical-partition-integrity read model (read-only).

A single projection over the FOUR partition-integrity sources, so KG Health
(aggregate), the REST drilldown and the MCP drilldown all read the same shape
without recomputing policy:

  1. ``cognitive_pending``        — IMP1 go-forward HOLD: a CognitiveConsolidationItem
     carrying reason ``canonical_learning_working_only_bug_evidence_pending``.
  2. ``canonical_debt``           — IMP2 historical remediation debt: a CanonicalDebt
     row with target_status ``canonical_learning_partition_integrity``.
  3. ``mixed_evidence_deferred``  — canonical Learning that has >=1 canonical Bug
     AND >=1 working Bug validates edge (working edge deferred, advisory).
  4. ``provenance_only_observed`` — canonical Learning that is NOT bug-derived
     (observed, non-blocking).

Sources 1/2 are CHEAP (store read + SQL). Sources 3/4 require a board-graph scan,
which lives ONLY here in the drilldown (paginated/filtered) — NEVER inline in KG
Health (kept light per tick). This module is strictly read-only: it never skips,
clears, retries or mutates. Enforcement of the human-only skip policy lives in
``cognitive_readiness`` / the MCP update tool, not here.
"""

from __future__ import annotations

import threading
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.kg.canonical_learning_partition import (
    HISTORICAL_DEBT_REASON,
    _is_bug_derived_ref,
)
from okto_pulse.core.kg.connectivity_guard import (
    CANONICAL_LEARNING_MIXED_DEFERRED_REASON,
    CANONICAL_LEARNING_PROVENANCE_ONLY_REASON,
    CANONICAL_LEARNING_WORKING_ONLY_REASON,
)
from okto_pulse.core.kg.cognitive_readiness import CognitiveReadinessError
from okto_pulse.core.kg.rebuild_audit import (
    CognitiveConsolidationItemStore,
    default_rebuild_base_dir,
    normalize_cognitive_artifact_id,
)
from okto_pulse.core.kg.source_maturity import (
    GRAPH_LAYER_CANONICAL,
    GRAPH_LAYER_WORKING,
)
from okto_pulse.core.services.canonical_debt_service import (
    OPEN_STATES,
    list_canonical_debt,
)

HEALTH_ISSUE_CODE = "canonical_partition_integrity"

# Bounded partition statuses (closed vocabulary; never free-text).
STATUS_COGNITIVE_PENDING = "cognitive_pending"
STATUS_CANONICAL_DEBT = "canonical_debt"
STATUS_MIXED_DEFERRED = "mixed_evidence_deferred"
STATUS_PROVENANCE_ONLY = "provenance_only_observed"
PARTITION_STATUSES: frozenset[str] = frozenset({
    STATUS_COGNITIVE_PENDING,
    STATUS_CANONICAL_DEBT,
    STATUS_MIXED_DEFERRED,
    STATUS_PROVENANCE_ONLY,
})

# Bounded reason codes valid as a ``reason_code`` filter value.
_FILTERABLE_REASON_CODES: frozenset[str] = frozenset({
    CANONICAL_LEARNING_WORKING_ONLY_REASON,
    HISTORICAL_DEBT_REASON,
    CANONICAL_LEARNING_MIXED_DEFERRED_REASON,
    CANONICAL_LEARNING_PROVENANCE_ONLY_REASON,
})

_FILTERABLE_GRAPH_LAYERS: frozenset[str] = frozenset({
    GRAPH_LAYER_CANONICAL,
    GRAPH_LAYER_WORKING,
})

# blocking vs observational (mixed/provenance are advisory, non-blocking).
_BLOCKING_STATUSES: frozenset[str] = frozenset({
    STATUS_COGNITIVE_PENDING,
    STATUS_CANONICAL_DEBT,
})

_OPERATOR_ACTION = (
    "Wait for source maturity or resolve canonical debt via retry after canonical "
    "evidence exists; an R7 hold/debt is human-only to skip/dismiss."
)

# OR1 — dedicated metric ``kg_canonical_partition_integrity_total``. Bounded
# labels only (closed reason_code / graph_layer / status vocab + board_id); never
# free-text. One sample per observed signal on each drilldown list (the read
# model is the single place that enumerates the partition); clean boards emit no
# blocking samples. Mirrors the project's in-memory emit/get/reset metric shape.
_PARTITION_INTEGRITY_METRIC_LABELS = ("reason_code", "graph_layer", "status", "board_id")
_partition_integrity_samples: list[dict[str, Any]] = []
_partition_integrity_lock = threading.Lock()


def emit_canonical_partition_integrity_sample(
    *, reason_code: str, graph_layer: str, status: str, board_id: str
) -> None:
    """Emit one sample on ``kg_canonical_partition_integrity_total`` (OR1)."""
    with _partition_integrity_lock:
        _partition_integrity_samples.append({
            "reason_code": reason_code,
            "graph_layer": graph_layer,
            "status": status,
            "board_id": board_id,
        })


def get_canonical_partition_integrity_count(
    *,
    reason_code: str | None = None,
    graph_layer: str | None = None,
    status: str | None = None,
    board_id: str | None = None,
) -> int:
    with _partition_integrity_lock:
        return sum(
            1
            for s in _partition_integrity_samples
            if (reason_code is None or s["reason_code"] == reason_code)
            and (graph_layer is None or s["graph_layer"] == graph_layer)
            and (status is None or s["status"] == status)
            and (board_id is None or s["board_id"] == board_id)
        )


def get_canonical_partition_integrity_counter_labels() -> tuple[str, ...]:
    return _PARTITION_INTEGRITY_METRIC_LABELS


def reset_canonical_partition_integrity_counter() -> None:
    with _partition_integrity_lock:
        _partition_integrity_samples.clear()


def _store() -> CognitiveConsolidationItemStore:
    return CognitiveConsolidationItemStore(base_dir=default_rebuild_base_dir())


def _scan_graph_learnings(board_id: str) -> dict[str, dict[str, Any]]:
    """Scan the board graph for canonical Learnings and their validates->Bug
    endpoints (with layers). Returns ``{node_id: {source_ref, canonical_bug_refs,
    working_bug_refs, is_bug_derived}}``. Degrades to ``{}`` on a degraded/missing
    graph so the read model never 500s (the caller maps that to 503)."""
    from okto_pulse.core.kg.schema import open_board_connection

    nodes: dict[str, dict[str, Any]] = {}
    with open_board_connection(board_id) as (_db, kconn):
        # 1. All canonical Learnings (covers provenance-only, which have no
        #    validates->Bug edge at all).
        res = kconn.execute(
            "MATCH (l:Learning) WHERE l.graph_layer = 'canonical' "
            "RETURN l.id, l.source_artifact_ref"
        )
        try:
            while res.has_next():
                row = res.get_next()
                node_id = str(row[0])
                if not node_id:
                    continue
                source_ref = str(row[1] or "")
                nodes[node_id] = {
                    "source_ref": source_ref,
                    "canonical_bug_refs": [],
                    "working_bug_refs": [],
                    "is_bug_derived": _is_bug_derived_ref(source_ref),
                }
        finally:
            try:
                res.close()
            except Exception:
                pass

        # 2. validates -> Bug endpoints with the Bug's layer.
        res = kconn.execute(
            "MATCH (l:Learning)-[r:validates]->(b:Bug) "
            "WHERE l.graph_layer = 'canonical' "
            "RETURN l.id, b.id, b.graph_layer"
        )
        try:
            while res.has_next():
                row = res.get_next()
                node_id = str(row[0])
                bug_id = str(row[1])
                bug_layer = str(row[2] or "")
                entry = nodes.get(node_id)
                if entry is None:
                    continue
                if bug_layer == GRAPH_LAYER_CANONICAL:
                    entry["canonical_bug_refs"].append(bug_id)
                else:
                    entry["working_bug_refs"].append(bug_id)
        finally:
            try:
                res.close()
            except Exception:
                pass
    return nodes


def _graph_signal(node_id: str, entry: dict[str, Any]) -> dict[str, Any] | None:
    """Classify ONE canonical Learning into a graph-derived partition signal
    (mixed_evidence_deferred / provenance_only_observed), or None when it is not
    a partition-integrity signal (e.g. a satisfied canonical-only bug Learning,
    or a working-only one which is surfaced via the store/debt sources instead).
    """
    source_ref = str(entry.get("source_ref") or "")
    canonical_bugs = entry.get("canonical_bug_refs") or []
    working_bugs = entry.get("working_bug_refs") or []
    if not entry.get("is_bug_derived"):
        return _signal(
            node_id=node_id,
            source_ref=source_ref,
            status=STATUS_PROVENANCE_ONLY,
            reason_code=CANONICAL_LEARNING_PROVENANCE_ONLY_REASON,
            canonical_degree=len(canonical_bugs),
            working_endpoint_refs=list(working_bugs),
        )
    if canonical_bugs and working_bugs:
        return _signal(
            node_id=node_id,
            source_ref=source_ref,
            status=STATUS_MIXED_DEFERRED,
            reason_code=CANONICAL_LEARNING_MIXED_DEFERRED_REASON,
            canonical_degree=len(canonical_bugs),
            working_endpoint_refs=list(working_bugs),
        )
    # Working-only (no canonical Bug) is the go-forward hold / historical debt —
    # surfaced authoritatively from the store/debt sources, not re-listed here
    # (avoids double-count). Canonical-only satisfied Learnings are healthy.
    return None


def _signal(
    *,
    node_id: str | None,
    source_ref: str,
    status: str,
    reason_code: str,
    canonical_degree: int = 0,
    working_endpoint_refs: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "node_id": node_id,
        "node_type": "Learning",
        "artifact_id": normalize_cognitive_artifact_id(source_ref),
        "source_artifact_ref": source_ref,
        "reason_code": reason_code,
        "graph_layer": GRAPH_LAYER_CANONICAL,
        "status": status,
        "blocking": status in _BLOCKING_STATUSES,
        "canonical_degree": canonical_degree,
        "working_endpoint_refs": list(working_endpoint_refs or []),
        "operator_action": _OPERATOR_ACTION,
    }


def _gather(db_debt_items: list[dict[str, Any]], *, board_id: str) -> list[dict[str, Any]]:
    """Compose the unified signal list from the 4 sources (already-fetched debt
    items + cognitive store + graph scan)."""
    signals: list[dict[str, Any]] = []

    # Source 1 — cognitive_pending go-forward holds (IMP1).
    store = _store()
    gen = store.latest_generation(board_id)
    if gen:
        active = {"pending", "in_progress", "failed"}
        for it in store.list_items(board_id, gen):
            if (
                it.status in active
                and str(getattr(it, "reason_code", "") or "")
                == CANONICAL_LEARNING_WORKING_ONLY_REASON
            ):
                signals.append(_signal(
                    node_id=None,
                    source_ref=str(it.source_ref or ""),
                    status=STATUS_COGNITIVE_PENDING,
                    reason_code=CANONICAL_LEARNING_WORKING_ONLY_REASON,
                    working_endpoint_refs=[],
                ))

    # Source 2 — historical canonical debt (IMP2).
    for row in db_debt_items:
        if str(row.get("failure_reason") or "") != HISTORICAL_DEBT_REASON:
            continue
        signals.append({
            "node_id": None,
            "node_type": "Learning",
            "artifact_id": normalize_cognitive_artifact_id(str(row.get("source_ref") or "")),
            "source_artifact_ref": str(row.get("source_ref") or ""),
            "reason_code": HISTORICAL_DEBT_REASON,
            "graph_layer": str(row.get("graph_layer") or GRAPH_LAYER_CANONICAL),
            "status": STATUS_CANONICAL_DEBT,
            "blocking": True,
            "canonical_degree": 0,
            "working_endpoint_refs": [],
            "operator_action": _OPERATOR_ACTION,
            "debt_id": row.get("id"),
            "canonical_state": row.get("canonical_state"),
        })

    # Sources 3+4 — graph-derived mixed / provenance-only (the only scan).
    graph_nodes = _scan_graph_learnings(board_id)
    for node_id, entry in graph_nodes.items():
        sig = _graph_signal(node_id, entry)
        if sig is not None:
            signals.append(sig)
    return signals


def _apply_filters(
    signals: list[dict[str, Any]],
    *,
    reason_code: str | None,
    graph_layer: str | None,
    source_ref: str | None,
    node_id: str | None,
    status: str | None,
) -> list[dict[str, Any]]:
    out = signals
    if reason_code:
        out = [s for s in out if s["reason_code"] == reason_code]
    if graph_layer:
        out = [s for s in out if s["graph_layer"] == graph_layer]
    if source_ref:
        out = [s for s in out if s["source_artifact_ref"] == source_ref]
    if node_id:
        out = [s for s in out if s.get("node_id") == node_id]
    if status:
        out = [s for s in out if s["status"] == status]
    return out


def _counts(signals: list[dict[str, Any]]) -> dict[str, int]:
    counts = {s: 0 for s in (
        STATUS_COGNITIVE_PENDING,
        STATUS_CANONICAL_DEBT,
        STATUS_MIXED_DEFERRED,
        STATUS_PROVENANCE_ONLY,
    )}
    for sig in signals:
        counts[sig["status"]] = counts.get(sig["status"], 0) + 1
    return counts


def _validate_filters(
    *, reason_code: str | None, graph_layer: str | None, status: str | None
) -> None:
    if reason_code and reason_code not in _FILTERABLE_REASON_CODES:
        raise CognitiveReadinessError(
            "invalid_filter",
            f"reason_code {reason_code!r} is not a canonical-partition-integrity "
            f"reason. Allowed: {sorted(_FILTERABLE_REASON_CODES)}.",
            http_status=400,
        )
    if graph_layer and graph_layer not in _FILTERABLE_GRAPH_LAYERS:
        raise CognitiveReadinessError(
            "invalid_filter",
            f"graph_layer {graph_layer!r} invalid. Allowed: "
            f"{sorted(_FILTERABLE_GRAPH_LAYERS)}.",
            http_status=400,
        )
    if status and status not in PARTITION_STATUSES:
        raise CognitiveReadinessError(
            "invalid_filter",
            f"status {status!r} invalid. Allowed: {sorted(PARTITION_STATUSES)}.",
            http_status=400,
        )


async def list_canonical_partition_integrity(
    db: AsyncSession,
    *,
    board_id: str,
    reason_code: str | None = None,
    graph_layer: str | None = None,
    source_ref: str | None = None,
    node_id: str | None = None,
    status: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    """Read-only unified list of canonical-partition-integrity signals.

    Errors: 400 ``invalid_filter`` (bad enum filter), 503 ``kg_health_unavailable``
    (board graph unreadable). Pagination is clamped to [1, 200] / >=0.
    """
    _validate_filters(reason_code=reason_code, graph_layer=graph_layer, status=status)
    bounded_limit = max(1, min(int(limit), 200))
    bounded_offset = max(0, int(offset))

    try:
        debt = await list_canonical_debt(db, board_id=board_id, limit=200)
        debt_items = list(debt.items)
    except Exception as exc:  # pragma: no cover - defensive
        raise CognitiveReadinessError(
            "kg_health_unavailable",
            f"canonical debt source unavailable: {type(exc).__name__}",
            http_status=503,
        ) from exc
    try:
        signals = _gather(debt_items, board_id=board_id)
    except CognitiveReadinessError:
        raise
    except Exception as exc:
        raise CognitiveReadinessError(
            "kg_health_unavailable",
            f"board graph unreadable: {type(exc).__name__}",
            http_status=503,
        ) from exc

    # OR1 — emit one bounded sample per observed signal (over the UNFILTERED set,
    # so the metric reflects the true open partition state regardless of filter).
    for sig in signals:
        emit_canonical_partition_integrity_sample(
            reason_code=sig["reason_code"],
            graph_layer=sig["graph_layer"],
            status=sig["status"],
            board_id=board_id,
        )

    counts = _counts(signals)  # counts over the UNFILTERED set (operational total)
    filtered = _apply_filters(
        signals,
        reason_code=reason_code,
        graph_layer=graph_layer,
        source_ref=source_ref,
        node_id=node_id,
        status=status,
    )
    filtered.sort(key=lambda s: (s["status"], s["source_artifact_ref"], str(s.get("node_id") or "")))
    page = filtered[bounded_offset:bounded_offset + bounded_limit]
    return {
        "board_id": board_id,
        "items": page,
        "counts": counts,
        "health_issue_code": HEALTH_ISSUE_CODE,
        "total": len(filtered),
        "limit": bounded_limit,
        "offset": bounded_offset,
    }


async def get_canonical_partition_integrity_detail(
    db: AsyncSession,
    *,
    board_id: str,
    node_id: str,
) -> dict[str, Any]:
    """Read-only detail for ONE canonical Learning node. Never mutates.

    Error: 404 ``canonical_partition_item_not_found``.
    """
    try:
        graph_nodes = _scan_graph_learnings(board_id)
    except Exception as exc:
        raise CognitiveReadinessError(
            "kg_health_unavailable",
            f"board graph unreadable: {type(exc).__name__}",
            http_status=503,
        ) from exc
    entry = graph_nodes.get(node_id)
    if entry is None:
        raise CognitiveReadinessError(
            "canonical_partition_item_not_found",
            f"no canonical Learning node {node_id!r} on board {board_id!r}.",
            http_status=404,
        )
    source_ref = str(entry.get("source_ref") or "")
    canonical_bugs = entry.get("canonical_bug_refs") or []
    working_bugs = entry.get("working_bug_refs") or []
    sig = _graph_signal(node_id, entry)
    status = sig["status"] if sig else (
        STATUS_CANONICAL_DEBT if canonical_bugs else STATUS_COGNITIVE_PENDING
    )

    # Correlate an open canonical debt for this artifact (precedence note).
    artifact_id = normalize_cognitive_artifact_id(source_ref)
    debt_row = None
    try:
        debt = await list_canonical_debt(db, board_id=board_id, limit=200)
        for row in debt.items:
            if (
                str(row.get("failure_reason") or "") == HISTORICAL_DEBT_REASON
                and normalize_cognitive_artifact_id(str(row.get("source_ref") or ""))
                == artifact_id
            ):
                debt_row = {
                    "id": row.get("id"),
                    "state": row.get("canonical_state"),
                    "open": str(row.get("canonical_state") or "") in OPEN_STATES,
                }
                break
    except Exception:  # pragma: no cover - detail debt correlation is best-effort
        debt_row = None

    precedence = (
        "canonical_debt_open outranks canonical_partition_integrity display "
        "duplication" if debt_row and debt_row.get("open")
        else "canonical_partition_integrity drilldown (no open debt precedence)"
    )
    return {
        "node_id": node_id,
        "node_type": "Learning",
        "artifact_id": artifact_id,
        "source_artifact_ref": source_ref,
        "status": status,
        "reason_code": (sig or {}).get("reason_code"),
        "canonical_edges": [
            {"rel_type": "validates", "to": b, "to_graph_layer": GRAPH_LAYER_CANONICAL}
            for b in canonical_bugs
        ],
        "working_edges": [
            {"rel_type": "validates", "to": b, "to_graph_layer": GRAPH_LAYER_WORKING}
            for b in working_bugs
        ],
        "debt": debt_row,
        "pending_item": None,
        "precedence_explanation": precedence,
    }


__all__ = [
    "HEALTH_ISSUE_CODE",
    "PARTITION_STATUSES",
    "STATUS_CANONICAL_DEBT",
    "STATUS_COGNITIVE_PENDING",
    "STATUS_MIXED_DEFERRED",
    "STATUS_PROVENANCE_ONLY",
    "emit_canonical_partition_integrity_sample",
    "get_canonical_partition_integrity_count",
    "get_canonical_partition_integrity_counter_labels",
    "get_canonical_partition_integrity_detail",
    "list_canonical_partition_integrity",
    "reset_canonical_partition_integrity_counter",
]
