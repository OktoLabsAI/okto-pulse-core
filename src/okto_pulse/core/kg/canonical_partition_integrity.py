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

S-KG-02 evolves the model from bug-centric to Learning-centric. Each surfaced
item (and the per-node detail) carries an additive ``classification`` — whether
the canonical Learning is REUSABLE canonical knowledge and, when not, why:
``canonical_learning_resolved`` (bug-derived with a canonical Bug, OR non-bug with
a canonical ``relates_to`` to one of the seven S-KG-01 taxonomy endpoints),
``weak_provenance`` (resolved source, no valid canonical association — the old
provenance_only_observed refined), ``missing_source`` / ``unresolved_source``
(no auditable source) and ``invalid_orphan_learning`` (an association edge OUTSIDE
the taxonomy — fail-closed). The existing ``status`` vocabulary and ``counts`` are
PRESERVED for KG Health / readiness / UI; ``classification`` is orthogonal. The
S-KG-01 taxonomy (``LEARNING_RELATES_TO_TARGETS``) is consumed, never redefined,
and the connectivity guard is never bypassed (no new edge, no mutation).
"""

from __future__ import annotations

import threading
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.kg.canonical_learning_partition import (
    HISTORICAL_DEBT_REASON,
    _is_bug_derived_ref,
)
from okto_pulse.core.kg.cognitive_policy import LEARNING_RELATES_TO_TARGETS
from okto_pulse.core.kg.cognitive_source_ref_resolver import (
    CognitiveRefResolutionStatus,
    resolve_cognitive_source_ref,
)
from okto_pulse.core.kg.connectivity_guard import (
    CANONICAL_LEARNING_MIXED_DEFERRED_REASON,
    CANONICAL_LEARNING_PROVENANCE_ONLY_REASON,
    CANONICAL_LEARNING_WORKING_ONLY_REASON,
)
from okto_pulse.core.kg.cognitive_readiness import CognitiveReadinessError
from okto_pulse.core.kg.rebuild_audit import (
    CognitiveConsolidationItemStore,
    normalize_cognitive_artifact_id,
    require_rebuild_audit_artifact_store,
)
from okto_pulse.core.observability.sample_buffer import BoundedCounterSampleBuffer
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

# ---------------------------------------------------------------------------
# S-KG-02 — Learning-centric canonical classification (read/diagnostic only).
#
# ORTHOGONAL to ``status``. ``status`` keeps the existing problem-signal bucket
# (cognitive_pending / canonical_debt / mixed_evidence_deferred /
# provenance_only_observed) consumed by KG Health / readiness / the UI — it is
# NOT removed. ``classification`` is the additive verdict on whether a canonical
# Learning is REUSABLE canonical knowledge and, when it is not, why: the read
# model evolves from bug-centric (does it have a canonical Bug?) to
# Learning-centric (does it have an auditable resolved source AND a canonical
# association valid under the S-KG-01 taxonomy?).
#
# This module CONSUMES the S-KG-01 taxonomy (``LEARNING_RELATES_TO_TARGETS`` from
# cognitive_policy) — it never redefines the seven endpoints. It is strictly a
# read model: it creates no edge, no mutation, and never bypasses the
# connectivity guard.
CLASSIFICATION_MISSING_SOURCE = "missing_source"
CLASSIFICATION_UNRESOLVED_SOURCE = "unresolved_source"
CLASSIFICATION_CANONICAL_LEARNING_RESOLVED = "canonical_learning_resolved"
CLASSIFICATION_WEAK_PROVENANCE = "weak_provenance"
CLASSIFICATION_INVALID_ORPHAN_LEARNING = "invalid_orphan_learning"
LEARNING_CLASSIFICATIONS: frozenset[str] = frozenset({
    CLASSIFICATION_MISSING_SOURCE,
    CLASSIFICATION_UNRESOLVED_SOURCE,
    CLASSIFICATION_CANONICAL_LEARNING_RESOLVED,
    CLASSIFICATION_WEAK_PROVENANCE,
    CLASSIFICATION_INVALID_ORPHAN_LEARNING,
})

# Resolution statuses that count as an auditable, resolved source (the shared
# RKG-02 resolver is the single authority — we never re-parse refs locally).
_RESOLVED_SOURCE_STATUSES: frozenset[str] = frozenset({
    CognitiveRefResolutionStatus.RESOLVED.value,
    CognitiveRefResolutionStatus.FINAL_REPORT_ALLOWLISTED.value,
})

# Canonical knowledge is non-blocking by definition; the actionable diagnostic
# classifications are the ones that justify operator attention.
_CANONICAL_CLASSIFICATION = CLASSIFICATION_CANONICAL_LEARNING_RESOLVED


def classify_canonical_learning(
    *,
    source_ref: str,
    is_bug_derived: bool,
    canonical_bug_count: int = 0,
    working_bug_count: int = 0,
    relates_to_endpoints: tuple[tuple[str, str | None], ...] = (),
) -> str:
    """Classify ONE canonical Learning into the S-KG-02 vocabulary (pure, no IO).

    Precedence (fail-closed):

    1. empty ``source_ref``                       -> ``missing_source``;
    2. source ref the shared resolver cannot
       resolve to an auditable artifact            -> ``unresolved_source``;
    3. bug-derived with >=1 ``validates`` -> a
       CANONICAL Bug                               -> ``canonical_learning_resolved``
       (preserves the IMP5 publication rule; a working-only Bug NEVER canonizes);
    4. NON-bug-derived with >=1 ``relates_to`` -> a
       CANONICAL endpoint among the seven S-KG-01
       taxonomy types                              -> ``canonical_learning_resolved``;
    5. NON-bug-derived whose only association edge
       points OUTSIDE the taxonomy                 -> ``invalid_orphan_learning``;
    6. otherwise (resolved source, no valid
       canonical association)                      -> ``weak_provenance``.

    ``relates_to_endpoints`` is a tuple of ``(endpoint_node_type, endpoint_layer)``
    — the read model collects them from the board graph; only an endpoint whose
    type is in :data:`LEARNING_RELATES_TO_TARGETS` AND whose layer is canonical
    counts toward completeness (mirrors the connectivity guard's
    ``required_target_layer`` — a working/NULL endpoint is fail-closed).
    """
    ref = (source_ref or "").strip()
    if not ref:
        return CLASSIFICATION_MISSING_SOURCE
    resolution = resolve_cognitive_source_ref(ref)
    if resolution.resolution_status not in _RESOLVED_SOURCE_STATUSES:
        return CLASSIFICATION_UNRESOLVED_SOURCE
    if is_bug_derived:
        # Bug-derived: canonical iff at least one canonical Bug validates edge.
        # Working-only evidence is held/debt elsewhere and never canonizes here.
        if int(canonical_bug_count) >= 1:
            return CLASSIFICATION_CANONICAL_LEARNING_RESOLVED
        return CLASSIFICATION_WEAK_PROVENANCE
    # Non-bug-derived: provenance is a resolved source + a canonical relates_to
    # to one of the seven S-KG-01 taxonomy endpoints (existing edge name reused).
    has_canonical_taxonomy = any(
        endpoint_type in LEARNING_RELATES_TO_TARGETS
        and endpoint_layer == GRAPH_LAYER_CANONICAL
        for endpoint_type, endpoint_layer in relates_to_endpoints
    )
    if has_canonical_taxonomy:
        return CLASSIFICATION_CANONICAL_LEARNING_RESOLVED
    has_off_taxonomy = any(
        endpoint_type not in LEARNING_RELATES_TO_TARGETS
        for endpoint_type, _endpoint_layer in relates_to_endpoints
    )
    if has_off_taxonomy:
        return CLASSIFICATION_INVALID_ORPHAN_LEARNING
    return CLASSIFICATION_WEAK_PROVENANCE

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
_partition_integrity_samples = BoundedCounterSampleBuffer(
    _PARTITION_INTEGRITY_METRIC_LABELS
)
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
        return _partition_integrity_samples.count(
            reason_code=reason_code,
            graph_layer=graph_layer,
            status=status,
            board_id=board_id,
        )


def get_canonical_partition_integrity_counter_labels() -> tuple[str, ...]:
    return _PARTITION_INTEGRITY_METRIC_LABELS


def reset_canonical_partition_integrity_counter() -> None:
    with _partition_integrity_lock:
        _partition_integrity_samples.clear()


def _store() -> CognitiveConsolidationItemStore:
    return CognitiveConsolidationItemStore(
        artifact_store=require_rebuild_audit_artifact_store()
    )


# ---------------------------------------------------------------------------
# R7 IMP5 — canonical-only completeness publication rule (FR8/AC8).
#
# Single source of the ``complete_canonical`` decision for a bug-derived
# Learning, reused by Global Discovery (outbox_worker) so a Learning whose
# mandatory semantic (Bug) evidence is working-only / pending / debt is NOT
# published as complete canonical. ``connected_all`` (the diagnostic "is it
# connected in the working+canonical graph") is NOT the rule — this is.
# ---------------------------------------------------------------------------

# Statuses that, while OPEN, must keep a Learning out of complete-canonical
# publication regardless of its current graph edges (#3 never-masked).
_ACTIVE_PENDING_STATUSES: frozenset[str] = frozenset({"pending", "in_progress", "failed"})


def evaluate_canonical_learning_publication(
    *,
    source_artifact_ref: str,
    canonical_bug_count: int,
    relates_to_endpoints: tuple[tuple[str, str | None], ...] = (),
    overlay_exclusion_reason: str | None = None,
) -> tuple[bool, str | None]:
    """Return ``(publishable_as_complete_canonical, exclusion_reason_code)``.

    Pure policy (no IO). Working evidence NEVER counts toward canonical
    completeness — only canonical Bug endpoints do.

    * an OPEN canonical_debt / active cognitive_pending overlay for this
      artifact => NOT publishable (#3 never mask), reason = the overlay reason;
    * a bug-derived Learning with >=1 canonical Bug evidence => publishable
      (#2 mixed allowed because at least one semantic endpoint is canonical);
    * a bug-derived Learning with 0 canonical Bug evidence (working-only) =>
      NOT publishable (#1), reason = working-only;
    * a NON-bug-derived Learning => publishable ONLY when it is
      ``canonical_learning_resolved`` under the S-KG-02 / S-KG-01 taxonomy: a
      RESOLVED auditable ``source_artifact_ref`` AND a canonical ``relates_to``
      to one of the seven taxonomy endpoints. Otherwise NOT publishable, reason =
      the precise classification (missing_source / unresolved_source /
      weak_provenance / invalid_orphan_learning). This shares the SINGLE
      classifier (:func:`classify_canonical_learning`) so the publication
      authority and the read-model diagnostic can never diverge (TR60 /
      BR-KG02-02): the legacy ``(True, None)`` shortcut for any non-bug Learning
      is removed (it let an un-sourced/un-associated non-bug Learning publish as
      canonical on the digest/parity path).

    ``relates_to_endpoints`` is the non-bug taxonomy evidence — a tuple of
    ``(endpoint_node_type, endpoint_layer)`` collected by the caller from the
    board graph; an empty tuple (no canonical association supplied) is fail-closed
    for a non-bug Learning. It is ignored for the bug-derived path.
    """
    if overlay_exclusion_reason:
        return (False, overlay_exclusion_reason)
    if _is_bug_derived_ref(str(source_artifact_ref or "")):
        if int(canonical_bug_count) >= 1:
            return (True, None)
        return (False, CANONICAL_LEARNING_WORKING_ONLY_REASON)
    classification = classify_canonical_learning(
        source_ref=str(source_artifact_ref or ""),
        is_bug_derived=False,
        relates_to_endpoints=relates_to_endpoints,
    )
    if classification == CLASSIFICATION_CANONICAL_LEARNING_RESOLVED:
        return (True, None)
    return (False, classification)


async def pending_or_debt_exclusions(
    db: AsyncSession, *, board_id: str
) -> dict[str, str]:
    """Map ``normalized artifact_id -> exclusion reason_code`` for the board's
    OPEN canonical debt (IMP2) and active cognitive holds (IMP1).

    Debt OUTRANKS a pending hold for the same artifact. Both source_refs are
    collapsed via :func:`normalize_cognitive_artifact_id` so the caller can match
    a graph Learning's ``source_artifact_ref`` against debt/cognitive refs in the
    SAME normalized space (handles ``card:``/``bug:`` aliasing). Best-effort: a
    degraded debt/store source returns the partial map it could read — the graph
    completeness check still excludes genuine working-only Learnings, so a missing
    overlay never *masks* an incomplete fact, it only narrows the #3 belt.
    """
    out: dict[str, str] = {}

    # canonical_debt (open) — outranks.
    try:
        debt = await list_canonical_debt(db, board_id=board_id, limit=200)
        for row in debt.items:
            if str(row.get("failure_reason") or "") != HISTORICAL_DEBT_REASON:
                continue
            if str(row.get("canonical_state") or "") not in OPEN_STATES:
                continue
            aid = normalize_cognitive_artifact_id(str(row.get("source_ref") or ""))
            if aid:
                out[aid] = HISTORICAL_DEBT_REASON
    except Exception:  # pragma: no cover - defensive; overlay is best-effort
        pass

    # cognitive_pending holds (working-only reason, still active).
    try:
        store = _store()
        gen = store.latest_generation(board_id)
        if gen:
            for it in store.list_items(board_id, gen):
                if (
                    it.status in _ACTIVE_PENDING_STATUSES
                    and str(getattr(it, "reason_code", "") or "")
                    == CANONICAL_LEARNING_WORKING_ONLY_REASON
                ):
                    aid = normalize_cognitive_artifact_id(str(it.source_ref or ""))
                    if aid and aid not in out:  # debt outranks
                        out[aid] = CANONICAL_LEARNING_WORKING_ONLY_REASON
    except Exception:  # pragma: no cover - defensive; overlay is best-effort
        pass

    return out


def _scan_graph_learnings(board_id: str) -> dict[str, dict[str, Any]]:
    """Scan the board graph for canonical Learnings and their canonical
    associations. Returns ``{node_id: {source_ref, canonical_bug_refs,
    working_bug_refs, is_bug_derived, relates_to_endpoints}}`` where
    ``relates_to_endpoints`` is a list of ``(endpoint_node_type, endpoint_layer)``
    (S-KG-02). Degrades to ``{}`` on a degraded/missing graph so the read model
    never 500s (the caller maps that to 503)."""
    from okto_pulse.core.kg.interfaces import get_kg_registry

    nodes: dict[str, dict[str, Any]] = {}
    cypher = get_kg_registry().cypher_executor

    # 1. All canonical Learnings (covers provenance-only, which have no
    #    validates->Bug edge at all).
    result = cypher.execute_read_only(
        board_id,
        "MATCH (l:Learning) WHERE l.graph_layer = 'canonical' "
        "RETURN l.id, l.source_artifact_ref",
        max_rows=10000,
    )
    for row in result.get("rows", []):
        node_id = str(row[0])
        if not node_id:
            continue
        source_ref = str(row[1] or "")
        nodes[node_id] = {
            "source_ref": source_ref,
            "canonical_bug_refs": [],
            "working_bug_refs": [],
            "is_bug_derived": _is_bug_derived_ref(source_ref),
            "relates_to_endpoints": [],
        }

    # 2. validates -> Bug endpoints with the Bug's layer.
    result = cypher.execute_read_only(
        board_id,
        "MATCH (l:Learning)-[r:validates]->(b:Bug) "
        "WHERE l.graph_layer = 'canonical' "
        "RETURN l.id, b.id, b.graph_layer",
        max_rows=10000,
    )
    for row in result.get("rows", []):
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

    # 3. relates_to -> taxonomy endpoints (S-KG-02): the cognitive provenance
    #    relation for a NON-bug-derived Learning. The endpoint label + layer
    #    let the classifier decide if it is a CANONICAL S-KG-01 taxonomy
    #    association (untyped target + label() so an off-taxonomy endpoint, if
    #    one ever materializes, is observed rather than silently dropped).
    result = cypher.execute_read_only(
        board_id,
        "MATCH (l:Learning)-[r:relates_to]->(t) "
        "WHERE l.graph_layer = 'canonical' "
        "RETURN l.id, label(t), t.graph_layer",
        max_rows=10000,
    )
    for row in result.get("rows", []):
        node_id = str(row[0])
        endpoint_type = str(row[1] or "")
        endpoint_layer = str(row[2] or "") or None
        entry = nodes.get(node_id)
        if entry is None:
            continue
        entry["relates_to_endpoints"].append((endpoint_type, endpoint_layer))
    return nodes


def _entry_classification(entry: dict[str, Any]) -> str:
    """S-KG-02 Learning-centric classification for a scanned graph ``entry``."""
    return classify_canonical_learning(
        source_ref=str(entry.get("source_ref") or ""),
        is_bug_derived=bool(entry.get("is_bug_derived")),
        canonical_bug_count=len(entry.get("canonical_bug_refs") or []),
        working_bug_count=len(entry.get("working_bug_refs") or []),
        relates_to_endpoints=tuple(entry.get("relates_to_endpoints") or []),
    )


def _graph_signal(node_id: str, entry: dict[str, Any]) -> dict[str, Any] | None:
    """Classify ONE canonical Learning into a graph-derived partition signal
    (mixed_evidence_deferred / provenance_only_observed), or None when it is not
    a partition-integrity signal (a Learning that S-KG-02 classifies as
    ``canonical_learning_resolved`` — a satisfied canonical-only bug Learning OR a
    non-bug Learning with a valid canonical taxonomy relates_to — or a working-only
    one which is surfaced via the store/debt sources instead).
    """
    source_ref = str(entry.get("source_ref") or "")
    canonical_bugs = entry.get("canonical_bug_refs") or []
    working_bugs = entry.get("working_bug_refs") or []
    classification = _entry_classification(entry)
    if not entry.get("is_bug_derived"):
        # S-KG-02 refinement: a non-bug Learning is only a provenance signal when
        # it is NOT canonical knowledge. A valid canonical taxonomy relates_to
        # (canonical_learning_resolved) is healthy and drops out of the problem
        # list — the prior model flagged EVERY non-bug Learning as provenance-only
        # because it never inspected the relates_to taxonomy. missing_source /
        # unresolved_source / invalid_orphan_learning / weak_provenance keep the
        # provenance_only_observed status (compat) with the precise classification.
        if classification == CLASSIFICATION_CANONICAL_LEARNING_RESOLVED:
            return None
        return _signal(
            node_id=node_id,
            source_ref=source_ref,
            status=STATUS_PROVENANCE_ONLY,
            reason_code=CANONICAL_LEARNING_PROVENANCE_ONLY_REASON,
            classification=classification,
            canonical_degree=len(canonical_bugs),
            working_endpoint_refs=list(working_bugs),
        )
    if canonical_bugs and working_bugs:
        # Mixed evidence is canonical knowledge (>=1 canonical Bug) yet still an
        # advisory signal: the working edges are deferred, never counted.
        return _signal(
            node_id=node_id,
            source_ref=source_ref,
            status=STATUS_MIXED_DEFERRED,
            reason_code=CANONICAL_LEARNING_MIXED_DEFERRED_REASON,
            classification=classification,
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
    classification: str,
    canonical_degree: int = 0,
    working_endpoint_refs: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "node_id": node_id,
        "node_type": "Learning",
        "artifact_id": normalize_cognitive_artifact_id(source_ref),
        "source_artifact_ref": source_ref,
        "reason_code": reason_code,
        # S-KG-02 Learning-centric verdict (additive; orthogonal to ``status``).
        "classification": classification,
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
                    # A held bug-derived Learning has working-only Bug evidence —
                    # not canonical knowledge yet (S-KG-02 weak_provenance).
                    classification=CLASSIFICATION_WEAK_PROVENANCE,
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
            # Historical bug-derived Learning lacking a canonical Bug edge — still
            # not canonical knowledge (S-KG-02 weak_provenance).
            "classification": CLASSIFICATION_WEAK_PROVENANCE,
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


def _classification_counts(signals: list[dict[str, Any]]) -> dict[str, int]:
    """S-KG-02 Learning-centric census over the surfaced signals (additive).

    Stable shape (all classifications present, zero when absent). Healthy
    ``canonical_learning_resolved`` non-bug Learnings are not surfaced as signals,
    so this counts the classifications of the partition-integrity items only —
    e.g. a mixed-evidence item is canonical knowledge yet still listed.
    """
    counts = {c: 0 for c in sorted(LEARNING_CLASSIFICATIONS)}
    for sig in signals:
        cls = str(sig.get("classification") or "")
        if cls:
            counts[cls] = counts.get(cls, 0) + 1
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
        # S-KG-02 Learning-centric census (additive; the existing ``counts`` and
        # status vocabulary are preserved for health/readiness/UI compat).
        "classification_counts": _classification_counts(signals),
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
    relates_to_endpoints = entry.get("relates_to_endpoints") or []
    classification = _entry_classification(entry)
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
        # S-KG-02 Learning-centric verdict (authoritative for canonical
        # reusability; ``status`` is the compat problem-signal bucket).
        "classification": classification,
        "reason_code": (sig or {}).get("reason_code"),
        "canonical_edges": [
            {"rel_type": "validates", "to": b, "to_graph_layer": GRAPH_LAYER_CANONICAL}
            for b in canonical_bugs
        ],
        "working_edges": [
            {"rel_type": "validates", "to": b, "to_graph_layer": GRAPH_LAYER_WORKING}
            for b in working_bugs
        ],
        # S-KG-02 relates_to taxonomy associations (the non-bug cognitive
        # provenance path). ``canonical_taxonomy_endpoint`` is the only one that
        # canonizes (type in the seven S-KG-01 endpoints AND canonical layer).
        "relates_to_edges": [
            {
                "rel_type": "relates_to",
                "to_node_type": endpoint_type,
                "to_graph_layer": endpoint_layer,
                "in_taxonomy": endpoint_type in LEARNING_RELATES_TO_TARGETS,
                "canonical_taxonomy_endpoint": (
                    endpoint_type in LEARNING_RELATES_TO_TARGETS
                    and endpoint_layer == GRAPH_LAYER_CANONICAL
                ),
            }
            for endpoint_type, endpoint_layer in relates_to_endpoints
        ],
        "debt": debt_row,
        "pending_item": None,
        "precedence_explanation": precedence,
    }


__all__ = [
    "CLASSIFICATION_CANONICAL_LEARNING_RESOLVED",
    "CLASSIFICATION_INVALID_ORPHAN_LEARNING",
    "CLASSIFICATION_MISSING_SOURCE",
    "CLASSIFICATION_UNRESOLVED_SOURCE",
    "CLASSIFICATION_WEAK_PROVENANCE",
    "HEALTH_ISSUE_CODE",
    "LEARNING_CLASSIFICATIONS",
    "PARTITION_STATUSES",
    "STATUS_CANONICAL_DEBT",
    "STATUS_COGNITIVE_PENDING",
    "STATUS_MIXED_DEFERRED",
    "STATUS_PROVENANCE_ONLY",
    "classify_canonical_learning",
    "emit_canonical_partition_integrity_sample",
    "evaluate_canonical_learning_publication",
    "get_canonical_partition_integrity_count",
    "get_canonical_partition_integrity_counter_labels",
    "get_canonical_partition_integrity_detail",
    "list_canonical_partition_integrity",
    "pending_or_debt_exclusions",
    "reset_canonical_partition_integrity_counter",
]
