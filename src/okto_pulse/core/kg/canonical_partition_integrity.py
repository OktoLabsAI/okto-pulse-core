"""Canonical Learning publication policy and internal debt/hold overlays.

F4 retires the public per-node inspector, its scans and exclusive metrics.
The classifier and automatic publication authorities remain unchanged.
"""

from __future__ import annotations

import json


from okto_pulse.core.kg.canonical_learning_partition import (
    CANONICAL_LEARNING_DEBT_REASONS,
    _is_bug_derived_ref,
)

from okto_pulse.core.kg.cognitive_policy import LEARNING_RELATES_TO_TARGETS

from okto_pulse.core.kg.cognitive_source_ref_resolver import (
    CognitiveRefResolutionStatus,
    resolve_cognitive_source_ref,
)

from okto_pulse.core.kg.connectivity_guard import (
    CANONICAL_LEARNING_WORKING_ONLY_REASON,
)


from okto_pulse.core.kg.rebuild_audit import (
    CognitiveConsolidationItemStore,
    normalize_cognitive_artifact_id,
    require_rebuild_audit_artifact_store,
)



from okto_pulse.core.kg.source_maturity import (
    GRAPH_LAYER_CANONICAL,
)

from okto_pulse.core.services.canonical_debt_service import (
    OPEN_STATES,
    list_canonical_debt,
)

CLASSIFICATION_MISSING_SOURCE = "missing_source"

CLASSIFICATION_UNRESOLVED_SOURCE = "unresolved_source"

CLASSIFICATION_CANONICAL_LEARNING_RESOLVED = "canonical_learning_resolved"

CLASSIFICATION_WEAK_PROVENANCE = "weak_provenance"

CLASSIFICATION_INVALID_ORPHAN_LEARNING = "invalid_orphan_learning"

_RESOLVED_SOURCE_STATUSES: frozenset[str] = frozenset({
    CognitiveRefResolutionStatus.RESOLVED.value,
    CognitiveRefResolutionStatus.FINAL_REPORT_ALLOWLISTED.value,
})

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

def _store() -> CognitiveConsolidationItemStore:
    return CognitiveConsolidationItemStore(
        artifact_store=require_rebuild_audit_artifact_store()
    )

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

async def canonical_debt_exclusions(
    db: object, *, board_id: str
) -> dict[str, str]:
    """Map the board's open relational canonical debt by artifact id."""

    out: dict[str, str] = {}
    try:
        debt = await list_canonical_debt(db, board_id=board_id, limit=200)
        for row in debt.items:
            reason = str(row.get("failure_reason") or "")
            if reason not in CANONICAL_LEARNING_DEBT_REASONS:
                continue
            if str(row.get("canonical_state") or "") not in OPEN_STATES:
                continue
            aid = normalize_cognitive_artifact_id(str(row.get("source_ref") or ""))
            if aid:
                out[aid] = reason
    except Exception:  # pragma: no cover - compatibility read remains best-effort
        pass
    return out

async def capture_canonical_debt_exclusions(db: object, *, board_id: str) -> dict[str, str]:
    """Complete recovery capture inside the caller's stable relational UoW.

    The compatibility read above is best-effort. A recovery certificate cannot
    interpret pagination, unavailable storage or a malformed page as absence
    of debt. Preserve the existing reason/state policy and page ordering.
    """
    if type(board_id) is not str or not board_id:
        raise ValueError('canonical_debt_capture_scope_invalid')
    out, seen, total, offset, budget = {}, set(), None, 0, 0
    while True:
        page = await list_canonical_debt(db, board_id=board_id, limit=200, offset=offset)
        if (type(page.total) is not int or not 0 <= page.total <= 100_000
                or type(page.items) is not list
                or len(page.items) != min(200, max(0, page.total - offset))
                or (total is not None and page.total != total)):
            raise ValueError('canonical_debt_capture_inventory_invalid')
        total = page.total
        for row in page.items:
            if (type(row) is not dict or type(row.get('id')) is not str or not row['id']
                    or row['id'] in seen or row.get('board_id') != board_id):
                raise ValueError('canonical_debt_capture_identity_invalid')
            seen.add(row['id'])
            budget += len(json.dumps(row, ensure_ascii=False, allow_nan=False).encode('utf-8'))
            if budget > 64 * 1024 * 1024:
                raise ValueError('canonical_debt_capture_inventory_limit')
            if (row.get('failure_reason') in CANONICAL_LEARNING_DEBT_REASONS
                    and row.get('canonical_state') in OPEN_STATES):
                source = row.get('source_ref')
                if type(source) is not str or not source:
                    raise ValueError('canonical_debt_capture_source_invalid')
                artifact = normalize_cognitive_artifact_id(source)
                if not artifact:
                    raise ValueError('canonical_debt_capture_source_invalid')
                out[artifact] = row['failure_reason']
        offset += len(page.items)
        if offset == total:
            return out

async def pending_or_debt_exclusions(
    db: object, *, board_id: str
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
    out = await canonical_debt_exclusions(db, board_id=board_id)

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
