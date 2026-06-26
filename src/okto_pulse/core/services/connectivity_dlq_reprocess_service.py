"""RKG-04 — safe diagnosis + reprocess of the connectivity-guard technical_dlq class.

The connectivity-guard DLQ class is the set of ConsolidationDeadLetter rows whose
terminal error is ``KG node connectivity guard rejected the commit before graph
mutation``. RKG-02 (existing-endpoint source_artifact_ref loading + bug-derived
ref resolution) is the ROOT-CAUSE fix; with RKG-02/RKG-03 applied, reprocessing a
member of this class re-runs consolidation through the connectivity guard, which
now passes, and the artifact materialises into graph.lbug.

This service NEVER mutates a DLQ unless its preconditions hold (RKG-02/RKG-03
applied, KG not quarantined, the selected DLQs still exist) — it fails closed
without removing any DLQ (FR2/BR2/AC1). Reprocessing reuses the existing
``reprocess_dead_letter_rows`` (idempotent, ConsolidationQueue-dedup; TR1). A
member that keeps failing returns to the DLQ as an actionable ``technical_dlq``
with ``last_error``/``next_action`` — partial success is never masked (TR3/AC3).
"""

from __future__ import annotations

from typing import Any, Awaitable, Callable, Iterable

from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.services.dead_letter_inspector_service import (
    list_dead_letter_rows,
    reprocess_dead_letter_rows,
)

# The terminal-error signature that defines the class (matches the live rows +
# the recursive RKG-05 finding b4dbc91f).
CONNECTIVITY_GUARD_SIGNATURE = (
    "KG node connectivity guard rejected the commit before graph mutation"
)

_PROBABLE_ROOT_CAUSE = (
    "consolidation commit rejected by the KG node connectivity guard: an existing "
    "endpoint's source_artifact_ref was not loaded / a bug-derived ref was not "
    "resolved to its canonical Bug (RKG-02 root cause)"
)
_REMEDIATION = (
    "confirm RKG-02/RKG-03 are applied and KG is not quarantined, then reprocess "
    "via reprocess_connectivity_guard_dlq; a member that keeps failing stays a "
    "technical_dlq for manual inspection"
)


def _is_connectivity_guard_row(row: dict[str, Any]) -> bool:
    blob = " ".join(
        str(row.get(k) or "") for k in ("last_error", "error_text")
    )
    for err in row.get("errors") or []:
        if isinstance(err, dict):
            blob += " " + str(err.get("message") or "")
    return CONNECTIVITY_GUARD_SIGNATURE in blob


async def diagnose_connectivity_guard_dlq(
    db: AsyncSession, board_id: str, *, limit: int = 200
) -> dict[str, Any]:
    """FR1/BR1: diagnose the LIVE connectivity-guard DLQ class at execution time.

    Returns the members with dead_letter_id, artifact_id, attempts, errors,
    last_error, the source_artifact_ref involved, the probable root cause and the
    next_action — so the class is actionable BEFORE any reprocessing."""
    listing = await list_dead_letter_rows(db, board_id, limit=limit)
    items: list[dict[str, Any]] = []
    for row in listing.get("rows", []):
        if not _is_connectivity_guard_row(row):
            continue
        items.append({
            "dead_letter_id": row.get("dead_letter_id") or row.get("id"),
            "artifact_type": row.get("artifact_type"),
            "artifact_id": row.get("artifact_id"),
            "attempts": row.get("attempts"),
            "errors": row.get("errors"),
            "last_error": row.get("last_error"),
            "error_text": row.get("error_text"),
            "source_artifact_ref": f"{row.get('artifact_type')}:{row.get('artifact_id')}",
            "probable_root_cause": _PROBABLE_ROOT_CAUSE,
            "next_action": row.get("next_action"),
            "remediation": _REMEDIATION,
        })
    return {
        "board_id": board_id,
        "dlq_class": "connectivity_guard",
        "count": len(items),
        "items": items,
        "dead_letter_ids": [i["dead_letter_id"] for i in items],
    }


def rkg_fixes_applied() -> bool:
    """True when the RKG-02 + RKG-03 root-cause fixes are present in this build.

    RKG-02: the shared source_ref resolver + the primitives endpoint source_ref
    loading / canonical-bug probe. RKG-03: the cognitive closeout production path
    + its dedicated worker. If any is reverted, the import fails and reprocessing
    is blocked (BR2) — we never reprocess into the same root cause."""
    try:
        from okto_pulse.core.kg.cognitive_source_ref_resolver import (  # noqa: F401
            resolve_cognitive_source_ref,
        )
        from okto_pulse.core.kg.primitives import (  # noqa: F401
            _graph_canonical_bug_probe,
            _lookup_node_source_ref_by_id,
        )
        from okto_pulse.core.kg.cognitive_closeout_production import (  # noqa: F401
            run_cognitive_closeout,
        )
        from okto_pulse.core.kg.workers.cognitive_closeout import (  # noqa: F401
            CognitiveCloseoutWorker,
        )
        return True
    except Exception:
        return False


async def _default_quarantine_probe(board_id: str, db: AsyncSession) -> bool:
    """True when the board's KG is quarantined (canonical overall_state)."""
    from okto_pulse.core.services.kg_health_service import get_kg_health

    health = await get_kg_health(board_id, db)
    state = health.get("overall_state") or health.get("state")
    return state == "quarantined"


async def check_reprocess_preconditions(
    db: AsyncSession,
    board_id: str,
    dead_letter_ids: Iterable[str],
    *,
    fixes_applied_probe: Callable[[], bool] = rkg_fixes_applied,
    quarantine_probe: Callable[[str, AsyncSession], Awaitable[bool]] | None = None,
) -> dict[str, Any]:
    """FR1/FR2/BR2/TR1/AC1: reprocessing is allowed ONLY when RKG-02/RKG-03 are
    applied, the KG is not quarantined, and EVERY selected DLQ both still exists
    AND belongs to the diagnosed connectivity-guard class. A selection that is
    empty, missing, or out-of-class is blocked — the tool never reprocesses an
    unanalysed or out-of-scope DLQ (TR1)."""
    ids = [str(i) for i in dead_letter_ids if str(i).strip()]
    reasons: list[str] = []

    # TR1: an explicit, analysed selection is required — never a broad reprocess.
    if not ids:
        reasons.append("no_dlq_selected")

    if not fixes_applied_probe():
        reasons.append("rkg02_rkg03_not_applied")

    probe = quarantine_probe or _default_quarantine_probe
    quarantined = await probe(board_id, db)
    if quarantined:
        reasons.append("kg_quarantined")

    # One listing → derive both the full existing set AND the diagnosed
    # connectivity-guard class set (codex: each selected id must PROVE class
    # membership, not merely exist).
    rows = (await list_dead_letter_rows(db, board_id, limit=200)).get("rows", [])
    existing = {r.get("dead_letter_id") or r.get("id") for r in rows}
    class_ids = {
        (r.get("dead_letter_id") or r.get("id"))
        for r in rows if _is_connectivity_guard_row(r)
    }
    missing = [i for i in ids if i not in existing]
    out_of_class = [i for i in ids if i in existing and i not in class_ids]
    if missing:
        reasons.append("selected_dlq_missing")
    if out_of_class:
        reasons.append("selected_dlq_out_of_class")

    return {
        "allowed": not reasons,
        "reasons": reasons,
        "missing_dlq_ids": missing,
        "out_of_class_dlq_ids": out_of_class,
        "rkg02_rkg03_applied": "rkg02_rkg03_not_applied" not in reasons,
        "kg_quarantined": quarantined,
    }


async def reprocess_connectivity_guard_dlq(
    db: AsyncSession,
    board_id: str,
    dead_letter_ids: Iterable[str],
    *,
    fixes_applied_probe: Callable[[], bool] = rkg_fixes_applied,
    quarantine_probe: Callable[[str, AsyncSession], Awaitable[bool]] | None = None,
) -> dict[str, Any]:
    """FR2/BR2/TR1: precondition-gated reprocess. Fails CLOSED — when any
    precondition fails, NO DLQ is removed and the block reason is returned."""
    ids = [str(i) for i in dead_letter_ids if str(i).strip()]
    pre = await check_reprocess_preconditions(
        db, board_id, ids,
        fixes_applied_probe=fixes_applied_probe, quarantine_probe=quarantine_probe,
    )
    if not pre["allowed"]:
        return {
            "success": False,
            "blocked": True,
            "removed_dlq": False,
            "reasons": pre["reasons"],
            "preconditions": pre,
        }

    # TR1: reuse the existing idempotent reprocess (ConsolidationQueue dedup).
    result = await reprocess_dead_letter_rows(
        db, board_id, dead_letter_ids=ids, limit=200)
    result["blocked"] = False
    result["preconditions"] = pre
    return result


async def verify_connectivity_class_cleared(
    db: AsyncSession,
    board_id: str,
    *,
    artifact_refs: Iterable[str] | None = None,
) -> dict[str, Any]:
    """FR3/BR3/TR2/TR3: after the consolidation worker drains the queue, the
    connectivity-guard class must be clear for the reprocessed artifacts. A member
    that returned to the DLQ stays VISIBLE (actionable) — never masked."""
    diag = await diagnose_connectivity_guard_dlq(db, board_id)
    scope = set(artifact_refs) if artifact_refs else None
    remaining = [
        item for item in diag["items"]
        if scope is None or item["source_artifact_ref"] in scope
    ]
    return {
        "class_cleared": not remaining,
        "remaining_count": len(remaining),
        "remaining_dlq": remaining,
    }
