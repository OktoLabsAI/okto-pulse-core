"""R7 IMP2 — historical canonical Learning partition integrity (CanonicalDebt).

IMP1 guards the GO-FORWARD commit path: a bug-derived canonical Learning that
only has working-Bug evidence is held as cognitive pending before mutation.

IMP2 handles the HISTORICAL case: canonical Learnings ALREADY materialized in
the graph (committed before the layer-aware guard existed) that are bug-derived
but lack a ``validates -> canonical Bug`` edge. Those are remediation debt and
are recorded in the existing ``CanonicalDebt`` ledger — NEVER cognitive pending
(that is go-forward, IMP1) and NEVER DLQ.

The debt is closed deterministically only when the SAME Learning later gains a
canonical Bug validates edge: a post-commit reconcile pass builds canonical-only
evidence and feeds it to ``reconcile_canonical_debt_with_evidence`` (which is
layer-blind by contract — so every caller MUST pre-filter to the canonical
layer first). Working-layer evidence can therefore never close the debt.

No new store, no new worker: detection + reconcile reuse ``canonical_debt_service``
and run inside the existing consolidation worker post-commit hook.
"""

from __future__ import annotations

import hashlib
import logging

from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.services.canonical_debt_service import (
    reconcile_canonical_debt_with_evidence,
    upsert_canonical_debt,
)

logger = logging.getLogger("okto_pulse.kg.canonical_learning_partition")

# Reason code for an ALREADY-materialized canonical Learning whose bug evidence
# is working-only (distinct from IMP1's go-forward pre-commit hold reason).
HISTORICAL_DEBT_REASON = "canonical_learning_historical_working_only_bug_evidence_debt"

# Stable target_status for the partition-integrity debt class (part of the
# CanonicalDebt idempotency key together with board/artifact/content_hash).
PARTITION_TARGET_STATUS = "canonical_learning_partition_integrity"

# evidence_layer marker the reconcile pre-filter requires.
EVIDENCE_LAYER_CANONICAL = "canonical"

_BUG_REF_PREFIXES = ("bug:", "card:bug:")


def _is_bug_derived_ref(source_ref: str) -> bool:
    """True iff the Learning's source_artifact_ref denotes a known bug source.

    Mirrors the IMP1 guard's bug-derived signal for graph nodes (where only the
    source_ref is available). Provenance-only Learnings never match, so they are
    excluded from the partition-integrity check (no false positives)."""
    return source_ref.startswith(_BUG_REF_PREFIXES) or ":bug:" in source_ref


def _bug_artifact_id(source_ref: str) -> str:
    """Extract a stable, <=36-char artifact_id (the source bug uuid) from a
    bug-derived Learning source_ref. Falls back to a truncated ref."""
    parts = source_ref.split(":")
    if source_ref.startswith("card:bug:") and len(parts) >= 3:
        return parts[2][:36]
    if source_ref.startswith("bug:") and len(parts) >= 2:
        return parts[1][:36]
    if ":bug:" in source_ref:
        idx = parts.index("bug") if "bug" in parts else -1
        if idx != -1 and idx + 1 < len(parts):
            return parts[idx + 1][:36]
    return source_ref[:36]


def _stable_content_hash(source_ref: str, node_id: str) -> str:
    """Deterministic content_hash for the debt + its closing evidence.

    Keyed by (source_ref, node_id) so the SAME Learning maps to the SAME debt
    row across re-detection, and so the reconcile pass emits evidence that
    matches the debt opened earlier. KG nodes carry no content_hash column, so
    this is the stable identity used for idempotent upsert + reconcile."""
    digest = hashlib.sha256(f"{source_ref}|{node_id}".encode("utf-8")).hexdigest()
    return f"clp_{digest}"


def _scan_partition(kconn) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    """Scan the board graph for canonical bug-derived Learnings.

    Returns ``(violating, satisfied)`` where each item is ``(node_id, source_ref)``:
    - violating: canonical bug-derived Learning WITHOUT a validates -> canonical
      Bug edge (NULL/working Bug is fail-closed, never counts).
    - satisfied: canonical bug-derived Learning WITH at least one validates ->
      canonical Bug edge.
    Uses two simple MATCH queries (no EXISTS-subquery) for LadybugDB safety.
    """
    all_learnings: list[tuple[str, str]] = []
    try:
        res = kconn.execute(
            "MATCH (l:Learning) WHERE l.graph_layer = 'canonical' "
            "RETURN l.id, l.source_artifact_ref"
        )
        try:
            while res.has_next():
                row = res.get_next()
                node_id = str(row[0])
                source_ref = str(row[1] or "")
                if node_id and _is_bug_derived_ref(source_ref):
                    all_learnings.append((node_id, source_ref))
        finally:
            try:
                res.close()
            except Exception:
                pass
    except Exception as exc:  # pragma: no cover - defensive (degraded graph)
        logger.warning("kg.clp.scan_learnings_failed err=%s", exc)
        return [], []

    satisfied_ids: set[str] = set()
    try:
        res = kconn.execute(
            "MATCH (l:Learning)-[r:validates]->(b:Bug) "
            "WHERE l.graph_layer = 'canonical' AND b.graph_layer = 'canonical' "
            "RETURN l.id"
        )
        try:
            while res.has_next():
                satisfied_ids.add(str(res.get_next()[0]))
        finally:
            try:
                res.close()
            except Exception:
                pass
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("kg.clp.scan_satisfied_failed err=%s", exc)

    violating = [(nid, ref) for nid, ref in all_learnings if nid not in satisfied_ids]
    satisfied = [(nid, ref) for nid, ref in all_learnings if nid in satisfied_ids]
    return violating, satisfied


def _canonical_only_evidence(
    evidence_rows: list[dict[str, str]],
) -> list[dict[str, str]]:
    """Pre-filter: keep ONLY evidence explicitly stamped canonical layer.

    ``reconcile_canonical_debt_with_evidence`` is layer-blind by contract, so
    this guard is what makes working-layer evidence incapable of closing
    canonical debt. Rows missing/!= canonical evidence_layer are dropped."""
    return [
        row
        for row in evidence_rows
        if str(row.get("evidence_layer") or "") == EVIDENCE_LAYER_CANONICAL
    ]


def _canonical_evidence_for(node_id: str, source_ref: str) -> dict[str, str]:
    return {
        "source_ref": source_ref,
        "content_hash": _stable_content_hash(source_ref, node_id),
        "evidence_layer": EVIDENCE_LAYER_CANONICAL,
        "graph_layer": EVIDENCE_LAYER_CANONICAL,
        "node_ref": node_id,
    }


async def detect_historical_canonical_learning_debt(
    db: AsyncSession,
    *,
    board_id: str,
    actor_id: str,
) -> dict:
    """Record CanonicalDebt for canonical bug-derived Learnings already
    materialized without canonical Bug evidence.

    Idempotent: upsert keyed by (board, artifact, target_status, content_hash).
    Storage is CanonicalDebt only — never cognitive pending / DLQ. Returns
    ``{opened, source_refs}``."""
    from okto_pulse.core.kg.schema import open_board_connection

    with open_board_connection(board_id) as (_kdb, kconn):
        violating, _satisfied = _scan_partition(kconn)

    opened: list[str] = []
    for node_id, source_ref in violating:
        await upsert_canonical_debt(
            db,
            board_id=board_id,
            artifact_type="bug",
            artifact_id=_bug_artifact_id(source_ref),
            source_ref=source_ref,
            content_hash=_stable_content_hash(source_ref, node_id),
            target_status=PARTITION_TARGET_STATUS,
            canonical_state="pending",
            graph_layer="canonical",
            maturity_status="canonical_eligible",
            failure_reason=HISTORICAL_DEBT_REASON,
            owner_agent_id=actor_id,
        )
        opened.append(source_ref)
    if opened:
        logger.info(
            "kg.clp.historical_debt_recorded board=%s count=%d",
            board_id,
            len(opened),
        )
    return {"opened": len(opened), "source_refs": opened}


async def reconcile_canonical_learning_partition_debt(
    db: AsyncSession,
    *,
    board_id: str,
    actor_id: str,
    extra_evidence: list[dict[str, str]] | None = None,
) -> dict:
    """Close partition-integrity debt for Learnings that now have a canonical
    Bug validates edge.

    Builds canonical-only evidence from the SATISFIED partition, pre-filters to
    the canonical layer (so working-layer evidence can never close debt), then
    calls the layer-blind ``reconcile_canonical_debt_with_evidence``.
    ``extra_evidence`` (optional) is folded through the SAME pre-filter, so a
    caller cannot smuggle working-layer evidence past the guard."""
    from okto_pulse.core.kg.schema import open_board_connection

    with open_board_connection(board_id) as (_kdb, kconn):
        _violating, satisfied = _scan_partition(kconn)

    evidence = [_canonical_evidence_for(nid, ref) for nid, ref in satisfied]
    if extra_evidence:
        evidence.extend(extra_evidence)
    canonical_evidence = _canonical_only_evidence(evidence)

    result = await reconcile_canonical_debt_with_evidence(
        db,
        board_id=board_id,
        canonical_evidence=canonical_evidence,
        actor_id=actor_id,
        report_ref=f"canonical_learning_partition:{board_id}",
    )
    if result.get("committed_count"):
        logger.info(
            "kg.clp.debt_reconciled board=%s closed=%d",
            board_id,
            result["committed_count"],
        )
    return result


async def run_canonical_learning_partition_maintenance(
    db: AsyncSession,
    *,
    board_id: str,
    actor_id: str,
) -> dict:
    """Single-scan post-commit hook: open historical debt for new violations and
    close debt for Learnings whose bug evidence is now canonical.

    Non-raising by contract for the worker: detection and reconcile are best
    effort; a failure here must never fail the (already successful) commit."""
    from okto_pulse.core.kg.schema import open_board_connection

    try:
        with open_board_connection(board_id) as (_kdb, kconn):
            violating, satisfied = _scan_partition(kconn)
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("kg.clp.maintenance_scan_failed board=%s err=%s", board_id, exc)
        return {"opened": 0, "closed": 0}

    opened = 0
    for node_id, source_ref in violating:
        await upsert_canonical_debt(
            db,
            board_id=board_id,
            artifact_type="bug",
            artifact_id=_bug_artifact_id(source_ref),
            source_ref=source_ref,
            content_hash=_stable_content_hash(source_ref, node_id),
            target_status=PARTITION_TARGET_STATUS,
            canonical_state="pending",
            graph_layer="canonical",
            maturity_status="canonical_eligible",
            failure_reason=HISTORICAL_DEBT_REASON,
            owner_agent_id=actor_id,
        )
        opened += 1

    canonical_evidence = _canonical_only_evidence(
        [_canonical_evidence_for(nid, ref) for nid, ref in satisfied]
    )
    reconciled = await reconcile_canonical_debt_with_evidence(
        db,
        board_id=board_id,
        canonical_evidence=canonical_evidence,
        actor_id=actor_id,
        report_ref=f"canonical_learning_partition:{board_id}",
    )
    return {"opened": opened, "closed": reconciled.get("committed_count", 0)}


__all__ = [
    "EVIDENCE_LAYER_CANONICAL",
    "HISTORICAL_DEBT_REASON",
    "PARTITION_TARGET_STATUS",
    "detect_historical_canonical_learning_debt",
    "reconcile_canonical_learning_partition_debt",
    "run_canonical_learning_partition_maintenance",
]
