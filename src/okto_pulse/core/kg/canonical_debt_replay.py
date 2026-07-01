"""R2-IMP3 — maturity replay of CanonicalDebt.

When the canonical evidence for an open ``CanonicalDebt`` becomes available (a
source matured to a canonical-eligible state with the SAME content), replay the
debt: reconcile/close it against that now-canonical evidence. This does NOT
introduce a parallel reconciler — it is a concrete TRIGGER over the existing
``canonical_debt_service`` contracts (``reconcile_canonical_debt_with_evidence``),
wired into the consolidation worker's async post-commit hook (alongside the R7
``run_canonical_learning_partition_maintenance``) so a status/maturity move or a
rebuild drain both replay through the SAME path.

Verification (R2-IMP3 gate): evidence must be CANONICAL by
``source_maturity.classify_source_for_kg`` (graph_layer + maturity) AND its
``content_hash`` must match the debt's recorded hash (``source_version`` checked
by the contract) — never closed on textual status alone. Idempotent: only
``OPEN_STATES`` debts are touched, so a repeated trigger/retry commits 0.

Boundaries: touches only ``CanonicalDebt`` rows. Cognitive pending and DLQ are
NEVER closed as a side effect. No Global Discovery sync (R2-IMP5). No agent-facing
mutating MCP tool.
"""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.kg.interfaces import get_kg_registry
from okto_pulse.core.kg.source_maturity import (
    GRAPH_LAYER_CANONICAL,
    classify_source_for_kg,
)
from okto_pulse.core.services.canonical_debt_service import (
    OPEN_STATES,
    list_canonical_debt,
    reconcile_canonical_debt_with_evidence,
)

logger = logging.getLogger("okto_pulse.kg.canonical_debt_replay")

REPLAY_ACTOR = "system:canonical_debt_replay"


def _build_source_index(board_id: str) -> dict[str, dict[str, Any]]:
    """``{artifact_id: {classification, content_hash, source_version, source_ref}}``
    from the registered source reader + the maturity classifier (TR6)."""

    reader = get_kg_registry().require_board_source_reader()
    out: dict[str, dict[str, Any]] = {}
    for row in reader.fetch(board_id):
        aid = str(row.get("id") or "")
        if not aid:
            continue
        out[aid] = {
            "classification": classify_source_for_kg(
                artifact_type=row.get("artifact_type"),
                artifact_status=row.get("source_artifact_status") or row.get("status"),
                content_hash=row.get("content_hash"),
                updated_at=row.get("updated_at"),
                has_minimal_evidence=bool(row.get("has_minimal_evidence", True)),
            ),
            "content_hash": str(row.get("content_hash") or ""),
            "source_version": row.get("source_version"),
            "source_ref": str(row.get("source_ref") or ""),
        }
    return out


def _target_ids(source_refs: list[str] | None) -> set[str] | None:
    if not source_refs:
        return None
    ids: set[str] = set()
    for ref in source_refs:
        parts = str(ref).split(":")
        if len(parts) >= 2 and parts[1]:
            ids.add(parts[1])
    return ids


async def replay_canonical_debt_by_maturity(
    db: AsyncSession,
    *,
    board_id: str,
    source_refs: list[str] | None = None,
    actor_id: str = REPLAY_ACTOR,
) -> dict[str, Any]:
    """Replay open CanonicalDebt whose canonical evidence is now available.

    ``source_refs`` scopes the run to specific artifacts (status/maturity
    fast-path); ``None`` is a full board sweep (rebuild post-run / sweep). Returns
    a summary with ``committed_count`` and the verification accounting. Idempotent.
    Reusable, testable entry point — the worker post-commit hook and any explicit
    rebuild_post_run call both go through here.
    """
    result: dict[str, Any] = {
        "board_id": board_id,
        "open_before": 0,
        "committed_count": 0,
        "skipped_non_canonical": 0,
        "skipped_hash_mismatch": 0,
    }
    debt = await list_canonical_debt(db, board_id=board_id, limit=200)
    open_debts = [
        r for r in debt.items
        if str(r.get("canonical_state") or "") in OPEN_STATES
    ]
    result["open_before"] = len(open_debts)
    if not open_debts:
        return result

    sources = _build_source_index(board_id)
    target_ids = _target_ids(source_refs)
    evidence: list[dict[str, Any]] = []
    for row in open_debts:
        aid = str(row.get("artifact_id") or "")
        if target_ids is not None and aid not in target_ids:
            continue
        src = sources.get(aid)
        if src is None:
            continue
        # #5: evidence must be CANONICAL by classifier (graph_layer + maturity),
        # not just a textual status.
        if src["classification"].graph_layer != GRAPH_LAYER_CANONICAL:
            result["skipped_non_canonical"] += 1
            continue
        debt_hash = str(row.get("content_hash") or "")
        src_hash = str(src["content_hash"] or "")
        # #5: content_hash must be compatible — never close on similar-id alone.
        if not debt_hash or not src_hash or debt_hash != src_hash:
            result["skipped_hash_mismatch"] += 1
            continue
        evidence.append({
            "source_ref": str(row.get("source_ref") or src["source_ref"]),
            "content_hash": debt_hash,
            "source_version": src["source_version"],
            "evidence_ref": f"maturity_replay:{aid}",
        })

    if evidence:
        rec = await reconcile_canonical_debt_with_evidence(
            db, board_id=board_id, canonical_evidence=evidence, actor_id=actor_id,
        )
        result["committed_count"] = int(rec.get("committed_count") or 0)
        result["open_count"] = rec.get("open_count")
    return result


async def replay_canonical_debt_post_commit(db: AsyncSession, *, board_id: str) -> None:
    """Best-effort post-commit trigger for the consolidation worker. A failure is
    logged and NEVER marks debt committed nor fails the commit (no silent success:
    only the existing verified reconcile contract commits anything)."""
    try:
        summary = await replay_canonical_debt_by_maturity(db, board_id=board_id)
        if summary.get("committed_count"):
            logger.info(
                "kg.canonical_debt_replay.committed board=%s count=%d",
                board_id, summary["committed_count"],
                extra={"event": "kg.canonical_debt_replay.committed",
                       "board_id": board_id,
                       "committed_count": summary["committed_count"]},
            )
    except Exception:
        logger.exception(
            "kg.canonical_debt_replay.failed board=%s", board_id,
        )


__all__ = [
    "REPLAY_ACTOR",
    "replay_canonical_debt_by_maturity",
    "replay_canonical_debt_post_commit",
]
