"""CanonicalDebt read/retry service.

The ledger records artifacts that are not safe to promote to the canonical KG
yet. Manual retry scheduling is deliberately queue-like and does not mutate the
graph directly; the consolidation agent can consume the marker when KG health
admits writes.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.models.db import CanonicalDebt


OPEN_STATES = frozenset({
    "pending",
    "retry_scheduled",
    "deferred",
    "failed",
    "blocked",
    # Back-compat for rows created by early local builds before the state
    # vocabulary was aligned with the spec.
    "retryable",
})
TERMINAL_STATES = frozenset({
    "committed",
    "not_applicable",
    "superseded",
    # Back-compat aliases.
    "promoted",
    "discarded",
})

RETRYABLE_STATES = frozenset({
    "pending",
    "retry_scheduled",
    "deferred",
    "failed",
    "blocked",
    "retryable",
})


@dataclass(frozen=True, slots=True)
class CanonicalDebtListResult:
    items: list[dict[str, Any]]
    counts: dict[str, int]
    total: int


def _iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat()


def _canonical_debt_next_action(canonical_state: str | None, dlq_ref: str | None) -> str:
    """Suggested next action for a canonical-debt row (SPEC4 card 2e913ac3,
    AC ac_26acf1db). Lets an agent act from the drill-down alone, without local
    file forensics."""
    state = (canonical_state or "").lower()
    if state == "retry_scheduled":
        return "wait_for_scheduled_retry"
    if state == "blocked":
        return "resolve_blocker_then_retry"
    if state in RETRYABLE_STATES:
        return (
            "reprocess_via_okto_pulse_kg_dead_letter_reprocess"
            if dlq_ref
            else "retry_eligible_inspect_failure_reason"
        )
    if state in TERMINAL_STATES:
        return "inspect_terminal_debt_no_auto_retry"
    return "inspect_canonical_debt"


def canonical_debt_to_dict(row: CanonicalDebt) -> dict[str, Any]:
    return {
        "id": row.id,
        "board_id": row.board_id,
        "artifact_type": row.artifact_type,
        "artifact_id": row.artifact_id,
        # SPEC4 (card 2e913ac3): bounded suggested next action so the drill-down
        # row is actionable without reading local files (AC ac_26acf1db).
        "next_action": _canonical_debt_next_action(row.canonical_state, row.dlq_ref),
        "source_ref": row.source_ref,
        "source_version": row.source_version,
        "content_hash": row.content_hash,
        "target_status": row.target_status,
        "canonical_state": row.canonical_state,
        "graph_layer": row.graph_layer,
        "maturity_status": row.maturity_status,
        "failure_reason": row.failure_reason,
        "last_error": row.last_error,
        "retry_count": int(row.retry_count or 0),
        "next_retry_at": _iso(row.next_retry_at),
        "last_attempt_at": _iso(row.last_attempt_at),
        "owner_agent_id": row.owner_agent_id,
        "correlation_id": row.correlation_id,
        "queue_ref": row.queue_ref,
        "dlq_ref": row.dlq_ref,
        "evidence_ref": row.evidence_ref,
        "created_at": _iso(row.created_at),
        "updated_at": _iso(row.updated_at),
    }


async def summarize_canonical_debt(
    db: AsyncSession,
    board_id: str,
) -> dict[str, Any]:
    rows = (await db.execute(
        select(CanonicalDebt.canonical_state, func.count())
        .where(CanonicalDebt.board_id == board_id)
        .group_by(CanonicalDebt.canonical_state)
    )).all()
    by_state = {str(state): int(count) for state, count in rows}
    open_count = sum(by_state.get(state, 0) for state in OPEN_STATES)
    terminal_count = sum(by_state.get(state, 0) for state in TERMINAL_STATES)
    retryable_count = sum(
        by_state.get(state, 0)
        for state in ("pending", "deferred", "failed", "retryable")
    )
    blocked_count = by_state.get("blocked", 0)
    scheduled_count = by_state.get("retry_scheduled", 0)
    return {
        "open_count": open_count,
        "retryable_count": retryable_count,
        "blocked_count": blocked_count,
        "retry_scheduled_count": scheduled_count,
        "terminal_count": terminal_count,
        "by_state": by_state,
    }


async def list_canonical_debt(
    db: AsyncSession,
    *,
    board_id: str,
    artifact_type: str | None = None,
    state: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> CanonicalDebtListResult:
    predicates = [CanonicalDebt.board_id == board_id]
    if artifact_type:
        predicates.append(CanonicalDebt.artifact_type == artifact_type)
    if state:
        predicates.append(CanonicalDebt.canonical_state == state)
    where_clause = and_(*predicates)

    total = int(await db.scalar(
        select(func.count()).select_from(CanonicalDebt).where(where_clause)
    ) or 0)
    rows = (await db.execute(
        select(CanonicalDebt)
        .where(where_clause)
        .order_by(CanonicalDebt.updated_at.desc(), CanonicalDebt.id.asc())
        .offset(max(0, offset))
        .limit(max(1, min(limit, 200)))
    )).scalars().all()
    summary = await summarize_canonical_debt(db, board_id)
    return CanonicalDebtListResult(
        items=[canonical_debt_to_dict(row) for row in rows],
        counts=summary,
        total=total,
    )


async def upsert_canonical_debt(
    db: AsyncSession,
    *,
    board_id: str,
    artifact_type: str,
    artifact_id: str,
    source_ref: str,
    content_hash: str,
    target_status: str,
    canonical_state: str = "failed",
    source_version: str | None = None,
    graph_layer: str = "canonical",
    maturity_status: str | None = "canonical_eligible",
    failure_reason: str | None = None,
    last_error: str | None = None,
    owner_agent_id: str | None = None,
    correlation_id: str | None = None,
    queue_ref: str | None = None,
    dlq_ref: str | None = None,
    evidence_ref: str | None = None,
) -> CanonicalDebt:
    """Create or update one semantic canonical debt marker.

    The unique key intentionally follows the spec's idempotency rule:
    board + artifact + target status + source hash. The caller owns commit
    boundaries so this helper can participate in larger workflow transactions.
    """

    if not content_hash:
        raise ValueError("content_hash is required for CanonicalDebt")
    now = datetime.now(timezone.utc)
    row = (await db.execute(
        select(CanonicalDebt).where(
            CanonicalDebt.board_id == board_id,
            CanonicalDebt.artifact_type == artifact_type,
            CanonicalDebt.artifact_id == artifact_id,
            CanonicalDebt.target_status == target_status,
            CanonicalDebt.content_hash == content_hash,
        )
    )).scalar_one_or_none()
    if row is None:
        row = CanonicalDebt(
            board_id=board_id,
            artifact_type=artifact_type,
            artifact_id=artifact_id,
            source_ref=source_ref,
            source_version=source_version,
            content_hash=content_hash,
            target_status=target_status,
            canonical_state=canonical_state,
            graph_layer=graph_layer,
            maturity_status=maturity_status,
            failure_reason=failure_reason,
            last_error=last_error,
            owner_agent_id=owner_agent_id,
            correlation_id=correlation_id,
            queue_ref=queue_ref,
            dlq_ref=dlq_ref,
            evidence_ref=evidence_ref,
            created_at=now,
            updated_at=now,
        )
        db.add(row)
        await db.flush()
        return row

    row.source_ref = source_ref
    row.source_version = source_version
    row.canonical_state = canonical_state
    row.graph_layer = graph_layer
    row.maturity_status = maturity_status
    row.failure_reason = failure_reason
    row.last_error = last_error
    row.owner_agent_id = owner_agent_id
    row.correlation_id = correlation_id
    row.queue_ref = queue_ref
    row.dlq_ref = dlq_ref
    row.evidence_ref = evidence_ref
    row.updated_at = now
    await db.flush()
    return row


async def schedule_canonical_debt_retry(
    db: AsyncSession,
    *,
    board_id: str,
    debt_id: str,
    actor_id: str,
    kg_health_state: str,
) -> dict[str, Any]:
    row = await db.get(CanonicalDebt, debt_id)
    if row is None or row.board_id != board_id:
        return {
            "ok": False,
            "error": "canonical_debt_not_found",
            "attempt_consumed": False,
        }
    if row.canonical_state not in RETRYABLE_STATES:
        return {
            "ok": False,
            "error": "canonical_debt_not_retryable",
            "attempt_consumed": False,
            "debt": canonical_debt_to_dict(row),
        }

    if kg_health_state in {"quarantined", "recovery_needed", "backpressure", "at_risk"}:
        row.canonical_state = "blocked"
        row.failure_reason = f"kg_health_{kg_health_state}"
        row.owner_agent_id = actor_id
        row.updated_at = datetime.now(timezone.utc)
        await db.commit()
        return {
            "ok": False,
            "error": "kg_health_blocks_retry",
            "kg_health_state": kg_health_state,
            "attempt_consumed": False,
            "debt": canonical_debt_to_dict(row),
        }

    now = datetime.now(timezone.utc)
    row.canonical_state = "retry_scheduled"
    row.next_retry_at = now
    row.owner_agent_id = actor_id
    row.updated_at = now
    await db.commit()
    await db.refresh(row)
    return {
        "ok": True,
        "attempt_consumed": False,
        "kg_health_state": kg_health_state,
        "debt": canonical_debt_to_dict(row),
    }


async def reconcile_canonical_debt_with_evidence(
    db: AsyncSession,
    *,
    board_id: str,
    canonical_evidence: list[dict[str, Any]],
    actor_id: str,
    report_ref: str | None = None,
) -> dict[str, Any]:
    """Mark open debts committed only when rebuild evidence matches.

    Evidence rows must carry at least ``source_ref`` and ``content_hash``.
    ``source_version`` is checked when both sides have it. This keeps rebuild
    reconciliation deterministic and prevents closing debt just because an
    artifact with a similar id exists.
    """

    now = datetime.now(timezone.utc)
    committed: list[dict[str, Any]] = []
    for evidence in canonical_evidence:
        source_ref = str(evidence.get("source_ref") or "")
        content_hash = str(evidence.get("content_hash") or "")
        if not source_ref or not content_hash:
            continue
        rows = (await db.execute(
            select(CanonicalDebt).where(
                CanonicalDebt.board_id == board_id,
                CanonicalDebt.source_ref == source_ref,
                CanonicalDebt.content_hash == content_hash,
                CanonicalDebt.canonical_state.in_(tuple(OPEN_STATES)),
            )
        )).scalars().all()
        for row in rows:
            evidence_version = evidence.get("source_version")
            if (
                evidence_version
                and row.source_version
                and str(evidence_version) != str(row.source_version)
            ):
                continue
            row.canonical_state = "committed"
            row.evidence_ref = (
                str(evidence.get("evidence_ref") or evidence.get("node_ref") or "")
                or report_ref
            )
            row.owner_agent_id = actor_id
            row.updated_at = now
            committed.append(canonical_debt_to_dict(row))
    await db.flush()
    summary = await summarize_canonical_debt(db, board_id)
    return {
        "committed_count": len(committed),
        "committed": committed,
        "open_count": summary["open_count"],
        "evidence_count": len(canonical_evidence),
    }


async def mark_canonical_debt_committed_for_artifact(
    db: AsyncSession,
    *,
    board_id: str,
    artifact_type: str,
    artifact_id: str,
    actor_id: str,
    evidence_ref: str | None = None,
    target_status: str = "canonical_consolidation",
) -> dict[str, Any]:
    """Resolve open queue-failure debts after a later successful commit.

    Queue failure debts use a retry-attempt hash, not the artifact content hash,
    so evidence-hash reconciliation cannot close them. A subsequent successful
    consolidation for the same artifact is the deterministic terminal evidence.
    """

    now = datetime.now(timezone.utc)
    rows = (await db.execute(
        select(CanonicalDebt).where(
            CanonicalDebt.board_id == board_id,
            CanonicalDebt.artifact_type == artifact_type,
            CanonicalDebt.artifact_id == artifact_id,
            CanonicalDebt.target_status == target_status,
            CanonicalDebt.canonical_state.in_(tuple(OPEN_STATES)),
        )
    )).scalars().all()
    for row in rows:
        row.canonical_state = "committed"
        row.evidence_ref = evidence_ref
        row.owner_agent_id = actor_id
        row.updated_at = now
    await db.flush()
    summary = await summarize_canonical_debt(db, board_id)
    return {
        "committed_count": len(rows),
        "open_count": summary["open_count"],
        "evidence_ref": evidence_ref,
    }


__all__ = [
    "CanonicalDebtListResult",
    "OPEN_STATES",
    "RETRYABLE_STATES",
    "TERMINAL_STATES",
    "canonical_debt_to_dict",
    "list_canonical_debt",
    "mark_canonical_debt_committed_for_artifact",
    "reconcile_canonical_debt_with_evidence",
    "schedule_canonical_debt_retry",
    "summarize_canonical_debt",
    "upsert_canonical_debt",
]
