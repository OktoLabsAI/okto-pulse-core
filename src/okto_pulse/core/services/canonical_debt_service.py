"""Canonical-debt rules over an edition-owned persistence store."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from okto_pulse.core.ports.canonical_debt import (
    CanonicalDebtRecord,
    get_canonical_debt_store,
)
from okto_pulse.core.kg.source_maturity import CANONICAL_ARTIFACT_TYPES


OPEN_STATES = frozenset(
    {"pending", "retry_scheduled", "deferred", "failed", "blocked", "retryable"}
)
TERMINAL_STATES = frozenset(
    {"committed", "not_applicable", "superseded", "promoted", "discarded"}
)
RETRYABLE_STATES = frozenset(
    {"pending", "retry_scheduled", "deferred", "failed", "blocked", "retryable"}
)
CANONICAL_DEBT_STATES = OPEN_STATES | TERMINAL_STATES
# Canonical debt can only be created for source kinds that the maturity
# contract recognizes as canonical. Keep this derived from that single source
# of truth so list filters cannot drift from writer/rebuild eligibility.
CANONICAL_DEBT_ARTIFACT_TYPES = frozenset(CANONICAL_ARTIFACT_TYPES)


class CanonicalDebtFilterError(ValueError):
    """Typed, transport-neutral rejection for closed canonical-debt filters."""

    code = "invalid_filter"
    http_status = 422

    def __init__(self, field: str, value: object, allowed: frozenset[str]) -> None:
        self.field = field
        self.value = value
        self.allowed = tuple(sorted(allowed))
        super().__init__(
            f"{field}={value!r} is not a valid canonical-debt filter; "
            f"allowed: {list(self.allowed)}"
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": self.code,
            "code": self.code,
            "message": str(self),
            "field": self.field,
            "value": self.value,
            "allowed": list(self.allowed),
        }


def validate_canonical_debt_filters(
    *,
    artifact_type: str | None = None,
    state: str | None = None,
) -> None:
    """Require exact canonical tokens; never coerce case or whitespace."""

    for field, value, allowed in (
        ("artifact_type", artifact_type, CANONICAL_DEBT_ARTIFACT_TYPES),
        ("state", state, CANONICAL_DEBT_STATES),
    ):
        if value is None:
            continue
        if not isinstance(value, str) or value not in allowed:
            raise CanonicalDebtFilterError(field, value, allowed)


@dataclass(frozen=True, slots=True)
class CanonicalDebtListResult:
    items: list[dict[str, Any]]
    counts: dict[str, Any]
    total: int


def _iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat()


def _canonical_debt_next_action(
    canonical_state: str | None, dlq_ref: str | None
) -> str:
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


def canonical_debt_to_dict(row: Any) -> dict[str, Any]:
    return {
        "id": row.id,
        "board_id": row.board_id,
        "artifact_type": row.artifact_type,
        "artifact_id": row.artifact_id,
        "next_action": _canonical_debt_next_action(
            row.canonical_state, row.dlq_ref
        ),
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
    db: object, board_id: str
) -> dict[str, Any]:
    by_state = await get_canonical_debt_store().counts_by_state(
        db, board_id=board_id
    )
    return {
        "open_count": sum(by_state.get(state, 0) for state in OPEN_STATES),
        "retryable_count": sum(
            by_state.get(state, 0)
            for state in ("pending", "deferred", "failed", "retryable")
        ),
        "blocked_count": by_state.get("blocked", 0),
        "retry_scheduled_count": by_state.get("retry_scheduled", 0),
        "terminal_count": sum(
            by_state.get(state, 0) for state in TERMINAL_STATES
        ),
        "by_state": by_state,
    }


async def list_canonical_debt(
    db: object,
    *,
    board_id: str,
    artifact_type: str | None = None,
    state: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> CanonicalDebtListResult:
    validate_canonical_debt_filters(
        artifact_type=artifact_type,
        state=state,
    )
    total, rows = await get_canonical_debt_store().list_records(
        db,
        board_id=board_id,
        artifact_type=artifact_type,
        state=state,
        limit=max(1, min(limit, 200)),
        offset=max(0, offset),
    )
    return CanonicalDebtListResult(
        items=[canonical_debt_to_dict(row) for row in rows],
        counts=await summarize_canonical_debt(db, board_id),
        total=total,
    )


async def upsert_canonical_debt(
    db: object,
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
) -> CanonicalDebtRecord:
    if not content_hash:
        raise ValueError("content_hash is required for CanonicalDebt")
    store = get_canonical_debt_store()
    row = await store.find_by_identity(
        db,
        board_id=board_id,
        artifact_type=artifact_type,
        artifact_id=artifact_id,
        target_status=target_status,
        content_hash=content_hash,
    )
    now = datetime.now(timezone.utc)
    if row is None:
        row = CanonicalDebtRecord(
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
    else:
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
    return await store.save(db, row)


async def schedule_canonical_debt_retry(
    db: object,
    *,
    board_id: str,
    debt_id: str,
    actor_id: str,
    kg_health_state: str,
) -> dict[str, Any]:
    store = get_canonical_debt_store()
    row = await store.get(db, debt_id=debt_id)
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
    now = datetime.now(timezone.utc)
    if kg_health_state in {
        "quarantined",
        "recovery_needed",
        "backpressure",
        "at_risk",
    }:
        row.canonical_state = "blocked"
        row.failure_reason = f"kg_health_{kg_health_state}"
        row.owner_agent_id = actor_id
        row.updated_at = now
        row = await store.save(db, row, commit=True)
        return {
            "ok": False,
            "error": "kg_health_blocks_retry",
            "kg_health_state": kg_health_state,
            "attempt_consumed": False,
            "debt": canonical_debt_to_dict(row),
        }
    row.canonical_state = "retry_scheduled"
    row.next_retry_at = now
    row.owner_agent_id = actor_id
    row.updated_at = now
    row = await store.save(db, row, commit=True)
    return {
        "ok": True,
        "attempt_consumed": False,
        "kg_health_state": kg_health_state,
        "debt": canonical_debt_to_dict(row),
    }


async def reconcile_canonical_debt_with_evidence(
    db: object,
    *,
    board_id: str,
    canonical_evidence: list[dict[str, Any]],
    actor_id: str,
    report_ref: str | None = None,
) -> dict[str, Any]:
    store = get_canonical_debt_store()
    now = datetime.now(timezone.utc)
    committed: list[dict[str, Any]] = []
    for evidence in canonical_evidence:
        source_ref = str(evidence.get("source_ref") or "")
        content_hash = str(evidence.get("content_hash") or "")
        if not source_ref or not content_hash:
            continue
        rows = await store.find_open_by_evidence(
            db,
            board_id=board_id,
            source_ref=source_ref,
            content_hash=content_hash,
            open_states=tuple(OPEN_STATES),
        )
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
            await store.save(db, row)
            committed.append(canonical_debt_to_dict(row))
    summary = await summarize_canonical_debt(db, board_id)
    return {
        "committed_count": len(committed),
        "committed": committed,
        "open_count": summary["open_count"],
        "evidence_count": len(canonical_evidence),
    }


async def mark_canonical_debt_committed_for_artifact(
    db: object,
    *,
    board_id: str,
    artifact_type: str,
    artifact_id: str,
    actor_id: str,
    evidence_ref: str | None = None,
    target_status: str = "canonical_consolidation",
) -> dict[str, Any]:
    store = get_canonical_debt_store()
    rows = await store.find_open_for_artifact(
        db,
        board_id=board_id,
        artifact_type=artifact_type,
        artifact_id=artifact_id,
        target_status=target_status,
        open_states=tuple(OPEN_STATES),
    )
    now = datetime.now(timezone.utc)
    for row in rows:
        row.canonical_state = "committed"
        row.evidence_ref = evidence_ref
        row.owner_agent_id = actor_id
        row.updated_at = now
        await store.save(db, row)
    summary = await summarize_canonical_debt(db, board_id)
    return {
        "committed_count": len(rows),
        "open_count": summary["open_count"],
        "evidence_ref": evidence_ref,
    }


__all__ = [
    "CANONICAL_DEBT_ARTIFACT_TYPES",
    "CANONICAL_DEBT_STATES",
    "CanonicalDebtFilterError",
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
    "validate_canonical_debt_filters",
]
