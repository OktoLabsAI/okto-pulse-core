"""Consolidation queue application processor (spec c48a5c33).

For each pending queue entry the worker:
    1. Loads the artifact (Spec/Sprint/Card) from the DB.
    2. Runs the pure `DeterministicWorker` (Layer 1) to extract every
       node + edge candidate that can be derived from structured fields,
       with full v0.2.0 provenance metadata (layer/rule_id/created_by).
    3. Drives the primitives pipeline: begin → propose_reconciliation →
       commit. The session uses `agent_id="system:historical_consolidation"`
       so the layer-ownership BR allows deterministic edges through.
    4. Runs the embedded graph backend safe-write lifecycle before the queue row is
       acknowledged, proving the graph is readable from disk after
       close/reopen.
    5. Marks the queue entry as `done` (or `failed`).

The cognitive agent picks up `missing_link_candidates` later and proposes
the residual semantic edges (capped at confidence 0.85 per BR `Cognitive
Fallback Confidence Cap`).
"""

from __future__ import annotations

import hashlib
import logging
import sys
import uuid
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Mapping

from okto_pulse.core.ports.consolidation import (
    ConsolidationQueueRecord,
    get_consolidation_persistence_port,
)
from okto_pulse.core.ports.delivery_ledger import (
    DeliveryState,
    DeliveryTransferClaimConflict,
    DeliveryTransferReceipt,
    DeliveryTransferRequest,
    build_attempt_event_key,
    get_delivery_ledger_port,
)
from okto_pulse.core.ports.stale_sweep import (
    StaleSweepBatchRequest,
    StaleSweepClaimConflict,
    StaleSweepRescheduleRequest,
    StaleSweepRunAction,
    StaleSweepRunReceipt,
    get_stale_sweep_port,
)
from okto_pulse.core.kg.schemas import (
    AddEdgeCandidateRequest,
    AbortConsolidationRequest,
    BeginConsolidationRequest,
    CommitConsolidationRequest,
    EdgeCandidate,
    KGEdgeType,
    KGNodeType,
    NodeCandidate,
    ProposeReconciliationRequest,
)
from okto_pulse.core.kg.primitives import (
    add_edge_candidate,
    abort_deferred_consolidation,
    abort_consolidation,
    begin_consolidation,
    commit_consolidation,
    finalize_deferred_consolidation,
    propose_reconciliation,
    run_cancellation_atomic,
)
from okto_pulse.core.kg.memory_pressure import FailureEvent
from okto_pulse.core.kg.memory_pressure_collector import record_failure
from okto_pulse.core.kg.interfaces.graph_errors import (
    GraphError,
    graph_memory_pressure_retry_after_seconds,
)
from okto_pulse.core.kg.interfaces.graph_transaction import (
    SpecLineageParentIntent,
)
from okto_pulse.core.application.processors.dead_letter import route_to_dead_letter
from okto_pulse.core.kg.schema_layer_guard import (
    ensure_graph_layer_schema,
    is_graph_layer_schema_error,
)
from okto_pulse.core.kg.safe_write_lifecycle import (
    STEP_CHECKPOINT,
    STEP_FLUSH,
    STEP_FSYNC,
)
from okto_pulse.core.kg.guarded_write import (
    GuardedWriteError,
    GuardedWriteLease,
    guarded_board_write,
)
from okto_pulse.core.kg.interfaces import get_kg_registry
from okto_pulse.core.kg.source_maturity import (
    GRAPH_LAYER_CANONICAL,
    GRAPH_LAYER_NONE,
    GRAPH_LAYER_WORKING,
    DISPOSITION_SKIPPED_CANCELLED,
    MATURITY_CANONICAL_ELIGIBLE,
    classify_source_for_kg,
)
from okto_pulse.core.ports.advisory_lock import advisory_lock
from okto_pulse.core.application.processors.deterministic_kg import (
    DeterministicWorker,
    EmittedEdge,
    EmittedNode,
    WORKER_VERSION,
    WorkerResult,
    _spec_child_ref,
)
from okto_pulse.core.services.canonical_debt_service import (
    mark_canonical_debt_committed_for_artifact,
    upsert_canonical_debt,
)
from okto_pulse.core.domain.worker_policy import RetryPolicy
from okto_pulse.core.ports.runtime_workers import (
    BlockingExecutionPort,
    WorkerClockPort,
)

logger = logging.getLogger("okto_pulse.kg.consolidation_worker")

AGENT_ID = "system:historical_consolidation"
CONSOLIDATION_COMMIT_OPERATION = "consolidation_worker_commit"
_CLAIMABLE_WORK_KINDS = frozenset({"consolidate", "stale_reconcile", "stale_sweep"})
_GOVERNED_DELETION_ARTIFACT_TYPES = frozenset(
    {"card", "ideation", "refinement", "spec", "sprint"}
)
_GraphWriteEnter = Callable[[str], GuardedWriteLease]


class _QueueClaimLostOrFenced(RuntimeError):
    """Neutral worker outcome: ownership or deletion generation changed."""


def _work_kind(entry: ConsolidationQueueRecord) -> str:
    return str(getattr(entry, "work_kind", None) or "consolidate")


def _generation(entry: ConsolidationQueueRecord) -> int:
    return int(getattr(entry, "generation", 0) or 0)


def _delete_event_id(entry: ConsolidationQueueRecord) -> str | None:
    value = getattr(entry, "delete_event_id", None)
    return str(value) if value is not None else None


def _claim_token(entry: ConsolidationQueueRecord) -> str | None:
    value = getattr(entry, "claim_token", None)
    return str(value) if value else None


def _same_claim(
    expected: ConsolidationQueueRecord,
    current: ConsolidationQueueRecord,
) -> bool:
    token = _claim_token(expected)
    return bool(
        token
        and current.status == "claimed"
        and _claim_token(current) == token
        and current.id == expected.id
        and current.board_id == expected.board_id
        and current.artifact_type == expected.artifact_type
        and current.artifact_id == expected.artifact_id
        and _work_kind(current) == _work_kind(expected)
        and _generation(current) == _generation(expected)
        and _delete_event_id(current) == _delete_event_id(expected)
    )


async def _queue_claim_is_current_and_unfenced(
    db: Any,
    entry: ConsolidationQueueRecord,
) -> bool:
    """Check the exact claim and governed-deletion fence in storage.

    Every row claimed by ``ConsolidationProcessor`` receives a token before
    processing. A missing token is therefore never authoritative.
    """

    token = _claim_token(entry)
    kind = _work_kind(entry)
    if token is None:
        return False
    return (
        await get_consolidation_persistence_port().queue_claim_is_current_and_unfenced(
            db,
            entry_id=entry.id,
            claim_token=token,
            board_id=entry.board_id,
            artifact_type=entry.artifact_type,
            artifact_id=entry.artifact_id,
            work_kind=kind,
            generation=_generation(entry),
            delete_event_id=_delete_event_id(entry),
        )
    )


async def _transfer_stale_reconcile_ownership(
    db: Any,
    entry: ConsolidationQueueRecord,
    *,
    reconcile_details: dict[str, object] | None = None,
    occurred_at: datetime | None = None,
) -> tuple[DeliveryTransferReceipt, str]:
    """Atomically hand a completed graph reconciliation to GD delivery.

    The concrete port stages all three relational effects in ``db``: the
    logical delivery owner, its physical attempt-zero outbox row (unless the
    circuit is degraded), and the exact queue compare-and-delete.  This helper
    intentionally does not commit; ``process_batch`` owns the transaction.
    """

    claim_token = _claim_token(entry)
    delete_event_id = _delete_event_id(entry)
    if claim_token is None or delete_event_id is None:
        raise DeliveryTransferClaimConflict(
            f"delivery_transfer_claim_identity_missing entry_id={entry.id}"
        )

    delivery = get_delivery_ledger_port()
    circuit = await delivery.read_circuit_snapshot(
        db,
        board_id=entry.board_id,
    )
    request = DeliveryTransferRequest(
        entry_id=entry.id,
        claim_token=claim_token,
        board_id=entry.board_id,
        artifact_type=entry.artifact_type,
        artifact_id=entry.artifact_id,
        generation=_generation(entry),
        delete_event_id=delete_event_id,
        target_state=(
            DeliveryState.DELIVERY_DEBT
            if circuit.degraded
            else DeliveryState.OUTBOX_PERSISTED
        ),
        reconcile_details={
            **dict(reconcile_details or {}),
            "circuit_reason": circuit.reason,
        },
        occurred_at=occurred_at,
    )
    receipt = await delivery.transfer_delivery_ownership(db, request)
    receipt_mismatch = receipt.delivery_key != request.delivery_key
    if receipt.replayed:
        current_event_key = build_attempt_event_key(
            receipt.delivery_key,
            attempt=receipt.attempt,
        )
        if receipt.state in {
            DeliveryState.OUTBOX_PERSISTED,
            DeliveryState.DELIVERED,
        }:
            receipt_mismatch = (
                receipt_mismatch or receipt.attempt_event_key != current_event_key
            )
        else:
            allowed_event_keys = {current_event_key}
            if receipt.attempt == 0:
                allowed_event_keys.add(None)
            receipt_mismatch = (
                receipt_mismatch or receipt.attempt_event_key not in allowed_event_keys
            )
    else:
        expected_event_key = (
            request.attempt_event_key
            if receipt.state is DeliveryState.OUTBOX_PERSISTED
            else None
        )
        receipt_mismatch = (
            receipt_mismatch
            or receipt.attempt != request.attempt
            or receipt.attempt_event_key != expected_event_key
            or receipt.state is not request.target_state
        )
    if receipt_mismatch:
        raise RuntimeError("delivery_transfer_receipt_mismatch")
    return receipt, circuit.reason


def _log_stale_reconcile_delivery_transfer(
    entry: ConsolidationQueueRecord,
    receipt: DeliveryTransferReceipt,
    *,
    circuit_reason: str,
) -> None:
    """Emit transfer telemetry only after the caller commits the hand-off."""

    delete_event_id = _delete_event_id(entry)
    logger.info(
        "kg.stale_reconcile.delivery_transferred entry=%s delivery=%s "
        "state=%s replayed=%s circuit_reason=%s",
        entry.id,
        receipt.delivery_key,
        receipt.state.value,
        receipt.replayed,
        circuit_reason,
        extra={
            "event": "kg.stale_reconcile.delivery_transferred",
            "board_id": entry.board_id,
            "delete_event_id": delete_event_id,
            "delivery_key": receipt.delivery_key,
            "delivery_state": receipt.state.value,
            "delivery_attempt": receipt.attempt,
            "delivery_replayed": receipt.replayed,
            "delivery_circuit_reason": circuit_reason,
        },
    )


def _capped_retry_delay(policy: RetryPolicy, attempts: int) -> int:
    """Return exponential backoff without making terminal mean data loss."""

    delay = 1
    for _ in range(max(0, attempts)):
        delay = min(delay * policy.base, policy.cap_seconds)
        if delay >= policy.cap_seconds:
            break
    return delay


def _validated_stale_reconcile_source_refs(
    entry: ConsolidationQueueRecord,
) -> list[str] | None:
    """Return the v1 fast-path refs, or ``None`` for a malformed intent."""

    payload = getattr(entry, "payload", None)
    delete_event_id = _delete_event_id(entry)
    expected_ref = f"{entry.artifact_type}:{entry.artifact_id}"
    if (
        entry.artifact_type not in _GOVERNED_DELETION_ARTIFACT_TYPES
        or _generation(entry) < 1
        or not delete_event_id
        or not isinstance(payload, dict)
        or payload.get("schema_version") != 1
        or payload.get("delete_event_id") != delete_event_id
        or payload.get("source_refs") != [expected_ref]
    ):
        return None
    return [expected_ref]


def _validated_stale_sweep_payload(
    entry: ConsolidationQueueRecord,
) -> tuple[str, int, int] | None:
    """Return ``(cursor, budget, attempt)`` for an exact board sweep row."""

    payload = getattr(entry, "payload", None)
    if (
        entry.artifact_type != "board"
        or entry.artifact_id != entry.board_id
        or _generation(entry) != 0
        or _delete_event_id(entry) is not None
        or not isinstance(payload, dict)
        or not {"cursor", "budget"}.issubset(payload)
        or not set(payload).issubset({"cursor", "budget", "attempt"})
    ):
        return None
    cursor = payload.get("cursor")
    budget = payload.get("budget")
    attempt = payload.get("attempt", 0)
    if (
        not isinstance(cursor, str)
        or isinstance(budget, bool)
        or not isinstance(budget, int)
        or budget < 1
        or isinstance(attempt, bool)
        or not isinstance(attempt, int)
        or attempt < 0
    ):
        return None
    from okto_pulse.core.kg.canonical_stale_reconciler import (
        decode_stale_sweep_cursor,
    )

    try:
        decode_stale_sweep_cursor(cursor)
    except ValueError:
        return None
    return (cursor, budget, attempt)


def _stale_sweep_retry_at(now: datetime) -> datetime:
    """Defer degraded work to the next configured daily-tick window."""

    from okto_pulse.core.infra.config import get_settings

    interval_minutes = int(
        getattr(get_settings(), "kg_decay_tick_interval_minutes", 1440)
    )
    return now + timedelta(minutes=max(1, interval_minutes))


def _log_stale_sweep_receipt(receipt: StaleSweepRunReceipt) -> None:
    event = {
        StaleSweepRunAction.ADVANCED: "kg.stale_sweep.page_staged",
        StaleSweepRunAction.COMPLETED: "kg.stale_sweep.completed.staged",
        StaleSweepRunAction.RESCHEDULED: "kg.stale_sweep.rescheduled.staged",
    }[receipt.action]
    logger.info(
        "%s board=%s entry=%s cursor=%s budget=%d attempt=%d "
        "enqueued=%d has_more=%s reason=%s transaction_state=%s "
        "commit_owner=%s",
        event,
        receipt.board_id,
        receipt.entry_id,
        receipt.cursor,
        receipt.budget,
        receipt.attempt,
        receipt.enqueued,
        receipt.has_more,
        receipt.reason,
        "pending_caller_commit",
        "consolidation_processor",
        extra={
            "event": event,
            "board_id": receipt.board_id,
            "entry_id": receipt.entry_id,
            "cursor": receipt.cursor,
            "budget": receipt.budget,
            "attempt": receipt.attempt,
            "enqueued": receipt.enqueued,
            "has_more": receipt.has_more,
            "reason": receipt.reason,
            "transaction_state": "pending_caller_commit",
            "commit_owner": "consolidation_processor",
        },
    )


_STALE_RECONCILE_GRAPH_PARTIAL_PREFIX = "stale_reconcile_graph_partial:"


def _stale_reconcile_is_complete(
    result: Any,
    *,
    previous_error: str | None = None,
) -> bool:
    """Require the explicit completeness contract before acknowledging work."""

    target_fields = (
        "target_identity_count",
        "target_found_count",
        "target_demoted_count",
        "target_already_converged_count",
        "target_skipped_cognitive_count",
        "target_preserved_canonical_count",
    )
    if isinstance(result, dict):
        if (
            "incomplete" not in result
            or "failed_types" not in result
            or any(field not in result for field in target_fields)
        ):
            return False
        incomplete = bool(result["incomplete"])
        failed_types = result["failed_types"] or ()
        target_values = {field: result[field] for field in target_fields}
    else:
        if (
            not hasattr(result, "incomplete")
            or not hasattr(result, "failed_types")
            or any(not hasattr(result, field) for field in target_fields)
        ):
            return False
        incomplete = bool(result.incomplete)
        failed_types = result.failed_types or ()
        target_values = {field: getattr(result, field) for field in target_fields}
    if incomplete or bool(failed_types):
        return False
    if any(
        isinstance(value, bool) or not isinstance(value, int) or value < 0
        for value in target_values.values()
    ):
        return False
    if target_values["target_identity_count"] < 1:
        return False
    if target_values["target_preserved_canonical_count"] != 0:
        return False
    if (
        target_values["target_found_count"] == 0
        and str(previous_error or "").startswith(
            _STALE_RECONCILE_GRAPH_PARTIAL_PREFIX
        )
    ):
        # A prior per-type failure may have auto-committed a partial mutation
        # before the embedded adapter raised.  An empty retry cannot prove
        # convergence: require a restored/found projection (or operator repair)
        # instead of ACKing disappearance as an initially-empty source.
        return False
    return target_values["target_found_count"] == (
        target_values["target_demoted_count"]
        + target_values["target_already_converged_count"]
        + target_values["target_skipped_cognitive_count"]
    )


def _stale_reconcile_failure_error(
    *,
    existing_error: str | None,
    reconcile_details: Mapping[str, object],
) -> str:
    """Persist whether a failed run may have auto-committed graph effects."""

    if str(existing_error or "").startswith(
        _STALE_RECONCILE_GRAPH_PARTIAL_PREFIX
    ):
        return str(existing_error)
    failed_types = reconcile_details.get("failed_types") or ()
    if isinstance(failed_types, (list, tuple)) and failed_types:
        normalized = sorted({str(item) for item in failed_types if str(item)})
        return _STALE_RECONCILE_GRAPH_PARTIAL_PREFIX + ",".join(normalized)
    return str(existing_error or "processing returned False")


def _stale_reconcile_telemetry_details(
    result: Any,
    entry: ConsolidationQueueRecord,
) -> dict[str, object]:
    """Normalize the governed run receipt before the worker returns a bool."""

    def _value(name: str, default: object) -> object:
        if isinstance(result, dict):
            return result.get(name, default)
        return getattr(result, name, default)

    demoted = _value("demoted", ()) or ()
    routed_to_debt = _value("routed_to_debt", ()) or ()
    failed_types = _value("failed_types", ()) or ()
    incomplete_cause = _value("incomplete_cause", None)
    details: dict[str, object] = {
        "queue_attempt": int(entry.attempts or 0),
        "scanned": int(_value("scanned", 0) or 0),
        "demoted_count": int(_value("demoted_count", len(demoted)) or 0),
        "routed_to_debt_count": int(
            _value("routed_to_debt_count", len(routed_to_debt)) or 0
        ),
        "incomplete": bool(_value("incomplete", True)),
        "incomplete_cause": (
            str(getattr(incomplete_cause, "value", incomplete_cause))
            if incomplete_cause is not None
            else None
        ),
        "failed_types": [str(item) for item in failed_types],
    }
    for field in (
        "target_identity_count",
        "target_found_count",
        "target_demoted_count",
        "target_already_converged_count",
        "target_skipped_cognitive_count",
        "target_preserved_canonical_count",
    ):
        value = _value(field, None)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(f"stale_reconcile_{field}_invalid")
        details[field] = value

    # A completed ontology scan is the proof that ACK/ownership transfer did
    # not skip a registered node type.  Keep the per-type receipt intact at
    # the boolean worker boundary so the final ``graph_demoted`` timeline row
    # remains independently auditable instead of carrying only an aggregate.
    raw_completed_types = _value("completed_types", ()) or ()
    completed_types = [str(item) for item in raw_completed_types]
    if completed_types:
        raw_scanned_by_type = _value("scanned_by_type", {}) or {}
        if not isinstance(raw_scanned_by_type, Mapping):
            raise ValueError("stale_reconcile_scanned_by_type_invalid")
        details["scanned_by_type"] = {
            str(node_type): int(count)
            for node_type, count in raw_scanned_by_type.items()
        }
        details["completed_types"] = completed_types
    return details


def _stale_reconcile_source_snapshot_is_incomplete(result: Any) -> bool:
    """Return whether reconciliation stopped before any graph mutation.

    A source-snapshot failure is the one incomplete outcome guaranteed by the
    reconciler to return before scanning or mutating graph nodes.  Keep the
    check explicit and fail closed: malformed results still run the durability
    lifecycle because they cannot prove that no graph write occurred.
    """

    if isinstance(result, dict):
        failed_types = result.get("failed_types")
        return (
            result.get("incomplete") is True
            and result.get("incomplete_cause") is not None
            and "failed_types" in result
            and isinstance(failed_types, (list, tuple))
            and not failed_types
        )
    failed_types = getattr(result, "failed_types", None)
    return (
        getattr(result, "incomplete", None) is True
        and getattr(result, "incomplete_cause", None) is not None
        and hasattr(result, "failed_types")
        and isinstance(failed_types, (list, tuple))
        and not failed_types
    )


async def _abort_open_consolidation_after_fence(
    *,
    entry: ConsolidationQueueRecord,
    session_id: str,
) -> None:
    """Best-effort cleanup for a deterministic session that lost authority."""

    try:
        await abort_consolidation(
            AbortConsolidationRequest(
                session_id=session_id,
                reason="queue_claim_lost_or_deletion_fenced",
            ),
            agent_id=AGENT_ID,
        )
    except Exception:
        logger.exception(
            "consolidation.abort_after_fence_failed entry=%s session=%s",
            entry.id,
            session_id,
        )


class _DirectBlockingExecution:
    """Task-free fallback for direct processor tests.

    Production editions inject an adapter that offloads and tracks blocking
    operations. This implementation intentionally performs no scheduling.
    """

    async def run(self, operation):
        return operation()

    async def join(self, timeout: float) -> int:
        del timeout
        return 0


# Spec 3d89c192 (FR-4): o commit incremental do worker usa o subset
# não-destrutivo do lifecycle — checkpoint real + verificação + fsync, SEM o
# close_reopen_probe. O probe fecha o Database compartilhado (use-after-close
# com leitores concorrentes) e só é necessário nas lanes de rebuild/recovery,
# que continuam usando DEFAULT_REQUIRED_STEPS (contrato api_1c9d19e1 prevê
# subset custom por caller).
WORKER_COMMIT_LIFECYCLE_STEPS: tuple[str, ...] = (
    STEP_CHECKPOINT,
    STEP_FLUSH,
    STEP_FSYNC,
)


def _apply_board_graph_lifecycle_after_commit(
    *,
    board_id: str,
    mutation_ref: str,
    write_lease: GuardedWriteLease,
    failure_timestamp: datetime | None = None,
):
    """Run checkpoint/flush/fsync/close-reopen before queue acknowledgement.

    FR3 (spec R2c): when the lifecycle fails, a FailureEvent with
    ``event_kind="kg.wal.flush.failed"`` is recorded in the collector
    ring-buffer before the RuntimeError is re-raised.  This is
    non-blocking (the record call is swallowed if it fails) so it never
    masks the original lifecycle error.
    """

    try:
        write_lease.ensure_durable(
            mutation_ref=mutation_ref,
            required_steps=WORKER_COMMIT_LIFECYCLE_STEPS,
        )
    except GuardedWriteError as exc:
        # FR3: record WAL/lifecycle failure before raising so the correlator
        # receives a real FailureEvent.  Swallow any collector error (TR2).
        try:
            record_failure(
                board_id,
                FailureEvent(
                    timestamp=failure_timestamp or datetime.now(timezone.utc),
                    event_kind="kg.wal.flush.failed",
                    graph_type="board",
                    correlation_id=uuid.uuid4().hex,
                ),
            )
        except Exception:
            pass
        raise RuntimeError(
            "board_graph_safe_lifecycle_failed "
            f"board_id={board_id} mutation_ref={mutation_ref} "
            f"failed_step={exc.details.get('failed_step')} "
            f"health_state_after={exc.details.get('health_state_after')} "
            f"correlation_id={exc.details.get('correlation_id')} "
            f"code={exc.code}"
        ) from exc
    return write_lease


async def _ensure_board_graph_durable(
    *,
    board_id: str,
    mutation_ref: str,
    write_lease: GuardedWriteLease,
    blocking_execution: BlockingExecutionPort | None,
    failure_timestamp: datetime | None = None,
) -> None:
    """Run the synchronous lifecycle cancellation-atomically under one fence."""

    executor = blocking_execution or _DirectBlockingExecution()

    async def _apply() -> None:
        await executor.run(
            lambda: _apply_board_graph_lifecycle_after_commit(
                board_id=board_id,
                mutation_ref=mutation_ref,
                write_lease=write_lease,
                failure_timestamp=failure_timestamp,
            )
        )

    await run_cancellation_atomic(
        _apply(),
        task_name="core.kg.consolidation_worker_graph_durability",
    )


async def _commit_consolidation_with_board_graph_lifecycle(
    *,
    entry: ConsolidationQueueRecord,
    session_id: str,
    summary_text: str,
    db: Any,
    blocking_execution: BlockingExecutionPort | None = None,
    now: datetime | None = None,
    defer_session_finalization: bool = False,
    enter_graph_write: _GraphWriteEnter | None = None,
):
    """Commit a queue item and prove the persisted graph before ACK.

    A previous implementation called ``commit_consolidation`` directly and
    then deleted the queue row. Field evidence showed the graph could be
    readable through the process handle while reopening after restart
    produced an empty/corrupt graph. This wrapper makes the ACK depend on
    the same embedded graph backend lifecycle used by explicit rebuild recovery.
    """

    mutation_ref = f"{entry.artifact_type}:{entry.artifact_id}:{session_id}"
    if _claim_token(
        entry
    ) is not None and not await _queue_claim_is_current_and_unfenced(db, entry):
        # The deterministic session is process-local and has not committed to
        # the graph yet. Drop it explicitly so a delete that won after claim
        # cannot leave an orphaned session behind.
        await _abort_open_consolidation_after_fence(
            entry=entry,
            session_id=session_id,
        )
        raise _QueueClaimLostOrFenced(f"queue_claim_lost_or_fenced entry_id={entry.id}")

    async def _commit_with_lease(write_lease: GuardedWriteLease):
        try:
            commit_resp = await commit_consolidation(
                CommitConsolidationRequest(
                    session_id=session_id,
                    summary_text=summary_text,
                ),
                agent_id=AGENT_ID,
                db=db,
                blocking_execution=blocking_execution,
                defer_session_finalization=defer_session_finalization,
            )
        finally:
            # The primitive may auto-commit and compensate before raising.
            # Drain either path under this same live lease; the outer worker
            # cannot infer possible graph effects from a missing response.
            await _ensure_board_graph_durable(
                board_id=entry.board_id,
                mutation_ref=mutation_ref,
                write_lease=write_lease,
                blocking_execution=blocking_execution,
                failure_timestamp=now,
            )
        return commit_resp

    if enter_graph_write is not None:
        return await _commit_with_lease(enter_graph_write(mutation_ref))

    with guarded_board_write(
        entry.board_id,
        operation=CONSOLIDATION_COMMIT_OPERATION,
        owner_id=(
            f"{AGENT_ID}:{entry.id}:"
            f"{_claim_token(entry) or 'direct'}"
        ),
        mutation_ref=mutation_ref,
        required_steps=WORKER_COMMIT_LIFECYCLE_STEPS,
    ) as write_lease:
        return await _commit_with_lease(write_lease)


async def _process_queue_entry_serialized(
    db: Any,
    entry: ConsolidationQueueRecord,
    *,
    blocking_execution: BlockingExecutionPort | None = None,
    clock: WorkerClockPort | None = None,
    stale_reconcile_telemetry: dict[str, object] | None = None,
    deferred_session_ids: list[str] | None = None,
    enter_graph_write: _GraphWriteEnter | None = None,
) -> bool | StaleSweepRunReceipt:
    """Process one queue row under a process-local per-board mutex.

    The queue claim contract prevents duplicate rows, but reprocess tools,
    background workers and rebuild waiters can instantiate more than one
    worker object in the same server process. embedded graph backend allows only one
    write transaction per graph. Without this guard, two workers can claim
    different rows for the same board and collide at commit time.
    """

    async with advisory_lock(entry.board_id, "consolidation"):
        return await _process_queue_entry(
            db,
            entry,
            blocking_execution=blocking_execution,
            clock=clock,
            stale_reconcile_telemetry=stale_reconcile_telemetry,
            deferred_session_ids=deferred_session_ids,
            enter_graph_write=enter_graph_write,
        )


# ---------------------------------------------------------------------------
# Adapter: SQLAlchemy artifact → DeterministicWorker dict shape
# ---------------------------------------------------------------------------


def _architecture_design_to_dict(design) -> dict:
    return {
        "id": design.id,
        "title": design.title,
        "global_description": design.global_description,
        "entities": design.entities or [],
        "interfaces": design.interfaces or [],
        "diagrams": design.diagrams or [],
        "version": design.version,
        "source_ref": design.source_ref,
        "source_version": design.source_version,
    }


def _spec_to_dict(spec: Any) -> dict:
    """Serialise a Spec row into the dict shape DeterministicWorker expects.
    Mirrors the JSON emitted by the Spec API routes so unit tests run under
    the same contract as production callers."""
    return {
        "id": spec.id,
        "board_id": spec.board_id,
        "ideation_id": getattr(spec, "ideation_id", None),
        "refinement_id": getattr(spec, "refinement_id", None),
        "title": spec.title,
        "description": spec.description,
        "context": spec.context,
        "status": getattr(
            getattr(spec, "status", None), "value", getattr(spec, "status", None)
        ),
        "functional_requirements": spec.functional_requirements or [],
        "technical_requirements": spec.technical_requirements or [],
        "acceptance_criteria": spec.acceptance_criteria or [],
        "business_rules": spec.business_rules or [],
        "test_scenarios": spec.test_scenarios or [],
        "api_contracts": spec.api_contracts or [],
        "integration_requirements": getattr(spec, "integration_requirements", None)
        or [],
        "observability_requirements": getattr(spec, "observability_requirements", None)
        or [],
        "decisions": spec.decisions or [],
        "architecture_designs": [
            _architecture_design_to_dict(design)
            for design in (getattr(spec, "architecture_designs", None) or [])
        ],
    }


def _story_to_dict(story: Any) -> dict:
    status = getattr(story, "status", None)
    return {
        "id": story.id,
        "board_id": story.board_id,
        "topic_id": story.topic_id,
        "title": story.title,
        "description": story.description,
        "actor": story.actor,
        "goal": story.goal,
        "benefit": story.benefit,
        "labels": story.labels or [],
        "status": getattr(status, "value", status) if status is not None else None,
    }


def _ideation_to_dict(ideation: Any) -> dict:
    status = getattr(ideation, "status", None)
    complexity = getattr(ideation, "complexity", None)
    return {
        "id": ideation.id,
        "board_id": ideation.board_id,
        "title": ideation.title,
        "description": ideation.description,
        "problem_statement": ideation.problem_statement,
        "proposed_approach": ideation.proposed_approach,
        "scope_assessment": ideation.scope_assessment or {},
        "complexity": getattr(complexity, "value", complexity)
        if complexity is not None
        else None,
        "status": getattr(status, "value", status) if status is not None else None,
        "labels": ideation.labels or [],
        "story_ids": [
            link.story_id
            for link in (getattr(ideation, "story_links", None) or [])
            if getattr(link, "story_id", None)
        ],
    }


def _refinement_to_dict(refinement: Any) -> dict:
    status = getattr(refinement, "status", None)
    return {
        "id": refinement.id,
        "board_id": refinement.board_id,
        "ideation_id": refinement.ideation_id,
        "title": refinement.title,
        "description": refinement.description,
        "in_scope": refinement.in_scope or [],
        "out_of_scope": refinement.out_of_scope or [],
        "analysis": refinement.analysis,
        "decisions": refinement.decisions or [],
        "status": getattr(status, "value", status) if status is not None else None,
        "labels": refinement.labels or [],
    }


def _sprint_to_dict(sprint: Any) -> dict:
    return {
        "id": sprint.id,
        "board_id": sprint.board_id,
        "title": sprint.title,
        "description": sprint.description,
        "objective": sprint.objective,
        "expected_outcome": sprint.expected_outcome,
        "status": getattr(
            getattr(sprint, "status", None), "value", getattr(sprint, "status", None)
        ),
        "spec_id": sprint.spec_id,
        "lane_type": getattr(
            getattr(sprint, "lane_type", None),
            "value",
            getattr(sprint, "lane_type", None),
        )
        or "normal",
        "origin_sprint_id": getattr(sprint, "origin_sprint_id", None),
        "origin_bug_id": getattr(sprint, "origin_bug_id", None),
    }


def _card_to_dict(card) -> dict:
    priority = getattr(card, "priority", None)
    severity = getattr(card, "severity", None)
    return {
        "id": card.id,
        "board_id": card.board_id,
        "title": card.title,
        "description": card.description,
        "status": getattr(
            getattr(card, "status", None), "value", getattr(card, "status", None)
        ),
        "card_type": getattr(card.card_type, "value", card.card_type)
        if getattr(card, "card_type", None)
        else "normal",
        "spec_id": card.spec_id,
        "sprint_id": card.sprint_id,
        "origin_task_id": getattr(card, "origin_task_id", None),
        "linked_test_task_ids": getattr(card, "linked_test_task_ids", None) or [],
        "priority": getattr(priority, "value", priority)
        if priority is not None
        else None,
        "severity": getattr(severity, "value", severity)
        if severity is not None
        else None,
        "has_minimal_evidence": _card_has_minimal_evidence(card),
        "architecture_designs": [
            _architecture_design_to_dict(design)
            for design in (getattr(card, "architecture_designs", None) or [])
        ],
    }


def _amendment_to_dict(amendment) -> dict:
    """Project an AmendmentHotfixRevision ORM row to the worker dict
    (spec 7ea1e4be). Enum fields are flattened to their string value."""
    status = getattr(amendment, "status", None)
    lineage_state = getattr(amendment, "lineage_state", None)
    return {
        "id": amendment.id,
        "board_id": amendment.board_id,
        "status": getattr(status, "value", status),
        "lineage_state": getattr(lineage_state, "value", lineage_state),
        "original_spec_id": amendment.original_spec_id,
        "origin_bug_id": amendment.origin_bug_id,
        "origin_task_ids": getattr(amendment, "origin_task_ids", None) or [],
        "affected_task_ids": getattr(amendment, "affected_task_ids", None) or [],
        "revision_spec_id": getattr(amendment, "revision_spec_id", None),
        "regression_scenario_ids": getattr(amendment, "regression_scenario_ids", None)
        or [],
        "regression_test_task_ids": getattr(amendment, "regression_test_task_ids", None)
        or [],
        "automated_regression_refs": getattr(
            amendment, "automated_regression_refs", None
        )
        or [],
    }


def _worker_node_to_candidate(node: EmittedNode) -> NodeCandidate:
    return NodeCandidate(
        candidate_id=node.candidate_id,
        node_type=KGNodeType(node.node_type),
        title=node.title,
        content=node.content,
        context=node.context or None,
        source_artifact_ref=node.source_artifact_ref,
        graph_layer=node.graph_layer,
        maturity_status=node.maturity_status,
        source_confidence=node.source_confidence,
        priority_boost=node.priority_boost,
    )


def _layer_attrs_for_artifact(
    artifact_type: str,
    status: Any,
    *,
    has_minimal_evidence: bool = True,
    lineage_complete: bool = True,
) -> tuple[str, str]:
    classification = classify_source_for_kg(
        artifact_type=artifact_type,
        artifact_status=status,
        content_hash="consolidation-lineage",
        has_minimal_evidence=has_minimal_evidence,
        lineage_complete=lineage_complete,
    )
    graph_layer = classification.graph_layer
    if graph_layer == GRAPH_LAYER_NONE:
        graph_layer = GRAPH_LAYER_WORKING
    return graph_layer, classification.maturity_status


def _card_source_artifact_type(card_type: Any) -> str:
    normalized = str(card_type or "normal").lower()
    if normalized == "test":
        return "test"
    if normalized == "bug":
        return "bug"
    return "task"


def _card_has_minimal_evidence(card: Any) -> bool:
    card_type = (
        getattr(card.card_type, "value", card.card_type) if card.card_type else "normal"
    )
    if card_type != "bug":
        return True
    has_text = any(
        str(getattr(card, field, "") or "").strip()
        for field in ("observed_behavior", "expected_behavior", "steps_to_reproduce")
    )
    return has_text and (
        bool(getattr(card, "linked_test_task_ids", None))
        or bool(getattr(card, "conclusions", None))
    )


def _worker_edge_to_candidate(edge: EmittedEdge) -> EdgeCandidate:
    return EdgeCandidate(
        candidate_id=edge.candidate_id,
        edge_type=KGEdgeType(edge.edge_type),
        from_candidate_id=edge.from_candidate_id,
        to_candidate_id=edge.to_candidate_id,
        confidence=edge.confidence,
        layer=edge.layer,
        rule_id=edge.rule_id,
        created_by=edge.created_by,
        fallback_reason=edge.fallback_reason or None,
    )


def _run_deterministic_worker(
    entry: ConsolidationQueueRecord, artifact: Any
) -> WorkerResult:
    worker = DeterministicWorker()
    if entry.artifact_type == "story":
        return worker.process_story(_story_to_dict(artifact))
    if entry.artifact_type == "ideation":
        return worker.process_ideation(_ideation_to_dict(artifact))
    if entry.artifact_type == "refinement":
        return worker.process_refinement(_refinement_to_dict(artifact))
    if entry.artifact_type == "spec":
        return worker.process_spec(_spec_to_dict(artifact))
    if entry.artifact_type == "sprint":
        return worker.process_sprint(_sprint_to_dict(artifact))
    if entry.artifact_type == "card":
        return worker.process_card(_card_to_dict(artifact))
    if entry.artifact_type == "amendment_hotfix_revision":
        return worker.process_amendment(_amendment_to_dict(artifact))
    raise ValueError(f"unknown artifact_type: {entry.artifact_type}")


def _edge_exists(result: WorkerResult, candidate_id: str) -> bool:
    return any(edge.candidate_id == candidate_id for edge in result.edges)


def _node_exists(result: WorkerResult, candidate_id: str) -> bool:
    return any(node.candidate_id == candidate_id for node in result.nodes)


def _append_card_entity_node(result: WorkerResult, card: Any) -> str:
    cid = f"card_{card.id[:8]}_entity"
    card_type = (
        getattr(card.card_type, "value", card.card_type) if card.card_type else "normal"
    )
    if not _node_exists(result, cid):
        graph_layer, maturity_status = _layer_attrs_for_artifact(
            _card_source_artifact_type(card_type),
            getattr(
                getattr(card, "status", None), "value", getattr(card, "status", None)
            ),
            has_minimal_evidence=_card_has_minimal_evidence(card),
        )
        result.nodes.append(
            EmittedNode(
                candidate_id=cid,
                node_type="Bug" if card_type == "bug" else "Entity",
                title=card.title or f"Card {card.id}",
                content=card.description or "",
                source_artifact_ref=f"card:{card.id}",
                graph_layer=graph_layer,
                maturity_status=maturity_status,
                source_confidence=1.0,
            )
        )
    if getattr(card, "board_id", None):
        _attach_entity_node_to_board_root(
            result,
            board_id=card.board_id,
            child_candidate_id=cid,
            rule_slot="card",
        )
    return cid


def _append_card_edge_target_entity_node(result: WorkerResult, card: Any) -> str:
    """Append an Entity projection for card relationship targets.

    Bug cards are materialized as ``Bug`` by their own consolidation path, but
    deterministic ``originates_from``/``covered_by`` endpoint contracts require
    the target side to be an ``Entity``. When a bug references another bug card,
    use a separate relationship-target projection instead of weakening the KG
    schema to allow ``Bug -> Bug``.
    """

    card_type = (
        getattr(card.card_type, "value", card.card_type) if card.card_type else "normal"
    )
    if card_type != "bug":
        return _append_card_entity_node(result, card)

    cid = f"card_{card.id[:8]}_target_entity"
    if not _node_exists(result, cid):
        graph_layer, maturity_status = _layer_attrs_for_artifact(
            _card_source_artifact_type(card_type),
            getattr(
                getattr(card, "status", None), "value", getattr(card, "status", None)
            ),
            has_minimal_evidence=_card_has_minimal_evidence(card),
        )
        result.nodes.append(
            EmittedNode(
                candidate_id=cid,
                node_type="Entity",
                title=card.title or f"Card {card.id}",
                content=card.description or "",
                source_artifact_ref=f"card_relationship_target:{card.id}",
                graph_layer=graph_layer,
                maturity_status=maturity_status,
                source_confidence=1.0,
            )
        )
    if getattr(card, "board_id", None):
        _attach_entity_node_to_board_root(
            result,
            board_id=card.board_id,
            child_candidate_id=cid,
            rule_slot="card_relationship_target",
        )
    return cid


def _append_spec_entity_node(result: WorkerResult, spec: Any) -> str:
    cid = f"spec_{spec.id[:8]}_entity"
    if not _node_exists(result, cid):
        content = "\n\n".join(
            p
            for p in (
                getattr(spec, "description", None),
                getattr(spec, "context", None),
            )
            if p
        )
        graph_layer, maturity_status = _layer_attrs_for_artifact(
            "spec",
            getattr(
                getattr(spec, "status", None), "value", getattr(spec, "status", None)
            ),
        )
        result.nodes.append(
            EmittedNode(
                candidate_id=cid,
                node_type="Entity",
                title=getattr(spec, "title", None) or f"Spec {spec.id}",
                content=content or getattr(spec, "title", None) or "",
                source_artifact_ref=f"spec:{spec.id}",
                graph_layer=graph_layer,
                maturity_status=maturity_status,
                source_confidence=1.0,
            )
        )
    if getattr(spec, "board_id", None):
        _attach_entity_node_to_board_root(
            result,
            board_id=spec.board_id,
            child_candidate_id=cid,
            rule_slot="spec",
        )
    return cid


def _append_story_entity_node(result: WorkerResult, story: Any) -> str:
    cid = f"story_{story.id[:8]}_entity"
    if _node_exists(result, cid):
        return cid
    graph_layer, maturity_status = _layer_attrs_for_artifact(
        "story",
        getattr(
            getattr(story, "status", None), "value", getattr(story, "status", None)
        ),
    )
    result.nodes.append(
        EmittedNode(
            candidate_id=cid,
            node_type="Entity",
            title=story.title or f"Story {story.id}",
            content=story.description or "",
            source_artifact_ref=f"story:{story.id}",
            graph_layer=graph_layer,
            maturity_status=maturity_status,
            source_confidence=1.0,
        )
    )
    return cid


def _append_ideation_entity_node(result: WorkerResult, ideation: Any) -> str:
    cid = f"ideation_{ideation.id[:8]}_entity"
    if _node_exists(result, cid):
        return cid
    content = "\n\n".join(
        p
        for p in (
            ideation.description,
            ideation.problem_statement,
            ideation.proposed_approach,
        )
        if p
    )
    graph_layer, maturity_status = _layer_attrs_for_artifact(
        "ideation",
        getattr(
            getattr(ideation, "status", None),
            "value",
            getattr(ideation, "status", None),
        ),
    )
    result.nodes.append(
        EmittedNode(
            candidate_id=cid,
            node_type="Entity",
            title=ideation.title or f"Ideation {ideation.id}",
            content=content or ideation.title or "",
            source_artifact_ref=f"ideation:{ideation.id}",
            graph_layer=graph_layer,
            maturity_status=maturity_status,
            source_confidence=1.0,
        )
    )
    return cid


def _append_refinement_entity_node(result: WorkerResult, refinement: Any) -> str:
    cid = f"refinement_{refinement.id[:8]}_entity"
    if _node_exists(result, cid):
        return cid
    content = "\n\n".join(p for p in (refinement.description, refinement.analysis) if p)
    graph_layer, maturity_status = _layer_attrs_for_artifact(
        "refinement",
        getattr(
            getattr(refinement, "status", None),
            "value",
            getattr(refinement, "status", None),
        ),
    )
    result.nodes.append(
        EmittedNode(
            candidate_id=cid,
            node_type="Entity",
            title=refinement.title or f"Refinement {refinement.id}",
            content=content or refinement.title or "",
            source_artifact_ref=f"refinement:{refinement.id}",
            graph_layer=graph_layer,
            maturity_status=maturity_status,
            source_confidence=1.0,
        )
    )
    return cid


def _board_root_candidate_id(board_id: str) -> str:
    return f"board_{board_id[:8]}_entity"


def _append_board_root_entity_node(result: WorkerResult, board_id: str) -> str:
    cid = _board_root_candidate_id(board_id)
    if _node_exists(result, cid):
        return cid
    result.nodes.append(
        EmittedNode(
            candidate_id=cid,
            node_type="Entity",
            title=f"Board {board_id}",
            content="Deterministic KG board root.",
            source_artifact_ref=f"board:{board_id}",
            source_confidence=1.0,
        )
    )
    return cid


def _attach_entity_node_to_board_root(
    result: WorkerResult,
    *,
    board_id: str,
    child_candidate_id: str,
    rule_slot: str,
) -> None:
    board_root_id = _append_board_root_entity_node(result, board_id)
    edge_id = f"{child_candidate_id}_belongs_to_board"
    if _edge_exists(result, edge_id):
        return
    result.edges.append(
        EmittedEdge(
            candidate_id=edge_id,
            edge_type="belongs_to",
            from_candidate_id=child_candidate_id,
            to_candidate_id=board_root_id,
            confidence=1.0,
            rule_id=f"belongs_to/{rule_slot}_to_board@{WORKER_VERSION}",
        )
    )


async def _materialize_lineage_endpoint_nodes(
    db: Any,
    entry: ConsolidationQueueRecord,
    artifact,
    result: WorkerResult,
) -> WorkerResult:
    """Add local parent/root nodes needed by deterministic lineage edges.

    Queue ordering is best-effort, but event-driven consolidation can process
    a child before its parent. Materialising the parent node in the same
    session keeps belongs_to edges from being silently skipped.
    """
    if entry.artifact_type == "ideation":
        story_ids = [
            link.story_id
            for link in (getattr(artifact, "story_links", None) or [])
            if getattr(link, "story_id", None)
        ]
        if story_ids:
            stories = await get_consolidation_persistence_port().list_artifacts(
                db,
                artifact_type="story",
                artifact_ids=story_ids,
            )
            for story in stories:
                story_cid = _append_story_entity_node(result, story)
                _attach_entity_node_to_board_root(
                    result,
                    board_id=entry.board_id,
                    child_candidate_id=story_cid,
                    rule_slot="story",
                )
        return result

    if entry.artifact_type == "refinement" and getattr(artifact, "ideation_id", None):
        ideation = await get_consolidation_persistence_port().load_artifact(
            db,
            artifact_type="ideation",
            artifact_id=artifact.ideation_id,
        )
        if ideation is not None:
            ideation_cid = _append_ideation_entity_node(result, ideation)
            _attach_entity_node_to_board_root(
                result,
                board_id=entry.board_id,
                child_candidate_id=ideation_cid,
                rule_slot="ideation",
            )
        return result

    if entry.artifact_type == "spec":
        if getattr(artifact, "refinement_id", None):
            refinement = await get_consolidation_persistence_port().load_artifact(
                db,
                artifact_type="refinement",
                artifact_id=artifact.refinement_id,
            )
            if refinement is not None:
                refinement_cid = _append_refinement_entity_node(result, refinement)
                _attach_entity_node_to_board_root(
                    result,
                    board_id=entry.board_id,
                    child_candidate_id=refinement_cid,
                    rule_slot="refinement",
                )
        elif getattr(artifact, "ideation_id", None):
            ideation = await get_consolidation_persistence_port().load_artifact(
                db,
                artifact_type="ideation",
                artifact_id=artifact.ideation_id,
            )
            if ideation is not None:
                ideation_cid = _append_ideation_entity_node(result, ideation)
                _attach_entity_node_to_board_root(
                    result,
                    board_id=entry.board_id,
                    child_candidate_id=ideation_cid,
                    rule_slot="ideation",
                )
        return result

    return result


def _test_scenario_content(ts: object) -> str:
    if not isinstance(ts, dict):
        return str(ts)
    parts = [
        f"Given: {ts.get('given', '')}",
        f"When: {ts.get('when', '')}",
        f"Then: {ts.get('then', '')}",
    ]
    return "\n".join(p for p in parts if p.split(": ", 1)[1])


def _scenario_candidate_for_id(
    spec: Any, scenario_id: str
) -> tuple[str, EmittedNode] | None:
    for index, ts in enumerate(spec.test_scenarios or []):
        if not isinstance(ts, dict):
            continue
        raw_id = ts.get("id") or ts.get("scenario_id")
        if str(raw_id) != str(scenario_id):
            continue
        cid = f"spec_{spec.id[:8]}_ts_{index}"
        title = ts.get("title") or f"TS-{index + 1}"
        return cid, EmittedNode(
            candidate_id=cid,
            node_type="TestScenario",
            title=title,
            content=_test_scenario_content(ts),
            source_artifact_ref=_spec_child_ref(spec.id, "test_scenario", ts, index),
            source_confidence=1.0,
        )
    return None


def _suggested_ref(candidate, prefix: str) -> str | None:
    for suggested in candidate.suggested_candidates or []:
        value = str(suggested)
        if value.startswith(prefix):
            return value.split(":", 1)[1]
    return None


async def _resolve_missing_link_candidates(
    db: Any,
    board_id: str,
    result: WorkerResult,
) -> WorkerResult:
    """Resolve structured cross-artifact Bug links before commit.

    The deterministic worker remains pure and emits MissingLinkCandidate rows
    when it sees a foreign key that requires repository lookup. This adapter
    turns the resolvable cases into final deterministic edges and keeps truly
    unresolved candidates available for audit/fallback.
    """
    origin_ids: set[str] = set()
    test_task_ids: set[str] = set()
    for candidate in result.missing_link_candidates:
        if candidate.reason == "origin_task_requires_cross_artifact_resolution":
            ref = _suggested_ref(candidate, "task:")
            if ref:
                origin_ids.add(ref)
        elif candidate.reason == "linked_test_task_requires_cross_artifact_resolution":
            ref = _suggested_ref(candidate, "test_task:")
            if ref:
                test_task_ids.add(ref)

    target_ids = origin_ids | test_task_ids
    if not target_ids:
        return result

    rows = await get_consolidation_persistence_port().list_artifacts(
        db,
        artifact_type="card",
        artifact_ids=tuple(target_ids),
        board_id=board_id,
    )
    cards_by_id = {card.id: card for card in rows}

    specs_by_id: dict[str, Any] = {}
    spec_ids = {
        card.spec_id
        for card in cards_by_id.values()
        if card.id in test_task_ids and card.spec_id
    }
    if spec_ids:
        specs = await get_consolidation_persistence_port().list_artifacts(
            db,
            artifact_type="spec",
            artifact_ids=tuple(spec_ids),
            board_id=board_id,
        )
        specs_by_id = {spec.id: spec for spec in specs}

    unresolved = []
    resolved_count = 0
    for candidate in result.missing_link_candidates:
        if candidate.reason == "origin_task_requires_cross_artifact_resolution":
            origin_id = _suggested_ref(candidate, "task:")
            origin_card = cards_by_id.get(origin_id or "")
            if origin_card is None:
                unresolved.append(candidate)
                continue
            target_cid = _append_card_edge_target_entity_node(result, origin_card)
            edge_id = (
                f"{candidate.from_candidate_id}_originates_from_{origin_card.id[:8]}"
            )
            if not _edge_exists(result, edge_id):
                result.edges.append(
                    EmittedEdge(
                        candidate_id=edge_id,
                        edge_type="originates_from",
                        from_candidate_id=candidate.from_candidate_id,
                        to_candidate_id=target_cid,
                        confidence=1.0,
                        rule_id=f"originates_from/origin_task_id@{WORKER_VERSION}",
                    )
                )
            resolved_count += 1
            continue

        if candidate.reason == "linked_test_task_requires_cross_artifact_resolution":
            test_task_id = _suggested_ref(candidate, "test_task:")
            test_card = cards_by_id.get(test_task_id or "")
            if test_card is None:
                unresolved.append(candidate)
                continue
            target_cid = _append_card_edge_target_entity_node(result, test_card)
            edge_id = (
                f"{candidate.from_candidate_id}_covered_by_card_{test_card.id[:8]}"
            )
            if not _edge_exists(result, edge_id):
                result.edges.append(
                    EmittedEdge(
                        candidate_id=edge_id,
                        edge_type="covered_by",
                        from_candidate_id=candidate.from_candidate_id,
                        to_candidate_id=target_cid,
                        confidence=1.0,
                        rule_id=f"covered_by/linked_test_task_id@{WORKER_VERSION}",
                    )
                )

            spec = specs_by_id.get(test_card.spec_id or "")
            for scenario_id in test_card.test_scenario_ids or []:
                if spec is None:
                    continue
                scenario = _scenario_candidate_for_id(spec, str(scenario_id))
                if scenario is None:
                    continue
                spec_cid = _append_spec_entity_node(result, spec)
                scenario_cid, scenario_node = scenario
                if not _node_exists(result, scenario_cid):
                    result.nodes.append(scenario_node)
                scenario_belongs_edge_id = (
                    f"{scenario_cid}_belongs_to_spec_{spec.id[:8]}"
                )
                if not _edge_exists(result, scenario_belongs_edge_id):
                    result.edges.append(
                        EmittedEdge(
                            candidate_id=scenario_belongs_edge_id,
                            edge_type="belongs_to",
                            from_candidate_id=scenario_cid,
                            to_candidate_id=spec_cid,
                            confidence=1.0,
                            rule_id=f"belongs_to/bug_linked_test_scenario@{WORKER_VERSION}",
                        )
                    )
                scenario_edge_id = (
                    f"{candidate.from_candidate_id}_covered_by_ts_"
                    f"{spec.id[:8]}_{str(scenario_id)[:8]}"
                )
                if not _edge_exists(result, scenario_edge_id):
                    result.edges.append(
                        EmittedEdge(
                            candidate_id=scenario_edge_id,
                            edge_type="covered_by",
                            from_candidate_id=candidate.from_candidate_id,
                            to_candidate_id=scenario_cid,
                            confidence=1.0,
                            rule_id=f"covered_by/linked_test_scenario@{WORKER_VERSION}",
                        )
                    )
            resolved_count += 1
            continue

        unresolved.append(candidate)

    result.missing_link_candidates = unresolved
    if resolved_count:
        logger.info(
            "consolidation.missing_links_resolved board=%s resolved=%d unresolved=%d",
            board_id,
            resolved_count,
            len(unresolved),
            extra={
                "event": "kg.consolidation.missing_links_resolved",
                "board_id": board_id,
                "resolved_count": resolved_count,
                "unresolved_count": len(unresolved),
            },
        )
    return result


# ---------------------------------------------------------------------------
# Process a single queue entry
# ---------------------------------------------------------------------------


async def _rollback_post_commit_maintenance_failure(
    db: Any,
    *,
    stage: str,
    entry: ConsolidationQueueRecord,
) -> None:
    """Restore the relational context after a best-effort hook fails.

    A SQLAlchemy flush error leaves its transaction unusable until rollback.
    The consolidation processor only depends on the persistence port here so
    edition-specific session behavior stays outside Core.  A rollback failure
    is deliberately re-raised: ``process_batch`` will then abandon this scope
    and perform its normal at-least-once retry with a fresh session.
    """

    try:
        await get_consolidation_persistence_port().rollback(db)
    except Exception:
        logger.exception(
            "kg.post_commit.rollback_failed stage=%s board=%s artifact=%s:%s",
            stage,
            entry.board_id,
            entry.artifact_type,
            entry.artifact_id,
        )
        raise


async def _run_post_commit_maintenance(
    db: Any,
    *,
    entry: ConsolidationQueueRecord,
    session_id: str,
) -> None:
    """Run relational maintenance without carrying a poisoned transaction.

    Each hook remains best effort after the graph commit: a hook failure is
    logged, rolled back through the Core persistence port, and the next hook
    may proceed.  If transactional recovery itself fails, the exception must
    reach the outer worker so the context is discarded and retried safely.
    """

    try:
        debt_result = await mark_canonical_debt_committed_for_artifact(
            db,
            board_id=entry.board_id,
            artifact_type=entry.artifact_type,
            artifact_id=entry.artifact_id,
            actor_id=AGENT_ID,
            evidence_ref=f"kg_session:{session_id}",
        )
        if debt_result["committed_count"]:
            logger.info(
                "canonical_debt.resolved board=%s artifact=%s:%s count=%d",
                entry.board_id,
                entry.artifact_type,
                entry.artifact_id,
                debt_result["committed_count"],
            )
    except Exception:
        logger.exception(
            "canonical_debt.resolve_failed board=%s artifact=%s:%s",
            entry.board_id,
            entry.artifact_type,
            entry.artifact_id,
        )
        await _rollback_post_commit_maintenance_failure(
            db,
            stage="canonical_debt.resolve",
            entry=entry,
        )

    # R7 IMP2: keep the canonical Learning partition-integrity ledger current.
    try:
        from okto_pulse.core.kg.canonical_learning_partition import (
            run_canonical_learning_partition_maintenance,
        )

        await run_canonical_learning_partition_maintenance(
            db, board_id=entry.board_id, actor_id=AGENT_ID
        )
    except Exception:
        logger.exception(
            "kg.clp.maintenance_failed board=%s artifact=%s:%s",
            entry.board_id,
            entry.artifact_type,
            entry.artifact_id,
        )
        await _rollback_post_commit_maintenance_failure(
            db,
            stage="canonical_learning_partition.maintenance",
            entry=entry,
        )

    # Use the propagating replay primitive here.  The convenience
    # ``replay_canonical_debt_post_commit`` wrapper intentionally swallows
    # errors, which prevents this worker from rolling back a failed flush.
    try:
        from okto_pulse.core.kg.canonical_debt_replay import (
            replay_canonical_debt_by_maturity,
        )

        replay_result = await replay_canonical_debt_by_maturity(
            db, board_id=entry.board_id
        )
        if replay_result.get("committed_count"):
            logger.info(
                "kg.canonical_debt_replay.committed board=%s count=%d",
                entry.board_id,
                replay_result["committed_count"],
                extra={
                    "event": "kg.canonical_debt_replay.committed",
                    "board_id": entry.board_id,
                    "committed_count": replay_result["committed_count"],
                },
            )
    except Exception:
        logger.exception(
            "kg.canonical_debt_replay.post_commit_failed board=%s artifact=%s:%s",
            entry.board_id,
            entry.artifact_type,
            entry.artifact_id,
        )
        await _rollback_post_commit_maintenance_failure(
            db,
            stage="canonical_debt.replay",
            entry=entry,
        )


async def _process_stale_sweep_entry(
    db: Any,
    entry: ConsolidationQueueRecord,
    *,
    clock: WorkerClockPort | None = None,
) -> StaleSweepRunReceipt | bool:
    """Enumerate one bounded catch-up page and stage its durable checkpoint."""

    validated = _validated_stale_sweep_payload(entry)
    claim_token = _claim_token(entry)
    if validated is None or claim_token is None:
        logger.error(
            "kg.stale_sweep.invalid_payload entry=%s board=%s payload=%r",
            entry.id,
            entry.board_id,
            entry.payload,
        )
        return False
    cursor, budget, attempt = validated
    now = clock.now() if clock is not None else datetime.now(timezone.utc)
    sweep_port = get_stale_sweep_port()

    async def _reschedule(reason: str) -> StaleSweepRunReceipt:
        receipt = await sweep_port.reschedule_stale_sweep(
            db,
            StaleSweepRescheduleRequest(
                entry_id=entry.id,
                claim_token=claim_token,
                board_id=entry.board_id,
                cursor=cursor,
                budget=budget,
                attempt=attempt,
                retry_at=_stale_sweep_retry_at(now),
                reason=reason,
            ),
        )
        _log_stale_sweep_receipt(receipt)
        return receipt

    store = get_consolidation_persistence_port()
    if not await store.board_exists(db, board_id=entry.board_id):
        return await _reschedule("board_absent")
    try:
        graph_available = get_kg_registry().graph_runtime_store.exists(entry.board_id)
    except Exception:
        logger.exception(
            "kg.stale_sweep.graph_runtime_probe_failed entry=%s board=%s",
            entry.id,
            entry.board_id,
        )
        return await _reschedule("graph_runtime_probe_failed")
    if not graph_available:
        return await _reschedule("graph_unavailable")

    from okto_pulse.core.kg.canonical_stale_reconciler import (
        enumerate_stale_sweep_page,
    )

    page = await enumerate_stale_sweep_page(
        entry.board_id,
        cursor=cursor,
        budget=budget,
    )
    if not page.complete:
        return await _reschedule(
            str(page.incomplete_cause or "sweep_inventory_incomplete")
        )

    if (
        page.board_id != entry.board_id
        or page.cursor != cursor
        or page.budget != budget
    ):
        return await _reschedule("sweep_page_contract_invalid")
    try:
        batch_request = StaleSweepBatchRequest(
            entry_id=entry.id,
            claim_token=claim_token,
            board_id=entry.board_id,
            cursor=cursor,
            budget=budget,
            attempt=attempt,
            candidates=page.candidates,
            next_cursor=page.next_cursor,
            has_more=page.has_more,
            now=now,
        )
    except ValueError:
        logger.exception(
            "kg.stale_sweep.page_contract_invalid entry=%s board=%s",
            entry.id,
            entry.board_id,
        )
        return await _reschedule("sweep_page_contract_invalid")
    receipt = await sweep_port.stage_stale_sweep_batch(db, batch_request)
    _log_stale_sweep_receipt(receipt)
    return receipt


async def _process_stale_reconcile_entry(
    db: Any,
    entry: ConsolidationQueueRecord,
    *,
    blocking_execution: BlockingExecutionPort | None = None,
    clock: WorkerClockPort | None = None,
    telemetry_details: dict[str, object] | None = None,
    enter_graph_write: _GraphWriteEnter | None = None,
) -> bool:
    """Drain one governed-delete reconciliation intent without source loading."""

    source_refs = _validated_stale_reconcile_source_refs(entry)
    if source_refs is None:
        logger.error(
            "kg.stale_reconcile.invalid_payload entry=%s artifact=%s:%s generation=%s",
            entry.id,
            entry.artifact_type,
            entry.artifact_id,
            _generation(entry),
        )
        return False

    # This is the final storage read before the graph write. It jointly checks
    # ownership and the exact tombstone generation/event while the board's
    # process-local writer mutex is held by the serialized caller.
    if not await _queue_claim_is_current_and_unfenced(db, entry):
        raise _QueueClaimLostOrFenced(f"queue_claim_lost_or_fenced entry_id={entry.id}")

    from okto_pulse.core.kg.canonical_stale_reconciler import (
        reconcile_stale_canonical,
    )

    delete_event_id = _delete_event_id(entry)
    mutation_ref = (
        f"stale_reconcile:{entry.artifact_type}:{entry.artifact_id}:"
        f"g{_generation(entry)}:{delete_event_id}"
    )

    async def _reconcile_with_enter(
        enter_write: _GraphWriteEnter,
    ):
        write_lease: GuardedWriteLease | None = None
        callback_invoked = False

        def _before_graph_write() -> None:
            nonlocal callback_invoked, write_lease
            if callback_invoked:
                raise RuntimeError(
                    "stale_reconcile_graph_write_callback_repeated"
                )
            callback_invoked = True
            write_lease = enter_write(mutation_ref)

        try:
            result = await reconcile_stale_canonical(
                db,
                board_id=entry.board_id,
                source_refs=source_refs,
                correlation_id=delete_event_id,
                before_graph_write=_before_graph_write,
                blocking_execution=blocking_execution,
            )
        except BaseException:
            if write_lease is not None:
                try:
                    await _ensure_board_graph_durable(
                        board_id=entry.board_id,
                        mutation_ref=f"{mutation_ref}:exception",
                        write_lease=write_lease,
                        blocking_execution=blocking_execution,
                        failure_timestamp=(
                            clock.now() if clock is not None else None
                        ),
                    )
                except BaseException as durability_exc:
                    logger.error(
                        "kg.stale_reconcile.exception_durability_ambiguous "
                        "entry=%s board=%s durability_applied=%s "
                        "write_may_be_applied=true lifecycle_error=%s",
                        entry.id,
                        entry.board_id,
                        write_lease.durability_applied,
                        type(durability_exc).__name__,
                        exc_info=True,
                        extra={
                            "event": (
                                "kg.stale_reconcile."
                                "exception_durability_ambiguous"
                            ),
                            "board_id": entry.board_id,
                            "entry_id": entry.id,
                            "durability_applied": (
                                write_lease.durability_applied
                            ),
                            "write_may_be_applied": True,
                        },
                    )
            raise

        if write_lease is not None:
            await _ensure_board_graph_durable(
                board_id=entry.board_id,
                mutation_ref=mutation_ref,
                write_lease=write_lease,
                blocking_execution=blocking_execution,
                failure_timestamp=clock.now() if clock is not None else None,
            )
        return result

    if enter_graph_write is not None:
        result = await _reconcile_with_enter(enter_graph_write)
    else:
        with ExitStack() as local_write_stack:

            def _enter_local_write(local_mutation_ref: str):
                return local_write_stack.enter_context(
                    guarded_board_write(
                        entry.board_id,
                        operation=CONSOLIDATION_COMMIT_OPERATION,
                        owner_id=(
                            f"{AGENT_ID}:{entry.id}:"
                            f"{_claim_token(entry) or 'direct'}"
                        ),
                        mutation_ref=local_mutation_ref,
                        required_steps=WORKER_COMMIT_LIFECYCLE_STEPS,
                    )
                )

            result = await _reconcile_with_enter(_enter_local_write)

    run_details = _stale_reconcile_telemetry_details(result, entry)
    if telemetry_details is not None:
        telemetry_details.update(run_details)

    if not _stale_reconcile_is_complete(
        result,
        previous_error=entry.last_error,
    ):
        logger.error(
            "kg.stale_reconcile.incomplete entry=%s board=%s failed_types=%s",
            entry.id,
            entry.board_id,
            (
                result.get("failed_types", ())
                if isinstance(result, dict)
                else getattr(result, "failed_types", ())
            ),
            extra={
                "event": "kg.stale_reconcile.run",
                "board_id": entry.board_id,
                "entry_id": entry.id,
                "delete_event_id": delete_event_id,
                "generation": _generation(entry),
                **run_details,
            },
        )
        return False

    logger.info(
        "kg.stale_reconcile.completed entry=%s board=%s generation=%d",
        entry.id,
        entry.board_id,
        _generation(entry),
        extra={
            "event": "kg.stale_reconcile.run",
            "board_id": entry.board_id,
            "entry_id": entry.id,
            "delete_event_id": delete_event_id,
            "generation": _generation(entry),
            **run_details,
        },
    )
    return True


async def _process_queue_entry(
    db: Any,
    entry: ConsolidationQueueRecord,
    *,
    blocking_execution: BlockingExecutionPort | None = None,
    clock: WorkerClockPort | None = None,
    stale_reconcile_telemetry: dict[str, object] | None = None,
    deferred_session_ids: list[str] | None = None,
    enter_graph_write: _GraphWriteEnter | None = None,
) -> bool | StaleSweepRunReceipt:
    """Process one queue entry through the primitives pipeline.
    Returns True on success, False on failure."""

    if _work_kind(entry) == "stale_sweep":
        return await _process_stale_sweep_entry(
            db,
            entry,
            clock=clock,
        )
    if _work_kind(entry) == "stale_reconcile":
        return await _process_stale_reconcile_entry(
            db,
            entry,
            blocking_execution=blocking_execution,
            clock=clock,
            telemetry_details=stale_reconcile_telemetry,
            enter_graph_write=enter_graph_write,
        )
    if _work_kind(entry) != "consolidate":
        logger.warning(
            "unsupported consolidation work_kind: %s",
            _work_kind(entry),
        )
        return False

    if entry.artifact_type not in {
        "story",
        "ideation",
        "refinement",
        "spec",
        "sprint",
        "card",
        "amendment_hotfix_revision",
    }:
        logger.warning("unknown artifact_type: %s", entry.artifact_type)
        return False
    artifact = await get_consolidation_persistence_port().load_artifact(
        db,
        artifact_type=entry.artifact_type,
        artifact_id=entry.artifact_id,
    )
    if not artifact:
        logger.warning(
            "%s not found: %s",
            entry.artifact_type,
            entry.artifact_id,
        )
        # RKG-04 AC3 (ts_317b11ef): a missing source row is a persistent
        # failure and must stay visible — False routes the entry through
        # _mark_failed -> backoff -> ConsolidationDeadLetter where diagnose
        # keeps it actionable. True would mask it as success and falsely
        # clear the connectivity class (stale legacy entries included).
        return False

    artifact_status = getattr(artifact, "status", None)
    artifact_status = getattr(artifact_status, "value", artifact_status)
    if bool(getattr(artifact, "archived", False)):
        artifact_status = "archived"
    maturity_artifact_type = entry.artifact_type
    if entry.artifact_type == "card":
        card_type = getattr(artifact, "card_type", None)
        card_type = getattr(card_type, "value", card_type)
        maturity_artifact_type = _card_source_artifact_type(card_type)
    classification = classify_source_for_kg(
        artifact_type=maturity_artifact_type,
        artifact_status=artifact_status,
        content_hash="consolidation-admission",
        has_minimal_evidence=(
            _card_has_minimal_evidence(artifact)
            if entry.artifact_type == "card"
            else True
        ),
        lineage_complete=(
            str(getattr(artifact, "lineage_state", "") or "").strip().lower()
            == "complete"
            if entry.artifact_type == "amendment_hotfix_revision"
            else True
        ),
    )
    if classification.disposition == DISPOSITION_SKIPPED_CANCELLED:
        logger.info(
            "consolidation.skipped_cancelled board=%s artifact=%s:%s status=%s",
            entry.board_id,
            entry.artifact_type,
            entry.artifact_id,
            artifact_status,
            extra={
                "event": "kg.consolidation.skipped_cancelled",
                "board_id": entry.board_id,
                "artifact_type": entry.artifact_type,
                "artifact_id": entry.artifact_id,
                "artifact_status": str(artifact_status or ""),
            },
        )
        return True

    worker_result = _run_deterministic_worker(entry, artifact)
    worker_result = await _materialize_lineage_endpoint_nodes(
        db,
        entry,
        artifact,
        worker_result,
    )
    worker_result = await _resolve_missing_link_candidates(
        db,
        entry.board_id,
        worker_result,
    )
    node_candidates = [_worker_node_to_candidate(n) for n in worker_result.nodes]
    edge_candidates = [_worker_edge_to_candidate(e) for e in worker_result.edges]
    raw_content = worker_result.raw_content

    logger.info(
        "consolidation.extracted board=%s artifact=%s:%s nodes=%d edges=%d missing=%d",
        entry.board_id,
        entry.artifact_type,
        entry.artifact_id,
        len(node_candidates),
        len(edge_candidates),
        len(worker_result.missing_link_candidates),
    )

    if not node_candidates:
        return True  # nothing to do, but not a failure

    # 1. begin_consolidation (db=None to skip dedup — historical is forced re-processing)
    begin_resp = await begin_consolidation(
        BeginConsolidationRequest(
            board_id=entry.board_id,
            artifact_type=entry.artifact_type,
            artifact_id=entry.artifact_id,
            raw_content=raw_content,
            deterministic_candidates=node_candidates,
        ),
        agent_id=AGENT_ID,
        db=None,
        force_reprocess=True,
        spec_lineage_parent_intent=getattr(
            worker_result,
            "spec_lineage_parent_intent",
            SpecLineageParentIntent.PRESERVE,
        ),
    )
    session_id = begin_resp.session_id
    if deferred_session_ids is not None:
        # ``process_batch`` owns the relational UOW and must be able to
        # compensate this session if any later await is cancelled or fails.
        # Register immediately after begin, before the first fallible step.
        deferred_session_ids.append(session_id)

    # 2. Add edge candidates
    for edge in edge_candidates:
        await add_edge_candidate(
            AddEdgeCandidateRequest(session_id=session_id, candidate=edge),
            agent_id=AGENT_ID,
        )

    # 3. propose_reconciliation
    await propose_reconciliation(
        ProposeReconciliationRequest(session_id=session_id),
        agent_id=AGENT_ID,
        db=None,
        force_reprocess=True,
    )

    # A governed delete may commit while extraction/proposal is paused. Check
    # at the final publication boundary, aborting the uncommitted session
    # before the commit/lifecycle wrapper is entered.
    if _claim_token(
        entry
    ) is not None and not await _queue_claim_is_current_and_unfenced(db, entry):
        await _abort_open_consolidation_after_fence(
            entry=entry,
            session_id=session_id,
        )
        raise _QueueClaimLostOrFenced(f"queue_claim_lost_or_fenced entry_id={entry.id}")

    # 4. commit + safe lifecycle. The queue row is only acknowledged after
    # board graph survives close/reopen from disk.
    commit_resp = await _commit_consolidation_with_board_graph_lifecycle(
        entry=entry,
        session_id=session_id,
        summary_text=(
            "Historical consolidation of "
            f"{entry.artifact_type} "
            f"'{getattr(artifact, 'title', entry.artifact_id)}'"
        ),
        db=db,
        blocking_execution=blocking_execution,
        now=clock.now() if clock is not None else None,
        defer_session_finalization=deferred_session_ids is not None,
        enter_graph_write=enter_graph_write,
    )

    logger.info(
        "consolidated %s:%s → nodes_added=%d edges_added=%d",
        entry.artifact_type,
        entry.artifact_id,
        commit_resp.nodes_added,
        commit_resp.edges_added,
    )
    # Canonical-debt/partition maintenance is intentionally run by
    # ``process_batch`` in a fresh transaction *after* the authoritative
    # ledger+audit+ACK commit.  Its best-effort rollback must never erase the
    # cognitive durable source staged for this graph mutation.
    return True


async def _classify_queue_entry_source_for_debt(
    db: Any,
    entry: ConsolidationQueueRecord,
):
    """Return the current source maturity for queue-failure accounting.

    CanonicalDebt is specifically canonical debt. A failed working-graph
    materialization remains operational debt in the queue/DLQ, but must not
    inflate the canonical-debt counters used by KG Health.
    """

    if entry.artifact_type == "card":
        card = await get_consolidation_persistence_port().load_artifact(
            db,
            artifact_type="card",
            artifact_id=entry.artifact_id,
        )
        if card is None:
            return None
        card_type = (
            getattr(card.card_type, "value", card.card_type)
            if card.card_type
            else "normal"
        )
        return classify_source_for_kg(
            artifact_type=_card_source_artifact_type(card_type),
            artifact_status=getattr(
                getattr(card, "status", None),
                "value",
                getattr(card, "status", None),
            ),
            content_hash="consolidation-failure",
            has_minimal_evidence=_card_has_minimal_evidence(card),
        )

    if entry.artifact_type not in {
        "story",
        "ideation",
        "refinement",
        "spec",
        "sprint",
    }:
        return None
    artifact = await get_consolidation_persistence_port().load_artifact(
        db,
        artifact_type=entry.artifact_type,
        artifact_id=entry.artifact_id,
    )
    if artifact is None:
        return None
    return classify_source_for_kg(
        artifact_type=entry.artifact_type,
        artifact_status=getattr(
            getattr(artifact, "status", None),
            "value",
            getattr(artifact, "status", None),
        ),
        content_hash="consolidation-failure",
    )


def _select_board_aware_entries(
    ready_entries: list[ConsolidationQueueRecord],
    *,
    claimed_board_ids: frozenset[str],
    limit: int,
) -> list[ConsolidationQueueRecord]:
    """Select at most one ready entry for each board without stealing claims.

    A single processor executes its returned entries sequentially. Claiming
    multiple rows for one board up front therefore lets later rows expire
    while they merely wait behind the first graph commit. The old "top-up"
    path also selected a pending row for a board that was already claimed by
    another owner, contradicting the per-board serialization contract.

    Keeping same-board backlog pending makes every lease represent work that
    can start in this batch. Successful/attempted batches are immediately
    followed by another bounded runner iteration, so throughput is preserved
    without stale-claim churn.
    """

    selected: list[ConsolidationQueueRecord] = []
    unavailable_boards = set(claimed_board_ids)
    for entry in ready_entries:
        # Card 8 owns sweep execution. Unknown/future kinds also remain
        # pending rather than being accidentally consumed by this worker.
        if _work_kind(entry) not in _CLAIMABLE_WORK_KINDS:
            continue
        if entry.board_id in unavailable_boards:
            continue
        selected.append(entry)
        unavailable_boards.add(entry.board_id)
        if len(selected) >= limit:
            break
    return selected


# ---------------------------------------------------------------------------
# Worker class
# ---------------------------------------------------------------------------


class ConsolidationProcessor:
    """Process consolidation commands without owning a runner or task."""

    # Entries claimed longer than this (minutes) are considered stuck.
    STALE_CLAIM_MINUTES: int = 30

    def __init__(
        self,
        relational_scope_factory=None,
        heartbeat_seconds: int = 30,
        batch_size: int = 5,
        stale_claim_minutes: int | None = None,
        *,
        clock: WorkerClockPort | None = None,
        blocking_execution: BlockingExecutionPort | None = None,
    ):
        if relational_scope_factory is None:
            from okto_pulse.core.ports.relational_runtime import get_db_session

            relational_scope_factory = get_db_session
        self.relational_scope_factory = relational_scope_factory
        self.heartbeat_seconds = heartbeat_seconds
        self.batch_size = batch_size
        self._stale_claim_minutes = stale_claim_minutes or self.STALE_CLAIM_MINUTES
        self._clock = clock
        self._blocking_execution = blocking_execution or _DirectBlockingExecution()
        self._last_attempted_count = 0
        # FR5/FR6 (spec R2c): per-board DLQ auto-drain state.
        # _dlq_drain_last_run: board_id -> datetime of last drain attempt.
        # _dlq_drain_last_requeued: board_id -> requeued_count of last run.
        self._dlq_drain_last_run: dict[str, datetime] = {}
        self._dlq_drain_last_requeued: dict[str, int] = {}

    def _now(self) -> datetime:
        return (
            self._clock.now() if self._clock is not None else datetime.now(timezone.utc)
        )

    @property
    def last_attempted_count(self) -> int:
        """Number of queue rows claimed by the latest batch invocation."""

        return self._last_attempted_count

    def get_dlq_drain_stats(self, board_id: str) -> dict:
        """Return DLQ auto-drain stats for ``board_id`` (FR6, spec R2c).

        Returns ``{last_run_at: ISO-string|None, requeued_count: int}``.
        Both values come from in-process tracking only — process restart
        resets them. The health endpoint uses this to populate the additive
        ``dlq_auto_drain_*`` fields without touching storage.
        """
        last_run = self._dlq_drain_last_run.get(board_id)
        return {
            "last_run_at": last_run.isoformat() if last_run is not None else None,
            "requeued_count": self._dlq_drain_last_requeued.get(board_id, 0),
        }

    async def recover_stale_claims(self) -> int:
        """Re-pending queue entries whose claim timeout has elapsed.

        Spec bdcda842 (BR Recovery scan + TR6): the new contract uses the
        per-row ``claim_timeout_at`` field set at claim time
        (now + kg_queue_claim_timeout_s). When the worker that holds the
        claim crashes or is killed, ``claim_timeout_at`` eventually elapses
        and the next recovery scan picks the row up.

        Falls back to the legacy ``stale_claim_minutes`` cutoff for rows
        claimed by an older binary that didn't populate ``claim_timeout_at``
        (so partial migrations don't strand work). Returns the count of
        rows reset to ``pending``.
        """
        now = self._now()
        legacy_cutoff = now - timedelta(minutes=self._stale_claim_minutes)
        async with self.relational_scope_factory() as db:
            store = get_consolidation_persistence_port()
            stale = list(
                await store.list_stale_claims(
                    db,
                    now=now,
                    legacy_cutoff=legacy_cutoff,
                )
            )
            if not stale:
                return 0
            for entry in stale:
                entry.status = "pending"
                entry.claimed_at = None
                entry.claim_timeout_at = None
                entry.worker_id = None
                entry.claimed_by_session_id = None
                entry.claim_token = None
            await store.save_queue_entries(db, stale)
            await store.commit(db)
        logger.info(
            "kg.consolidation_worker.recovered count=%d",
            len(stale),
            extra={
                "event": "kg.queue.recovered",
                "count": len(stale),
            },
        )
        return len(stale)

    async def process_batch(self) -> int:
        """Process up to batch_size pending entries. Returns count processed.

        Spec bdcda842 (Sprint 2):
            * **Claim board-aware** — claim at most one item per board and
              never select a board already claimed by another owner. Distinct
              boards may share a batch; same-board backlog remains pending
              until the current lease is ACKed or re-pended.
            * **Backoff-aware claim** — skip items where ``next_retry_at``
              hasn't elapsed yet (BR Dead-letter / exp backoff).
            * **DELETE-on-ack** — successful processing removes the row from
              ConsolidationQueue (at-least-once semantics: row stays until
              the consolidate+commit pipeline confirmed).
            * **Failure path** — increment ``attempts``, persist
              ``last_error``, schedule ``next_retry_at = now + min(2^N, 300)s``
              and put the row back to ``pending`` for the next claim. The
              dead-letter routing (after ``kg_queue_max_attempts``) is
              entered through the same path and lives in IMPL-3 wiring.

        Adaptive batch sizing keeps the catch-up behaviour from the prior
        implementation; each entry processed in its own session to keep
        SQLite transactions short.
        """
        from okto_pulse.core.infra.config import get_settings

        processed = 0
        self._last_attempted_count = 0
        settings = get_settings()
        claim_timeout_s = settings.kg_queue_claim_timeout_s

        # Step 1: Claim entries (fast DB update, single session).
        async with self.relational_scope_factory() as db:
            store = get_consolidation_persistence_port()
            pending_depth = await store.count_pending(db)

            if pending_depth > 200:
                effective_batch = 50
            elif pending_depth > 100:
                effective_batch = 20
            elif pending_depth > 50:
                effective_batch = 10
            else:
                effective_batch = self.batch_size

            if effective_batch != self.batch_size:
                logger.info(
                    "consolidation.adaptive_batch depth=%d batch_size=%d",
                    pending_depth,
                    effective_batch,
                )

            now = self._now()

            claimed_boards = await store.list_claimed_board_ids(db)
            ready_entries = await store.list_ready_pending(db, now=now)
            entries = _select_board_aware_entries(
                list(ready_entries),
                claimed_board_ids=claimed_boards,
                limit=effective_batch,
            )

            claim_timeout_at = now + timedelta(seconds=claim_timeout_s)
            for entry in entries:
                entry.status = "claimed"
                entry.claimed_at = now
                entry.claim_timeout_at = claim_timeout_at
                worker_id = f"worker_{uuid.uuid4().hex[:8]}"
                entry.worker_id = worker_id
                # A token is never reused, including after recovery/reclaim.
                entry.claim_token = uuid.uuid4().hex
                # Keep claimed_by_session_id populated for backward-compat
                # with cognitive-session inspectors that still read it.
                entry.claimed_by_session_id = worker_id
            await store.save_queue_entries(db, entries)
            await store.commit(db)
            self._last_attempted_count = len(entries)

            # Spec bdcda842 (TR13): claims_per_min sliding window for
            # /api/v1/kg/queue/health. Recorded after a successful claim
            # commit so retries don't double-count.
            if entries:
                from okto_pulse.core.services.queue_health_service import (
                    record_claim,
                )

                for _ in entries:
                    record_claim(now=now)

        # Step 2: Process each entry with its own session (short-lived tx).
        max_attempts = settings.kg_queue_max_attempts
        for entry in entries:
            deferred_session_ids: list[str] = []
            relational_commit_confirmed = False
            try:
                acknowledged = False
                delivery_transfer: tuple[DeliveryTransferReceipt, str] | None = None
                stale_reconcile_telemetry: dict[str, object] = {}
                graph_write_stack = ExitStack()
                graph_write_lease: GuardedWriteLease | None = None

                def _enter_graph_write(
                    mutation_ref: str,
                ) -> GuardedWriteLease:
                    nonlocal graph_write_lease
                    if graph_write_lease is not None:
                        raise RuntimeError(
                            "consolidation_graph_write_boundary_reentered"
                        )
                    graph_write_lease = graph_write_stack.enter_context(
                        guarded_board_write(
                            entry.board_id,
                            operation=CONSOLIDATION_COMMIT_OPERATION,
                            owner_id=(
                                f"{AGENT_ID}:{entry.id}:"
                                f"{_claim_token(entry) or 'unclaimed'}"
                            ),
                            mutation_ref=mutation_ref,
                            required_steps=WORKER_COMMIT_LIFECYCLE_STEPS,
                        )
                    )
                    return graph_write_lease

                try:
                    async with self.relational_scope_factory() as db:
                        store = get_consolidation_persistence_port()
                        outcome = await _process_queue_entry_serialized(
                            db,
                            entry,
                            blocking_execution=self._blocking_execution,
                            clock=self._clock,
                            stale_reconcile_telemetry=stale_reconcile_telemetry,
                            deferred_session_ids=deferred_session_ids,
                            enter_graph_write=_enter_graph_write,
                        )
                        if isinstance(outcome, StaleSweepRunReceipt):
                            if (
                                _work_kind(entry) != "stale_sweep"
                                or outcome.entry_id != entry.id
                                or outcome.board_id != entry.board_id
                                or outcome.action
                                not in {
                                    StaleSweepRunAction.ADVANCED,
                                    StaleSweepRunAction.COMPLETED,
                                    StaleSweepRunAction.RESCHEDULED,
                                }
                            ):
                                raise RuntimeError("stale_sweep_receipt_mismatch")
                            # The adapter already performed the exact claim CAS and
                            # either re-pended or deleted this sweep row. Its
                            # synthetic tombstones, reconcile intents and checkpoint
                            # are committed together here.
                            acknowledged = True
                        else:
                            success = outcome is True
                            fresh = await store.get_queue_entry(db, entry_id=entry.id)
                            if fresh is None:
                                # Once a graph commit has been deferred, a missing
                                # queue row is an ownership loss.  Roll back the
                                # relational ledger and let the deferred cleanup
                                # compensate the graph instead of publishing an
                                # unowned mutation.
                                if deferred_session_ids:
                                    await store.rollback(db)
                                    raise _QueueClaimLostOrFenced(
                                        "queue_claim_lost_after_graph_commit "
                                        f"entry_id={entry.id}"
                                    )
                                # A missing row is a lost claim, never a successful
                                # ACK. Stale reconciliation owns separate transfer
                                # semantics; legacy mock paths preserve their prior
                                # transaction behavior when no graph session exists.
                                if _work_kind(entry) == "stale_reconcile":
                                    await store.rollback(db)
                                else:
                                    await store.commit(db)
                                continue

                            if success:
                                if graph_write_lease is not None:
                                    graph_write_lease.ensure_owned(
                                        failure_phase="before_queue_ack",
                                    )
                                token = _claim_token(entry)
                                if token is not None:
                                    if _work_kind(entry) == "stale_reconcile":
                                        try:
                                            delivery_transfer = await _transfer_stale_reconcile_ownership(
                                                db,
                                                entry,
                                                reconcile_details=(
                                                    stale_reconcile_telemetry
                                                ),
                                                occurred_at=self._now(),
                                            )
                                        except DeliveryTransferClaimConflict as exc:
                                            # CAS=0 is a neutral ownership loss.
                                            await store.rollback(db)
                                            raise _QueueClaimLostOrFenced(
                                                str(exc)
                                            ) from exc
                                        except Exception:
                                            await store.rollback(db)
                                            raise
                                        acknowledged = True
                                    else:
                                        # Legacy consolidation retains its
                                        # standalone exact compare-and-delete ACK.
                                        acknowledged = (
                                            await store.ack_claimed_queue_entry(
                                                db,
                                                entry_id=entry.id,
                                                claim_token=token,
                                                generation=_generation(entry),
                                                delete_event_id=_delete_event_id(entry),
                                            )
                                        )
                                if deferred_session_ids and not acknowledged:
                                    await store.rollback(db)
                                    raise _QueueClaimLostOrFenced(
                                        "queue_ack_lost_after_graph_commit "
                                        f"entry_id={entry.id}"
                                    )
                            elif _same_claim(
                                entry, fresh
                            ) and await store.queue_claim_is_current_and_unfenced(
                                db,
                                entry_id=entry.id,
                                claim_token=_claim_token(entry) or "",
                                board_id=entry.board_id,
                                artifact_type=entry.artifact_type,
                                artifact_id=entry.artifact_id,
                                work_kind=_work_kind(entry),
                                generation=_generation(entry),
                                delete_event_id=_delete_event_id(entry),
                            ):
                                await self._mark_failed(
                                    db,
                                    fresh,
                                    error_text=_stale_reconcile_failure_error(
                                        existing_error=fresh.last_error,
                                        reconcile_details=(
                                            stale_reconcile_telemetry
                                        ),
                                    ),
                                    max_attempts=max_attempts,
                                )

                        if deferred_session_ids:

                            async def _commit_and_finalize() -> None:
                                nonlocal relational_commit_confirmed
                                if graph_write_lease is not None:
                                    graph_write_lease.ensure_owned(
                                        failure_phase=(
                                            "before_relational_ack"
                                        ),
                                    )
                                await store.commit(db)
                                relational_commit_confirmed = True
                                for deferred_session_id in deferred_session_ids:
                                    await finalize_deferred_consolidation(
                                        deferred_session_id,
                                        agent_id=AGENT_ID,
                                    )
                                if graph_write_lease is not None:
                                    graph_write_lease.ensure_owned(
                                        failure_phase=(
                                            "after_relational_ack"
                                        ),
                                    )

                            await run_cancellation_atomic(
                                _commit_and_finalize(),
                                task_name=(
                                    "core.kg.consolidation_worker_commit_and_finalize"
                                ),
                            )
                        else:
                            if graph_write_lease is not None:
                                graph_write_lease.ensure_owned(
                                    failure_phase="before_relational_ack",
                                )
                            await store.commit(db)
                            if graph_write_lease is not None:
                                graph_write_lease.ensure_owned(
                                    failure_phase="after_relational_ack",
                                )
                except BaseException as processing_exc:
                    if deferred_session_ids:

                        async def _settle_deferred_sessions() -> None:
                            if graph_write_lease is not None:
                                graph_write_lease.ensure_owned(
                                    failure_phase=(
                                        "before_deferred_compensation"
                                        if not relational_commit_confirmed
                                        else "before_deferred_finalization_retry"
                                    ),
                                )
                            for deferred_session_id in deferred_session_ids:
                                if relational_commit_confirmed:
                                    # The relational ledger/ACK is already durable.
                                    # Retry terminal in-memory finalization only;
                                    # never release this snapshot for INSERT replay.
                                    await finalize_deferred_consolidation(
                                        deferred_session_id,
                                        agent_id=AGENT_ID,
                                    )
                                else:
                                    # The owning transaction rolled back (or never
                                    # committed), so compensate graph auto-commits
                                    # before the queue can be retried from scratch.
                                    await abort_deferred_consolidation(
                                        deferred_session_id,
                                        agent_id=AGENT_ID,
                                        blocking_execution=self._blocking_execution,
                                    )
                            if graph_write_lease is not None:
                                graph_write_lease.ensure_owned(
                                    failure_phase=(
                                        "after_deferred_compensation"
                                        if not relational_commit_confirmed
                                        else "after_deferred_finalization_retry"
                                    ),
                                )

                        try:
                            await run_cancellation_atomic(
                                _settle_deferred_sessions(),
                                task_name=(
                                    "core.kg.consolidation_worker_deferred_cleanup"
                                ),
                            )
                        except BaseException:
                            logger.exception(
                                "consolidation.deferred_cleanup_failed entry=%s "
                                "relational_commit_confirmed=%s sessions=%s",
                                entry.id,
                                relational_commit_confirmed,
                                deferred_session_ids,
                            )
                        finally:
                            if (
                                not relational_commit_confirmed
                                and graph_write_lease is not None
                            ):
                                try:
                                    await _ensure_board_graph_durable(
                                        board_id=entry.board_id,
                                        mutation_ref=(
                                            "consolidation-worker-abort:"
                                            f"{entry.id}:"
                                            f"{','.join(deferred_session_ids)}"
                                        ),
                                        write_lease=graph_write_lease,
                                        blocking_execution=(
                                            self._blocking_execution
                                        ),
                                        failure_timestamp=self._now(),
                                    )
                                except BaseException as durability_exc:
                                    logger.error(
                                        "consolidation.compensation_"
                                        "durability_ambiguous entry=%s "
                                        "board=%s processing_error=%s "
                                        "lifecycle_error=%s "
                                        "prior_durability_applied=%s "
                                        "compensation_durability_applied=false "
                                        "write_may_be_applied=true",
                                        entry.id,
                                        entry.board_id,
                                        type(processing_exc).__name__,
                                        type(durability_exc).__name__,
                                        graph_write_lease.durability_applied,
                                        exc_info=True,
                                        extra={
                                            "event": (
                                                "consolidation.compensation_"
                                                "durability_ambiguous"
                                            ),
                                            "board_id": entry.board_id,
                                            "entry_id": entry.id,
                                            "processing_error_type": type(
                                                processing_exc
                                            ).__name__,
                                            "lifecycle_error_type": type(
                                                durability_exc
                                            ).__name__,
                                            "prior_durability_applied": (
                                                graph_write_lease
                                                .durability_applied
                                            ),
                                            "compensation_durability_applied": (
                                                False
                                            ),
                                            "write_may_be_applied": True,
                                        },
                                    )
                    raise
                finally:
                    graph_write_stack.__exit__(*sys.exc_info())

                if acknowledged and deferred_session_ids:
                    # Maintenance is best-effort and uses rollback internally.
                    # Isolate it from the already-durable cognitive ledger,
                    # consolidation audit/outbox and queue ACK.
                    try:
                        async with self.relational_scope_factory() as maintenance_db:
                            await _run_post_commit_maintenance(
                                maintenance_db,
                                entry=entry,
                                session_id=deferred_session_ids[-1],
                            )
                            await get_consolidation_persistence_port().commit(
                                maintenance_db
                            )
                    except Exception:
                        logger.exception(
                            "kg.post_commit.maintenance_transaction_failed "
                            "board=%s artifact=%s:%s",
                            entry.board_id,
                            entry.artifact_type,
                            entry.artifact_id,
                        )
                if acknowledged:
                    if delivery_transfer is not None:
                        receipt, circuit_reason = delivery_transfer
                        _log_stale_reconcile_delivery_transfer(
                            entry,
                            receipt,
                            circuit_reason=circuit_reason,
                        )
                    processed += 1
            except _QueueClaimLostOrFenced as exc:
                logger.info(
                    "consolidation.claim_lost_or_fenced entry=%s reason=%s",
                    entry.id,
                    exc,
                )
            except StaleSweepClaimConflict as exc:
                logger.info(
                    "consolidation.stale_sweep_claim_lost entry=%s reason=%s",
                    entry.id,
                    exc,
                )
            except Exception as exc:
                logger.error(
                    "consolidation failed for %s:%s: %s",
                    entry.artifact_type,
                    entry.artifact_id,
                    exc,
                    exc_info=True,
                )
                try:
                    async with self.relational_scope_factory() as db:
                        store = get_consolidation_persistence_port()
                        fresh = await store.get_queue_entry(db, entry_id=entry.id)
                        token = _claim_token(entry)
                        claim_is_current = bool(
                            fresh is not None
                            and token is not None
                            and _same_claim(entry, fresh)
                            and await store.queue_claim_is_current_and_unfenced(
                                db,
                                entry_id=entry.id,
                                claim_token=token,
                                board_id=entry.board_id,
                                artifact_type=entry.artifact_type,
                                artifact_id=entry.artifact_id,
                                work_kind=_work_kind(entry),
                                generation=_generation(entry),
                                delete_event_id=_delete_event_id(entry),
                            )
                        )
                        if claim_is_current and fresh is not None:
                            error_text = (
                                f"{exc.code}:{str(exc)[:480]}"
                                if isinstance(exc, GraphError)
                                else f"{type(exc).__name__}: {str(exc)[:480]}"
                            )
                            retry_after_s = graph_memory_pressure_retry_after_seconds(
                                exc
                            )
                            if retry_after_s is not None:
                                await self._defer_graph_memory_pressure(
                                    db,
                                    fresh,
                                    error_text=error_text,
                                    retry_after_s=retry_after_s,
                                )
                            else:
                                await self._mark_failed(
                                    db,
                                    fresh,
                                    error_text=error_text,
                                    max_attempts=max_attempts,
                                )
                        await store.commit(db)
                except Exception:
                    pass

        return processed

    async def _defer_graph_memory_pressure(
        self,
        db: Any,
        entry: ConsolidationQueueRecord,
        *,
        error_text: str,
        retry_after_s: int,
    ) -> None:
        """Re-pend typed allocation pressure without spending delivery budget."""

        entry.last_error = error_text
        entry.status = "pending"
        entry.next_retry_at = self._now() + timedelta(seconds=retry_after_s)
        entry.claim_timeout_at = None
        entry.worker_id = None
        entry.claimed_at = None
        entry.claimed_by_session_id = None
        entry.claim_token = None
        await get_consolidation_persistence_port().save_queue_entries(
            db,
            (entry,),
        )
        logger.warning(
            "consolidation.graph_memory_pressure_deferred "
            "artifact=%s:%s attempts=%d retry_after_s=%d",
            entry.artifact_type,
            entry.artifact_id,
            int(entry.attempts or 0),
            retry_after_s,
            extra={
                "event": "consolidation.graph_memory_pressure_deferred",
                "board_id": entry.board_id,
                "artifact_type": entry.artifact_type,
                "artifact_id": entry.artifact_id,
                "attempts": int(entry.attempts or 0),
                "retry_after_s": retry_after_s,
            },
        )

    async def _mark_failed(
        self,
        db: Any,
        entry: ConsolidationQueueRecord,
        *,
        error_text: str,
        max_attempts: int,
    ) -> None:
        """Common failure handler: increment attempts, schedule exp backoff,
        re-pending the row. When ``attempts >= max_attempts``, only legacy
        ``consolidate`` work is routed to ``ConsolidationDeadLetter`` (IMPL-3
        wiring) and deleted from the queue. Governed ``stale_reconcile`` and
        coordinator ``stale_sweep`` work remain in their identity-bearing
        queue rows with capped backoff because the legacy DLQ cannot preserve
        their generation/checkpoint payloads.
        This method persists the effective queue record (including a record
        reloaded after rollback) whenever a persistence context is supplied;
        the caller is responsible only for committing the transaction.

        FR3 (spec R2c): when the entry is routed to the dead-letter queue,
        a FailureEvent with ``event_kind="kg.commit.failed"`` is recorded
        in the collector ring-buffer so the MemoryPressureCorrelator
        receives a real commit-failure signal.  Non-blocking/non-raising.

        FR6 (spec eaf185c9 / card 81a96a49): a legacy board missing the
        graph_layer/maturity_status schema raises ``Cannot find property
        graph_layer for n``. Before that raw string becomes the sole DLQ
        diagnostic we try the idempotent schema migration+backfill. If it
        actually repairs the schema we re-pending the entry for an immediate
        retry instead of counting it toward the dead-letter threshold; if it
        cannot, we replace the raw error with a structured, actionable
        diagnostic (or_1f52d4fd) so the dead-letter row names the operational
        action rather than the opaque binder error.
        """
        claimed_entry = entry
        token = _claim_token(claimed_entry)
        if token is not None and not await _queue_claim_is_current_and_unfenced(
            db,
            claimed_entry,
        ):
            raise _QueueClaimLostOrFenced(
                f"queue_claim_lost_or_fenced entry_id={claimed_entry.id}"
            )

        retry_after_s = graph_memory_pressure_retry_after_seconds(error_text)
        if retry_after_s is not None:
            await self._defer_graph_memory_pressure(
                db,
                entry,
                error_text=error_text,
                retry_after_s=retry_after_s,
            )
            return

        if is_graph_layer_schema_error(error_text):
            remediation = ensure_graph_layer_schema(
                entry.board_id, raw_error=error_text
            )
            if remediation.recovered:
                # Schema repaired in place — re-pending for an immediate retry
                # rather than charging this attempt against the DLQ threshold.
                entry.last_error = None
                entry.status = "pending"
                entry.next_retry_at = self._now()
                entry.claim_timeout_at = None
                entry.worker_id = None
                entry.claimed_at = None
                entry.claimed_by_session_id = None
                entry.claim_token = None
                if db is not None:
                    await get_consolidation_persistence_port().save_queue_entries(
                        db,
                        (entry,),
                    )
                logger.info(
                    "consolidation.schema_layer_recovered artifact=%s:%s "
                    "board=%s columns_added=%s",
                    entry.artifact_type,
                    entry.artifact_id,
                    entry.board_id,
                    remediation.columns_added,
                )
                return
            if remediation.needs_structured_error and remediation.structured_message:
                # Could not migrate — make the DLQ diagnostic actionable so the
                # raw binder error is never the only thing operators see.
                error_text = remediation.structured_message

        correlation_id = uuid.uuid4().hex
        entry_id = entry.id
        entry_board_id = entry.board_id
        entry_artifact_type = entry.artifact_type
        entry_artifact_id = entry.artifact_id
        entry_triggered_at = entry.triggered_at
        entry_worker_id = entry.worker_id
        try:
            classification = await _classify_queue_entry_source_for_debt(db, entry)
            is_canonical_failure = (
                classification is not None
                and classification.graph_layer == GRAPH_LAYER_CANONICAL
                and classification.maturity_status == MATURITY_CANONICAL_ELIGIBLE
            )
            if is_canonical_failure:
                board_exists = await get_consolidation_persistence_port().board_exists(
                    db,
                    board_id=entry_board_id,
                )
                if not board_exists:
                    logger.warning(
                        "canonical_debt.skipped_missing_board board=%s artifact=%s:%s",
                        entry_board_id,
                        entry_artifact_type,
                        entry_artifact_id,
                    )
                    is_canonical_failure = False
            if is_canonical_failure:
                debt_hash = hashlib.sha256(
                    "|".join(
                        [
                            entry_board_id,
                            entry_artifact_type,
                            entry_artifact_id,
                            entry_triggered_at.isoformat()
                            if entry_triggered_at
                            else "",
                        ]
                    ).encode("utf-8")
                ).hexdigest()
                await upsert_canonical_debt(
                    db,
                    board_id=entry_board_id,
                    artifact_type=entry_artifact_type,
                    artifact_id=entry_artifact_id,
                    source_ref=f"{entry_artifact_type}:{entry_artifact_id}",
                    content_hash=debt_hash,
                    target_status="canonical_consolidation",
                    canonical_state="failed",
                    failure_reason="consolidation_failed",
                    last_error=error_text,
                    owner_agent_id=entry_worker_id or AGENT_ID,
                    correlation_id=correlation_id,
                    queue_ref=entry_id,
                    graph_layer=classification.graph_layer,
                    maturity_status=classification.maturity_status,
                )
        except Exception as debt_exc:
            logger.error(
                "canonical_debt.persist_failed board=%s artifact=%s:%s err=%s",
                entry_board_id,
                entry_artifact_type,
                entry_artifact_id,
                debt_exc,
            )
            # A failed flush leaves SQLAlchemy sessions in PendingRollback.
            # Roll back and reload the queue row before updating attempts/DLQ.
            try:
                await get_consolidation_persistence_port().rollback(db)
            except Exception:
                logger.exception(
                    "canonical_debt.persist_rollback_failed board=%s artifact=%s:%s",
                    entry_board_id,
                    entry_artifact_type,
                    entry_artifact_id,
                )
                return
            reloaded = await get_consolidation_persistence_port().get_queue_entry(
                db,
                entry_id=entry_id,
            )
            if reloaded is None:
                return
            if token is not None and (
                not _same_claim(claimed_entry, reloaded)
                or not await _queue_claim_is_current_and_unfenced(db, reloaded)
            ):
                raise _QueueClaimLostOrFenced(
                    f"queue_claim_lost_or_fenced entry_id={entry_id}"
                )
            entry = reloaded

        entry.attempts = (entry.attempts or 0) + 1
        entry.last_error = error_text
        retry_policy = RetryPolicy(max_attempts=max_attempts)
        retry = retry_policy.after_failure(entry.attempts)
        if retry.terminal and _work_kind(entry) == "consolidate":
            await route_to_dead_letter(db, entry, error_text=error_text)
            # FR3: record commit failure for the memory-pressure correlator.
            try:
                record_failure(
                    entry.board_id,
                    FailureEvent(
                        timestamp=self._now(),
                        event_kind="kg.commit.failed",
                        graph_type="board",
                        correlation_id=correlation_id,
                    ),
                )
            except Exception:
                pass
            return
        backoff_s = (
            _capped_retry_delay(retry_policy, entry.attempts)
            if retry.terminal
            else retry.delay_seconds
        )
        entry.status = "pending"
        entry.next_retry_at = self._now() + timedelta(seconds=backoff_s)
        entry.claim_timeout_at = None
        entry.worker_id = None
        entry.claimed_at = None
        entry.claimed_by_session_id = None
        entry.claim_token = None
        await get_consolidation_persistence_port().save_queue_entries(
            db,
            (entry,),
        )
        logger.info(
            "consolidation.attempt_failed artifact=%s:%s attempts=%d next_retry_in=%ds",
            entry.artifact_type,
            entry.artifact_id,
            entry.attempts,
            backoff_s,
        )

    async def run_dlq_auto_drain(self) -> None:
        """FR5/FR6 (spec R2c): opt-in automatic DLQ reprocess on each heartbeat.

        For every board that has ``dlq_auto_drain_enabled=True`` in its
        ``Board.settings`` JSON column AND has dead-letter rows pending, we
        call ``reprocess_dead_letter_rows`` at most once per
        ``kg_queue_dlq_auto_drain_backoff_s`` seconds (in-process per-board
        cooldown dictionary, not persisted).

        Poison-pill guard: DLQ rows whose ``attempts`` counter has reached
        ``kg_queue_dlq_auto_drain_max_requeue_attempts`` are permanently
        deleted and a WARN log is emitted so operators know they have an
        artifact that cannot be consolidated.

        The settings are re-read from the DB on every heartbeat so operators
        can enable/disable the feature per board at runtime without a restart.
        """
        from okto_pulse.core.infra.config import get_settings
        from okto_pulse.core.services.dead_letter_inspector_service import (
            reprocess_dead_letter_rows,
        )

        try:
            settings = get_settings()
            backoff_s: int = settings.kg_queue_dlq_auto_drain_backoff_s
            max_attempts: int = settings.kg_queue_dlq_auto_drain_max_requeue_attempts
        except Exception:
            return  # defensive: don't break the heartbeat on config failure

        now = self._now()

        try:
            async with self.relational_scope_factory() as db:
                enabled_board_ids = await get_consolidation_persistence_port().list_dlq_auto_drain_board_ids(
                    db
                )

            for board_id in enabled_board_ids:
                # Backoff: skip if we ran recently for this board
                last_run = self._dlq_drain_last_run.get(board_id)
                if last_run is not None:
                    elapsed = (now - last_run).total_seconds()
                    if elapsed < backoff_s:
                        continue  # AC11: still within backoff window

                # Check if the board actually has DLQ rows
                async with self.relational_scope_factory() as db:
                    dlq_count = (
                        await get_consolidation_persistence_port().count_dead_letters(
                            db,
                            board_id=board_id,
                        )
                    )

                if dlq_count == 0:
                    continue

                # Poison-pill exclusion: remove rows at or beyond max attempts
                # before passing them to the requeue path (so they don't just
                # cycle back to DLQ again immediately).
                skipped_poison: list[str] = []
                async with self.relational_scope_factory() as db:
                    store = get_consolidation_persistence_port()
                    poison_rows = await store.delete_poison_dead_letters(
                        db,
                        board_id=board_id,
                        max_attempts=max_attempts,
                    )
                    for row in poison_rows:
                        skipped_poison.append(row.id)
                        logger.warning(
                            "kg.dlq.auto_drain.poison_pill_excluded "
                            "board_id=%s dlq_id=%s attempts=%d max=%d",
                            board_id,
                            row.id,
                            row.attempts,
                            max_attempts,
                            extra={
                                "event": "kg.dlq.auto_drain.poison_pill_excluded",
                                "board_id": board_id,
                                "dlq_id": row.id,
                                "attempts": row.attempts,
                                "max_requeue_attempts": max_attempts,
                            },
                        )
                    if poison_rows:
                        await store.commit(db)

                # Reprocess the remaining (non-poison) rows
                async with self.relational_scope_factory() as db:
                    result = await reprocess_dead_letter_rows(db, board_id, limit=50)
                    await get_consolidation_persistence_port().commit(db)

                requeued_count: int = result.get("requeued_count", 0)
                already_queued_count: int = result.get("already_queued_count", 0)

                self._dlq_drain_last_run[board_id] = now
                self._dlq_drain_last_requeued[board_id] = requeued_count

                logger.info(
                    "kg.dlq.auto_drain board_id=%s requeued=%d already_queued=%d skipped=%d",
                    board_id,
                    requeued_count,
                    already_queued_count,
                    len(skipped_poison),
                    extra={
                        "event": "kg.dlq.auto_drain",
                        "board_id": board_id,
                        "requeued": requeued_count,
                        "already_queued": already_queued_count,
                        "skipped": len(skipped_poison),
                    },
                )

        except Exception as exc:
            logger.warning(
                "kg.dlq.auto_drain.failed: %s",
                exc,
                exc_info=True,
            )


__all__ = [
    "AGENT_ID",
    "CONSOLIDATION_COMMIT_OPERATION",
    "ConsolidationProcessor",
    "WORKER_COMMIT_LIFECYCLE_STEPS",
]
