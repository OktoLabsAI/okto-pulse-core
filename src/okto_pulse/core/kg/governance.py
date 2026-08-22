"""Governance module — historical opt-in, ACL violation log, admin audit,
undo mechanism, audit retention, right-to-erasure.

Consolidates all governance operations that the REST API and MCP tools call.
Depends on ConsolidationQueue/ConsolidationAudit models from models/db.py
and the global_discovery cascade from global_discovery/clustering.py.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Mapping
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from threading import Lock
from typing import Any, AsyncIterator

from okto_pulse.core.runtime_context import runtime_state
from okto_pulse.core.ports.kg_events import HISTORICAL_PROGRESS_SETTINGS_KEY
from okto_pulse.core.ports.kg_governance import (
    BoostAuditRecord,
    HistoricalBoardRecord,
    HistoricalQueueInsert,
    get_kg_governance_store,
)
from okto_pulse.core.kg.source_maturity import (
    CANCELLATION_REVOCATION_REASON,
    CANCELLATION_SCORE_PENALTY,
)

logger = logging.getLogger("okto_pulse.kg.governance")


class BoardErasureError(RuntimeError):
    """A strict board erasure could not prove a safe result."""


class BoardErasureLockContention(BoardErasureError):
    """Another KG writer owns the board fence."""


class BoardErasureLeaseLost(BoardErasureError):
    """The administrative board fence expired or changed owner."""


class BoardErasureVerificationError(BoardErasureError):
    """A destructive KG step did not prove the target absent."""


def _require_verified_physical_erasure(
    result: object,
    *,
    board_id: str,
    capability: str,
) -> dict[str, object]:
    """Normalize and prove one board-scoped physical erasure receipt."""

    if not isinstance(result, Mapping):
        raise BoardErasureVerificationError(
            f"{capability}_receipt_invalid board={board_id}"
        )
    receipt = dict(result)
    if (
        receipt.get("board_id") != board_id
        or receipt.get("verified_absent") is not True
        or receipt.get("status") not in {"purged", "not_found"}
    ):
        raise BoardErasureVerificationError(
            f"{capability}_absence_unverified board={board_id} receipt={receipt}"
        )
    return receipt


class BoardErasureLease:
    """Live administrative writer lease held across source delete + KG purge."""

    def __init__(
        self,
        *,
        board_id: str,
        writer_lock: Any,
        owner_token: str,
        ttl_seconds: int,
        operation_reservation: Any | None = None,
        reservation_token: str | None = None,
    ) -> None:
        self.board_id = board_id
        self._writer_lock = writer_lock
        self._owner_token = owner_token
        self._operation_reservation = operation_reservation
        self._reservation_token = reservation_token
        self._ttl_seconds = ttl_seconds
        self._lost = False
        self._renew_lock = Lock()
        self._global_lease: Any | None = None

    def mark_lost(self) -> None:
        self._lost = True

    def attach_global_lease(self, global_lease: Any) -> None:
        self._global_lease = global_lease

    def renew(self) -> bool:
        with self._renew_lock:
            if self._lost:
                return False
            try:
                reservation_renewed = True
                if (
                    self._operation_reservation is not None
                    and self._reservation_token is not None
                ):
                    reservation_renewed = self._operation_reservation.renew(
                        board_id=self.board_id,
                        owner_token=self._reservation_token,
                        ttl_seconds=self._ttl_seconds,
                    )
                if not reservation_renewed:
                    self._lost = True
                    return False
                writer_renewed = self._writer_lock.renew(
                    board_id=self.board_id,
                    owner_token=self._owner_token,
                    ttl_seconds=self._ttl_seconds,
                )
            except Exception:
                self._lost = True
                raise
            renewed = bool(reservation_renewed and writer_renewed)
            if not renewed:
                self._lost = True
            return renewed

    def ensure_owned(self) -> None:
        if self._lost or not self.renew():
            self._lost = True
            raise BoardErasureLeaseLost(
                f"board_erasure_lease_lost board={self.board_id}"
            )
        if self._global_lease is None:
            raise BoardErasureLeaseLost(
                f"board_erasure_global_lease_missing board={self.board_id}"
            )
        try:
            self._global_lease.assert_fenced()
        except Exception as exc:
            self._lost = True
            raise BoardErasureLeaseLost(
                f"board_erasure_global_lease_lost board={self.board_id}"
            ) from exc


async def _run_destructive_thread(func: Any, /, *args: Any, **kwargs: Any) -> Any:
    """Do not release destructive-operation fences while a thread still runs."""

    task = asyncio.create_task(asyncio.to_thread(func, *args, **kwargs))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                continue
        if not task.cancelled() and task.exception() is not None:
            logger.error(
                "board_erasure.background_step_failed_after_cancellation err=%s",
                task.exception(),
            )
        raise


@asynccontextmanager
async def board_erasure_scope(
    board_id: str,
    *,
    actor_id: str,
) -> AsyncIterator[BoardErasureLease]:
    """Fence every board writer until strict erasure and source commit finish."""

    from okto_pulse.core.kg.single_writer_lock import (
        DEFAULT_TTL_SECONDS,
        KGAdministrativeOperationReservation,
        KGSingleWriterLock,
    )
    from okto_pulse.core.kg.write_barrier import under_safe_write

    operation = "board_delete_erasure"
    writer_lock = KGSingleWriterLock()
    operation_reservation = KGAdministrativeOperationReservation(
        write_lock_port=writer_lock.bind_write_lock_port()
    )
    reservation_acquisition = operation_reservation.acquire(
        board_id=board_id,
        operation=f"{operation}.reservation",
        owner_id=f"{actor_id}:board-delete-reservation:{uuid.uuid4().hex}",
        ttl_seconds=DEFAULT_TTL_SECONDS,
        admin_lane=True,
    )
    if not reservation_acquisition.acquired or not reservation_acquisition.owner_token:
        raise BoardErasureLockContention(
            f"board_erasure_reservation_contention board={board_id} "
            f"current_owner={reservation_acquisition.current_owner}"
        )
    try:
        acquisition = writer_lock.acquire(
            board_id=board_id,
            operation=operation,
            owner_id=f"{actor_id}:board-delete:{uuid.uuid4().hex}",
            ttl_seconds=DEFAULT_TTL_SECONDS,
            admin_lane=True,
        )
    except BaseException:
        try:
            operation_reservation.release(
                board_id=board_id,
                owner_token=reservation_acquisition.owner_token,
            )
        except BaseException:
            logger.exception(
                "board_erasure.writer_acquire_reservation_release_failed board=%s",
                board_id,
            )
        raise
    if not acquisition.acquired or not acquisition.owner_token:
        contention = BoardErasureLockContention(
            f"board_erasure_lock_contention board={board_id} "
            f"current_owner={acquisition.current_owner}"
        )
        try:
            operation_reservation.release(
                board_id=board_id,
                owner_token=reservation_acquisition.owner_token,
            )
        except BaseException:
            logger.exception(
                "board_erasure.writer_contention_reservation_release_failed board=%s",
                board_id,
            )
        raise contention

    lease = BoardErasureLease(
        board_id=board_id,
        writer_lock=writer_lock,
        owner_token=acquisition.owner_token,
        operation_reservation=operation_reservation,
        reservation_token=reservation_acquisition.owner_token,
        ttl_seconds=DEFAULT_TTL_SECONDS,
    )
    stop_heartbeat = asyncio.Event()

    async def _heartbeat() -> None:
        interval = max(1.0, min(30.0, DEFAULT_TTL_SECONDS / 3))
        while not stop_heartbeat.is_set():
            try:
                await asyncio.wait_for(stop_heartbeat.wait(), timeout=interval)
                return
            except TimeoutError:
                try:
                    renewed = await asyncio.to_thread(lease.renew)
                except Exception as exc:
                    lease.mark_lost()
                    logger.error(
                        "board_erasure.heartbeat_failed board=%s err=%s",
                        board_id,
                        exc,
                    )
                    return
                if not renewed:
                    return

    heartbeat: asyncio.Task[None] | None = None
    try:
        from okto_pulse.core.kg.global_discovery_writer import (
            global_discovery_writer_scope,
        )

        with under_safe_write(board_id, acquisition.owner_token, operation):
            with global_discovery_writer_scope(
                operation=f"{operation}.global",
                owner_id=f"{actor_id}:board-delete-global:{uuid.uuid4().hex}",
                admin_lane=True,
            ) as global_lease:
                lease.attach_global_lease(global_lease)
                heartbeat = asyncio.create_task(
                    _heartbeat(),
                    name=f"board-erasure-heartbeat:{board_id}",
                )
                try:
                    yield lease
                finally:
                    stop_heartbeat.set()
                    try:
                        await asyncio.shield(heartbeat)
                    except asyncio.CancelledError:
                        heartbeat.cancel()
                        try:
                            await heartbeat
                        except asyncio.CancelledError:
                            pass
                    except Exception:
                        logger.exception(
                            "board_erasure.heartbeat_cleanup_failed board=%s",
                            board_id,
                        )
    finally:
        try:
            released = writer_lock.release(
                board_id=board_id,
                owner_token=acquisition.owner_token,
            )
            if not released:
                logger.error(
                    "board_erasure.release_failed board=%s owner_token=%s",
                    board_id,
                    acquisition.owner_token,
                )
        except BaseException:
            logger.exception(
                "board_erasure.release_exception board=%s",
                board_id,
            )
        finally:
            try:
                reservation_released = operation_reservation.release(
                    board_id=board_id,
                    owner_token=reservation_acquisition.owner_token,
                )
                if not reservation_released:
                    logger.error(
                        "board_erasure.reservation_release_failed board=%s "
                        "owner_token=%s",
                        board_id,
                        reservation_acquisition.owner_token,
                    )
            except BaseException:
                logger.exception(
                    "board_erasure.reservation_release_exception board=%s",
                    board_id,
                )


def _historical_progress_state(
    board: HistoricalBoardRecord | None,
) -> dict[str, Any]:
    if board is None or not isinstance(board.settings, dict):
        return {}
    value = board.settings.get(HISTORICAL_PROGRESS_SETTINGS_KEY)
    return value if isinstance(value, dict) else {}


def _set_historical_progress_state(
    board: HistoricalBoardRecord | None,
    *,
    total: int,
    status: str,
) -> None:
    if board is None:
        return
    settings = dict(board.settings or {})
    current = settings.get(HISTORICAL_PROGRESS_SETTINGS_KEY)
    current_state = current if isinstance(current, dict) else {}
    settings[HISTORICAL_PROGRESS_SETTINGS_KEY] = {
        **current_state,
        "total": max(0, int(total)),
        "status": status,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "started_at": current_state.get("started_at")
        or datetime.now(timezone.utc).isoformat(),
    }
    board.settings = settings


async def _historical_queue_counts(
    db: Any,
    board_id: str,
) -> dict[str, int]:
    counts = {"pending": 0, "claimed": 0, "done": 0, "failed": 0, "paused": 0}
    counts.update(await get_kg_governance_store().queue_counts(db, board_id=board_id))
    return counts


async def _has_materialized_kg_nodes(board_id: str) -> bool:
    """Best-effort check that the per-board KG still contains user nodes.

    Historical progress is persisted in board settings, while the graph file
    can be wiped/recreated independently. If the persisted state says a prior
    backfill completed but the graph has no materialized nodes, callers must be
    allowed to run historical consolidation again.
    """
    try:
        from okto_pulse.core.kg.kg_service import get_kg_service

        rows = await asyncio.to_thread(
            get_kg_service().get_all_nodes,
            board_id,
            min_confidence=0.0,
            min_relevance=0.0,
            max_rows=1,
        )
        return bool(rows)
    except Exception as exc:  # pragma: no cover - defensive runtime guard
        logger.debug(
            "governance.historical_progress_graph_probe_failed board=%s err=%s",
            board_id,
            exc,
        )
        return True


async def _purge_stale_metadata_if_graph_empty(
    db: Any,
    board_id: str,
) -> bool:
    """Drop SQLite KG mirrors when the physical board graph has no user nodes."""
    has_nodes = await _has_materialized_kg_nodes(board_id)
    if has_nodes:
        return False

    await get_kg_governance_store().purge_stale_metadata(
        db,
        board_id=board_id,
    )
    logger.info(
        "governance.historical_start.purged_stale_metadata board=%s",
        board_id,
        extra={
            "event": "governance.historical_start.purged_stale_metadata",
            "board_id": board_id,
        },
    )
    return True


# ---------------------------------------------------------------------------
# Historical opt-in flow (FR-0 through FR-6)
# ---------------------------------------------------------------------------


async def start_historical_consolidation(
    db: Any,
    board_id: str,
) -> dict:
    """Populate consolidation_queue with low-priority entries for all done
    specs/sprints in the board. Returns counts."""
    import uuid

    store = get_kg_governance_store()
    board = await store.get_board(db, board_id=board_id)

    # Check if already in progress
    live_queue = await store.list_live_queue(db, board_id=board_id)
    if any(
        row.source == "historical_backfill" and row.status in {"pending", "claimed"}
        for row in live_queue
    ):
        counts = await _historical_queue_counts(db, board_id)
        live_total = sum(counts.values())
        current_total = int(_historical_progress_state(board).get("total") or 0)
        if live_total > 0 and current_total < live_total:
            _set_historical_progress_state(
                board,
                total=live_total,
                status="in_progress",
            )
            if board is not None:
                await store.save_board(db, board)
            await store.commit(db)
        return {"status": "already_in_progress", "board_id": board_id}

    await _purge_stale_metadata_if_graph_empty(db, board_id)

    artifacts = await store.list_historical_artifacts(db, board_id=board_id)
    by_type = {
        artifact_type: [row for row in artifacts if row.artifact_type == artifact_type]
        for artifact_type in (
            "story",
            "ideation",
            "refinement",
            "spec",
            "sprint",
            "card",
        )
    }

    # Remove completed/failed entries so they can be re-queued.
    # NOTE: we purposely do NOT filter by source — terminal rows from
    # event-driven enqueues (event:card.created, retry_from_ui, …) must
    # also be cleared so the historical pass can reprocess every artifact.
    # The UNIQUE constraint (board_id, artifact_type, artifact_id) means
    # only one row per artifact can exist, so deleting all terminal rows
    # is equivalent to clearing the slot for re-queueing.
    await store.delete_terminal_queue(db, board_id=board_id)

    # Collect live entries only — pending, claimed, or paused. Terminal
    # rows (done/failed) have just been deleted above, so dedup against
    # them would be incorrect. Including `paused` covers the case where
    # a prior historical run was paused and is still reachable via
    # resume_historical.
    existing_rows = await store.list_live_queue(db, board_id=board_id)
    already_queued = {(row.artifact_type, row.artifact_id) for row in existing_rows}
    existing_historical = {
        (row.artifact_type, row.artifact_id)
        for row in existing_rows
        if row.source == "historical_backfill"
    }

    entries = [
        HistoricalQueueInsert(
            id=str(uuid.uuid4()),
            board_id=board_id,
            artifact_type=artifact_type,
            artifact_id=artifact.artifact_id,
        )
        for artifact_type in (
            "story",
            "ideation",
            "refinement",
            "spec",
            "sprint",
            "card",
        )
        for artifact in by_type[artifact_type]
        if (artifact_type, artifact.artifact_id) not in already_queued
    ]
    total = len(entries)
    await store.add_queue_entries(db, entries)

    run_total = total + len(existing_historical)
    _set_historical_progress_state(
        board,
        total=run_total,
        status="in_progress" if run_total > 0 else "inactive",
    )
    if board is not None:
        await store.save_board(db, board)
    await store.commit(db)

    logger.info(
        "governance.historical_start board=%s stories=%d ideations=%d "
        "refinements=%d specs=%d sprints=%d cards=%d total=%d",
        board_id,
        len(by_type["story"]),
        len(by_type["ideation"]),
        len(by_type["refinement"]),
        len(by_type["spec"]),
        len(by_type["sprint"]),
        len(by_type["card"]),
        total,
    )

    if total > 0:
        # Fase 4 — wake the background worker immediately so the freshly
        # enqueued rows start processing without waiting for a heartbeat.
        try:
            from okto_pulse.core.application.runtime_workers import (
                signal_runtime_worker,
            )

            signal_runtime_worker("consolidation_worker")
        except Exception:  # pragma: no cover — signal is best-effort
            pass

    return {"status": "queueing", "board_id": board_id, "total_artifacts": run_total}


async def pause_historical(db: Any, board_id: str) -> dict:
    """Mark low-priority backfill entries as paused."""
    store = get_kg_governance_store()
    await store.update_historical_status(
        db,
        board_id=board_id,
        old_status="pending",
        new_status="paused",
    )
    await store.commit(db)
    return {"status": "paused", "board_id": board_id}


async def resume_historical(db: Any, board_id: str) -> dict:
    """Resume paused backfill entries."""
    store = get_kg_governance_store()
    await store.update_historical_status(
        db,
        board_id=board_id,
        old_status="paused",
        new_status="pending",
    )
    await store.commit(db)
    return {"status": "resumed", "board_id": board_id}


async def cancel_historical(db: Any, board_id: str) -> dict:
    """Delete pending low-priority entries. Already-consolidated preserved."""
    store = get_kg_governance_store()
    board = await store.get_board(db, board_id=board_id)
    removed = await store.delete_historical_pending(db, board_id=board_id)
    current_total = int(_historical_progress_state(board).get("total") or 0)
    _set_historical_progress_state(board, total=current_total, status="cancelled")
    if board is not None:
        await store.save_board(db, board)
    await store.commit(db)
    return {"status": "cancelled", "board_id": board_id, "removed": removed}


async def retry_pending_entry(
    db: Any,
    board_id: str,
    queue_entry_id: str,
    *,
    recursive: bool = False,
    include_code_traceability: bool = True,
) -> dict | None:
    """Re-queue a failed/done ConsolidationQueue entry so the worker reprocesses
    it (write, commits internally). ``recursive=True`` also re-enqueues
    descendants below the artifact in the Ideation→Refinement→Spec→Sprint→Card
    hierarchy.

    Returns ``None`` when the entry does not exist (so this module stays
    transport-free — the use case maps that to ``EntityNotFoundError`` → HTTP
    404). Reproduces the legacy ``retry_pending_entry`` endpoint byte-for-byte:
    the mutation, the recursive descendant sweep, the single ``commit`` and the
    best-effort worker signal all live here, exactly as the endpoint relied on
    when it passed ``db`` straight through.

    Idempotency: content_hash BR still owns "nothing actually changed" no-op
    behaviour downstream, so retrying an unchanged artifact is a cheap round-trip
    that touches the outbox once.
    """
    from okto_pulse.core.ports.kg_operational import get_kg_worker_queue_port

    queue = get_kg_worker_queue_port()
    result = (
        await queue.retry_pending_entry(
            db,
            board_id=board_id,
            queue_entry_id=queue_entry_id,
            recursive=recursive,
        )
        if include_code_traceability
        else await queue.retry_pending_entry(
            db,
            board_id=board_id,
            queue_entry_id=queue_entry_id,
            recursive=recursive,
            include_code_traceability=False,
        )
    )
    if result is None:
        return None

    # Fase 4 — wake the background worker so retried rows are picked up
    # immediately instead of waiting for the heartbeat tick.
    try:
        from okto_pulse.core.application.runtime_workers import signal_runtime_worker

        signal_runtime_worker("consolidation_worker")
    except Exception:  # pragma: no cover — signal is best-effort
        pass

    return dict(result)


async def get_historical_progress(db: Any, board_id: str) -> dict:
    """Return progress of historical consolidation."""
    board = await get_kg_governance_store().get_board(db, board_id=board_id)
    state = _historical_progress_state(board)
    counts = await _historical_queue_counts(db, board_id)
    live_total = sum(counts.values())
    total = max(int(state.get("total") or 0), live_total)
    remaining = counts["pending"] + counts["claimed"] + counts["paused"]
    processed = max(0, min(total, total - remaining))
    if state.get("status") == "cancelled" and remaining == 0:
        status = "cancelled"
    elif counts["pending"] or counts["claimed"]:
        status = "in_progress"
    elif counts["paused"]:
        status = "paused"
    elif total > 0 and counts["failed"] > 0:
        status = "completed_with_errors"
    elif total > 0 and processed >= total:
        status = "completed"
    else:
        status = "inactive"

    stale = False
    if (
        status in {"completed", "completed_with_errors"}
        and total > 0
        and remaining == 0
    ):
        has_nodes = await _has_materialized_kg_nodes(board_id)
        if not has_nodes:
            stale = True
            total = 0
            processed = 0
            status = "inactive"

    return {
        "enabled": total > 0,
        "status": status,
        "total": total,
        "progress": processed,
        "pending": counts["pending"],
        "claimed": counts["claimed"],
        "paused": counts["paused"],
        "failed": counts["failed"],
        "stale": stale,
    }


# ---------------------------------------------------------------------------
# Undo mechanism (FR-11 through FR-14)
# ---------------------------------------------------------------------------


async def undo_session(
    db: Any,
    board_id: str,
    session_id: str,
    *,
    force: bool = False,
) -> dict:
    """Soft-delete nodes/edges from a consolidation session.

    Returns 409 cascade_blocked if other sessions reference nodes from this
    session, unless force=True (admin).
    """
    store = get_kg_governance_store()
    fact = await store.get_undo_fact(
        db,
        board_id=board_id,
        session_id=session_id,
    )
    if fact is None:
        return {"error": "not_found", "session_id": session_id}
    if fact.undo_status == "undone":
        return {"error": "already_undone", "session_id": session_id}
    if not force and fact.blocking_sessions:
        return {
            "error": "cascade_blocked",
            "session_id": session_id,
            "blocking_sessions": list(fact.blocking_sessions),
        }

    # Mark as undone
    await store.mark_session_undone(
        db,
        session_id=session_id,
        undone_at=datetime.now(timezone.utc),
    )
    await store.commit(db)

    # graph backend soft-delete would happen here via TransactionOrchestrator.compensate
    # pattern. For MVP: mark in SQLite only.
    return {
        "session_id": session_id,
        "status": "undone",
        "nodes_removed": len(fact.node_ids),
        "force_used": force,
    }


# ---------------------------------------------------------------------------
# Audit retention + purge (FR-15, FR-16)
# ---------------------------------------------------------------------------


async def purge_expired_audit(
    db: Any,
    board_id: str,
    retention_days: int | None = None,
) -> dict:
    """Delete audit entries older than retention_days. None = skip (unlimited)."""
    if retention_days is None or retention_days <= 0:
        return {"board_id": board_id, "purged": 0, "retention": "unlimited"}

    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    store = get_kg_governance_store()
    purged = await store.purge_expired_audit(
        db,
        board_id=board_id,
        cutoff=cutoff,
    )
    await store.commit(db)
    return {
        "board_id": board_id,
        "purged": purged,
        "retention_days": retention_days,
        "cutoff": cutoff.isoformat(),
    }


# ---------------------------------------------------------------------------
# ACL violation log (FR-9, FR-10)
# ---------------------------------------------------------------------------

_acl_violations = runtime_state("kg.governance.acl_violations", list)
_ACL_ALERT_THRESHOLD = 10
_ACL_ALERT_WINDOW = 3600  # 1 hour


def log_acl_violation(user_id: str, board_id: str, resource: str) -> None:
    """Record an ACL violation. Alert if threshold exceeded."""
    now = datetime.now(timezone.utc)
    _acl_violations.append(
        {
            "user_id": user_id,
            "board_id": board_id,
            "resource": resource,
            "timestamp": now.isoformat(),
        }
    )

    # Check alert threshold
    window_start = now - timedelta(seconds=_ACL_ALERT_WINDOW)
    recent = [
        v
        for v in _acl_violations
        if v["user_id"] == user_id and v["timestamp"] > window_start.isoformat()
    ]
    if len(recent) >= _ACL_ALERT_THRESHOLD:
        logger.warning(
            "acl.alert user=%s violations=%d window=1h",
            user_id,
            len(recent),
            extra={
                "event": "acl.alert",
                "user_id": user_id,
                "violation_count": len(recent),
            },
        )


def get_acl_violations(user_id: str | None = None, limit: int = 100) -> list[dict]:
    """Return recent ACL violations, optionally filtered by user."""
    results = (
        _acl_violations
        if not user_id
        else [v for v in _acl_violations if v["user_id"] == user_id]
    )
    return results[-limit:]


def clear_acl_violations_for_tests() -> None:
    _acl_violations.clear()


# ---------------------------------------------------------------------------
# Right to erasure (FR-18, FR-19)
# ---------------------------------------------------------------------------


async def _authoritative_survivor_board_ids(
    db: Any,
    *,
    erased_board_id: str,
) -> tuple[str, ...]:
    """Read current relational board truth for Global rewrite fencing."""

    from okto_pulse.core.ports.application_persistence import (
        ApplicationQuery,
        get_application_persistence_port,
    )

    async def _read(context: Any) -> tuple[str, ...]:
        rows = await get_application_persistence_port().list(
            context,
            ApplicationQuery(
                entity="board",
                order_by=(("id", False),),
                select_fields=("id",),
            ),
        )
        return tuple(
            sorted({str(row.id) for row in rows if str(row.id) != erased_board_id})
        )

    if db is not None:
        return await _read(db)

    from okto_pulse.core.ports.relational_runtime import get_db_session

    async with get_db_session() as authoritative_db:
        return await _read(authoritative_db)


async def right_to_erasure(
    db: Any,
    board_id: str,
    *,
    strict: bool = False,
    commit: bool = True,
    global_writer_guarded: bool = False,
    purge_relational: bool = True,
) -> dict:
    """Wipe all KG data for a board via logical runtime and audit purges.

    By default each step is best-effort so partial erasure still removes as
    much as possible. ``strict=True`` propagates the first failure. Combined
    with ``commit=False``, callers can stage relational KG cleanup and the
    board deletion in one UnitOfWork commit.
    """
    counts: dict[str, Any] = {"board_id": board_id}

    # 1. Global discovery cascade
    try:
        from okto_pulse.core.kg.global_discovery.clustering import board_delete_cascade

        cascade = await _run_destructive_thread(
            board_delete_cascade,
            board_id,
            strict=strict,
            purge_board_graph=not strict,
            purge_relational_runtime=not strict,
            global_writer_guarded=global_writer_guarded,
        )
        counts["global_cascade"] = cascade
    except Exception as exc:
        if strict:
            raise
        counts["global_cascade_error"] = str(exc)

    # 2. Physical Global Discovery rewrite. The active database, inactive
    # generations and recovery snapshots can all retain deleted bytes; the
    # runtime rewrites a fresh target-free database and restores surviving
    # boards before returning its verified receipt.
    try:
        from okto_pulse.core.kg.interfaces import get_kg_registry

        survivor_board_ids = await _authoritative_survivor_board_ids(
            db,
            erased_board_id=board_id,
        )
        global_runtime = get_kg_registry().require_global_discovery_runtime()
        erase_global = getattr(
            global_runtime,
            "erase_storage_for_privacy",
            None,
        )
        if not callable(erase_global):
            raise BoardErasureVerificationError(
                "global_discovery_physical_erasure_unavailable"
            )
        global_storage_purge = await _run_destructive_thread(
            erase_global,
            board_id=board_id,
            reason="board_right_to_erasure",
            survivor_board_ids=survivor_board_ids,
        )
        counts["global_storage_purge"] = (
            _require_verified_physical_erasure(
                global_storage_purge,
                board_id=board_id,
                capability="global_discovery_storage",
            )
            if strict
            else dict(global_storage_purge)
        )
    except Exception as exc:
        if strict:
            raise
        counts["global_storage_purge_error"] = str(exc)

    # 3. Per-board graph purge through the logical runtime capability.
    try:
        from okto_pulse.core.kg.interfaces import get_kg_registry

        graph_store = get_kg_registry().graph_runtime_store
        erase = (
            getattr(graph_store, "erase_board_graph", None)
            if strict
            else graph_store.purge_board_graph
        )
        if not callable(erase):
            raise BoardErasureVerificationError(
                "board_graph_physical_erasure_unavailable"
            )
        purge = erase(board_id, reason="right_to_erasure")
        counts["graph_purge"] = asdict(purge)
        if strict:
            if purge.status not in {"erased", "purged", "not_found"}:
                raise BoardErasureVerificationError(
                    "board_graph_purge_failed "
                    f"board={board_id} status={purge.status} "
                    f"error_code={purge.error_code}"
                )
            from okto_pulse.core.kg.interfaces import (
                GraphRuntimeObservationState,
            )

            graph_state = graph_store.graph_state(board_id)
            if (
                graph_state.normalized_state
                is not GraphRuntimeObservationState.CONFIRMED_ABSENT
            ):
                raise BoardErasureVerificationError(
                    "board_graph_absence_unverified "
                    f"board={board_id} state={graph_state.normalized_state.value} "
                    f"reason={graph_state.reason_code}"
                )
            counts["graph_verified_absent"] = True
    except Exception as exc:
        if strict:
            raise
        counts["graph_purge_error"] = str(exc)

    # 4. Uploaded attachment objects. The strict capability returns an explicit
    # board-scoped absence receipt; its default implementation fails closed.
    try:
        from okto_pulse.core.infra.storage import get_storage_provider

        attachment_purge = await get_storage_provider().purge_board(board_id)
        counts["attachment_purge"] = (
            _require_verified_physical_erasure(
                attachment_purge,
                board_id=board_id,
                capability="attachment_storage",
            )
            if strict
            else dict(attachment_purge)
        )
    except Exception as exc:
        if strict:
            raise
        counts["attachment_purge_error"] = str(exc)

    # 5. Rebuild/audit/cognitive/quarantine artifacts on durable storage.
    try:
        from okto_pulse.core.kg.interfaces import get_kg_registry

        artifact_purge = await _run_destructive_thread(
            get_kg_registry()
            .require_rebuild_audit_artifact_store()
            .purge_board_artifacts,
            board_id,
        )
        counts["artifact_purge"] = (
            _require_verified_physical_erasure(
                artifact_purge,
                board_id=board_id,
                capability="rebuild_artifact_storage",
            )
            if strict
            else dict(artifact_purge)
        )
    except Exception as exc:
        if strict:
            raise
        counts["artifact_purge_error"] = str(exc)

    # 6. SQLite audit/refs/outbox/KB purge. The board DELETE use case stages
    # and commits this first, then invokes the external phase with
    # ``purge_relational=False``. That ordering guarantees a failed relational
    # commit cannot leave a live source board after physical erasure.
    if purge_relational:
        try:
            await stage_board_relational_erasure(db, board_id)
            if commit:
                await get_kg_governance_store().commit(db)
            counts["sqlite_purged"] = True
        except Exception as exc:
            if strict:
                raise
            counts["sqlite_purge_error"] = str(exc)

    logger.info(
        "governance.erasure board=%s",
        board_id,
        extra={"event": "governance.erasure", **counts},
    )
    return counts


async def stage_board_relational_erasure(
    db: Any,
    board_id: str,
    *,
    actor_id: str | None = None,
) -> None:
    """Stage and verify all board-scoped relational KG/KB cleanup.

    Board deletion supplies ``actor_id`` so the same transaction also persists
    a continuation row. The row intentionally survives the source delete and is
    removed only after every idempotent external erasure receipt is verified.
    Legacy KG-only callers omit it and retain their historical behavior.
    """

    store = get_kg_governance_store()
    if actor_id is not None:
        await store.stage_board_erasure_job(
            db,
            board_id=board_id,
            actor_id=actor_id,
        )
    await store.purge_board_metadata(db, board_id=board_id)


async def get_board_erasure_job(db: Any, board_id: str):
    """Return a pending durable board-erasure continuation, if one exists."""

    return await get_kg_governance_store().get_board_erasure_job(
        db,
        board_id=board_id,
    )


async def record_board_erasure_failure(
    db: Any,
    board_id: str,
    error: Exception,
) -> None:
    """Persist bounded retry state without depending on the deleted Board."""

    store = get_kg_governance_store()
    job = await store.get_board_erasure_job(db, board_id=board_id)
    if job is None:
        return
    next_attempt_number = max(1, int(job.attempts) + 1)
    delay_seconds = min(3600, 2 ** min(next_attempt_number, 10))
    await store.record_board_erasure_failure(
        db,
        board_id=board_id,
        error=f"{error.__class__.__name__}: {error}"[:2048],
        next_attempt_at=datetime.now(timezone.utc) + timedelta(seconds=delay_seconds),
    )


async def complete_board_erasure_job(db: Any, board_id: str) -> bool:
    """Remove the durable continuation after physical absence is proven."""

    return await get_kg_governance_store().complete_board_erasure_job(
        db,
        board_id=board_id,
    )


# ===========================================================================
# Node relevance boost (spec R01A REST-FU5-S4 — kg_routes.boost_node)
# ===========================================================================


class BoostPersistError(Exception):
    """Raised when the graph SET that persists a node boost fails.

    The legacy ``api/kg_routes.boost_node`` endpoint caught the SET exception
    inline and returned a 500 ``graph_error`` RFC 7807 problem; the REST adapter
    catches this and reproduces that exact problem body."""


BOOST_DELTA = 0.3
BOOST_CLAMP_MIN = 0.0
BOOST_CLAMP_MAX = 1.5


@dataclass(frozen=True, slots=True)
class BoostNodeMutation:
    """Graph result plus an audit record that is still safe to stage on-loop."""

    payload: dict[str, Any]
    audit: BoostAuditRecord


async def mutate_boost_node_graph(
    board_id: str,
    node_id: str,
    *,
    actor_id: str,
) -> BoostNodeMutation | None:
    """Boost a graph node and return, but do not stage, its relational audit.

    The graph read/SET runs through the #06 ``GraphTransaction`` port (the embedded
    store auto-commits each statement), and idempotency is NOT enforced — each call
    stacks another +0.3 until the clamp is reached. The boost response body and the
    +0.3/clamp arithmetic reproduce the legacy ``api/kg_routes.boost_node``
    byte-for-byte.

    Returns the mutation on success. Returns ``None`` when the node is
    absent in every node type of the board graph (this module stays
    transport-free — the use case maps that to ``EntityNotFoundError`` → the
    adapter's 404 problem). A failure to persist the SET raises
    :class:`BoostPersistError` (adapter → 500 ``graph_error``).

    The returned audit row carries ALL required NOT-NULL columns (``artifact_type``,
    ``started_at``, …) so it persists on a successful boost — bug 547a2aa8 fix; the
    legacy row omitted those columns, so its commit always raised IntegrityError and
    was silently swallowed (200 with no audit row). Splitting graph mutation from
    audit staging lets async request paths execute native graph IO in a worker without
    moving their event-loop-bound UnitOfWork to another loop."""
    import uuid

    from okto_pulse.core.domain.code_traceability_kg import (
        CodeTraceabilityKGWriteViolation,
        is_code_traceability_subtype,
    )
    from okto_pulse.core.kg.interfaces.registry import get_kg_registry
    from okto_pulse.core.kg.schema_contract import NODE_TYPES

    score_before: float | None = None
    node_type: str | None = None
    revocation_reason: str | None = None
    pre_cancellation_score: float | None = None
    # Stamp the audit start before the graph read/SET so the persisted
    # ConsolidationAudit row carries a truthful ``started_at`` (bug 547a2aa8 fix).
    started_at = datetime.now(timezone.utc)
    # Read+write through the #06 GraphTransaction port — behaviour-identical to
    # the legacy direct (db, conn) tuple (embedded auto-commits per statement on
    # the SET); the relational ``db`` only carries the audit row.
    async with await get_kg_registry().graph_transaction.begin(board_id) as scope:
        for ntype in NODE_TYPES:
            try:
                res = scope.execute(
                    f"MATCH (n:{ntype} {{id: $nid}}) "
                    "RETURN n.relevance_score, n.revocation_reason, "
                    "n.pre_cancellation_relevance_score, n.kind_of",
                    {"nid": node_id},
                )
            except Exception:
                continue
            if res.rows:
                row = res.rows[0]
                if ntype == "Entity" and is_code_traceability_subtype(
                    row[3] if len(row) > 3 else None
                ):
                    raise CodeTraceabilityKGWriteViolation(
                        "Code Traceability KG projections cannot be boosted"
                    )
                score_before = float(row[0]) if row[0] is not None else 0.5
                revocation_reason = (
                    str(row[1]) if len(row) > 1 and row[1] is not None else None
                )
                pre_cancellation_score = (
                    float(row[2]) if len(row) > 2 and row[2] is not None else None
                )
                node_type = ntype
                break

        if node_type is None or score_before is None:
            return None

        is_cancelled = revocation_reason == CANCELLATION_REVOCATION_REASON
        base_before = (
            pre_cancellation_score
            if is_cancelled and pre_cancellation_score is not None
            else (
                score_before + CANCELLATION_SCORE_PENALTY
                if is_cancelled
                else score_before
            )
        )
        base_after = max(
            BOOST_CLAMP_MIN, min(BOOST_CLAMP_MAX, base_before + BOOST_DELTA)
        )
        score_after = (
            max(BOOST_CLAMP_MIN, base_after - CANCELLATION_SCORE_PENALTY)
            if is_cancelled
            else base_after
        )
        try:
            if is_cancelled:
                scope.execute(
                    f"MATCH (n:{node_type} {{id: $nid}}) "
                    "SET n.relevance_score = $score, "
                    "n.pre_cancellation_relevance_score = $base_score",
                    {
                        "nid": node_id,
                        "score": score_after,
                        "base_score": base_after,
                    },
                )
            else:
                scope.execute(
                    f"MATCH (n:{node_type} {{id: $nid}}) "
                    "SET n.relevance_score = $score",
                    {"nid": node_id, "score": score_after},
                )
        except Exception as exc:
            raise BoostPersistError(f"Failed to persist boost: {exc}") from exc

    boosted_at = datetime.now(timezone.utc)
    boosted_by = actor_id

    # Build the boost audit row with ALL required NOT-NULL columns populated
    # (bug 547a2aa8 fix): the legacy staging omitted ``artifact_type`` and
    # ``started_at``, so its commit always raised IntegrityError and was swallowed,
    # dropping the row while the boost still returned 200. ``artifact_type="boost"``
    # is intentionally OUTSIDE ``CONSOLIDABLE_ARTIFACT_TYPES`` — a boost bumps an
    # existing node's relevance_score, it is not an artifact consolidation, so the
    # rebuild/health counters must not treat it as one (and nodes_added/edges_added=0
    # keep it out of every count). The caller (BoostNodeUseCase) commits this row;
    # that commit stays best-effort only for the already-mutated-graph split-brain
    # case, NOT to mask a deterministic schema violation.
    return BoostNodeMutation(
        payload={
            "node_id": node_id,
            "node_type": node_type,
            "score_before": round(score_before, 4),
            "score_after": round(score_after, 4),
            "boosted_at": boosted_at.isoformat(),
            "boosted_by": boosted_by,
        },
        audit=BoostAuditRecord(
            session_id=(
                f"boost-{node_id[:8]}-{int(boosted_at.timestamp())}"
                f"-{uuid.uuid4().hex[:8]}"
            ),
            board_id=board_id,
            artifact_id=node_id,
            agent_id=boosted_by,
            started_at=started_at,
            committed_at=boosted_at,
        ),
    )


def stage_boost_node_audit(
    db: Any,
    mutation: BoostNodeMutation,
) -> dict[str, Any]:
    """Stage a prepared audit on the caller's original UnitOfWork loop."""

    get_kg_governance_store().add_boost_audit(db, mutation.audit)
    return dict(mutation.payload)


async def boost_node(
    db: Any,
    board_id: str,
    node_id: str,
    *,
    actor_id: str,
) -> dict[str, Any] | None:
    """Compatibility composition of graph mutation and on-loop audit staging."""

    mutation = await mutate_boost_node_graph(
        board_id,
        node_id,
        actor_id=actor_id,
    )
    if mutation is None:
        return None
    return stage_boost_node_audit(db, mutation)
