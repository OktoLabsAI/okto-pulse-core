"""KG governance: ACL, audit, undo, retention and right to erasure.

Persistence and concrete graph effects are supplied through edition-owned ports.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Mapping
from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from threading import Lock
from typing import Any, AsyncIterator

from okto_pulse.core.runtime_context import runtime_state
from okto_pulse.core.ports.kg_governance import get_kg_governance_store

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
