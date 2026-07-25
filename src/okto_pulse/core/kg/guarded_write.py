"""Reusable single-writer + durability boundary for board graph mutations.

Callers enter the boundary before the first graph mutation, invoke
``ensure_durable`` after the mutation, and leave it only after any associated
relational acknowledgement/finalization has completed.  The helper deliberately
owns no mutation semantics; it composes the existing lock, write barrier and
graph-lifecycle ports into one fail-closed contract.
"""

from __future__ import annotations

import logging
import uuid
from contextlib import contextmanager
from contextvars import ContextVar, copy_context
from dataclasses import dataclass, field
from math import isfinite
from threading import Event, Lock, Thread
from typing import Iterator, Sequence

from okto_pulse.core.kg.interfaces.registry import get_kg_registry
from okto_pulse.core.kg.safe_write_lifecycle import (
    KGSafeWriteLifecycle,
    LockOwnerProbe,
    STEP_CHECKPOINT,
    STEP_FLUSH,
    STEP_FSYNC,
    SafeWriteLifecycleError,
    SafeWriteLifecycleStatus,
)
from okto_pulse.core.kg.single_writer_lock import (
    DEFAULT_TTL_SECONDS,
    KGSingleWriterLock,
    SingleWriterLockError,
)
from okto_pulse.core.kg.write_barrier import under_safe_write

logger = logging.getLogger("okto_pulse.kg.guarded_write")


BOARD_COMMIT_LIFECYCLE_STEPS: tuple[str, ...] = (
    STEP_CHECKPOINT,
    STEP_FLUSH,
    STEP_FSYNC,
)
_MAX_HEARTBEAT_JOIN_SECONDS = 1.0


class GuardedWriteError(RuntimeError):
    """Typed failure raised before a guarded mutation can be acknowledged."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        retryable: bool,
        details: dict[str, object] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.retryable = retryable
        self.details = dict(details or {})


@dataclass(slots=True)
class GuardedWriteLease:
    """Live board-writer fence exposed to one mutation boundary."""

    board_id: str
    operation: str
    owner_token: str
    mutation_ref: str
    required_steps: tuple[str, ...]
    _lifecycle: KGSafeWriteLifecycle
    _writer_lock: KGSingleWriterLock
    _ttl_seconds: int
    _durability_applied: bool = False
    _state_lock: Lock = field(
        default_factory=Lock,
        repr=False,
        compare=False,
    )
    _fence_lost: bool = field(default=False, repr=False, compare=False)
    _fence_failure: BaseException | None = field(
        default=None,
        repr=False,
        compare=False,
    )

    @property
    def durability_applied(self) -> bool:
        return self._durability_applied

    def _lost_error(self, *, failure_phase: str) -> GuardedWriteError:
        return GuardedWriteError(
            "writer_lease_lost",
            "board graph writer lease was lost",
            retryable=True,
            details={
                "board_id": self.board_id,
                "operation": self.operation,
                "failure_phase": failure_phase,
                "failure_type": (
                    type(self._fence_failure).__name__
                    if self._fence_failure is not None
                    else None
                ),
                "durability_applied": self._durability_applied,
                "write_may_be_applied": self._durability_applied,
            },
        )

    def ensure_owned(self, *, failure_phase: str = "ownership_check") -> None:
        """Fail closed unless this lease still owns the durable writer token."""

        with self._state_lock:
            if self._fence_lost:
                raise self._lost_error(failure_phase=failure_phase)

        try:
            owns_fence = self._writer_lock.is_owner(
                self.board_id,
                self.owner_token,
            )
        except Exception as exc:
            with self._state_lock:
                self._fence_lost = True
                self._fence_failure = exc
            raise self._lost_error(failure_phase=failure_phase) from exc

        with self._state_lock:
            if self._fence_lost:
                raise self._lost_error(failure_phase=failure_phase)
            if not owns_fence:
                self._fence_lost = True
                raise self._lost_error(failure_phase=failure_phase)

    def renew(self) -> None:
        """Renew the exact live token or poison the lease permanently."""

        with self._state_lock:
            if self._fence_lost:
                raise self._lost_error(failure_phase="heartbeat")

        try:
            renewed = self._writer_lock.renew(
                board_id=self.board_id,
                owner_token=self.owner_token,
                ttl_seconds=self._ttl_seconds,
            )
        except Exception as exc:
            with self._state_lock:
                self._fence_lost = True
                self._fence_failure = exc
            raise self._lost_error(failure_phase="heartbeat") from exc

        with self._state_lock:
            if self._fence_lost:
                raise self._lost_error(failure_phase="heartbeat")
            if not renewed:
                self._fence_lost = True
                raise self._lost_error(failure_phase="heartbeat")

    def _poison(self, failure: BaseException) -> None:
        """Permanently fail this lease without waiting on an external port."""

        with self._state_lock:
            self._fence_lost = True
            self._fence_failure = failure

    def ensure_durable(
        self,
        *,
        mutation_ref: str | None = None,
        required_steps: Sequence[str] | None = None,
    ) -> None:
        """Apply the required lifecycle or raise a typed fail-closed error."""

        self.ensure_owned(failure_phase="before_lifecycle")
        try:
            response = self._lifecycle.apply(
                board_id=self.board_id,
                graph_type="board_graph",
                operation=self.operation,
                owner_token=self.owner_token,
                mutation_ref=mutation_ref or self.mutation_ref,
                required_steps=(
                    tuple(required_steps)
                    if required_steps is not None
                    else self.required_steps
                ),
            )
        except SafeWriteLifecycleError as exc:
            raise GuardedWriteError(
                exc.code.value,
                "board graph durability lifecycle could not be applied",
                retryable=exc.retryable,
                details={
                    "board_id": self.board_id,
                    "operation": self.operation,
                    "correlation_id": exc.correlation_id,
                    "reason": exc.reason,
                },
            ) from exc

        if response.status is not SafeWriteLifecycleStatus.APPLIED:
            raise GuardedWriteError(
                "safe_lifecycle_failed",
                "board graph durability lifecycle failed",
                retryable=True,
                details={
                    "board_id": self.board_id,
                    "operation": self.operation,
                    "correlation_id": response.correlation_id,
                    "failed_step": response.failed_step,
                    "applied_steps": list(response.applied_steps),
                    "health_state_after": response.health_state_after,
                },
            )
        self._durability_applied = True
        self.ensure_owned(failure_phase="after_lifecycle")


_active_board_write_leases: ContextVar[tuple[GuardedWriteLease, ...]] = (
    ContextVar("okto_pulse_active_board_write_leases", default=())
)


def revalidate_active_board_write_lease(
    board_id: str,
    *,
    failure_phase: str = "graph_statement_precommit",
    expected_owner_token: str | None = None,
) -> GuardedWriteLease | None:
    """Revalidate the active Core lease immediately before a graph write.

    Edition adapters call this at their final ``execute`` boundary for mutating
    statements.  ``None`` means this context has no Core guarded lease, so
    administrative/legacy lanes may continue to enforce their own fence.  Once
    any guarded lease is active, a board mismatch or token mismatch is a
    fail-closed boundary violation.
    """

    leases = _active_board_write_leases.get()
    for lease in reversed(leases):
        if lease.board_id != board_id:
            continue
        if (
            expected_owner_token is not None
            and lease.owner_token != expected_owner_token
        ):
            raise GuardedWriteError(
                "writer_lease_token_mismatch",
                "active board graph writer token does not match",
                retryable=False,
                details={
                    "board_id": board_id,
                    "operation": lease.operation,
                    "failure_phase": failure_phase,
                },
            )
        lease.ensure_owned(failure_phase=failure_phase)
        return lease

    if leases:
        raise GuardedWriteError(
            "writer_lease_board_mismatch",
            "a graph write targeted a board outside the active writer lease",
            retryable=False,
            details={
                "board_id": board_id,
                "active_board_ids": sorted(
                    {lease.board_id for lease in leases}
                ),
                "failure_phase": failure_phase,
            },
        )
    return None


@contextmanager
def _activate_board_write_lease(
    lease: GuardedWriteLease,
) -> Iterator[None]:
    current = _active_board_write_leases.get()
    token = _active_board_write_leases.set(current + (lease,))
    try:
        yield
    finally:
        _active_board_write_leases.reset(token)


@contextmanager
def guarded_board_write(
    board_id: str,
    *,
    operation: str,
    owner_id: str,
    mutation_ref: str,
    required_steps: Sequence[str] = BOARD_COMMIT_LIFECYCLE_STEPS,
    ttl_seconds: int = DEFAULT_TTL_SECONDS,
    writer_lock: KGSingleWriterLock | None = None,
    lifecycle: KGSafeWriteLifecycle | None = None,
    renew_interval_seconds: float | None = None,
) -> Iterator[GuardedWriteLease]:
    """Acquire a real board writer, install the barrier, and release in ``finally``.

    ``lifecycle`` and ``writer_lock`` are injectable ports for focused tests.
    Production callers omit them and consume the composed registry/lock ports.
    """

    if not isinstance(board_id, str) or not board_id.strip():
        raise GuardedWriteError(
            "invalid_board_id",
            "board_id is required for a guarded graph write",
            retryable=False,
        )
    if not operation:
        raise GuardedWriteError(
            "boundary_violation",
            "operation is required for a guarded graph write",
            retryable=False,
        )

    active_lock = writer_lock or KGSingleWriterLock()
    try:
        acquisition = active_lock.acquire(
            board_id=board_id,
            operation=operation,
            owner_id=f"{owner_id}:{operation}:{uuid.uuid4().hex}",
            ttl_seconds=ttl_seconds,
        )
    except SingleWriterLockError as exc:
        raise GuardedWriteError(
            exc.code.value,
            "board graph writer lock could not be acquired",
            retryable=exc.retryable,
            details={"board_id": board_id, "operation": operation},
        ) from exc
    except Exception as exc:
        raise GuardedWriteError(
            "writer_lock_unavailable",
            "board graph writer lock is unavailable",
            retryable=True,
            details={
                "board_id": board_id,
                "operation": operation,
                "failure_type": type(exc).__name__,
            },
        ) from exc

    if not acquisition.acquired or not acquisition.owner_token:
        raise GuardedWriteError(
            "lock_contention",
            "another writer currently owns the board graph",
            retryable=True,
            details={"board_id": board_id, "operation": operation},
        )

    owner_token = acquisition.owner_token
    active_lifecycle = lifecycle
    if active_lifecycle is None:
        provider_failure_type: str | None = None
        try:
            graph_lifecycle = get_kg_registry().graph_lifecycle
        except Exception as exc:
            graph_lifecycle = None
            provider_failure_type = type(exc).__name__
            logger.error(
                "kg.guarded_write.lifecycle_resolution_failed "
                "board=%s operation=%s",
                board_id,
                operation,
                exc_info=True,
            )
        if graph_lifecycle is None:
            release_failure_type: str | None = None
            try:
                released = active_lock.release(
                    board_id=board_id,
                    owner_token=owner_token,
                )
                if not released:
                    release_failure_type = "LockOwnershipLost"
            except Exception as exc:
                release_failure_type = type(exc).__name__
                logger.error(
                    "kg.guarded_write.release_failed_without_lifecycle "
                    "board=%s operation=%s",
                    board_id,
                    operation,
                    exc_info=True,
                )
            raise GuardedWriteError(
                "safe_lifecycle_unavailable",
                "board graph lifecycle provider is not configured",
                retryable=False,
                details={
                    "board_id": board_id,
                    "operation": operation,
                    "provider_failure_type": provider_failure_type,
                    "release_failure_type": release_failure_type,
                },
            )
        active_lifecycle = KGSafeWriteLifecycle(
            step_adapter=graph_lifecycle.apply_step,
            owner_probe=LockOwnerProbe(is_active_owner=active_lock.is_owner),
        )

    lease = GuardedWriteLease(
        board_id=board_id,
        operation=operation,
        owner_token=owner_token,
        mutation_ref=mutation_ref,
        required_steps=tuple(required_steps),
        _lifecycle=active_lifecycle,
        _writer_lock=active_lock,
        _ttl_seconds=ttl_seconds,
    )
    renew_interval = (
        ttl_seconds / 3
        if renew_interval_seconds is None
        else float(renew_interval_seconds)
    )
    if (
        not isfinite(renew_interval)
        or renew_interval <= 0
        or renew_interval > ttl_seconds / 3
    ):
        try:
            active_lock.release(
                board_id=board_id,
                owner_token=owner_token,
            )
        finally:
            raise GuardedWriteError(
                "invalid_renew_interval",
                "writer renew interval must be positive and no greater "
                "than one third of ttl_seconds",
                retryable=False,
                details={
                    "board_id": board_id,
                    "operation": operation,
                },
            )

    body_failed = False
    stop_heartbeat = Event()

    def _renew_until_stopped() -> None:
        while not stop_heartbeat.wait(renew_interval):
            try:
                lease.renew()
            except BaseException:
                logger.error(
                    "kg.guarded_write.heartbeat_failed "
                    "board=%s operation=%s",
                    board_id,
                    operation,
                    exc_info=True,
                )
                return

    heartbeat_thread: Thread | None = None
    heartbeat_shutdown_timed_out = False
    heartbeat_join_timeout = min(
        max(renew_interval, 0.05),
        min(float(ttl_seconds), _MAX_HEARTBEAT_JOIN_SECONDS),
    )

    def _stop_heartbeat_bounded(*, failure_phase: str) -> None:
        nonlocal heartbeat_thread, heartbeat_shutdown_timed_out
        stop_heartbeat.set()
        thread = heartbeat_thread
        if thread is None:
            return
        thread.join(timeout=heartbeat_join_timeout)
        if thread.is_alive():
            heartbeat_shutdown_timed_out = True
            timeout_error = TimeoutError(
                "board writer heartbeat did not stop within the bounded join"
            )
            lease._poison(timeout_error)
            raise GuardedWriteError(
                "writer_heartbeat_shutdown_timeout",
                "board graph writer heartbeat did not stop safely",
                retryable=True,
                details={
                    "board_id": board_id,
                    "operation": operation,
                    "failure_phase": failure_phase,
                    "join_timeout_seconds": heartbeat_join_timeout,
                    "durability_applied": lease.durability_applied,
                    "write_may_be_applied": lease.durability_applied,
                    "lock_release_skipped": True,
                },
            )
        heartbeat_thread = None

    try:
        with (
            under_safe_write(board_id, owner_token, operation),
            _activate_board_write_lease(lease),
        ):
            lease.ensure_owned(failure_phase="guard_enter")
            heartbeat_context = copy_context()
            heartbeat_thread = Thread(
                target=heartbeat_context.run,
                args=(_renew_until_stopped,),
                name="pulse-board-writer-renewal",
                daemon=True,
            )
            heartbeat_thread.start()
            try:
                yield lease
                _stop_heartbeat_bounded(failure_phase="guard_exit")
                lease.ensure_owned(failure_phase="guard_exit")
                if not lease.durability_applied:
                    raise GuardedWriteError(
                        "durability_not_applied",
                        "guarded graph write exited without a successful "
                        "durability lifecycle",
                        retryable=False,
                        details={
                            "board_id": board_id,
                            "operation": operation,
                        },
                    )
            except BaseException:
                body_failed = True
                raise
    finally:
        if not heartbeat_shutdown_timed_out:
            try:
                _stop_heartbeat_bounded(failure_phase="exception_exit")
            except GuardedWriteError:
                logger.error(
                    "kg.guarded_write.heartbeat_shutdown_timeout "
                    "board=%s operation=%s lock_release_skipped=true",
                    board_id,
                    operation,
                    exc_info=True,
                )
                if not body_failed:
                    raise
        if heartbeat_shutdown_timed_out:
            logger.critical(
                "kg.guarded_write.lock_retained_after_heartbeat_timeout "
                "board=%s operation=%s",
                board_id,
                operation,
                extra={
                    "event": (
                        "kg.guarded_write."
                        "lock_retained_after_heartbeat_timeout"
                    ),
                    "board_id": board_id,
                    "operation": operation,
                    "lock_release_skipped": True,
                },
            )
            # A renew call is still in-flight. Releasing here could race a late
            # renewal and resurrect/extend a token after another writer enters.
            # Retain the manifest for TTL/stale recovery and surface the error.
        else:
            try:
                released = active_lock.release(
                    board_id=board_id,
                    owner_token=owner_token,
                )
            except Exception as exc:
                if body_failed:
                    logger.error(
                        "kg.guarded_write.release_failed board=%s operation=%s",
                        board_id,
                        operation,
                        exc_info=True,
                    )
                else:
                    raise GuardedWriteError(
                        "writer_lock_release_failed",
                        "board graph writer lock could not be released",
                        retryable=True,
                        details={
                            "board_id": board_id,
                            "operation": operation,
                            "failure_type": type(exc).__name__,
                            "durability_applied": lease.durability_applied,
                            "failure_phase": "release_after_durability",
                            "write_may_be_applied": lease.durability_applied,
                        },
                    ) from exc
            else:
                if not released:
                    if body_failed:
                        logger.error(
                            "kg.guarded_write.release_token_mismatch "
                            "board=%s operation=%s",
                            board_id,
                            operation,
                        )
                    else:
                        raise GuardedWriteError(
                            "writer_lock_release_failed",
                            "board graph writer lock ownership was lost before "
                            "release",
                            retryable=True,
                            details={
                                "board_id": board_id,
                                "operation": operation,
                                "durability_applied": lease.durability_applied,
                                "failure_phase": "release_after_durability",
                                "write_may_be_applied": lease.durability_applied,
                            },
                        )


__all__ = [
    "BOARD_COMMIT_LIFECYCLE_STEPS",
    "GuardedWriteError",
    "GuardedWriteLease",
    "guarded_board_write",
    "revalidate_active_board_write_lease",
]
