"""Thread-safe in-memory contract provider for recovery-run tests."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
from threading import RLock

from okto_pulse.core.kg.global_discovery_recovery_control import (
    RecoveryAttempt,
    RecoveryBindingConflict,
    RecoveryCheckpoint,
    RecoveryCheckpointConflict,
    RecoveryInProgress,
    RecoveryLeaseTakeoverPolicy,
    RecoveryPreparationCommand,
    RecoveryPreparedResult,
    RecoveryRunPhase,
    RecoveryRunNotFound,
    RecoveryRunStatus,
    RecoveryResumePolicy,
    RecoveryResumeRejected,
    RecoveryStartCommand,
    RecoveryTerminalAttempt,
    RecoveryWorkerResult,
)


class MemoryRecoveryRunStore:
    """Linearizable test double for the edition-owned durable store port."""

    def __init__(self) -> None:
        self._lock = RLock()
        self._attempts: dict[tuple[str, int], RecoveryAttempt] = {}
        self._successful_checkpoint_writes = 0

    @property
    def successful_checkpoint_writes(self) -> int:
        with self._lock:
            return self._successful_checkpoint_writes

    def create_attempt(self, attempt: RecoveryAttempt) -> RecoveryAttempt:
        key = (attempt.run_id, attempt.epoch)
        with self._lock:
            if key in self._attempts:
                raise ValueError("recovery_attempt_already_exists")
            self._attempts[key] = attempt
            return attempt

    def get_attempt(self, *, run_id: str, epoch: int) -> RecoveryAttempt:
        with self._lock:
            return self._attempts[(str(run_id), int(epoch))]

    def compare_and_set_checkpoint(
        self,
        checkpoint: RecoveryCheckpoint,
    ) -> RecoveryAttempt:
        key = (checkpoint.run_id, checkpoint.epoch)
        with self._lock:
            current = self._attempts[key]
            updated = current.advance_checkpoint(checkpoint)
            self._attempts[key] = updated
            self._successful_checkpoint_writes += 1
            return updated


class MemoryRecoveryControlStore:
    """Linearizable lifecycle store for control-plane contract tests."""

    def __init__(self, *, clock: Callable[[], datetime] | None = None) -> None:
        self._lock = RLock()
        self._runs: dict[str, RecoveryRunStatus] = {}
        self._attempt_history: dict[tuple[str, int], RecoveryRunStatus] = {}
        self._active_slot_run_id: str | None = None
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def admit_preparation(
        self,
        command: RecoveryPreparationCommand,
    ) -> tuple[RecoveryRunStatus, bool]:
        with self._lock:
            if self._active_slot_run_id is not None:
                incumbent = self._runs[self._active_slot_run_id]
                if not incumbent.state.is_terminal:
                    if incumbent.run_id == command.binding.run_id:
                        if (
                            incumbent.binding != command.binding
                            or incumbent.started_at != command.admitted_at
                            or incumbent.counts != command.counts
                            or incumbent.attempt_budget_ms
                            != command.attempt_budget_ms
                        ):
                            raise RecoveryBindingConflict(
                                run_id=command.binding.run_id
                            )
                        return incumbent, False
                    raise RecoveryInProgress()
                self._active_slot_run_id = None
            existing = self._runs.get(command.binding.run_id)
            if existing is not None:
                if existing.binding != command.binding:
                    raise RecoveryBindingConflict(run_id=command.binding.run_id)
                return existing, False
            created = RecoveryRunStatus.initial_preparation(command)
            self._runs[created.run_id] = created
            self._active_slot_run_id = created.run_id
            return created, True

    def create_run(
        self,
        command: RecoveryStartCommand,
    ) -> tuple[RecoveryRunStatus, bool]:
        with self._lock:
            return self.admit_prepared_start(command)

    def admit_prepared_start(
        self,
        command: RecoveryStartCommand,
    ) -> tuple[RecoveryRunStatus, bool]:
        with self._lock:
            current = self._require(command.binding.run_id)
            self._require_epoch(current, expected_epoch=command.expected_epoch)
            if current.phase is RecoveryRunPhase.PREPARED:
                observed_at = self._clock()
                if observed_at.tzinfo is None or observed_at.utcoffset() is None:
                    raise ValueError("store clock must be timezone-aware")
                if current.expires_at is None or observed_at >= current.expires_at:
                    raise ValueError(
                        "prepared manifest expired before store admission CAS"
                    )
                updated = current.mark_confirmed(command)
                self._runs[current.run_id] = updated
                return updated, True
            if (
                current.binding == command.binding
                and current.epoch == command.expected_epoch
                and current.counts == command.counts
                and current.confirmed_by_actor_id
                == command.confirmed_by_actor_id
                and current.confirmation_consumed_at
                == command.confirmation_consumed_at
                and current.attempt_budget_ms == command.attempt_budget_ms
            ):
                return current, False
            raise RecoveryBindingConflict(run_id=command.binding.run_id)

    def get_status(self, *, run_id: str) -> RecoveryRunStatus | None:
        with self._lock:
            return self._runs.get(str(run_id))

    def get_attempt_history(
        self, *, run_id: str, epoch: int
    ) -> RecoveryRunStatus | None:
        with self._lock:
            return self._attempt_history.get((str(run_id), int(epoch)))

    def request_cancel(
        self,
        *,
        run_id: str,
        expected_epoch: int,
        requested_at: datetime,
        requested_by_actor_id: str,
        reason: str | None = None,
    ) -> RecoveryRunStatus:
        with self._lock:
            current = self._require(run_id)
            self._require_epoch(current, expected_epoch=expected_epoch)
            updated = current.request_cancel(
                requested_at=requested_at,
                requested_by_actor_id=requested_by_actor_id,
                reason=reason,
            )
            self._runs[current.run_id] = updated
            return updated

    def cancel_prepared(
        self,
        *,
        run_id: str,
        expected_epoch: int,
        requested_at: datetime,
        requested_by_actor_id: str,
        reason: str | None,
    ) -> RecoveryRunStatus:
        with self._lock:
            current = self._require(run_id)
            self._require_epoch(current, expected_epoch=expected_epoch)
            updated = current.cancel_prepared(
                requested_at=requested_at,
                requested_by_actor_id=requested_by_actor_id,
                reason=reason,
            )
            self._runs[current.run_id] = updated
            if updated.state.is_terminal and self._active_slot_run_id == current.run_id:
                self._active_slot_run_id = None
            return updated

    def admit_explicit_resume(
        self,
        *,
        run_id: str,
        expected_epoch: int,
        requested_at: datetime,
        requested_by_actor_id: str,
        reason: str | None = None,
    ) -> tuple[RecoveryRunStatus, bool]:
        with self._lock:
            current = self._require(run_id)
            if (
                current.epoch == int(expected_epoch) + 1
                and current.supersedes_epoch == int(expected_epoch)
                and current.resume_requested_by_actor_id
                == str(requested_by_actor_id).strip()
                and current.resume_audit_reason
                == (None if reason is None else str(reason).strip())
            ):
                return current, False
            self._require_epoch(current, expected_epoch=expected_epoch)

            if (
                self._active_slot_run_id is not None
                and self._active_slot_run_id != current.run_id
                and not self._runs[self._active_slot_run_id].state.is_terminal
            ):
                raise RecoveryInProgress()

            if current.state.is_terminal:
                assert current.terminal_outcome is not None
                decision = RecoveryResumePolicy().evaluate(
                    RecoveryTerminalAttempt(
                        run_id=current.run_id,
                        epoch=current.epoch,
                        outcome=current.terminal_outcome,
                        reason_code=current.reason_code,
                        retryable=current.retryable,
                        active_elapsed_ms=current.active_elapsed_ms,
                        cumulative_active_ms=current.cumulative_active_ms,
                    )
                )
                if not decision.admitted:
                    raise RecoveryResumeRejected(
                        code=decision.reason_code,
                        run_id=current.run_id,
                        epoch=current.epoch,
                    )
                updated = current.resume_terminal(
                    decision=decision,
                    requested_at=requested_at,
                    requested_by_actor_id=requested_by_actor_id,
                    reason=reason,
                )
            elif current.state.value == "running":
                if current.cancel_requested_at is not None:
                    raise RecoveryResumeRejected(
                        code="recovery_cancel_pending",
                        run_id=current.run_id,
                        epoch=current.epoch,
                    )
                decision = RecoveryLeaseTakeoverPolicy().evaluate(
                    current,
                    now=requested_at,
                )
                if not decision.admitted:
                    raise RecoveryResumeRejected(
                        code=decision.reason_code,
                        run_id=current.run_id,
                        epoch=current.epoch,
                    )
                updated = current.take_over_lease(
                    decision=decision,
                    requested_at=requested_at,
                    requested_by_actor_id=requested_by_actor_id,
                    reason=reason,
                )
            else:
                code = (
                    "prepared_run_not_adoptable"
                    if current.phase is RecoveryRunPhase.PREPARED
                    else "recovery_dispatch_not_adoptable"
                )
                raise RecoveryResumeRejected(
                    code=code,
                    run_id=current.run_id,
                    epoch=current.epoch,
                )

            self._attempt_history[(current.run_id, current.epoch)] = current
            self._runs[current.run_id] = updated
            self._active_slot_run_id = current.run_id
            return updated, True

    def mark_preparing(
        self,
        *,
        run_id: str,
        epoch: int,
        at: datetime,
    ) -> RecoveryRunStatus:
        return self.mark_running(run_id=run_id, epoch=epoch, at=at)

    def mark_prepared(
        self,
        *,
        run_id: str,
        epoch: int,
        expected_progress_seq: int,
        prepared: RecoveryPreparedResult,
    ) -> RecoveryRunStatus:
        with self._lock:
            current = self._require(run_id)
            self._require_epoch(current, expected_epoch=epoch)
            updated = current.mark_prepared(
                prepared=prepared,
                expected_progress_seq=expected_progress_seq,
            )
            self._runs[current.run_id] = updated
            return updated

    def mark_running(
        self,
        *,
        run_id: str,
        epoch: int,
        at: datetime,
    ) -> RecoveryRunStatus:
        with self._lock:
            current = self._require(run_id)
            self._require_epoch(current, expected_epoch=epoch)
            updated = current.mark_running(at=at)
            self._runs[current.run_id] = updated
            if updated.state.is_terminal and self._active_slot_run_id == current.run_id:
                self._active_slot_run_id = None
            return updated

    def compare_and_set_checkpoint(
        self,
        checkpoint: RecoveryCheckpoint,
    ) -> RecoveryRunStatus:
        with self._lock:
            current = self._require(checkpoint.run_id)
            updated = current.advance_checkpoint(checkpoint)
            self._runs[current.run_id] = updated
            return updated

    def complete(
        self,
        *,
        run_id: str,
        epoch: int,
        expected_progress_seq: int,
        completed_at: datetime,
        active_elapsed_ms: int,
        result: RecoveryWorkerResult,
    ) -> RecoveryRunStatus:
        with self._lock:
            current = self._require(run_id)
            self._require_epoch(current, expected_epoch=epoch)
            if current.progress_seq != int(expected_progress_seq):
                raise RecoveryCheckpointConflict(
                    code="recovery_progress_conflict",
                    run_id=current.run_id,
                    expected_epoch=epoch,
                    actual_epoch=current.epoch,
                    expected_progress_seq=expected_progress_seq,
                    actual_progress_seq=current.progress_seq,
                )
            updated = current.complete(
                result=result,
                completed_at=completed_at,
                active_elapsed_ms=active_elapsed_ms,
            )
            self._runs[current.run_id] = updated
            if updated.state.is_terminal and self._active_slot_run_id == current.run_id:
                self._active_slot_run_id = None
            return updated

    def _require(self, run_id: str) -> RecoveryRunStatus:
        current = self._runs.get(str(run_id))
        if current is None:
            raise RecoveryRunNotFound(run_id=str(run_id))
        return current

    @staticmethod
    def _require_epoch(
        current: RecoveryRunStatus,
        *,
        expected_epoch: int,
    ) -> None:
        if current.epoch != int(expected_epoch):
            raise RecoveryCheckpointConflict(
                code="recovery_epoch_conflict",
                run_id=current.run_id,
                expected_epoch=expected_epoch,
                actual_epoch=current.epoch,
                expected_progress_seq=current.progress_seq,
                actual_progress_seq=current.progress_seq,
            )


__all__ = ["MemoryRecoveryControlStore", "MemoryRecoveryRunStore"]
