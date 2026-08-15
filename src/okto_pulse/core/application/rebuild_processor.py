"""Deterministic rebuild state machine and effect contracts.

Core owns ordering, timeout semantics, promotion eligibility and compensation.
Edition adapters execute idempotent effects and persist checkpoints/receipts.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from enum import Enum
import json
from types import MappingProxyType
from typing import Protocol


class RebuildState(str, Enum):
    PLANNED = "planned"
    SNAPSHOTTED = "snapshotted"
    QUARANTINED = "quarantined"
    ENQUEUED = "enqueued"
    DRAINING = "draining"
    RESTORED = "restored"
    PROMOTED = "promoted"
    COMPENSATING = "compensating"
    COMPLETED = "completed"
    FAILED = "failed"
    COMPENSATION_FAILED = "compensation_failed"
    BLOCKED = "blocked"


class RebuildOutcomeCode(str, Enum):
    COMPLETED = "completed"
    SALVAGE_PENDING = "salvage_pending"
    CANCELLED = "cancelled"
    LEASE_LOST = "lease_lost"
    SNAPSHOT_FAILED = "snapshot_failed"
    QUARANTINE_FAILED = "quarantine_failed"
    ENQUEUE_FAILED = "enqueue_failed"
    DRAIN_STALLED = "drain_stalled"
    HARD_TIMEOUT = "hard_timeout"
    MANIFEST_DRIFT = "manifest_drift"
    RESTORE_FAILED = "restore_failed"
    PROMOTION_FAILED = "promotion_failed"
    EFFECT_FAILED = "effect_failed"
    COMPENSATION_FAILED = "compensation_failed"
    RESUME_REQUIRES_NEW_MANIFEST = "rebuild_resume_requires_new_manifest"
    LEGACY_MANUAL_RESTORE_QUEUE_RECONCILED = (
        "legacy_manual_restore_queue_only_reconciled"
    )


class CompensationAction(str, Enum):
    CANCEL_ENQUEUED_SOURCES = "cancel_enqueued_sources"
    DEMOTE_CANDIDATE_GENERATION = "demote_candidate_generation"
    DISCARD_CANDIDATE_GENERATION = "discard_candidate_generation"
    RESTORE_QUARANTINE = "restore_quarantine"


@dataclass(frozen=True, slots=True)
class RebuildPlan:
    stall_timeout_seconds: float = 900.0
    hard_timeout_seconds: float = 14_400.0
    observation_wait_seconds: float = 0.5
    final_grace_seconds: float = 0.0
    low_depth_threshold: int = 0

    def __post_init__(self) -> None:
        if self.stall_timeout_seconds <= 0:
            raise ValueError("stall_timeout_seconds must be positive")
        if self.hard_timeout_seconds < self.stall_timeout_seconds:
            raise ValueError("hard_timeout_seconds must be >= stall timeout")
        if self.observation_wait_seconds <= 0:
            raise ValueError("observation_wait_seconds must be positive")
        if self.final_grace_seconds < 0:
            raise ValueError("final_grace_seconds cannot be negative")
        if self.low_depth_threshold < 0:
            raise ValueError("low_depth_threshold cannot be negative")


@dataclass(frozen=True, slots=True)
class RebuildCommand:
    run_id: str
    board_id: str
    manifest_ref: str
    operation: str
    actor_id: str
    reason: str
    source_rows: tuple[Mapping[str, object], ...] = ()
    previous_generation_id: str | None = None
    candidate_generation_id: str | None = None
    owner_token: str | None = field(default=None, repr=False, compare=False)
    salvage_pending: bool = False


@dataclass(frozen=True, slots=True)
class RebuildEffectReceipt:
    effect_key: str
    effect: str
    ok: bool
    code: str = "ok"
    details: Mapping[str, object] = field(default_factory=lambda: MappingProxyType({}))


@dataclass(frozen=True, slots=True)
class QueueObservation:
    depth: int
    observed_at: datetime
    sequence: int
    blocking_reason: str | None = None


class QueueDrainDecision(str, Enum):
    CONTINUE = "continue"
    IDLE = "idle"
    STALLED = "stalled"
    HARD_TIMEOUT = "hard_timeout"


@dataclass(frozen=True, slots=True)
class QueueDrainPolicy:
    stall_timeout_seconds: float
    hard_timeout_seconds: float
    final_grace_seconds: float = 0.0
    low_depth_threshold: int = 0


@dataclass(frozen=True, slots=True)
class QueueDrainTracker:
    started_at: datetime
    stall_deadline: datetime
    hard_deadline: datetime
    best_depth: int | None = None
    progress_events: int = 0
    grace_applied: bool = False
    grace_reason: str | None = None


@dataclass(frozen=True, slots=True)
class QueueDrainEvaluation:
    decision: QueueDrainDecision
    tracker: QueueDrainTracker


def start_queue_drain(policy: QueueDrainPolicy, *, now: datetime) -> QueueDrainTracker:
    return QueueDrainTracker(
        started_at=now,
        stall_deadline=now + timedelta(seconds=policy.stall_timeout_seconds),
        hard_deadline=now + timedelta(seconds=policy.hard_timeout_seconds),
    )


def evaluate_queue_depth(
    policy: QueueDrainPolicy,
    tracker: QueueDrainTracker,
    *,
    depth: int,
    now: datetime,
) -> QueueDrainEvaluation:
    if depth < 0:
        raise ValueError("queue depth cannot be negative")
    if depth == 0:
        return QueueDrainEvaluation(QueueDrainDecision.IDLE, tracker)

    progressed = tracker.best_depth is None or depth < tracker.best_depth
    if progressed:
        tracker = replace(
            tracker,
            best_depth=depth,
            progress_events=tracker.progress_events
            + (0 if tracker.best_depth is None else 1),
            stall_deadline=now + timedelta(seconds=policy.stall_timeout_seconds),
        )
    if now >= tracker.hard_deadline:
        return QueueDrainEvaluation(QueueDrainDecision.HARD_TIMEOUT, tracker)
    if now >= tracker.stall_deadline:
        if (
            not tracker.grace_applied
            and policy.final_grace_seconds > 0
            and depth <= policy.low_depth_threshold
        ):
            tracker = replace(
                tracker,
                grace_applied=True,
                grace_reason="low_depth_near_timeout",
                stall_deadline=now + timedelta(seconds=policy.final_grace_seconds),
            )
            return QueueDrainEvaluation(QueueDrainDecision.CONTINUE, tracker)
        return QueueDrainEvaluation(QueueDrainDecision.STALLED, tracker)
    return QueueDrainEvaluation(QueueDrainDecision.CONTINUE, tracker)


@dataclass(frozen=True, slots=True)
class CompensationCommand:
    run_id: str
    board_id: str
    failed_state: RebuildState
    actions: tuple[CompensationAction, ...]
    receipt_keys: tuple[str, ...]
    mutation_guard: Callable[[], bool] | None = field(
        default=None,
        repr=False,
        compare=False,
    )
    reconciliation_intent: RebuildEffectReceipt | None = None


@dataclass(frozen=True, slots=True)
class RebuildCheckpoint:
    command: RebuildCommand
    state: RebuildState
    started_at: datetime
    last_progress_at: datetime
    best_queue_depth: int | None = None
    last_sequence: int = 0
    queue_progress_events: int = 0
    queue_grace_applied: bool = False
    queue_grace_reason: str | None = None
    writer_handoff_count: int = 0
    writer_reacquire_count: int = 0
    compensation_failed_state: RebuildState | None = None
    compensation_failure_code: RebuildOutcomeCode | None = None
    compensation_failure_detail: str | None = None
    compensation_actions: tuple[CompensationAction, ...] = ()
    receipts: Mapping[str, RebuildEffectReceipt] = field(
        default_factory=lambda: MappingProxyType({})
    )


@dataclass(frozen=True, slots=True)
class RebuildOutcome:
    run_id: str
    board_id: str
    state: RebuildState
    code: RebuildOutcomeCode
    promotion_allowed: bool
    compensation_actions: tuple[CompensationAction, ...] = ()
    receipts: tuple[RebuildEffectReceipt, ...] = ()
    detail: str | None = None


class RebuildEffects(Protocol):
    def load_checkpoint(self, run_id: str) -> RebuildCheckpoint | None: ...

    def save_checkpoint(self, checkpoint: RebuildCheckpoint) -> None: ...

    def snapshot(
        self, command: RebuildCommand, *, effect_key: str
    ) -> RebuildEffectReceipt: ...

    def quarantine(
        self, command: RebuildCommand, *, effect_key: str
    ) -> RebuildEffectReceipt: ...

    def enqueue(
        self, command: RebuildCommand, *, effect_key: str
    ) -> RebuildEffectReceipt: ...

    def wait_for_queue_observation(
        self,
        command: RebuildCommand,
        *,
        after_sequence: int,
        max_wait_seconds: float,
    ) -> QueueObservation: ...

    def restore(
        self, command: RebuildCommand, *, effect_key: str
    ) -> RebuildEffectReceipt: ...

    def promote(
        self, command: RebuildCommand, *, effect_key: str
    ) -> RebuildEffectReceipt: ...

    def compensate(
        self, command: CompensationCommand, *, effect_key: str
    ) -> RebuildEffectReceipt: ...

    def record_audit(
        self, outcome: RebuildOutcome, *, effect_key: str
    ) -> RebuildEffectReceipt: ...


Clock = Callable[[], datetime]
ReceiptReplayRequired = Callable[[str, RebuildEffectReceipt], bool]
LegacyBlockedIntentProbe = Callable[[RebuildCommand, RebuildEffectReceipt], bool]


_LEGACY_BLOCKED_INTENT_EFFECT = "legacy_manually_restored_blocked_after_enqueue_intent"
_LEGACY_BLOCKED_INTENT_CODE = "legacy_manual_restore_queue_only_authorized"
_LEGACY_BLOCKED_COMPENSATION_CODE = (
    RebuildOutcomeCode.LEGACY_MANUAL_RESTORE_QUEUE_RECONCILED.value
)
_LEGACY_BLOCKED_RECONCILIATION_KIND = "legacy_manual_restore_queue_only"
_LEGACY_BLOCKED_PREAPPLIED_ACTIONS = (
    CompensationAction.RESTORE_QUARANTINE.value,
    CompensationAction.DISCARD_CANDIDATE_GENERATION.value,
)
_LEGACY_BLOCKED_REMAINING_ACTIONS = (CompensationAction.CANCEL_ENQUEUED_SOURCES.value,)


@dataclass(frozen=True, slots=True)
class _DrainFailure:
    code: RebuildOutcomeCode
    detail: str


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class RebuildProcessor:
    def __init__(
        self,
        effects: RebuildEffects,
        *,
        clock: Clock = _utc_now,
        plan: RebuildPlan | None = None,
        cancel_requested: Callable[[], bool] | None = None,
        lease_renew: Callable[[], bool] | None = None,
        orchestration_renew: Callable[[], bool] | None = None,
        release_writer_for_drain: Callable[[], bool] | None = None,
        reacquire_writer_after_drain: Callable[[], str | None] | None = None,
        source_revalidate: Callable[[], bool] | None = None,
        receipt_replay_required: ReceiptReplayRequired | None = None,
        legacy_blocked_intent_probe: LegacyBlockedIntentProbe | None = None,
    ) -> None:
        if (release_writer_for_drain is None) != (reacquire_writer_after_drain is None):
            raise ValueError(
                "release_writer_for_drain and reacquire_writer_after_drain "
                "must be configured together"
            )
        self._effects = effects
        self._clock = clock
        self._plan = plan or RebuildPlan()
        self._cancel_requested = cancel_requested
        self._lease_renew = lease_renew
        self._orchestration_renew = orchestration_renew
        self._release_writer_for_drain = release_writer_for_drain
        self._reacquire_writer_after_drain = reacquire_writer_after_drain
        self._source_revalidate = source_revalidate
        self._receipt_replay_required = receipt_replay_required
        self._legacy_blocked_intent_probe = legacy_blocked_intent_probe

    def execute(self, command: RebuildCommand) -> RebuildOutcome:
        if command.salvage_pending:
            return self._finish(
                command,
                RebuildState.BLOCKED,
                RebuildOutcomeCode.SALVAGE_PENDING,
                promotion_allowed=False,
                detail="destructive rebuild is forbidden while salvage is pending",
            )

        checkpoint = self._effects.load_checkpoint(command.run_id)
        if checkpoint is None:
            now = self._clock()
            checkpoint = RebuildCheckpoint(
                command=command,
                state=RebuildState.PLANNED,
                started_at=now,
                last_progress_at=now,
            )
            self._effects.save_checkpoint(checkpoint)
        elif checkpoint.command != command:
            raise ValueError("run_id already belongs to a different rebuild command")
        else:
            # Runtime-only capabilities (for example the current writer token)
            # are deliberately excluded from checkpoint identity/persistence.
            # Reattach them from the live invocation before any resumed effect.
            checkpoint = replace(checkpoint, command=command)

        if self._has_legacy_blocked_intent(checkpoint):
            return self._finish(
                checkpoint.command,
                RebuildState.BLOCKED,
                RebuildOutcomeCode.LEASE_LOST,
                promotion_allowed=False,
                receipts=tuple(checkpoint.receipts.values()),
                detail="legacy_blocked_recovery_authority_required",
                record_audit=False,
            )

        # Compensation invalidates the materialization attempt represented by
        # this manifest checkpoint.  Reusing its successful enqueue receipt
        # would observe an empty fenced queue and could authorize promotion of
        # a baseline that was deliberately discarded/restored.  A fresh
        # manifest/run is the governed retry boundary; the live f06 recovery
        # (BLOCKED with no compensation receipt) remains resumable.
        compensated = any(
            receipt.effect == "compensate" and receipt.ok
            for receipt in checkpoint.receipts.values()
        )
        if compensated:
            return self._finish(
                checkpoint.command,
                RebuildState.BLOCKED,
                RebuildOutcomeCode.RESUME_REQUIRES_NEW_MANIFEST,
                promotion_allowed=False,
                receipts=tuple(checkpoint.receipts.values()),
                detail="rebuild_resume_requires_new_manifest",
            )
        if checkpoint.state in {
            RebuildState.COMPENSATING,
            RebuildState.COMPENSATION_FAILED,
        }:
            return self._resume_compensation(checkpoint)

        step_specs = (
            ("snapshot", RebuildState.SNAPSHOTTED, RebuildOutcomeCode.SNAPSHOT_FAILED),
            (
                "quarantine",
                RebuildState.QUARANTINED,
                RebuildOutcomeCode.QUARANTINE_FAILED,
            ),
            ("enqueue", RebuildState.ENQUEUED, RebuildOutcomeCode.ENQUEUE_FAILED),
        )
        for effect_name, target_state, failure_code in step_specs:
            control_failure = self._check_control(checkpoint)
            if control_failure is not None:
                return control_failure
            checkpoint, receipt = self._run_effect(
                checkpoint, effect_name=effect_name, target_state=target_state
            )
            fence_failure = self._check_fences(checkpoint)
            if fence_failure is not None:
                return fence_failure
            if not receipt.ok:
                return self._fail(checkpoint, failure_code, receipt.code)

        checkpoint = self._set_state(checkpoint, RebuildState.DRAINING)
        writer_released = False
        if self._release_writer_for_drain is not None:
            # Enqueue is durable, but no queue worker can enter its ordinary
            # guarded write while the rebuild owns the exclusive admin lease.
            # Prove the current fence immediately before yielding it.
            control_failure = self._check_control(checkpoint)
            if control_failure is not None:
                return control_failure
            try:
                writer_released = bool(self._release_writer_for_drain())
            except Exception as exc:
                return self._block_after_lease_loss(
                    checkpoint,
                    f"writer lease release failed:{type(exc).__name__}",
                )
            if not writer_released:
                return self._block_after_lease_loss(
                    checkpoint,
                    "writer lease could not be released for queue drain",
                )

        drain_result: RebuildCheckpoint | RebuildOutcome | _DrainFailure
        drain_exception: BaseException | None = None
        try:
            if writer_released:
                checkpoint = replace(
                    checkpoint,
                    writer_handoff_count=checkpoint.writer_handoff_count + 1,
                )
                self._effects.save_checkpoint(checkpoint)
            drain_result = self._drain(
                checkpoint,
                writer_required=not writer_released,
            )
        except BaseException as exc:
            # Reacquire the writer before an exception crosses the boundary.
            # This keeps cancellation/crash cleanup from running unfenced.
            drain_exception = exc
            drain_result = checkpoint

        if writer_released:
            assert self._reacquire_writer_after_drain is not None
            try:
                reacquired_owner_token = self._reacquire_writer_after_drain()
            except BaseException as exc:
                if drain_exception is not None:
                    raise drain_exception from exc
                if not isinstance(exc, Exception):
                    raise
                reservation_failure = self._orchestration_failure_detail()
                return self._block_after_lease_loss(
                    checkpoint,
                    reservation_failure
                    or f"writer lease reacquire failed:{type(exc).__name__}",
                )
            if not reacquired_owner_token:
                if drain_exception is not None:
                    raise drain_exception
                reservation_failure = self._orchestration_failure_detail()
                if (
                    reservation_failure is None
                    and isinstance(drain_result, _DrainFailure)
                    and drain_result.code is RebuildOutcomeCode.LEASE_LOST
                ):
                    reservation_failure = drain_result.detail
                return self._block_after_lease_loss(
                    checkpoint,
                    reservation_failure
                    or "writer lease could not be reacquired after queue drain",
                )
            checkpoint = replace(
                checkpoint,
                command=replace(
                    checkpoint.command,
                    owner_token=reacquired_owner_token,
                ),
                writer_reacquire_count=checkpoint.writer_reacquire_count + 1,
            )
            try:
                self._effects.save_checkpoint(checkpoint)
            except BaseException as exc:
                if drain_exception is not None:
                    raise drain_exception from exc
                raise

        if drain_exception is not None:
            raise drain_exception
        if isinstance(drain_result, RebuildOutcome):
            return drain_result
        if isinstance(drain_result, _DrainFailure):
            if drain_result.code is RebuildOutcomeCode.LEASE_LOST:
                return self._block_after_lease_loss(
                    checkpoint,
                    drain_result.detail,
                )
            return self._fail(
                checkpoint,
                drain_result.code,
                drain_result.detail,
            )
        checkpoint = replace(
            drain_result,
            command=checkpoint.command,
            writer_handoff_count=checkpoint.writer_handoff_count,
            writer_reacquire_count=checkpoint.writer_reacquire_count,
        )

        control_failure = self._check_control(checkpoint)
        if control_failure is not None:
            return control_failure
        if self._source_revalidate is not None:
            try:
                source_equivalent = bool(self._source_revalidate())
            except Exception as exc:
                return self._fail(
                    checkpoint,
                    RebuildOutcomeCode.MANIFEST_DRIFT,
                    f"source_revalidation_exception:{type(exc).__name__}",
                )
            # A source revalidation can perform non-trivial I/O.  Re-prove
            # both the orchestration reservation and writer B before deciding
            # whether compensation is authorized.
            fence_failure = self._check_fences(checkpoint)
            if fence_failure is not None:
                return fence_failure
            if not source_equivalent:
                return self._fail(
                    checkpoint,
                    RebuildOutcomeCode.MANIFEST_DRIFT,
                    "source_set_hash drift during rebuild drain",
                )
        checkpoint, restore = self._run_effect(
            checkpoint,
            effect_name="restore",
            target_state=RebuildState.RESTORED,
        )
        fence_failure = self._check_fences(checkpoint)
        if fence_failure is not None:
            return fence_failure
        if not restore.ok:
            return self._fail(
                checkpoint,
                RebuildOutcomeCode.RESTORE_FAILED,
                restore.code,
            )

        control_failure = self._check_control(checkpoint)
        if control_failure is not None:
            return control_failure
        checkpoint, promotion = self._run_effect(
            checkpoint,
            effect_name="promote",
            target_state=RebuildState.PROMOTED,
        )
        fence_failure = self._check_fences(checkpoint)
        if fence_failure is not None:
            return fence_failure
        if not promotion.ok:
            return self._fail(
                checkpoint,
                RebuildOutcomeCode.PROMOTION_FAILED,
                promotion.code,
            )

        control_failure = self._check_control(checkpoint)
        if control_failure is not None:
            return control_failure
        checkpoint = self._set_state(checkpoint, RebuildState.COMPLETED)
        return self._finish(
            command,
            checkpoint.state,
            RebuildOutcomeCode.COMPLETED,
            promotion_allowed=True,
            receipts=tuple(checkpoint.receipts.values()),
        )

    def fail_existing(
        self,
        command: RebuildCommand,
        *,
        code: RebuildOutcomeCode,
        detail: str,
    ) -> RebuildOutcome:
        """Fail/compensate a started checkpoint without replaying new effects.

        Source or manifest drift may be discovered before a live source
        resolver is safe to call. This recovery entry point therefore never
        creates a checkpoint and never runs snapshot/quarantine/enqueue. The
        persisted command remains authoritative; only runtime-only fields may
        be rebound by the caller.
        """

        checkpoint = self._effects.load_checkpoint(command.run_id)
        if checkpoint is None:
            raise RuntimeError("rebuild_existing_checkpoint_required")
        if checkpoint.command != command:
            raise ValueError("run_id already belongs to a different rebuild command")
        checkpoint = replace(checkpoint, command=command)
        if self._has_legacy_blocked_intent(checkpoint):
            raise RuntimeError("legacy_blocked_recovery_authority_required")
        compensated = any(
            receipt.effect == "compensate" and receipt.ok
            for receipt in checkpoint.receipts.values()
        )
        if compensated:
            return self._finish(
                checkpoint.command,
                RebuildState.BLOCKED,
                RebuildOutcomeCode.RESUME_REQUIRES_NEW_MANIFEST,
                promotion_allowed=False,
                receipts=tuple(checkpoint.receipts.values()),
                detail="rebuild_resume_requires_new_manifest",
            )
        if checkpoint.state in {
            RebuildState.COMPENSATING,
            RebuildState.COMPENSATION_FAILED,
        }:
            return self._resume_compensation(checkpoint)
        return self._fail(checkpoint, code, detail)

    def reconcile_manually_restored_blocked_after_enqueue(
        self,
        command: RebuildCommand,
        *,
        intent_receipt: RebuildEffectReceipt,
        recovery_actor_id: str,
        recovery_reason: str,
    ) -> RebuildOutcome:
        """Cancel only a legacy queue whose predecessor was already restored.

        This seam is deliberately narrower than :meth:`fail_existing`.  Some
        pre-receipt rebuilds persisted ``BLOCKED`` after enqueue and were later
        restored by the governed manual quarantine tool.  Re-running ordinary
        compensation would restore the old snapshot a second time and erase
        graph evolution that happened after that restore.  The edition-owned
        recovery runner must instead persist and re-prove a physical-evidence
        intent, then this state machine fences only the exact remaining queue.

        The intent receipt is part of the durable checkpoint before the first
        cancellation mutation.  A crash therefore resumes the same intent and
        can never widen generic ``BLOCKED`` compensation semantics.
        """

        intent_receipt = self._canonical_legacy_intent_receipt(intent_receipt)
        checkpoint = self._effects.load_checkpoint(command.run_id)
        if checkpoint is None:
            raise RuntimeError("legacy_blocked_checkpoint_required")
        if checkpoint.command != command:
            raise ValueError("run_id already belongs to a different rebuild command")
        checkpoint = replace(checkpoint, command=command)
        self._validate_legacy_blocked_receipts(checkpoint, intent_receipt)
        self._validate_legacy_blocked_intent(
            command,
            intent_receipt,
            recovery_actor_id=recovery_actor_id,
            recovery_reason=recovery_reason,
        )
        if (
            self._lease_renew is None
            or self._orchestration_renew is None
            or self._legacy_blocked_intent_probe is None
        ):
            raise RuntimeError("legacy_blocked_recovery_authority_required")

        existing_intent = checkpoint.receipts.get(intent_receipt.effect_key)
        expected_actions = (CompensationAction.CANCEL_ENQUEUED_SOURCES,)
        compensation = checkpoint.receipts.get(f"{command.run_id}:compensate")
        if existing_intent is None:
            expected_prefix = {
                f"{command.run_id}:snapshot",
                f"{command.run_id}:quarantine",
                f"{command.run_id}:enqueue",
            }
            if (
                set(checkpoint.receipts) != expected_prefix
                or checkpoint.state is not RebuildState.BLOCKED
                or checkpoint.compensation_failed_state is not None
                or checkpoint.compensation_failure_code is not None
                or checkpoint.compensation_failure_detail is not None
                or checkpoint.compensation_actions
            ):
                raise RuntimeError("legacy_blocked_first_admission_invalid")
        else:
            if existing_intent != intent_receipt:
                raise RuntimeError("legacy_blocked_intent_conflict")
            if (
                checkpoint.compensation_actions != expected_actions
                or checkpoint.compensation_failed_state is not RebuildState.ENQUEUED
                or checkpoint.compensation_failure_code
                is not RebuildOutcomeCode.LEGACY_MANUAL_RESTORE_QUEUE_RECONCILED
                or checkpoint.state
                not in {
                    RebuildState.COMPENSATING,
                    RebuildState.COMPENSATION_FAILED,
                    RebuildState.FAILED,
                }
            ):
                raise RuntimeError("legacy_blocked_compensation_context_invalid")
            if checkpoint.state is RebuildState.COMPENSATING:
                state_receipt_valid = compensation is None
            elif checkpoint.state is RebuildState.COMPENSATION_FAILED:
                state_receipt_valid = bool(
                    compensation is not None
                    and not compensation.ok
                    and compensation.code != _LEGACY_BLOCKED_COMPENSATION_CODE
                )
            else:
                state_receipt_valid = bool(compensation is not None and compensation.ok)
                if state_receipt_valid:
                    self._validate_legacy_blocked_compensation_receipt(
                        checkpoint.command,
                        intent_receipt,
                        compensation,
                    )
            if not state_receipt_valid:
                raise RuntimeError("legacy_blocked_state_receipt_mismatch")

        fence_failure = self._check_legacy_recovery_fences(checkpoint)
        if fence_failure is not None:
            return fence_failure
        try:
            probe_receipt = self._canonical_legacy_intent_receipt(intent_receipt)
            intent_current = bool(
                self._legacy_blocked_intent_probe(command, probe_receipt)
            )
        except BaseException:
            intent_current = False
            probe_receipt = None
        if probe_receipt != intent_receipt:
            intent_current = False
        if not intent_current:
            raise RuntimeError("legacy_blocked_intent_not_current")
        # The physical proof may perform I/O or wait behind the artifact-store
        # transaction. Re-prove both mutation fences after it returns.
        fence_failure = self._check_legacy_recovery_fences(checkpoint)
        if fence_failure is not None:
            return fence_failure
        self._validate_legacy_blocked_intent(
            command,
            intent_receipt,
            recovery_actor_id=recovery_actor_id,
            recovery_reason=recovery_reason,
        )
        refreshed_checkpoint = self._effects.load_checkpoint(command.run_id)
        if (
            refreshed_checkpoint is None
            or replace(
                refreshed_checkpoint,
                command=command,
            )
            != checkpoint
        ):
            raise RuntimeError("legacy_blocked_checkpoint_changed_during_probe")

        if compensation is not None and compensation.ok:
            if existing_intent is None or checkpoint.state is not RebuildState.FAILED:
                raise RuntimeError("legacy_blocked_compensation_context_invalid")
            self._validate_legacy_blocked_compensation_receipt(
                checkpoint.command,
                intent_receipt,
                compensation,
            )
            return self._finish(
                checkpoint.command,
                RebuildState.FAILED,
                RebuildOutcomeCode.LEGACY_MANUAL_RESTORE_QUEUE_RECONCILED,
                promotion_allowed=False,
                receipts=tuple(checkpoint.receipts.values()),
                compensation_actions=expected_actions,
                detail="legacy_blocked_after_enqueue_predecessor_already_restored",
            )

        receipts = dict(checkpoint.receipts)
        receipts[intent_receipt.effect_key] = intent_receipt
        checkpoint = replace(
            checkpoint,
            state=RebuildState.COMPENSATING,
            compensation_failed_state=RebuildState.ENQUEUED,
            compensation_failure_code=(
                RebuildOutcomeCode.LEGACY_MANUAL_RESTORE_QUEUE_RECONCILED
            ),
            compensation_failure_detail=(
                "legacy_blocked_after_enqueue_predecessor_already_restored"
            ),
            compensation_actions=expected_actions,
            receipts=MappingProxyType(receipts),
        )
        self._effects.save_checkpoint(checkpoint)
        return self._resume_compensation(checkpoint)

    @staticmethod
    def _canonical_legacy_intent_receipt(
        receipt: RebuildEffectReceipt,
    ) -> RebuildEffectReceipt:
        try:
            encoded = json.dumps(
                dict(receipt.details),
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            )
            details = json.loads(encoded)
        except (TypeError, ValueError) as exc:
            raise RuntimeError("legacy_blocked_intent_noncanonical") from exc
        if not isinstance(details, dict):
            raise RuntimeError("legacy_blocked_intent_noncanonical")
        return replace(receipt, details=MappingProxyType(details))

    @staticmethod
    def _validate_legacy_blocked_receipts(
        checkpoint: RebuildCheckpoint,
        intent_receipt: RebuildEffectReceipt,
    ) -> None:
        run_id = checkpoint.command.run_id
        expected = {
            f"{run_id}:snapshot": "snapshot",
            f"{run_id}:quarantine": "quarantine",
            f"{run_id}:enqueue": "enqueue",
        }
        allowed = {
            *expected,
            intent_receipt.effect_key,
            f"{run_id}:compensate",
        }
        if not set(checkpoint.receipts).issubset(allowed):
            raise RuntimeError("legacy_blocked_receipt_set_invalid")
        if (
            checkpoint.writer_handoff_count != 0
            or checkpoint.writer_reacquire_count != 0
        ):
            raise RuntimeError("legacy_blocked_writer_history_invalid")
        for effect_key, effect_name in expected.items():
            receipt = checkpoint.receipts.get(effect_key)
            if (
                receipt is None
                or receipt.effect_key != effect_key
                or receipt.effect != effect_name
                or not receipt.ok
            ):
                raise RuntimeError("legacy_blocked_receipt_prefix_invalid")
        compensation = checkpoint.receipts.get(f"{run_id}:compensate")
        if compensation is not None and (
            compensation.effect_key != f"{run_id}:compensate"
            or compensation.effect != "compensate"
        ):
            raise RuntimeError("legacy_blocked_compensation_receipt_invalid")

    @staticmethod
    def _validate_legacy_blocked_intent(
        command: RebuildCommand,
        receipt: RebuildEffectReceipt,
        *,
        recovery_actor_id: str,
        recovery_reason: str,
    ) -> None:
        effect_key = f"{command.run_id}:{_LEGACY_BLOCKED_INTENT_EFFECT}"
        details = dict(receipt.details)
        normalized_actor = str(recovery_actor_id).strip()
        normalized_reason = str(recovery_reason).strip()
        intent_digest = str(details.get("intent_digest") or "")
        if (
            receipt.effect_key != effect_key
            or receipt.effect != _LEGACY_BLOCKED_INTENT_EFFECT
            or not receipt.ok
            or receipt.code != _LEGACY_BLOCKED_INTENT_CODE
            or str(details.get("legacy_run_id") or "") != command.run_id
            or str(details.get("board_id") or "") != command.board_id
            or str(details.get("manifest_ref") or "") != command.manifest_ref
            or str(details.get("intent_ref") or "") == ""
            or len(intent_digest) != 64
            or any(character not in "0123456789abcdef" for character in intent_digest)
            or str(details.get("recovery_run_id") or "") == ""
            or normalized_actor == ""
            or normalized_reason == ""
            or str(details.get("recovery_actor_id") or "") != normalized_actor
            or str(details.get("recovery_reason") or "") != normalized_reason
            or tuple(details.get("preapplied_actions") or ())
            != _LEGACY_BLOCKED_PREAPPLIED_ACTIONS
            or tuple(details.get("remaining_actions") or ())
            != _LEGACY_BLOCKED_REMAINING_ACTIONS
        ):
            raise RuntimeError("legacy_blocked_intent_invalid")

    @staticmethod
    def _validate_legacy_blocked_compensation_receipt(
        command: RebuildCommand,
        intent_receipt: RebuildEffectReceipt,
        receipt: RebuildEffectReceipt,
    ) -> None:
        expected_key = f"{command.run_id}:compensate"
        details = dict(receipt.details)
        intent_details = dict(intent_receipt.details)
        queue = details.get("queue")
        intent_queue = intent_details.get("queue")
        queue_details = dict(queue) if isinstance(queue, Mapping) else {}
        intent_queue_details = (
            dict(intent_queue) if isinstance(intent_queue, Mapping) else {}
        )
        intent_rows = intent_queue_details.get("rows")
        expected_row_count = (
            len(intent_rows)
            if isinstance(intent_rows, Sequence)
            and not isinstance(intent_rows, (str, bytes, bytearray))
            else -1
        )
        terminal_fingerprint = str(queue_details.get("terminal_fingerprint") or "")
        count_fields = (
            "pending_compensated",
            "claimed_compensated",
            "already_compensated",
            "active_remaining",
            "live_intents_restored",
            "total_compensated",
            "expected_row_count",
        )
        counts_are_ints = all(
            type(queue_details.get(field)) is int and queue_details[field] >= 0
            for field in count_fields
        )
        if (
            receipt.effect_key != expected_key
            or receipt.effect != "compensate"
            or not receipt.ok
            or receipt.code != _LEGACY_BLOCKED_COMPENSATION_CODE
            or set(details)
            != {
                "actions",
                "reconciliation_kind",
                "intent_digest",
                "queue",
            }
            or tuple(details.get("actions") or ()) != _LEGACY_BLOCKED_REMAINING_ACTIONS
            or details.get("reconciliation_kind") != _LEGACY_BLOCKED_RECONCILIATION_KIND
            or details.get("intent_digest") != intent_details.get("intent_digest")
            or set(queue_details)
            != {
                "source",
                "expected_row_count",
                "terminal_fingerprint",
                "pending_compensated",
                "claimed_compensated",
                "already_compensated",
                "active_remaining",
                "live_intents_restored",
                "total_compensated",
                "evidence_digest",
            }
            or not counts_are_ints
            or expected_row_count < 1
            or queue_details.get("source") != intent_queue_details.get("source")
            or queue_details.get("expected_row_count") != expected_row_count
            or len(terminal_fingerprint) != 64
            or any(
                character not in "0123456789abcdef"
                for character in terminal_fingerprint
            )
            or queue_details.get("evidence_digest")
            != intent_details.get("intent_digest")
            or queue_details.get("active_remaining") != 0
            or queue_details.get("live_intents_restored") != 0
            or (
                queue_details.get("pending_compensated", 0)
                + queue_details.get("claimed_compensated", 0)
                + queue_details.get("already_compensated", 0)
                != expected_row_count
            )
            or queue_details.get("total_compensated")
            != (
                queue_details.get("pending_compensated", 0)
                + queue_details.get("claimed_compensated", 0)
            )
        ):
            raise RuntimeError("legacy_blocked_compensation_receipt_invalid")

    @staticmethod
    def _has_legacy_blocked_intent(checkpoint: RebuildCheckpoint) -> bool:
        return any(
            receipt.effect == _LEGACY_BLOCKED_INTENT_EFFECT
            for receipt in checkpoint.receipts.values()
        )

    def _run_effect(
        self,
        checkpoint: RebuildCheckpoint,
        *,
        effect_name: str,
        target_state: RebuildState,
    ) -> tuple[RebuildCheckpoint, RebuildEffectReceipt]:
        effect_key = f"{checkpoint.command.run_id}:{effect_name}"
        existing = checkpoint.receipts.get(effect_key)
        replay_required = bool(
            existing is not None
            and self._receipt_replay_required is not None
            and self._receipt_replay_required(effect_name, existing)
        )
        if existing is not None and not replay_required:
            return self._set_state(checkpoint, target_state), existing
        if replay_required:
            # A governed adapter contract migration may require replaying one
            # otherwise-idempotent effect (for example resequencing rows from
            # an older rebuild queue order). Reset only drain timing/progress;
            # prior snapshot/quarantine receipts remain authoritative, so a
            # blocked resume cannot create a second backup/swap.
            now = self._clock()
            checkpoint = replace(
                checkpoint,
                started_at=now,
                last_progress_at=now,
                best_queue_depth=None,
                last_sequence=0,
                queue_progress_events=0,
                queue_grace_applied=False,
                queue_grace_reason=None,
            )

        effect = getattr(self._effects, effect_name)
        try:
            receipt = effect(checkpoint.command, effect_key=effect_key)
        except Exception as exc:
            receipt = RebuildEffectReceipt(
                effect_key=effect_key,
                effect=effect_name,
                ok=False,
                code=f"{type(exc).__name__}:{exc}",
            )
        if receipt.effect_key != effect_key or receipt.effect != effect_name:
            raise ValueError(f"invalid receipt returned by {effect_name}")
        receipts = dict(checkpoint.receipts)
        receipts[effect_key] = receipt
        checkpoint = replace(
            checkpoint,
            state=target_state,
            receipts=MappingProxyType(receipts),
        )
        self._effects.save_checkpoint(checkpoint)
        return checkpoint, receipt

    def _drain(
        self,
        checkpoint: RebuildCheckpoint,
        *,
        writer_required: bool,
    ) -> RebuildCheckpoint | RebuildOutcome | _DrainFailure:
        policy = QueueDrainPolicy(
            stall_timeout_seconds=self._plan.stall_timeout_seconds,
            hard_timeout_seconds=self._plan.hard_timeout_seconds,
            final_grace_seconds=self._plan.final_grace_seconds,
            low_depth_threshold=self._plan.low_depth_threshold,
        )
        tracker = QueueDrainTracker(
            started_at=checkpoint.started_at,
            stall_deadline=checkpoint.last_progress_at
            + timedelta(seconds=self._plan.stall_timeout_seconds),
            hard_deadline=checkpoint.started_at
            + timedelta(seconds=self._plan.hard_timeout_seconds),
            best_depth=checkpoint.best_queue_depth,
            progress_events=checkpoint.queue_progress_events,
            grace_applied=checkpoint.queue_grace_applied,
            grace_reason=checkpoint.queue_grace_reason,
        )

        while True:
            control_failure = self._check_drain_control(
                checkpoint,
                writer_required=writer_required,
            )
            if control_failure is not None:
                return control_failure
            now = self._clock()
            try:
                observation = self._effects.wait_for_queue_observation(
                    checkpoint.command,
                    after_sequence=checkpoint.last_sequence,
                    max_wait_seconds=min(
                        self._plan.observation_wait_seconds,
                        max(0.0, (tracker.hard_deadline - now).total_seconds()),
                        max(0.0, (tracker.stall_deadline - now).total_seconds()),
                    ),
                )
            except Exception as exc:
                # Queue observation is an external effect just like snapshot,
                # enqueue and restore. An adapter failure must produce a
                # durable fail-closed outcome (and compensation), never escape
                # the processor and discard the already-created receipts.
                return _DrainFailure(
                    RebuildOutcomeCode.EFFECT_FAILED,
                    f"queue_observation_failed:{type(exc).__name__}",
                )
            if observation.sequence <= checkpoint.last_sequence:
                return _DrainFailure(
                    RebuildOutcomeCode.EFFECT_FAILED,
                    "queue_observation_sequence_not_increasing",
                )

            evaluation = evaluate_queue_depth(
                policy,
                tracker,
                depth=observation.depth,
                now=observation.observed_at,
            )
            tracker = evaluation.tracker
            checkpoint = replace(
                checkpoint,
                best_queue_depth=tracker.best_depth,
                last_progress_at=tracker.stall_deadline
                - timedelta(seconds=self._plan.stall_timeout_seconds),
                last_sequence=observation.sequence,
                queue_progress_events=tracker.progress_events,
                queue_grace_applied=tracker.grace_applied,
                queue_grace_reason=tracker.grace_reason,
            )
            self._effects.save_checkpoint(checkpoint)
            # A blocker is authoritative independently of queue depth.  In
            # particular, the last rebuild row can move to the DLQ and leave
            # depth at zero; treating that observation as IDLE would promote
            # an incomplete graph.
            if observation.blocking_reason:
                return _DrainFailure(
                    RebuildOutcomeCode.DRAIN_STALLED,
                    f"queue blocked:{observation.blocking_reason}",
                )
            if evaluation.decision is QueueDrainDecision.IDLE:
                return checkpoint
            if evaluation.decision is QueueDrainDecision.HARD_TIMEOUT:
                return _DrainFailure(
                    RebuildOutcomeCode.HARD_TIMEOUT,
                    "hard timeout elapsed",
                )
            if evaluation.decision is QueueDrainDecision.STALLED:
                return _DrainFailure(
                    RebuildOutcomeCode.DRAIN_STALLED,
                    "queue made no semantic progress",
                )

    def _check_drain_control(
        self,
        checkpoint: RebuildCheckpoint,
        *,
        writer_required: bool,
    ) -> RebuildOutcome | _DrainFailure | None:
        if writer_required:
            return self._check_control(checkpoint)
        reservation_failure = self._orchestration_failure_detail()
        if reservation_failure is not None:
            return _DrainFailure(
                RebuildOutcomeCode.LEASE_LOST,
                reservation_failure,
            )

        # The admin writer is deliberately absent while the normal worker
        # drains. Cancellation remains responsive, but compensation is deferred
        # until execute() has reacquired and rebound the writer fence.
        if self._cancel_requested is None:
            return None
        try:
            cancelled = bool(self._cancel_requested())
        except Exception as exc:
            return _DrainFailure(
                RebuildOutcomeCode.CANCELLED,
                f"cancellation probe failed:{type(exc).__name__}",
            )
        if cancelled:
            return _DrainFailure(
                RebuildOutcomeCode.CANCELLED,
                "cancellation requested",
            )
        return None

    def _check_control(self, checkpoint: RebuildCheckpoint) -> RebuildOutcome | None:
        # Prove/renew the writer fence before honoring cancellation.  A caller
        # may request cancellation immediately after a long effect; restoring
        # quarantine without a live fence would be a second unsafe mutation.
        fence_failure = self._check_fences(checkpoint)
        if fence_failure is not None:
            return fence_failure

        if self._cancel_requested is not None:
            try:
                cancelled = bool(self._cancel_requested())
            except Exception as exc:
                return self._fail(
                    checkpoint,
                    RebuildOutcomeCode.CANCELLED,
                    f"cancellation probe failed:{type(exc).__name__}",
                )
            if cancelled:
                return self._fail(
                    checkpoint,
                    RebuildOutcomeCode.CANCELLED,
                    "cancellation requested",
                )
        return None

    def _check_fences(
        self,
        checkpoint: RebuildCheckpoint,
    ) -> RebuildOutcome | None:
        """Renew both mutation authorities without evaluating cancellation."""

        reservation_failure = self._orchestration_failure_detail()
        if reservation_failure is not None:
            return self._block_after_lease_loss(checkpoint, reservation_failure)
        if self._lease_renew is None:
            return None
        try:
            renewed = bool(self._lease_renew())
        except Exception as exc:
            return self._block_after_lease_loss(
                checkpoint,
                f"lease renewal failed:{type(exc).__name__}",
            )
        if not renewed:
            return self._block_after_lease_loss(
                checkpoint,
                "single-writer lease lost",
            )
        return None

    def _check_legacy_recovery_fences(
        self,
        checkpoint: RebuildCheckpoint,
    ) -> RebuildOutcome | None:
        """Probe the nominal lane without creating generic recovery state."""

        detail = self._orchestration_failure_detail()
        if detail is None and self._lease_renew is not None:
            try:
                if not self._lease_renew():
                    detail = "single-writer lease lost"
            except Exception as exc:
                detail = f"lease renewal failed:{type(exc).__name__}"
        if detail is None:
            return None
        return self._finish(
            checkpoint.command,
            checkpoint.state,
            RebuildOutcomeCode.LEASE_LOST,
            promotion_allowed=False,
            compensation_actions=checkpoint.compensation_actions,
            receipts=tuple(checkpoint.receipts.values()),
            detail=detail,
            record_audit=False,
        )

    def _mutation_guard(self) -> bool:
        """Return whether compensation may perform its next mutation."""

        if self._orchestration_failure_detail() is not None:
            return False
        if self._lease_renew is None:
            return True
        try:
            return bool(self._lease_renew())
        except Exception:
            return False

    def _orchestration_failure_detail(self) -> str | None:
        if self._orchestration_renew is None:
            return None
        try:
            renewed = bool(self._orchestration_renew())
        except Exception as exc:
            return f"orchestration reservation renewal failed:{type(exc).__name__}"
        if not renewed:
            return "orchestration reservation lost"
        return None

    def _block_after_lease_loss(
        self,
        checkpoint: RebuildCheckpoint,
        detail: str,
    ) -> RebuildOutcome:
        # No compensation is legal after the writer fence is lost.  Preserve
        # the durable checkpoint and require governed recovery/manual salvage
        # instead of mutating graph storage with a stale token.
        if self._has_legacy_blocked_intent(checkpoint):
            # The nominal recovery lane encodes its resumable phase
            # bijectively: COMPENSATING means no durable terminal receipt,
            # COMPENSATION_FAILED means an explicitly failed receipt, and
            # FAILED means the exact queue-only receipt is durable. Rewriting
            # any of these to generic BLOCKED would make a post-effect fence
            # loss impossible to resume safely under a fresh capability.
            return self._finish(
                checkpoint.command,
                checkpoint.state,
                RebuildOutcomeCode.LEASE_LOST,
                promotion_allowed=False,
                compensation_actions=checkpoint.compensation_actions,
                receipts=tuple(checkpoint.receipts.values()),
                detail=detail,
                record_audit=False,
            )
        reservation_lost = detail.startswith("orchestration reservation")
        checkpoint = (
            replace(checkpoint, state=RebuildState.BLOCKED)
            if reservation_lost
            else self._set_state(checkpoint, RebuildState.BLOCKED)
        )
        return self._finish(
            checkpoint.command,
            checkpoint.state,
            RebuildOutcomeCode.LEASE_LOST,
            promotion_allowed=False,
            receipts=tuple(checkpoint.receipts.values()),
            detail=detail,
            record_audit=not reservation_lost,
        )

    def _set_state(
        self, checkpoint: RebuildCheckpoint, state: RebuildState
    ) -> RebuildCheckpoint:
        if checkpoint.state == state:
            return checkpoint
        checkpoint = replace(checkpoint, state=state)
        self._effects.save_checkpoint(checkpoint)
        return checkpoint

    def _fail(
        self,
        checkpoint: RebuildCheckpoint,
        code: RebuildOutcomeCode,
        detail: str,
    ) -> RebuildOutcome:
        fence_failure = self._check_fences(checkpoint)
        if fence_failure is not None:
            return fence_failure
        failed_state = checkpoint.state
        actions = self._compensation_actions(failed_state)
        if not actions:
            return self._finish(
                checkpoint.command,
                RebuildState.FAILED,
                code,
                promotion_allowed=False,
                receipts=tuple(checkpoint.receipts.values()),
                detail=detail,
            )

        checkpoint = replace(
            checkpoint,
            state=RebuildState.COMPENSATING,
            compensation_failed_state=failed_state,
            compensation_failure_code=code,
            compensation_failure_detail=detail,
            compensation_actions=actions,
        )
        self._effects.save_checkpoint(checkpoint)
        return self._resume_compensation(checkpoint)

    def _resume_compensation(
        self,
        checkpoint: RebuildCheckpoint,
    ) -> RebuildOutcome:
        """Finish an interrupted idempotent compensation attempt.

        ``COMPENSATING`` is a durable intent, not proof that compensation ran.
        Only an ``ok=True`` compensation receipt closes the materialization
        attempt. Failed receipts and a crash after persisting the intent are
        retried with the original failure context and action set.
        """

        fence_failure = self._check_fences(checkpoint)
        if fence_failure is not None:
            return fence_failure
        failed_state = checkpoint.compensation_failed_state or RebuildState.COMPENSATING
        code = checkpoint.compensation_failure_code or RebuildOutcomeCode.EFFECT_FAILED
        detail = (
            checkpoint.compensation_failure_detail or "interrupted compensation resumed"
        )
        actions = checkpoint.compensation_actions or self._compensation_actions(
            failed_state
        )
        effect_key = f"{checkpoint.command.run_id}:compensate"
        command = CompensationCommand(
            run_id=checkpoint.command.run_id,
            board_id=checkpoint.command.board_id,
            failed_state=failed_state,
            actions=actions,
            receipt_keys=tuple(checkpoint.receipts),
            mutation_guard=self._mutation_guard,
            reconciliation_intent=next(
                (
                    receipt
                    for receipt in checkpoint.receipts.values()
                    if receipt.effect == _LEGACY_BLOCKED_INTENT_EFFECT
                ),
                None,
            ),
        )
        reconciliation_intent = command.reconciliation_intent
        try:
            receipt = self._effects.compensate(command, effect_key=effect_key)
        except Exception as exc:
            receipt = RebuildEffectReceipt(
                effect_key=effect_key,
                effect="compensate",
                ok=False,
                code=f"{type(exc).__name__}:{exc}",
            )
        if reconciliation_intent is not None and receipt.ok:
            try:
                self._validate_legacy_blocked_compensation_receipt(
                    checkpoint.command,
                    reconciliation_intent,
                    receipt,
                )
            except RuntimeError:
                receipt = RebuildEffectReceipt(
                    effect_key=effect_key,
                    effect="compensate",
                    ok=False,
                    code="legacy_blocked_compensation_receipt_invalid",
                    details=MappingProxyType(
                        {
                            "intent_digest": dict(reconciliation_intent.details).get(
                                "intent_digest"
                            ),
                        }
                    ),
                )
        receipts = dict(checkpoint.receipts)
        receipts[effect_key] = receipt
        terminal_state = (
            RebuildState.FAILED if receipt.ok else RebuildState.COMPENSATION_FAILED
        )
        checkpoint = replace(
            checkpoint,
            state=terminal_state,
            receipts=MappingProxyType(receipts),
        )
        self._effects.save_checkpoint(checkpoint)
        # Compensation is itself durable.  Persist its receipt and terminal
        # state before probing the fences again so a lease loss immediately
        # after the effect cannot erase the only reconciliation evidence or
        # make the next resume repeat an already-applied restore/discard.
        fence_failure = self._check_fences(checkpoint)
        if fence_failure is not None:
            return fence_failure
        terminal_code = code
        terminal_detail = detail
        if reconciliation_intent is not None and not receipt.ok:
            terminal_code = RebuildOutcomeCode.COMPENSATION_FAILED
            terminal_detail = (
                f"legacy_manual_restore_queue_only_compensation_failed:{receipt.code}"
            )
        return self._finish(
            checkpoint.command,
            terminal_state,
            # Preserve the primary failure code/detail. The terminal state and
            # compensation receipt carry the independent secondary failure;
            # replacing the code here would mask actionable causes such as a
            # cognitive-preservation integrity error.
            terminal_code,
            promotion_allowed=False,
            compensation_actions=actions,
            receipts=tuple(checkpoint.receipts.values()),
            detail=terminal_detail,
        )

    @staticmethod
    def _compensation_actions(
        state: RebuildState,
    ) -> tuple[CompensationAction, ...]:
        if state in {RebuildState.PLANNED, RebuildState.SNAPSHOTTED}:
            return ()
        actions: list[CompensationAction] = []
        if state in {
            RebuildState.ENQUEUED,
            RebuildState.DRAINING,
            RebuildState.RESTORED,
            RebuildState.PROMOTED,
            RebuildState.COMPLETED,
            RebuildState.COMPENSATING,
        }:
            actions.append(CompensationAction.CANCEL_ENQUEUED_SOURCES)
        if state in {RebuildState.PROMOTED, RebuildState.COMPLETED}:
            actions.append(CompensationAction.DEMOTE_CANDIDATE_GENERATION)
        actions.extend(
            (
                CompensationAction.RESTORE_QUARANTINE,
                CompensationAction.DISCARD_CANDIDATE_GENERATION,
            )
        )
        return tuple(actions)

    def _finish(
        self,
        command: RebuildCommand,
        state: RebuildState,
        code: RebuildOutcomeCode,
        *,
        promotion_allowed: bool,
        compensation_actions: Sequence[CompensationAction] = (),
        receipts: Sequence[RebuildEffectReceipt] = (),
        detail: str | None = None,
        record_audit: bool = True,
    ) -> RebuildOutcome:
        outcome = RebuildOutcome(
            run_id=command.run_id,
            board_id=command.board_id,
            state=state,
            code=code,
            promotion_allowed=promotion_allowed,
            compensation_actions=tuple(compensation_actions),
            receipts=tuple(receipts),
            detail=detail,
        )
        if record_audit:
            audit_key = f"{command.run_id}:audit:{code.value}"
            try:
                self._effects.record_audit(outcome, effect_key=audit_key)
            except Exception:
                # Audit failure cannot rewrite the already persisted technical
                # outcome; adapters must surface/retry it by the same idempotency
                # key.
                pass
        return outcome


def canonicalize_legacy_manual_restore_queue_only_intent_receipt(
    receipt: RebuildEffectReceipt,
) -> RebuildEffectReceipt:
    """Return an isolated canonical snapshot of one nominal recovery intent."""

    return RebuildProcessor._canonical_legacy_intent_receipt(receipt)


def validate_legacy_manual_restore_queue_only_outcome(
    outcome: RebuildOutcome,
    *,
    command: RebuildCommand,
    intent_receipt: RebuildEffectReceipt,
) -> None:
    """Prove the adapter returned the exact durable queue-only terminal set."""

    canonical_intent = RebuildProcessor._canonical_legacy_intent_receipt(intent_receipt)
    expected_keys = {
        f"{command.run_id}:snapshot",
        f"{command.run_id}:quarantine",
        f"{command.run_id}:enqueue",
        canonical_intent.effect_key,
        f"{command.run_id}:compensate",
    }
    if type(outcome) is not RebuildOutcome or any(
        type(receipt) is not RebuildEffectReceipt for receipt in outcome.receipts
    ):
        raise RuntimeError("legacy_manual_restore_queue_only_outcome_invalid")
    receipts = {receipt.effect_key: receipt for receipt in outcome.receipts}
    if (
        outcome.run_id != command.run_id
        or outcome.board_id != command.board_id
        or outcome.state is not RebuildState.FAILED
        or outcome.code is not RebuildOutcomeCode.LEGACY_MANUAL_RESTORE_QUEUE_RECONCILED
        or outcome.promotion_allowed
        or outcome.compensation_actions != (CompensationAction.CANCEL_ENQUEUED_SOURCES,)
        or outcome.detail != "legacy_blocked_after_enqueue_predecessor_already_restored"
        or len(outcome.receipts) != len(expected_keys)
        or set(receipts) != expected_keys
        or receipts.get(canonical_intent.effect_key) != canonical_intent
    ):
        raise RuntimeError("legacy_manual_restore_queue_only_outcome_invalid")
    for effect in ("snapshot", "quarantine", "enqueue"):
        effect_key = f"{command.run_id}:{effect}"
        receipt = receipts[effect_key]
        if (
            receipt.effect_key != effect_key
            or receipt.effect != effect
            or not receipt.ok
        ):
            raise RuntimeError("legacy_manual_restore_queue_only_outcome_invalid")
    RebuildProcessor._validate_legacy_blocked_compensation_receipt(
        command,
        canonical_intent,
        receipts[f"{command.run_id}:compensate"],
    )


__all__ = [
    "CompensationAction",
    "CompensationCommand",
    "QueueDrainDecision",
    "QueueDrainEvaluation",
    "QueueDrainPolicy",
    "QueueDrainTracker",
    "QueueObservation",
    "RebuildCheckpoint",
    "RebuildCommand",
    "RebuildEffectReceipt",
    "RebuildEffects",
    "RebuildOutcome",
    "RebuildOutcomeCode",
    "RebuildPlan",
    "RebuildProcessor",
    "RebuildState",
    "evaluate_queue_depth",
    "start_queue_drain",
    "canonicalize_legacy_manual_restore_queue_only_intent_receipt",
    "validate_legacy_manual_restore_queue_only_outcome",
]
