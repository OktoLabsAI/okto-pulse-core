"""Deterministic rebuild state machine and effect contracts.

Core owns ordering, timeout semantics, promotion eligibility and compensation.
Edition adapters execute idempotent effects and persist checkpoints/receipts.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from enum import Enum
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
    SNAPSHOT_FAILED = "snapshot_failed"
    QUARANTINE_FAILED = "quarantine_failed"
    ENQUEUE_FAILED = "enqueue_failed"
    DRAIN_STALLED = "drain_stalled"
    HARD_TIMEOUT = "hard_timeout"
    RESTORE_FAILED = "restore_failed"
    PROMOTION_FAILED = "promotion_failed"
    EFFECT_FAILED = "effect_failed"
    COMPENSATION_FAILED = "compensation_failed"


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
    salvage_pending: bool = False


@dataclass(frozen=True, slots=True)
class RebuildEffectReceipt:
    effect_key: str
    effect: str
    ok: bool
    code: str = "ok"
    details: Mapping[str, object] = field(
        default_factory=lambda: MappingProxyType({})
    )


@dataclass(frozen=True, slots=True)
class QueueObservation:
    depth: int
    observed_at: datetime
    sequence: int


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


def start_queue_drain(
    policy: QueueDrainPolicy, *, now: datetime
) -> QueueDrainTracker:
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
            progress_events=tracker.progress_events + (
                0 if tracker.best_depth is None else 1
            ),
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


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class RebuildProcessor:
    def __init__(
        self,
        effects: RebuildEffects,
        *,
        clock: Clock = _utc_now,
        plan: RebuildPlan | None = None,
    ) -> None:
        self._effects = effects
        self._clock = clock
        self._plan = plan or RebuildPlan()

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

        step_specs = (
            ("snapshot", RebuildState.SNAPSHOTTED, RebuildOutcomeCode.SNAPSHOT_FAILED),
            ("quarantine", RebuildState.QUARANTINED, RebuildOutcomeCode.QUARANTINE_FAILED),
            ("enqueue", RebuildState.ENQUEUED, RebuildOutcomeCode.ENQUEUE_FAILED),
        )
        for effect_name, target_state, failure_code in step_specs:
            checkpoint, receipt = self._run_effect(
                checkpoint, effect_name=effect_name, target_state=target_state
            )
            if not receipt.ok:
                return self._fail(checkpoint, failure_code, receipt.code)

        checkpoint = self._set_state(checkpoint, RebuildState.DRAINING)
        drain_failure = self._drain(checkpoint)
        if isinstance(drain_failure, RebuildOutcome):
            return drain_failure
        checkpoint = drain_failure

        checkpoint, restore = self._run_effect(
            checkpoint,
            effect_name="restore",
            target_state=RebuildState.RESTORED,
        )
        if not restore.ok:
            return self._fail(
                checkpoint,
                RebuildOutcomeCode.RESTORE_FAILED,
                restore.code,
            )

        checkpoint, promotion = self._run_effect(
            checkpoint,
            effect_name="promote",
            target_state=RebuildState.PROMOTED,
        )
        if not promotion.ok:
            return self._fail(
                checkpoint,
                RebuildOutcomeCode.PROMOTION_FAILED,
                promotion.code,
            )

        checkpoint = self._set_state(checkpoint, RebuildState.COMPLETED)
        return self._finish(
            command,
            checkpoint.state,
            RebuildOutcomeCode.COMPLETED,
            promotion_allowed=True,
            receipts=tuple(checkpoint.receipts.values()),
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
        if existing is not None:
            return self._set_state(checkpoint, target_state), existing

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
        self, checkpoint: RebuildCheckpoint
    ) -> RebuildCheckpoint | RebuildOutcome:
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
            now = self._clock()
            observation = self._effects.wait_for_queue_observation(
                checkpoint.command,
                after_sequence=checkpoint.last_sequence,
                max_wait_seconds=min(
                    self._plan.observation_wait_seconds,
                    max(0.0, (tracker.hard_deadline - now).total_seconds()),
                    max(0.0, (tracker.stall_deadline - now).total_seconds()),
                ),
            )
            if observation.sequence <= checkpoint.last_sequence:
                raise ValueError("queue observation sequence must increase")

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
            if evaluation.decision is QueueDrainDecision.IDLE:
                return checkpoint
            if evaluation.decision is QueueDrainDecision.HARD_TIMEOUT:
                return self._fail(
                    checkpoint,
                    RebuildOutcomeCode.HARD_TIMEOUT,
                    "hard timeout elapsed",
                )
            if evaluation.decision is QueueDrainDecision.STALLED:
                return self._fail(
                    checkpoint,
                    RebuildOutcomeCode.DRAIN_STALLED,
                    "queue made no semantic progress",
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

        checkpoint = self._set_state(checkpoint, RebuildState.COMPENSATING)
        effect_key = f"{checkpoint.command.run_id}:compensate"
        command = CompensationCommand(
            run_id=checkpoint.command.run_id,
            board_id=checkpoint.command.board_id,
            failed_state=failed_state,
            actions=actions,
            receipt_keys=tuple(checkpoint.receipts),
        )
        try:
            receipt = self._effects.compensate(command, effect_key=effect_key)
        except Exception as exc:
            receipt = RebuildEffectReceipt(
                effect_key=effect_key,
                effect="compensate",
                ok=False,
                code=f"{type(exc).__name__}:{exc}",
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
        return self._finish(
            checkpoint.command,
            terminal_state,
            code if receipt.ok else RebuildOutcomeCode.COMPENSATION_FAILED,
            promotion_allowed=False,
            compensation_actions=actions,
            receipts=tuple(checkpoint.receipts.values()),
            detail=detail,
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
            RebuildState.COMPENSATING,
        }:
            actions.append(CompensationAction.CANCEL_ENQUEUED_SOURCES)
        if state is RebuildState.PROMOTED:
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
        audit_key = f"{command.run_id}:audit:{code.value}"
        try:
            self._effects.record_audit(outcome, effect_key=audit_key)
        except Exception:
            # Audit failure cannot rewrite the already persisted technical outcome;
            # adapters must surface/retry it by the same idempotency key.
            pass
        return outcome


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
]
