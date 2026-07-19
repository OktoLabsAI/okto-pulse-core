"""Edition-neutral control-plane values for durable Global Discovery recovery.

This module contains no persistence, transport, filesystem, or native graph
implementation.  Editions provide the atomic storage boundary; Core defines
the immutable checkpoint contract that boundary must enforce.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from enum import Enum
from typing import Protocol, runtime_checkable


DEFAULT_RECOVERY_ATTEMPT_BUDGET_MS = 10 * 60 * 1_000
MAX_RECOVERY_CUMULATIVE_BUDGET_MS = 15 * 60 * 1_000
RECOVERY_HEARTBEAT_INTERVAL_MS = 5 * 1_000
RECOVERY_WORKER_LEASE_MS = 15 * 1_000
RECOVERY_PREPARED_TTL_SECONDS = 300
GLOBAL_RECOVERY_SLOT_ID = "_global"
GLOBAL_RECOVERY_STATUS_TOOL = "okto_pulse_kg_global_discovery_recovery_status"


def _require_non_empty(value: str, *, field: str) -> str:
    normalized = str(value).strip()
    if not normalized:
        raise ValueError(f"{field} must be non-empty")
    return normalized


def _require_non_negative(value: int, *, field: str) -> int:
    normalized = int(value)
    if normalized < 0:
        raise ValueError(f"{field} must be non-negative")
    return normalized


def _require_aware(value: datetime, *, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value


def _optional_non_empty(value: str | None, *, field: str) -> str | None:
    if value is None:
        return None
    return _require_non_empty(value, field=field)


def recovery_attempt_id(run_id: str, epoch: int) -> str:
    """Return the immutable physical-attempt identity for one logical run."""

    normalized_run_id = _require_non_empty(run_id, field="run_id")
    normalized_epoch = int(epoch)
    if normalized_epoch < 1:
        raise ValueError("epoch must be at least 1")
    return f"{normalized_run_id}/attempt-{normalized_epoch}"


@dataclass(frozen=True, slots=True)
class RecoveryProgressCounts:
    """Closed, never-omitted progress counters exposed by recovery status."""

    boards_total: int = 0
    boards_scanned: int = 0
    sources_total: int = 0
    sources_processed: int = 0
    nodes_written: int = 0
    edges_written: int = 0
    outbox_events_drained: int = 0
    errors: int = 0

    def __post_init__(self) -> None:
        for field in (
            "boards_total",
            "boards_scanned",
            "sources_total",
            "sources_processed",
            "nodes_written",
            "edges_written",
            "outbox_events_drained",
            "errors",
        ):
            object.__setattr__(
                self,
                field,
                _require_non_negative(getattr(self, field), field=field),
            )
        if self.sources_processed > self.sources_total:
            raise ValueError("sources_processed cannot exceed sources_total")
        if self.boards_scanned > self.boards_total:
            raise ValueError("boards_scanned cannot exceed boards_total")

    def to_dict(self) -> dict[str, int]:
        return {
            "boards_total": self.boards_total,
            "boards_scanned": self.boards_scanned,
            "sources_total": self.sources_total,
            "sources_processed": self.sources_processed,
            "nodes_written": self.nodes_written,
            "edges_written": self.edges_written,
            "outbox_events_drained": self.outbox_events_drained,
            "errors": self.errors,
        }


class RecoveryCheckpointConflict(RuntimeError):
    """Typed optimistic-fence conflict; raising it never authorizes mutation."""

    def __init__(
        self,
        *,
        code: str,
        run_id: str,
        expected_epoch: int,
        actual_epoch: int,
        expected_progress_seq: int,
        actual_progress_seq: int,
    ) -> None:
        self.code = _require_non_empty(code, field="code")
        self.run_id = _require_non_empty(run_id, field="run_id")
        self.expected_epoch = int(expected_epoch)
        self.actual_epoch = int(actual_epoch)
        self.expected_progress_seq = int(expected_progress_seq)
        self.actual_progress_seq = int(actual_progress_seq)
        super().__init__(
            f"{self.code}: run={self.run_id} "
            f"epoch={self.expected_epoch}/{self.actual_epoch} "
            f"progress={self.expected_progress_seq}/{self.actual_progress_seq}"
        )


class RecoveryBindingConflict(RuntimeError):
    """The same run identity was presented with a different immutable binding."""

    code = "recovery_binding_conflict"

    def __init__(self, *, run_id: str) -> None:
        self.run_id = _require_non_empty(run_id, field="run_id")
        super().__init__(f"{self.code}: run={self.run_id}")


class RecoveryRunNotFound(LookupError):
    code = "recovery_run_not_found"

    def __init__(self, *, run_id: str) -> None:
        self.run_id = _require_non_empty(run_id, field="run_id")
        super().__init__(f"{self.code}: run={self.run_id}")


class RecoveryInProgress(RuntimeError):
    """Non-disclosing contention error for the single global slot."""

    code = "recovery_in_progress"

    def __init__(self) -> None:
        super().__init__(self.code)


class RecoveryDispatchClaimConflict(RuntimeError):
    """A stale or losing durable dispatch-claim CAS attempted ownership."""

    code = "recovery_dispatch_claim_conflict"

    def __init__(self, *, run_id: str, epoch: int) -> None:
        self.run_id = _require_non_empty(run_id, field="run_id")
        self.epoch = int(epoch)
        super().__init__(f"{self.code}: run={self.run_id} epoch={self.epoch}")


class RecoveryResumeRejected(RuntimeError):
    """Typed, deterministic refusal of one explicit resume request."""

    def __init__(self, *, code: str, run_id: str, epoch: int) -> None:
        self.code = _require_non_empty(code, field="code")
        self.run_id = _require_non_empty(run_id, field="run_id")
        self.epoch = int(epoch)
        super().__init__(f"{self.code}: run={self.run_id} epoch={self.epoch}")


class RecoveryPhaseIncoherent(ValueError):
    """A persisted top-level state/phase pair is outside the closed matrix."""

    code = "recovery_phase_incoherent"

    def __init__(self, *, state: str, phase: str) -> None:
        self.state = str(state)
        self.phase = str(phase)
        super().__init__(f"{self.code}: state={self.state} phase={self.phase}")


class RecoveryAuditReasonInvalid(ValueError):
    """An optional operator audit reason is blank or exceeds 512 characters."""

    code = "recovery_audit_reason_invalid"

    def __init__(self) -> None:
        super().__init__(self.code)


class RecoveryProgressInvariantViolation(ValueError):
    """A checkpoint changed immutable totals or regressed persisted progress."""

    code = "recovery_progress_invariant_violation"

    def __init__(self, *, field: str, previous: int, proposed: int) -> None:
        self.field = _require_non_empty(field, field="field")
        self.previous = int(previous)
        self.proposed = int(proposed)
        super().__init__(
            f"{self.code}: {self.field}={self.previous}/{self.proposed}"
        )


def normalize_recovery_audit_reason(reason: str | None) -> str | None:
    if reason is None:
        return None
    normalized = str(reason).strip()
    if not normalized or len(normalized) > 512:
        raise RecoveryAuditReasonInvalid()
    return normalized


_RECOVERY_TOTAL_FIELDS = ("boards_total", "sources_total")
_RECOVERY_COUNTER_FIELDS = (
    "boards_scanned",
    "sources_processed",
    "nodes_written",
    "edges_written",
    "outbox_events_drained",
    "errors",
)


def _validate_progress_transition(
    previous: RecoveryProgressCounts,
    proposed: RecoveryProgressCounts,
    *,
    allow_total_initialization: bool = False,
) -> None:
    for field in _RECOVERY_TOTAL_FIELDS:
        before = getattr(previous, field)
        after = getattr(proposed, field)
        if after != before and not (allow_total_initialization and before == 0):
            raise RecoveryProgressInvariantViolation(
                field=field,
                previous=before,
                proposed=after,
            )
    for field in _RECOVERY_COUNTER_FIELDS:
        before = getattr(previous, field)
        after = getattr(proposed, field)
        if after < before:
            raise RecoveryProgressInvariantViolation(
                field=field,
                previous=before,
                proposed=after,
            )


class RecoveryRunState(str, Enum):
    """Closed lifecycle projection; terminal values mirror terminal outcomes."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"
    PARTIAL = "partial"

    @property
    def is_terminal(self) -> bool:
        return self not in {self.PENDING, self.RUNNING}


class RecoveryRunPhase(str, Enum):
    """Closed phase vocabulary underneath the stable top-level state enum."""

    QUEUED = "queued"
    PREPARING = "preparing"
    PREPARED = "prepared"
    CONFIRMED = "confirmed"
    CUTOVER = "cutover"
    TERMINAL = "terminal"


class RecoveryConfirmationState(str, Enum):
    UNCONFIRMED = "unconfirmed"
    PREPARED = "prepared"
    CONSUMED = "consumed"


class RecoveryDispatchKind(str, Enum):
    PREPARATION = "preparation"
    RECOVERY = "recovery"


_RECOVERY_METRIC_OTHER = "other"
_RECOVERY_METRIC_OPERATIONS = frozenset(
    {
        "preflight",
        "confirm",
        "start",
        "status",
        "cancel",
        "resume",
        "heartbeat",
        "dispatch",
        "complete",
        "reconcile",
        "recovery_terminal",
    }
)
_RECOVERY_METRIC_OUTCOMES = frozenset(
    {
        "accepted",
        "replayed",
        "queued",
        "running",
        "success",
        "failed",
        "cancelled",
        "timeout",
        "partial",
        "rejected",
        "conflict",
        "noop",
    }
)
_RECOVERY_METRIC_REASON_CODES = frozenset(
    {
        "global_discovery_recovery_completed",
        "global_discovery_recovery_preparation_cancelled",
        "global_discovery_recovery_preparation_failed",
        "global_discovery_recovery_preparation_retry_exhausted",
        "global_discovery_recovery_rolled_back",
        "native_operation_failed",
        "recovery_attempt_budget_exhausted",
        "recovery_attempt_not_active",
        "recovery_binding_conflict",
        "recovery_cancel_pending",
        "recovery_cancel_requested",
        "recovery_cancelled",
        "recovery_confirmed",
        "recovery_cumulative_budget_exhausted",
        "recovery_cutover_running",
        "recovery_dispatch_claim_conflict",
        "recovery_dispatch_not_adoptable",
        "recovery_epoch_conflict",
        "recovery_failure_not_retryable",
        "recovery_phase_incoherent",
        "recovery_physical_reconciliation_pending",
        "recovery_preparation_queued",
        "recovery_prepared",
        "recovery_prepared_cancelled",
        "recovery_preparing",
        "recovery_progress_conflict",
        "recovery_progress_invariant_violation",
        "recovery_queued",
        "recovery_resume_admitted",
        "recovery_run_conflict",
        "recovery_run_not_found",
        "recovery_success_not_resumable",
        "recovery_takeover_admitted",
        "recovery_timeout_not_resumable",
        "recovery_worker_claim_expired",
        "worker_lease_active",
        "worker_lease_expired",
    }
)

_RECOVERY_TRANSITION_OPERATION_BY_REASON = {
    "recovery_preparation_queued": "preflight",
    "recovery_preparing": "dispatch",
    "recovery_prepared": "complete",
    "recovery_confirmed": "confirm",
    "recovery_cutover_running": "dispatch",
    "recovery_cancel_requested": "cancel",
    "recovery_resume_admitted": "resume",
    "recovery_takeover_admitted": "resume",
}


def _bounded_recovery_metric_label(value: str, *, allowed: frozenset[str]) -> str:
    """Keep metric values low-cardinality without discarding event detail."""

    return value if value in allowed else _RECOVERY_METRIC_OTHER


@dataclass(frozen=True, slots=True)
class RecoveryTransitionEvent:
    """Low-cardinality transition labels plus structured high-cardinality facts."""

    operation: str
    outcome: str
    phase: RecoveryRunPhase
    reason_code: str
    state: RecoveryRunState
    run_id: str
    attempt_id: str
    epoch: int
    progress_seq: int

    def __post_init__(self) -> None:
        for field in ("operation", "outcome", "reason_code", "run_id", "attempt_id"):
            object.__setattr__(
                self, field, _require_non_empty(getattr(self, field), field=field)
            )
        object.__setattr__(self, "phase", RecoveryRunPhase(self.phase))
        object.__setattr__(self, "state", RecoveryRunState(self.state))
        epoch = int(self.epoch)
        if epoch < 1:
            raise ValueError("epoch must be at least 1")
        object.__setattr__(self, "epoch", epoch)
        object.__setattr__(
            self,
            "progress_seq",
            _require_non_negative(self.progress_seq, field="progress_seq"),
        )

    @classmethod
    def from_status(
        cls,
        status: "RecoveryRunStatus",
        *,
        previous: "RecoveryRunStatus | None" = None,
    ) -> "RecoveryTransitionEvent":
        """Project one durable status mutation into one bounded event identity."""

        if not isinstance(status, RecoveryRunStatus):
            raise TypeError("status must be RecoveryRunStatus")
        if previous is not None:
            if not isinstance(previous, RecoveryRunStatus):
                raise TypeError("previous must be RecoveryRunStatus")
            if (
                previous.run_id != status.run_id
                or previous.attempt_id != status.attempt_id
                or previous.epoch != status.epoch
            ):
                raise ValueError("transition statuses must identify one attempt")
            if status.progress_seq <= previous.progress_seq:
                raise ValueError("transition progress_seq must advance")

        if status.terminal_outcome is not None:
            operation = "recovery_terminal"
            outcome = status.terminal_outcome.value
        else:
            operation = _RECOVERY_TRANSITION_OPERATION_BY_REASON.get(
                status.reason_code,
                "heartbeat" if previous is not None else "preflight",
            )
            if status.state is RecoveryRunState.RUNNING:
                outcome = "running"
            elif status.phase is RecoveryRunPhase.QUEUED:
                outcome = "queued"
            else:
                outcome = "accepted"

        return cls(
            operation=operation,
            outcome=outcome,
            phase=status.phase,
            reason_code=status.reason_code,
            state=status.state,
            run_id=status.run_id,
            attempt_id=status.attempt_id,
            epoch=status.epoch,
            progress_seq=status.progress_seq,
        )

    @property
    def metric_labels(self) -> dict[str, str]:
        return {
            "operation": _bounded_recovery_metric_label(
                self.operation,
                allowed=_RECOVERY_METRIC_OPERATIONS,
            ),
            "outcome": _bounded_recovery_metric_label(
                self.outcome,
                allowed=_RECOVERY_METRIC_OUTCOMES,
            ),
            "phase": self.phase.value,
            "reason_code": _bounded_recovery_metric_label(
                self.reason_code,
                allowed=_RECOVERY_METRIC_REASON_CODES,
            ),
        }


@runtime_checkable
class RecoveryTransitionObserver(Protocol):
    def on_transition(self, event: RecoveryTransitionEvent) -> None: ...


class RecoveryTerminalOutcome(str, Enum):
    """Closed terminal vocabulary for every recovery attempt."""

    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"
    PARTIAL = "partial"


@dataclass(frozen=True, slots=True)
class RecoveryRunBinding:
    """Immutable authorization/work identity bound to a deterministic run id."""

    run_id: str
    actor_id: str
    confirmation_fingerprint: str | None = None
    manifest_ref: str | None = None
    preflight_hash: str | None = None
    reason: str | None = None

    def __post_init__(self) -> None:
        for field in ("run_id", "actor_id"):
            object.__setattr__(
                self,
                field,
                _require_non_empty(getattr(self, field), field=field),
            )
        for field in ("confirmation_fingerprint", "manifest_ref", "preflight_hash"):
            object.__setattr__(
                self,
                field,
                _optional_non_empty(getattr(self, field), field=field),
            )
        object.__setattr__(self, "reason", normalize_recovery_audit_reason(self.reason))

    @property
    def is_prepared(self) -> bool:
        return self.manifest_ref is not None and self.preflight_hash is not None

    @property
    def is_confirmed(self) -> bool:
        return (
            self.is_prepared
            and self.confirmation_fingerprint is not None
            and self.reason is not None
        )


@dataclass(frozen=True, slots=True)
class RecoveryPreparationCommand:
    """Admission command for bounded asynchronous preparation."""

    binding: RecoveryRunBinding
    admitted_at: datetime
    counts: RecoveryProgressCounts
    attempt_budget_ms: int = DEFAULT_RECOVERY_ATTEMPT_BUDGET_MS

    def __post_init__(self) -> None:
        if not isinstance(self.binding, RecoveryRunBinding):
            raise TypeError("binding must be RecoveryRunBinding")
        if self.binding.is_prepared or self.binding.is_confirmed:
            raise ValueError("preparation binding cannot already be prepared")
        object.__setattr__(
            self, "admitted_at", _require_aware(self.admitted_at, field="admitted_at")
        )
        if not isinstance(self.counts, RecoveryProgressCounts):
            raise TypeError("counts must be RecoveryProgressCounts")
        budget = _require_non_negative(self.attempt_budget_ms, field="attempt_budget_ms")
        if budget == 0 or budget > DEFAULT_RECOVERY_ATTEMPT_BUDGET_MS:
            raise ValueError("attempt_budget_ms must be within 1..600000")
        object.__setattr__(self, "attempt_budget_ms", budget)


@dataclass(frozen=True, slots=True)
class RecoveryPreparedResult:
    """Complete-only output of the preparation worker."""

    manifest_ref: str
    preflight_hash: str
    snapshot_fingerprint: str
    prepared_at: datetime
    expires_at: datetime
    counts: RecoveryProgressCounts

    def __post_init__(self) -> None:
        for field in ("manifest_ref", "preflight_hash", "snapshot_fingerprint"):
            object.__setattr__(
                self, field, _require_non_empty(getattr(self, field), field=field)
            )
        prepared_at = _require_aware(self.prepared_at, field="prepared_at")
        expires_at = _require_aware(self.expires_at, field="expires_at")
        if expires_at != prepared_at + timedelta(seconds=RECOVERY_PREPARED_TTL_SECONDS):
            raise ValueError("prepared manifest TTL must be exactly 300 seconds")
        object.__setattr__(self, "prepared_at", prepared_at)
        object.__setattr__(self, "expires_at", expires_at)
        if not isinstance(self.counts, RecoveryProgressCounts):
            raise TypeError("counts must be RecoveryProgressCounts")


@dataclass(frozen=True, slots=True)
class RecoverySlotOwnership:
    slot_id: str
    run_id: str
    epoch: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "slot_id", _require_non_empty(self.slot_id, field="slot_id"))
        object.__setattr__(self, "run_id", _require_non_empty(self.run_id, field="run_id"))
        epoch = int(self.epoch)
        if epoch < 1:
            raise ValueError("epoch must be at least 1")
        object.__setattr__(self, "epoch", epoch)


@dataclass(frozen=True, slots=True)
class RecoveryPhysicalTruth:
    """Durable physical facts that cancellation must never rewrite."""

    attempt_id: str
    journal_phase: str
    pointer_replaced: bool
    rollback_performed: bool
    evidence_ref: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "attempt_id", _require_non_empty(self.attempt_id, field="attempt_id"))
        object.__setattr__(self, "journal_phase", _require_non_empty(self.journal_phase, field="journal_phase"))
        if not isinstance(self.pointer_replaced, bool):
            raise TypeError("pointer_replaced must be bool")
        if not isinstance(self.rollback_performed, bool):
            raise TypeError("rollback_performed must be bool")
        object.__setattr__(self, "evidence_ref", _optional_non_empty(self.evidence_ref, field="evidence_ref"))


@dataclass(frozen=True, slots=True)
class RecoveryStartCommand:
    binding: RecoveryRunBinding
    started_at: datetime
    counts: RecoveryProgressCounts
    attempt_budget_ms: int = DEFAULT_RECOVERY_ATTEMPT_BUDGET_MS
    expected_epoch: int = 1
    confirmed_by_actor_id: str | None = None
    confirmation_consumed_at: datetime | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.binding, RecoveryRunBinding):
            raise TypeError("binding must be RecoveryRunBinding")
        object.__setattr__(
            self,
            "started_at",
            _require_aware(self.started_at, field="started_at"),
        )
        if not isinstance(self.counts, RecoveryProgressCounts):
            raise TypeError("counts must be RecoveryProgressCounts")
        if not self.binding.is_confirmed:
            raise ValueError("recovery start binding must be freshly confirmed")
        attempt_budget_ms = _require_non_negative(
            self.attempt_budget_ms,
            field="attempt_budget_ms",
        )
        if attempt_budget_ms == 0:
            raise ValueError("attempt_budget_ms must be positive")
        if attempt_budget_ms > DEFAULT_RECOVERY_ATTEMPT_BUDGET_MS:
            raise ValueError(
                "attempt_budget_ms cannot exceed the 10-minute default cap"
            )
        object.__setattr__(self, "attempt_budget_ms", attempt_budget_ms)
        expected_epoch = int(self.expected_epoch)
        if expected_epoch < 1:
            raise ValueError("expected_epoch must be at least 1")
        object.__setattr__(self, "expected_epoch", expected_epoch)
        confirmed_actor = self.confirmed_by_actor_id or self.binding.actor_id
        object.__setattr__(
            self,
            "confirmed_by_actor_id",
            _require_non_empty(confirmed_actor, field="confirmed_by_actor_id"),
        )
        consumed_at = self.confirmation_consumed_at or self.started_at
        object.__setattr__(
            self,
            "confirmation_consumed_at",
            _require_aware(consumed_at, field="confirmation_consumed_at"),
        )


@dataclass(frozen=True, slots=True)
class RecoveryWorkerResult:
    """Edition-neutral terminal result returned by the owned worker."""

    outcome: RecoveryTerminalOutcome
    reason_code: str
    retryable: bool
    counts: RecoveryProgressCounts
    physical_truth: RecoveryPhysicalTruth | None = None

    def __post_init__(self) -> None:
        outcome = RecoveryTerminalOutcome(self.outcome)
        object.__setattr__(self, "outcome", outcome)
        object.__setattr__(
            self,
            "reason_code",
            _require_non_empty(self.reason_code, field="reason_code"),
        )
        if not isinstance(self.retryable, bool):
            raise TypeError("retryable must be bool")
        if self.retryable and outcome is not RecoveryTerminalOutcome.FAILED:
            raise ValueError("retryable is only valid for failed outcomes")
        if not isinstance(self.counts, RecoveryProgressCounts):
            raise TypeError("counts must be RecoveryProgressCounts")
        if self.physical_truth is not None and not isinstance(
            self.physical_truth, RecoveryPhysicalTruth
        ):
            raise TypeError("physical_truth must be RecoveryPhysicalTruth")


@dataclass(frozen=True, slots=True)
class RecoveryRunStatus:
    """Authoritative immutable projection of the latest recovery attempt."""

    binding: RecoveryRunBinding
    epoch: int
    state: RecoveryRunState
    progress_seq: int
    phase: RecoveryRunPhase
    counts: RecoveryProgressCounts
    heartbeat_at: datetime
    started_at: datetime
    updated_at: datetime
    active_elapsed_ms: int
    active_deadline_at: datetime
    cumulative_active_ms: int
    cancel_requested_at: datetime | None = None
    terminal_outcome: RecoveryTerminalOutcome | None = None
    reason_code: str = "recovery_queued"
    retryable: bool = False
    supersedes_epoch: int | None = None
    superseded_by_epoch: int | None = None
    confirmation_state: RecoveryConfirmationState = RecoveryConfirmationState.UNCONFIRMED
    prepared_at: datetime | None = None
    expires_at: datetime | None = None
    snapshot_fingerprint: str | None = None
    confirmed_by_actor_id: str | None = None
    confirmation_consumed_at: datetime | None = None
    audit_reason: str | None = None
    physical_truth: RecoveryPhysicalTruth | None = None
    cancel_requested_by_actor_id: str | None = None
    resume_requested_at: datetime | None = None
    resume_requested_by_actor_id: str | None = None
    resume_audit_reason: str | None = None
    attempt_budget_ms: int = DEFAULT_RECOVERY_ATTEMPT_BUDGET_MS

    def __post_init__(self) -> None:
        if not isinstance(self.binding, RecoveryRunBinding):
            raise TypeError("binding must be RecoveryRunBinding")
        epoch = int(self.epoch)
        if epoch < 1:
            raise ValueError("epoch must be at least 1")
        object.__setattr__(self, "epoch", epoch)
        state = RecoveryRunState(self.state)
        object.__setattr__(self, "state", state)
        object.__setattr__(
            self,
            "progress_seq",
            _require_non_negative(self.progress_seq, field="progress_seq"),
        )
        phase = RecoveryRunPhase(self.phase)
        object.__setattr__(self, "phase", phase)
        if not isinstance(self.counts, RecoveryProgressCounts):
            raise TypeError("counts must be RecoveryProgressCounts")
        for field in (
            "heartbeat_at",
            "started_at",
            "updated_at",
            "active_deadline_at",
        ):
            object.__setattr__(
                self,
                field,
                _require_aware(getattr(self, field), field=field),
            )
        if self.cancel_requested_at is not None:
            object.__setattr__(
                self,
                "cancel_requested_at",
                _require_aware(
                    self.cancel_requested_at,
                    field="cancel_requested_at",
                ),
            )
        for field in (
            "prepared_at",
            "expires_at",
            "confirmation_consumed_at",
            "resume_requested_at",
        ):
            value = getattr(self, field)
            if value is not None:
                object.__setattr__(self, field, _require_aware(value, field=field))
        if self.updated_at < self.started_at:
            raise ValueError("updated_at cannot precede started_at")
        if self.heartbeat_at < self.started_at:
            raise ValueError("heartbeat_at cannot precede started_at")
        if self.active_deadline_at < self.started_at:
            raise ValueError("active_deadline_at cannot precede started_at")
        object.__setattr__(
            self,
            "active_elapsed_ms",
            _require_non_negative(self.active_elapsed_ms, field="active_elapsed_ms"),
        )
        object.__setattr__(
            self,
            "cumulative_active_ms",
            _require_non_negative(
                self.cumulative_active_ms,
                field="cumulative_active_ms",
            ),
        )
        if self.active_elapsed_ms > self.cumulative_active_ms:
            raise ValueError("active_elapsed_ms cannot exceed cumulative_active_ms")
        attempt_budget_ms = _require_non_negative(
            self.attempt_budget_ms,
            field="attempt_budget_ms",
        )
        if attempt_budget_ms == 0 or attempt_budget_ms > DEFAULT_RECOVERY_ATTEMPT_BUDGET_MS:
            raise ValueError("attempt_budget_ms must be within 1..600000")
        object.__setattr__(self, "attempt_budget_ms", attempt_budget_ms)
        if self.active_elapsed_ms > attempt_budget_ms:
            raise ValueError("active_elapsed_ms exceeds attempt budget")
        if self.cumulative_active_ms > MAX_RECOVERY_CUMULATIVE_BUDGET_MS:
            raise ValueError("cumulative_active_ms exceeds cumulative budget")
        if not isinstance(self.retryable, bool):
            raise TypeError("retryable must be bool")
        object.__setattr__(
            self,
            "reason_code",
            _require_non_empty(self.reason_code, field="reason_code"),
        )
        terminal_outcome = (
            None
            if self.terminal_outcome is None
            else RecoveryTerminalOutcome(self.terminal_outcome)
        )
        object.__setattr__(self, "terminal_outcome", terminal_outcome)
        if state.is_terminal:
            if terminal_outcome is None or terminal_outcome.value != state.value:
                raise ValueError("terminal state and outcome must match")
        elif terminal_outcome is not None:
            raise ValueError("non-terminal state cannot expose terminal_outcome")
        if self.retryable and terminal_outcome is not RecoveryTerminalOutcome.FAILED:
            raise ValueError("retryable is only valid for failed outcomes")
        for field in ("supersedes_epoch", "superseded_by_epoch"):
            value = getattr(self, field)
            if value is not None and int(value) < 1:
                raise ValueError(f"{field} must be at least 1")
            if value is not None:
                object.__setattr__(self, field, int(value))
        confirmation_state = RecoveryConfirmationState(self.confirmation_state)
        object.__setattr__(self, "confirmation_state", confirmation_state)
        object.__setattr__(
            self,
            "snapshot_fingerprint",
            _optional_non_empty(self.snapshot_fingerprint, field="snapshot_fingerprint"),
        )
        object.__setattr__(
            self,
            "confirmed_by_actor_id",
            _optional_non_empty(self.confirmed_by_actor_id, field="confirmed_by_actor_id"),
        )
        object.__setattr__(
            self,
            "audit_reason",
            normalize_recovery_audit_reason(self.audit_reason),
        )
        for field in (
            "cancel_requested_by_actor_id",
            "resume_requested_by_actor_id",
        ):
            object.__setattr__(
                self,
                field,
                _optional_non_empty(getattr(self, field), field=field),
            )
        object.__setattr__(
            self,
            "resume_audit_reason",
            normalize_recovery_audit_reason(self.resume_audit_reason),
        )
        if (self.cancel_requested_at is None) != (
            self.cancel_requested_by_actor_id is None
        ):
            raise ValueError(
                "cancel_requested_at and cancel_requested_by_actor_id must coexist"
            )
        if (self.resume_requested_at is None) != (
            self.resume_requested_by_actor_id is None
        ):
            raise ValueError(
                "resume_requested_at and resume_requested_by_actor_id must coexist"
            )
        if self.physical_truth is not None:
            if not isinstance(self.physical_truth, RecoveryPhysicalTruth):
                raise TypeError("physical_truth must be RecoveryPhysicalTruth")
            if self.physical_truth.attempt_id != self.attempt_id:
                raise ValueError("physical_truth attempt_id does not match status")

        allowed_phases = {
            RecoveryRunState.PENDING: {
                RecoveryRunPhase.QUEUED,
                RecoveryRunPhase.PREPARED,
                RecoveryRunPhase.CONFIRMED,
            },
            RecoveryRunState.RUNNING: {
                RecoveryRunPhase.PREPARING,
                RecoveryRunPhase.CUTOVER,
            },
            RecoveryRunState.SUCCESS: {RecoveryRunPhase.TERMINAL},
            RecoveryRunState.FAILED: {RecoveryRunPhase.TERMINAL},
            RecoveryRunState.CANCELLED: {RecoveryRunPhase.TERMINAL},
            RecoveryRunState.TIMEOUT: {RecoveryRunPhase.TERMINAL},
            RecoveryRunState.PARTIAL: {RecoveryRunPhase.TERMINAL},
        }
        if phase not in allowed_phases[state]:
            raise RecoveryPhaseIncoherent(state=state.value, phase=phase.value)

        prepared_values = (self.prepared_at, self.expires_at, self.snapshot_fingerprint)
        if any(value is not None for value in prepared_values):
            if not all(value is not None for value in prepared_values):
                raise ValueError("prepared metadata must be complete")
            assert self.prepared_at is not None and self.expires_at is not None
            if self.expires_at != self.prepared_at + timedelta(
                seconds=RECOVERY_PREPARED_TTL_SECONDS
            ):
                raise ValueError("prepared manifest TTL must be exactly 300 seconds")

        if phase in {RecoveryRunPhase.QUEUED, RecoveryRunPhase.PREPARING}:
            if confirmation_state is not RecoveryConfirmationState.UNCONFIRMED:
                raise RecoveryPhaseIncoherent(state=state.value, phase=phase.value)
            if any(value is not None for value in prepared_values):
                raise RecoveryPhaseIncoherent(state=state.value, phase=phase.value)
        elif phase is RecoveryRunPhase.PREPARED:
            if (
                confirmation_state is not RecoveryConfirmationState.PREPARED
                or not self.binding.is_prepared
                or self.binding.is_confirmed
                or not all(value is not None for value in prepared_values)
            ):
                raise RecoveryPhaseIncoherent(state=state.value, phase=phase.value)
        elif phase in {RecoveryRunPhase.CONFIRMED, RecoveryRunPhase.CUTOVER}:
            if (
                confirmation_state is not RecoveryConfirmationState.CONSUMED
                or not self.binding.is_confirmed
                or self.confirmed_by_actor_id is None
                or self.confirmation_consumed_at is None
                or not all(value is not None for value in prepared_values)
            ):
                raise RecoveryPhaseIncoherent(state=state.value, phase=phase.value)

    @property
    def run_id(self) -> str:
        return self.binding.run_id

    @property
    def actor_id(self) -> str:
        return self.binding.actor_id

    @property
    def attempt_id(self) -> str:
        return recovery_attempt_id(self.run_id, self.epoch)

    @property
    def slot_ownership(self) -> RecoverySlotOwnership:
        return RecoverySlotOwnership(
            slot_id=GLOBAL_RECOVERY_SLOT_ID,
            run_id=self.run_id,
            epoch=self.epoch,
        )

    @property
    def preparation_state(self) -> str:
        if self.phase in {
            RecoveryRunPhase.QUEUED,
            RecoveryRunPhase.PREPARING,
            RecoveryRunPhase.PREPARED,
        }:
            return self.phase.value
        if self.prepared_at is not None:
            return RecoveryRunPhase.PREPARED.value
        if self.state is RecoveryRunState.CANCELLED:
            return "cancelled"
        if self.state.is_terminal:
            return "preparation_failed"
        return RecoveryRunPhase.QUEUED.value

    @classmethod
    def initial_preparation(
        cls, command: RecoveryPreparationCommand
    ) -> RecoveryRunStatus:
        return cls(
            binding=command.binding,
            epoch=1,
            state=RecoveryRunState.PENDING,
            progress_seq=0,
            phase=RecoveryRunPhase.QUEUED,
            counts=command.counts,
            heartbeat_at=command.admitted_at,
            started_at=command.admitted_at,
            updated_at=command.admitted_at,
            active_elapsed_ms=0,
            active_deadline_at=command.admitted_at
            + timedelta(milliseconds=command.attempt_budget_ms),
            cumulative_active_ms=0,
            attempt_budget_ms=command.attempt_budget_ms,
            confirmation_state=RecoveryConfirmationState.UNCONFIRMED,
            reason_code="recovery_preparation_queued",
        )

    def to_dict(self) -> dict[str, object]:
        action_required: str | None
        if self.phase is RecoveryRunPhase.PREPARED:
            action_required = "call_okto_pulse_kg_global_discovery_recovery_confirm"
        elif self.state.is_terminal:
            action_required = None
        else:
            action_required = GLOBAL_RECOVERY_STATUS_TOOL
        return {
            "run_id": self.run_id,
            "actor_id": self.actor_id,
            "epoch": self.epoch,
            "attempt_id": self.attempt_id,
            "manifest_ref": self.binding.manifest_ref,
            "preflight_hash": self.binding.preflight_hash,
            "state": self.state.value,
            "progress_seq": self.progress_seq,
            "phase": self.phase.value,
            "preparation_state": self.preparation_state,
            "confirmation_state": self.confirmation_state.value,
            "status_tool": GLOBAL_RECOVERY_STATUS_TOOL,
            "action_required": action_required,
            "counts": self.counts.to_dict(),
            "heartbeat_at": self.heartbeat_at.isoformat(),
            "started_at": self.started_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "active_elapsed_ms": self.active_elapsed_ms,
            "active_deadline_at": self.active_deadline_at.isoformat(),
            "cumulative_active_ms": self.cumulative_active_ms,
            "attempt_budget_ms": self.attempt_budget_ms,
            "cancel_requested_at": (
                None
                if self.cancel_requested_at is None
                else self.cancel_requested_at.isoformat()
            ),
            "terminal_outcome": (
                None if self.terminal_outcome is None else self.terminal_outcome.value
            ),
            "reason_code": self.reason_code,
            "retryable": self.retryable,
            "supersedes_epoch": self.supersedes_epoch,
            "superseded_by_epoch": self.superseded_by_epoch,
            "prepared_at": (
                None if self.prepared_at is None else self.prepared_at.isoformat()
            ),
            "expires_at": (
                None if self.expires_at is None else self.expires_at.isoformat()
            ),
            "snapshot_fingerprint": self.snapshot_fingerprint,
            "confirmed_by_actor_id": self.confirmed_by_actor_id,
            "confirmation_consumed_at": (
                None
                if self.confirmation_consumed_at is None
                else self.confirmation_consumed_at.isoformat()
            ),
            "audit_reason": self.audit_reason,
            "cancel_requested_by_actor_id": self.cancel_requested_by_actor_id,
            "resume_requested_at": (
                None
                if self.resume_requested_at is None
                else self.resume_requested_at.isoformat()
            ),
            "resume_requested_by_actor_id": self.resume_requested_by_actor_id,
            "resume_audit_reason": self.resume_audit_reason,
            "physical_truth": (
                None
                if self.physical_truth is None
                else {
                    "attempt_id": self.physical_truth.attempt_id,
                    "journal_phase": self.physical_truth.journal_phase,
                    "pointer_replaced": self.physical_truth.pointer_replaced,
                    "rollback_performed": self.physical_truth.rollback_performed,
                    "evidence_ref": self.physical_truth.evidence_ref,
                }
            ),
        }

    def mark_running(self, *, at: datetime) -> RecoveryRunStatus:
        at = _require_aware(at, field="at")
        if self.state is RecoveryRunState.RUNNING:
            return self
        if self.state is not RecoveryRunState.PENDING:
            raise ValueError("only a pending recovery can start running")
        if self.phase is RecoveryRunPhase.QUEUED:
            next_phase = RecoveryRunPhase.PREPARING
            reason_code = "recovery_preparing"
        elif self.phase is RecoveryRunPhase.CONFIRMED:
            next_phase = RecoveryRunPhase.CUTOVER
            reason_code = "recovery_cutover_running"
        else:
            raise RecoveryResumeRejected(
                code="prepared_run_not_adoptable",
                run_id=self.run_id,
                epoch=self.epoch,
            )
        if at < self.updated_at:
            raise ValueError("running timestamp cannot move backwards")
        return replace(
            self,
            state=RecoveryRunState.RUNNING,
            progress_seq=self.progress_seq + 1,
            phase=next_phase,
            heartbeat_at=at,
            updated_at=at,
            reason_code=reason_code,
        )

    @property
    def effective_active_cap_ms(self) -> int:
        """Hard active-time cap for this attempt: the admission budget bounded
        by the cumulative allowance remaining across earlier attempts."""

        previous_attempts_ms = self.cumulative_active_ms - self.active_elapsed_ms
        cumulative_allowance_ms = max(
            0, MAX_RECOVERY_CUMULATIVE_BUDGET_MS - previous_attempts_ms
        )
        return min(self.attempt_budget_ms, cumulative_allowance_ms)

    def settle_expired_reclaim(
        self,
        *,
        old_claim_expires_at: datetime,
        claimed_at: datetime,
    ) -> RecoveryRunStatus:
        """Charge the crashed owner's lease window exactly once and rebase
        liveness on the winning reclaim (A5R2).

        The accountable window is [heartbeat_at, min(old_claim_expires_at,
        active_deadline_at)]; the unowned downtime between the old claim
        expiry and the new claim is never charged.  ``claimed_at`` becomes
        the NEW proof of life (heartbeat and updated), so a follow-up crash
        before the first heartbeat charges only from this claim onward.
        Charged active time is bounded by ``effective_active_cap_ms`` so
        neither the attempt budget nor the cumulative budget can ever be
        exceeded.
        """

        old_claim_expires_at = _require_aware(
            old_claim_expires_at, field="old_claim_expires_at"
        )
        claimed_at = _require_aware(claimed_at, field="claimed_at")
        if (
            self.state is not RecoveryRunState.RUNNING
            or self.phase is not RecoveryRunPhase.CUTOVER
        ):
            raise ValueError(
                "only a running cutover recovery can settle an expired reclaim"
            )
        if old_claim_expires_at > claimed_at:
            raise ValueError("reclaim requires an expired claim")
        if claimed_at < self.updated_at or claimed_at < self.heartbeat_at:
            raise ValueError("reclaim timestamp cannot move backwards")
        charge_through = min(old_claim_expires_at, self.active_deadline_at)
        raw_gap_ms = max(
            0,
            int((charge_through - self.heartbeat_at).total_seconds() * 1_000),
        )
        previous_attempts_ms = self.cumulative_active_ms - self.active_elapsed_ms
        charged_active_ms = min(
            self.active_elapsed_ms + raw_gap_ms,
            self.effective_active_cap_ms,
        )
        return replace(
            self,
            progress_seq=self.progress_seq + 1,
            active_elapsed_ms=charged_active_ms,
            cumulative_active_ms=previous_attempts_ms + charged_active_ms,
            heartbeat_at=claimed_at,
            updated_at=claimed_at,
        )

    def mark_prepared(
        self,
        *,
        prepared: RecoveryPreparedResult,
        expected_progress_seq: int,
    ) -> RecoveryRunStatus:
        if self.state is not RecoveryRunState.RUNNING or self.phase is not RecoveryRunPhase.PREPARING:
            raise ValueError("only a preparing recovery can become prepared")
        if int(expected_progress_seq) != self.progress_seq:
            raise RecoveryCheckpointConflict(
                code="recovery_progress_conflict",
                run_id=self.run_id,
                expected_epoch=self.epoch,
                actual_epoch=self.epoch,
                expected_progress_seq=expected_progress_seq,
                actual_progress_seq=self.progress_seq,
            )
        if not isinstance(prepared, RecoveryPreparedResult):
            raise TypeError("prepared must be RecoveryPreparedResult")
        if prepared.prepared_at < self.started_at:
            raise ValueError("prepared_at cannot predate attempt start")
        if prepared.expires_at <= self.updated_at:
            raise ValueError("prepared manifest expired before status publication")
        _validate_progress_transition(
            self.counts,
            prepared.counts,
            allow_total_initialization=True,
        )
        transition_at = max(self.updated_at, prepared.prepared_at)
        return replace(
            self,
            binding=replace(
                self.binding,
                manifest_ref=prepared.manifest_ref,
                preflight_hash=prepared.preflight_hash,
            ),
            state=RecoveryRunState.PENDING,
            progress_seq=self.progress_seq + 1,
            phase=RecoveryRunPhase.PREPARED,
            counts=prepared.counts,
            heartbeat_at=transition_at,
            updated_at=transition_at,
            confirmation_state=RecoveryConfirmationState.PREPARED,
            prepared_at=prepared.prepared_at,
            expires_at=prepared.expires_at,
            snapshot_fingerprint=prepared.snapshot_fingerprint,
            reason_code="recovery_prepared",
        )

    def mark_confirmed(self, command: RecoveryStartCommand) -> RecoveryRunStatus:
        if self.state is not RecoveryRunState.PENDING or self.phase is not RecoveryRunPhase.PREPARED:
            raise ValueError("only a prepared recovery can consume confirmation")
        if self.cancel_requested_at is not None:
            raise RecoveryResumeRejected(
                code="recovery_cancel_pending",
                run_id=self.run_id,
                epoch=self.epoch,
            )
        if command.binding.run_id != self.run_id or command.expected_epoch != self.epoch:
            raise RecoveryBindingConflict(run_id=command.binding.run_id)
        if (
            command.binding.actor_id != self.binding.actor_id
            or command.binding.manifest_ref != self.binding.manifest_ref
            or command.binding.preflight_hash != self.binding.preflight_hash
        ):
            raise RecoveryBindingConflict(run_id=self.run_id)
        consumed_at = command.confirmation_consumed_at or command.started_at
        if consumed_at < self.updated_at:
            raise ValueError("confirmation_consumed_at cannot move backwards")
        if self.expires_at is None or consumed_at >= self.expires_at:
            raise ValueError("prepared manifest expired before confirmation consumption")
        if command.counts != self.counts:
            for field, previous in self.counts.to_dict().items():
                proposed = command.counts.to_dict()[field]
                if proposed != previous:
                    raise RecoveryProgressInvariantViolation(
                        field=field,
                        previous=previous,
                        proposed=proposed,
                    )
        assert command.binding.confirmation_fingerprint is not None
        return replace(
            self,
            binding=replace(
                self.binding,
                confirmation_fingerprint=command.binding.confirmation_fingerprint,
                reason=command.binding.reason,
            ),
            progress_seq=self.progress_seq + 1,
            phase=RecoveryRunPhase.CONFIRMED,
            heartbeat_at=consumed_at,
            updated_at=consumed_at,
            active_elapsed_ms=0,
            active_deadline_at=consumed_at
            + timedelta(milliseconds=command.attempt_budget_ms),
            attempt_budget_ms=command.attempt_budget_ms,
            confirmation_state=RecoveryConfirmationState.CONSUMED,
            confirmed_by_actor_id=command.confirmed_by_actor_id,
            confirmation_consumed_at=consumed_at,
            audit_reason=command.binding.reason,
            reason_code="recovery_confirmed",
        )

    def request_cancel(
        self,
        *,
        requested_at: datetime,
        requested_by_actor_id: str,
        reason: str | None = None,
    ) -> RecoveryRunStatus:
        requested_at = _require_aware(requested_at, field="requested_at")
        requested_by = _require_non_empty(
            requested_by_actor_id,
            field="requested_by_actor_id",
        )
        audit_reason = normalize_recovery_audit_reason(reason)
        if self.state.is_terminal or self.cancel_requested_at is not None:
            return self
        if requested_at < self.updated_at:
            raise ValueError("cancel timestamp cannot move backwards")
        return replace(
            self,
            progress_seq=self.progress_seq + 1,
            updated_at=requested_at,
            cancel_requested_at=requested_at,
            cancel_requested_by_actor_id=requested_by,
            reason_code="recovery_cancel_requested",
            audit_reason=audit_reason or self.audit_reason,
        )

    def cancel_prepared(
        self,
        *,
        requested_at: datetime,
        requested_by_actor_id: str,
        reason: str | None = None,
    ) -> RecoveryRunStatus:
        requested_at = _require_aware(requested_at, field="requested_at")
        requested_by = _require_non_empty(
            requested_by_actor_id,
            field="requested_by_actor_id",
        )
        audit_reason = normalize_recovery_audit_reason(reason)
        if self.state.is_terminal:
            return self
        if self.state is not RecoveryRunState.PENDING or self.phase is not RecoveryRunPhase.PREPARED:
            raise ValueError("only a prepared recovery can use prepared cancellation")
        if requested_at < self.updated_at:
            raise ValueError("cancel timestamp cannot move backwards")
        return replace(
            self,
            state=RecoveryRunState.CANCELLED,
            progress_seq=self.progress_seq + 1,
            phase=RecoveryRunPhase.TERMINAL,
            heartbeat_at=requested_at,
            updated_at=requested_at,
            cancel_requested_at=requested_at,
            cancel_requested_by_actor_id=requested_by,
            terminal_outcome=RecoveryTerminalOutcome.CANCELLED,
            reason_code="recovery_prepared_cancelled",
            audit_reason=audit_reason,
        )

    def advance_checkpoint(self, checkpoint: RecoveryCheckpoint) -> RecoveryRunStatus:
        if checkpoint.run_id != self.run_id:
            raise self._checkpoint_conflict("recovery_run_conflict", checkpoint)
        if checkpoint.epoch != self.epoch:
            raise self._checkpoint_conflict("recovery_epoch_conflict", checkpoint)
        if checkpoint.expected_progress_seq != self.progress_seq:
            raise self._checkpoint_conflict("recovery_progress_conflict", checkpoint)
        if self.state is not RecoveryRunState.RUNNING:
            raise ValueError("only a running recovery accepts checkpoints")
        if RecoveryRunPhase(checkpoint.phase) is not self.phase:
            raise RecoveryPhaseIncoherent(
                state=self.state.value,
                phase=str(checkpoint.phase),
            )
        if checkpoint.active_elapsed_ms < self.active_elapsed_ms:
            raise ValueError("active_elapsed_ms cannot move backwards")
        if checkpoint.heartbeat_at < self.heartbeat_at:
            raise ValueError("heartbeat_at cannot move backwards")
        if checkpoint.active_elapsed_ms > self.attempt_budget_ms:
            raise ValueError("active_elapsed_ms exceeds attempt budget")
        _validate_progress_transition(
            self.counts,
            checkpoint.counts,
            allow_total_initialization=(self.phase is RecoveryRunPhase.PREPARING),
        )
        previous_attempts_ms = self.cumulative_active_ms - self.active_elapsed_ms
        proposed_cumulative_ms = previous_attempts_ms + checkpoint.active_elapsed_ms
        if proposed_cumulative_ms > MAX_RECOVERY_CUMULATIVE_BUDGET_MS:
            raise ValueError("cumulative_active_ms exceeds cumulative budget")
        return replace(
            self,
            progress_seq=self.progress_seq + 1,
            phase=self.phase,
            heartbeat_at=checkpoint.heartbeat_at,
            updated_at=max(self.updated_at, checkpoint.heartbeat_at),
            active_elapsed_ms=checkpoint.active_elapsed_ms,
            cumulative_active_ms=proposed_cumulative_ms,
            counts=checkpoint.counts,
        )

    def complete(
        self,
        *,
        result: RecoveryWorkerResult,
        completed_at: datetime,
        active_elapsed_ms: int,
    ) -> RecoveryRunStatus:
        if self.state is not RecoveryRunState.RUNNING:
            raise ValueError("only a running recovery can complete")
        if not isinstance(result, RecoveryWorkerResult):
            raise TypeError("result must be RecoveryWorkerResult")
        completed_at = _require_aware(completed_at, field="completed_at")
        active_elapsed_ms = _require_non_negative(
            active_elapsed_ms,
            field="active_elapsed_ms",
        )
        if completed_at < self.updated_at:
            raise ValueError("completed_at cannot move backwards")
        if active_elapsed_ms < self.active_elapsed_ms:
            raise ValueError("active_elapsed_ms cannot move backwards")
        if active_elapsed_ms > self.attempt_budget_ms:
            raise ValueError("active_elapsed_ms exceeds attempt budget")
        _validate_progress_transition(self.counts, result.counts)
        previous_attempts_ms = self.cumulative_active_ms - self.active_elapsed_ms
        proposed_cumulative_ms = previous_attempts_ms + active_elapsed_ms
        if proposed_cumulative_ms > MAX_RECOVERY_CUMULATIVE_BUDGET_MS:
            raise ValueError("cumulative_active_ms exceeds cumulative budget")
        return replace(
            self,
            state=RecoveryRunState(result.outcome.value),
            progress_seq=self.progress_seq + 1,
            phase="terminal",
            counts=result.counts,
            heartbeat_at=completed_at,
            updated_at=completed_at,
            active_elapsed_ms=active_elapsed_ms,
            cumulative_active_ms=proposed_cumulative_ms,
            terminal_outcome=result.outcome,
            reason_code=result.reason_code,
            retryable=result.retryable,
            physical_truth=result.physical_truth or self.physical_truth,
        )

    def resume_terminal(
        self,
        *,
        decision: RecoveryResumeDecision,
        requested_at: datetime,
        requested_by_actor_id: str,
        reason: str | None,
    ) -> RecoveryRunStatus:
        """Create the next pending attempt without changing the original actor."""

        if not self.state.is_terminal:
            raise ValueError("only a terminal recovery can resume")
        if not decision.admitted or decision.next_epoch != self.epoch + 1:
            raise RecoveryResumeRejected(
                code=decision.reason_code,
                run_id=self.run_id,
                epoch=self.epoch,
            )
        requested_at = _require_aware(requested_at, field="requested_at")
        requested_by = _require_non_empty(
            requested_by_actor_id,
            field="requested_by_actor_id",
        )
        audit_reason = normalize_recovery_audit_reason(reason)
        if requested_at < self.updated_at:
            raise ValueError("resume timestamp cannot move backwards")

        if self.confirmation_state is RecoveryConfirmationState.CONSUMED:
            next_phase = RecoveryRunPhase.CONFIRMED
            next_binding = self.binding
            next_confirmation_state = RecoveryConfirmationState.CONSUMED
            prepared_at = self.prepared_at
            expires_at = self.expires_at
            snapshot_fingerprint = self.snapshot_fingerprint
            next_counts = self.counts
        else:
            next_phase = RecoveryRunPhase.QUEUED
            next_binding = replace(
                self.binding,
                confirmation_fingerprint=None,
                manifest_ref=None,
                preflight_hash=None,
                reason=None,
            )
            next_confirmation_state = RecoveryConfirmationState.UNCONFIRMED
            prepared_at = None
            expires_at = None
            snapshot_fingerprint = None
            next_counts = RecoveryProgressCounts()

        return replace(
            self,
            binding=next_binding,
            epoch=decision.next_epoch,
            state=RecoveryRunState.PENDING,
            progress_seq=self.progress_seq + 1,
            phase=next_phase,
            counts=next_counts,
            heartbeat_at=requested_at,
            started_at=requested_at,
            updated_at=requested_at,
            active_elapsed_ms=0,
            active_deadline_at=requested_at
            + timedelta(milliseconds=decision.attempt_budget_ms),
            attempt_budget_ms=decision.attempt_budget_ms,
            cancel_requested_at=None,
            cancel_requested_by_actor_id=None,
            terminal_outcome=None,
            reason_code="recovery_resume_admitted",
            retryable=False,
            supersedes_epoch=self.epoch,
            superseded_by_epoch=None,
            confirmation_state=next_confirmation_state,
            prepared_at=prepared_at,
            expires_at=expires_at,
            snapshot_fingerprint=snapshot_fingerprint,
            resume_requested_at=requested_at,
            resume_requested_by_actor_id=requested_by,
            resume_audit_reason=audit_reason,
            physical_truth=None,
        )

    def take_over_lease(
        self,
        *,
        decision: RecoveryLeaseTakeoverDecision,
        requested_at: datetime,
        requested_by_actor_id: str,
        reason: str | None,
    ) -> RecoveryRunStatus:
        """Fence an expired running attempt into the next physical epoch."""

        if self.state is not RecoveryRunState.RUNNING:
            raise ValueError("only a running recovery accepts lease takeover")
        if self.cancel_requested_at is not None:
            raise RecoveryResumeRejected(
                code="recovery_cancel_pending",
                run_id=self.run_id,
                epoch=self.epoch,
            )
        if not decision.admitted or decision.next_epoch != self.epoch + 1:
            raise RecoveryResumeRejected(
                code=decision.reason_code,
                run_id=self.run_id,
                epoch=self.epoch,
            )
        requested_at = _require_aware(requested_at, field="requested_at")
        requested_by = _require_non_empty(
            requested_by_actor_id,
            field="requested_by_actor_id",
        )
        audit_reason = normalize_recovery_audit_reason(reason)
        return replace(
            self,
            epoch=decision.next_epoch,
            progress_seq=self.progress_seq + 1,
            heartbeat_at=requested_at,
            started_at=requested_at,
            updated_at=requested_at,
            active_elapsed_ms=0,
            active_deadline_at=requested_at
            + timedelta(milliseconds=decision.attempt_budget_ms),
            cumulative_active_ms=decision.charged_cumulative_ms,
            attempt_budget_ms=decision.attempt_budget_ms,
            reason_code="recovery_takeover_admitted",
            retryable=False,
            supersedes_epoch=self.epoch,
            superseded_by_epoch=None,
            resume_requested_at=requested_at,
            resume_requested_by_actor_id=requested_by,
            resume_audit_reason=audit_reason,
            physical_truth=None,
        )

    def _checkpoint_conflict(
        self,
        code: str,
        checkpoint: RecoveryCheckpoint,
    ) -> RecoveryCheckpointConflict:
        return RecoveryCheckpointConflict(
            code=code,
            run_id=self.run_id,
            expected_epoch=checkpoint.epoch,
            actual_epoch=self.epoch,
            expected_progress_seq=checkpoint.expected_progress_seq,
            actual_progress_seq=self.progress_seq,
        )


@dataclass(frozen=True, slots=True)
class RecoveryTerminalAttempt:
    """Persisted terminal facts used to decide an explicit same-run resume."""

    run_id: str
    epoch: int
    outcome: RecoveryTerminalOutcome
    reason_code: str
    retryable: bool
    active_elapsed_ms: int
    cumulative_active_ms: int

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "run_id", _require_non_empty(self.run_id, field="run_id")
        )
        epoch = int(self.epoch)
        if epoch < 1:
            raise ValueError("epoch must be at least 1")
        object.__setattr__(self, "epoch", epoch)
        object.__setattr__(self, "outcome", RecoveryTerminalOutcome(self.outcome))
        object.__setattr__(
            self,
            "reason_code",
            _require_non_empty(self.reason_code, field="reason_code"),
        )
        if not isinstance(self.retryable, bool):
            raise TypeError("retryable must be bool")
        object.__setattr__(
            self,
            "active_elapsed_ms",
            _require_non_negative(self.active_elapsed_ms, field="active_elapsed_ms"),
        )
        object.__setattr__(
            self,
            "cumulative_active_ms",
            _require_non_negative(
                self.cumulative_active_ms,
                field="cumulative_active_ms",
            ),
        )
        if self.active_elapsed_ms > self.cumulative_active_ms:
            raise ValueError("active_elapsed_ms cannot exceed cumulative_active_ms")


@dataclass(frozen=True, slots=True)
class RecoveryResumeDecision:
    """Deterministic admission result for one explicit resume request."""

    admitted: bool
    reason_code: str
    run_id: str
    previous_epoch: int
    next_epoch: int | None
    attempt_budget_ms: int
    remaining_cumulative_ms: int


@dataclass(frozen=True, slots=True)
class RecoveryResumePolicy:
    """Enforce the closed terminal matrix and persisted active-time budgets."""

    attempt_budget_ms: int = DEFAULT_RECOVERY_ATTEMPT_BUDGET_MS
    cumulative_budget_ms: int = MAX_RECOVERY_CUMULATIVE_BUDGET_MS

    def __post_init__(self) -> None:
        attempt_budget_ms = _require_non_negative(
            self.attempt_budget_ms,
            field="attempt_budget_ms",
        )
        cumulative_budget_ms = _require_non_negative(
            self.cumulative_budget_ms,
            field="cumulative_budget_ms",
        )
        if attempt_budget_ms == 0:
            raise ValueError("attempt_budget_ms must be positive")
        if cumulative_budget_ms == 0:
            raise ValueError("cumulative_budget_ms must be positive")
        if attempt_budget_ms > cumulative_budget_ms:
            raise ValueError("attempt_budget_ms cannot exceed cumulative_budget_ms")
        if cumulative_budget_ms > MAX_RECOVERY_CUMULATIVE_BUDGET_MS:
            raise ValueError(
                "cumulative_budget_ms cannot exceed the 15-minute hard cap"
            )
        object.__setattr__(self, "attempt_budget_ms", attempt_budget_ms)
        object.__setattr__(self, "cumulative_budget_ms", cumulative_budget_ms)

    def evaluate(self, attempt: RecoveryTerminalAttempt) -> RecoveryResumeDecision:
        """Return a fail-closed decision without mutating the terminal attempt."""

        if not isinstance(attempt, RecoveryTerminalAttempt):
            raise TypeError("attempt must be RecoveryTerminalAttempt")

        remaining_ms = max(
            0,
            self.cumulative_budget_ms - attempt.cumulative_active_ms,
        )

        denial_reason: str | None = None
        if attempt.outcome is RecoveryTerminalOutcome.SUCCESS:
            denial_reason = "recovery_success_not_resumable"
        elif attempt.outcome is RecoveryTerminalOutcome.TIMEOUT:
            denial_reason = "recovery_timeout_not_resumable"
        elif (
            attempt.outcome is RecoveryTerminalOutcome.FAILED and not attempt.retryable
        ):
            denial_reason = "recovery_failure_not_retryable"
        elif remaining_ms == 0:
            denial_reason = "recovery_cumulative_budget_exhausted"

        if denial_reason is not None:
            return RecoveryResumeDecision(
                admitted=False,
                reason_code=denial_reason,
                run_id=attempt.run_id,
                previous_epoch=attempt.epoch,
                next_epoch=None,
                attempt_budget_ms=0,
                remaining_cumulative_ms=remaining_ms,
            )

        return RecoveryResumeDecision(
            admitted=True,
            reason_code="recovery_resume_admitted",
            run_id=attempt.run_id,
            previous_epoch=attempt.epoch,
            next_epoch=attempt.epoch + 1,
            attempt_budget_ms=min(self.attempt_budget_ms, remaining_ms),
            remaining_cumulative_ms=remaining_ms,
        )


@dataclass(frozen=True, slots=True)
class RecoveryLeaseTakeoverDecision:
    admitted: bool
    reason_code: str
    run_id: str
    previous_epoch: int
    next_epoch: int | None
    lease_expires_at: datetime
    charged_cumulative_ms: int
    attempt_budget_ms: int


@dataclass(frozen=True, slots=True)
class RecoveryLeaseTakeoverPolicy:
    """Fail-closed explicit takeover policy for an abandoned active attempt."""

    lease_duration_ms: int = RECOVERY_WORKER_LEASE_MS
    attempt_budget_ms: int = DEFAULT_RECOVERY_ATTEMPT_BUDGET_MS
    cumulative_budget_ms: int = MAX_RECOVERY_CUMULATIVE_BUDGET_MS

    def __post_init__(self) -> None:
        for field in (
            "lease_duration_ms",
            "attempt_budget_ms",
            "cumulative_budget_ms",
        ):
            value = _require_non_negative(getattr(self, field), field=field)
            if value == 0:
                raise ValueError(f"{field} must be positive")
            object.__setattr__(self, field, value)
        if self.attempt_budget_ms > self.cumulative_budget_ms:
            raise ValueError("attempt_budget_ms cannot exceed cumulative_budget_ms")
        if self.cumulative_budget_ms > MAX_RECOVERY_CUMULATIVE_BUDGET_MS:
            raise ValueError(
                "cumulative_budget_ms cannot exceed the 15-minute hard cap"
            )

    def evaluate(
        self,
        status: RecoveryRunStatus,
        *,
        now: datetime,
    ) -> RecoveryLeaseTakeoverDecision:
        if not isinstance(status, RecoveryRunStatus):
            raise TypeError("status must be RecoveryRunStatus")
        now = _require_aware(now, field="now")
        lease_expires_at = status.heartbeat_at + timedelta(
            milliseconds=self.lease_duration_ms
        )
        if status.state is RecoveryRunState.PENDING:
            reason_code = (
                "prepared_run_not_adoptable"
                if status.phase is RecoveryRunPhase.PREPARED
                else "recovery_dispatch_not_adoptable"
            )
            return RecoveryLeaseTakeoverDecision(
                admitted=False,
                reason_code=reason_code,
                run_id=status.run_id,
                previous_epoch=status.epoch,
                next_epoch=None,
                lease_expires_at=lease_expires_at,
                charged_cumulative_ms=status.cumulative_active_ms,
                attempt_budget_ms=0,
            )
        if status.state is not RecoveryRunState.RUNNING:
            return RecoveryLeaseTakeoverDecision(
                admitted=False,
                reason_code="recovery_attempt_not_active",
                run_id=status.run_id,
                previous_epoch=status.epoch,
                next_epoch=None,
                lease_expires_at=lease_expires_at,
                charged_cumulative_ms=status.cumulative_active_ms,
                attempt_budget_ms=0,
            )
        if now < lease_expires_at:
            return RecoveryLeaseTakeoverDecision(
                admitted=False,
                reason_code="worker_lease_active",
                run_id=status.run_id,
                previous_epoch=status.epoch,
                next_epoch=None,
                lease_expires_at=lease_expires_at,
                charged_cumulative_ms=status.cumulative_active_ms,
                attempt_budget_ms=0,
            )

        charge_through = min(lease_expires_at, status.active_deadline_at)
        crash_charge_ms = max(
            0,
            int((charge_through - status.heartbeat_at).total_seconds() * 1_000),
        )
        charged_active_ms = status.active_elapsed_ms + crash_charge_ms
        previous_attempts_ms = status.cumulative_active_ms - status.active_elapsed_ms
        charged_cumulative_ms = previous_attempts_ms + charged_active_ms

        denial_reason: str | None = None
        if charge_through >= status.active_deadline_at:
            denial_reason = "recovery_attempt_budget_exhausted"
        elif charged_cumulative_ms >= self.cumulative_budget_ms:
            denial_reason = "recovery_cumulative_budget_exhausted"

        remaining_ms = max(0, self.cumulative_budget_ms - charged_cumulative_ms)
        if denial_reason is not None:
            return RecoveryLeaseTakeoverDecision(
                admitted=False,
                reason_code=denial_reason,
                run_id=status.run_id,
                previous_epoch=status.epoch,
                next_epoch=None,
                lease_expires_at=lease_expires_at,
                charged_cumulative_ms=charged_cumulative_ms,
                attempt_budget_ms=0,
            )
        return RecoveryLeaseTakeoverDecision(
            admitted=True,
            reason_code="recovery_takeover_admitted",
            run_id=status.run_id,
            previous_epoch=status.epoch,
            next_epoch=status.epoch + 1,
            lease_expires_at=lease_expires_at,
            charged_cumulative_ms=charged_cumulative_ms,
            attempt_budget_ms=min(self.attempt_budget_ms, remaining_ms),
        )


@runtime_checkable
class RecoveryRunStore(Protocol):
    """Atomic edition-owned storage port used by bounded control calls."""

    def get_status(self, *, run_id: str) -> RecoveryRunStatus | None: ...

    def request_cancel(
        self,
        *,
        run_id: str,
        expected_epoch: int,
        requested_at: datetime,
        requested_by_actor_id: str,
        reason: str | None = None,
    ) -> RecoveryRunStatus: ...


@runtime_checkable
class RecoveryPreparationStore(RecoveryRunStore, Protocol):
    """Atomic whole-lifecycle store for the single global preparation slot.

    ``admit_prepared_start`` must observe its own current time inside the same
    transaction/CAS and reject at ``expires_at`` even when the caller supplied
    an earlier confirmation timestamp.
    """

    def admit_preparation(
        self,
        command: RecoveryPreparationCommand,
    ) -> tuple[RecoveryRunStatus, bool]: ...

    def mark_preparing(
        self,
        *,
        run_id: str,
        epoch: int,
        at: datetime,
    ) -> RecoveryRunStatus: ...

    def mark_prepared(
        self,
        *,
        run_id: str,
        epoch: int,
        expected_progress_seq: int,
        prepared: RecoveryPreparedResult,
    ) -> RecoveryRunStatus: ...

    def cancel_prepared(
        self,
        *,
        run_id: str,
        expected_epoch: int,
        requested_at: datetime,
        requested_by_actor_id: str,
        reason: str | None,
    ) -> RecoveryRunStatus: ...

    def admit_prepared_start(
        self,
        command: RecoveryStartCommand,
    ) -> tuple[RecoveryRunStatus, bool]: ...


@runtime_checkable
class RecoveryWorkerRunStore(RecoveryRunStore, Protocol):
    """Atomic worker-side extension implemented by an edition's durable store."""

    def mark_running(
        self,
        *,
        run_id: str,
        epoch: int,
        at: datetime,
    ) -> RecoveryRunStatus: ...

    def compare_and_set_checkpoint(
        self,
        checkpoint: RecoveryCheckpoint,
    ) -> RecoveryRunStatus: ...

    def complete(
        self,
        *,
        run_id: str,
        epoch: int,
        expected_progress_seq: int,
        completed_at: datetime,
        active_elapsed_ms: int,
        result: RecoveryWorkerResult,
    ) -> RecoveryRunStatus: ...


@runtime_checkable
class RecoveryExplicitResumeStore(RecoveryRunStore, Protocol):
    """Atomic store port for terminal resume or expired-lease takeover."""

    def admit_explicit_resume(
        self,
        *,
        run_id: str,
        expected_epoch: int,
        requested_at: datetime,
        requested_by_actor_id: str,
        reason: str | None = None,
    ) -> tuple[RecoveryRunStatus, bool]: ...


@runtime_checkable
class RecoveryWorkerDispatcher(Protocol):
    """Dispatch must only enqueue owned work; it must never run native work inline."""

    def dispatch(
        self,
        *,
        run_id: str,
        epoch: int,
        attempt_id: str,
        kind: RecoveryDispatchKind,
    ) -> None: ...


@dataclass(frozen=True, slots=True)
class RecoveryControlPlane:
    """Transport-free bounded start/status/cancel use case."""

    store: RecoveryRunStore
    dispatcher: RecoveryWorkerDispatcher

    def __post_init__(self) -> None:
        if not isinstance(self.store, RecoveryRunStore):
            raise TypeError("store must implement RecoveryRunStore")
        if not isinstance(self.dispatcher, RecoveryWorkerDispatcher):
            raise TypeError("dispatcher must implement RecoveryWorkerDispatcher")

    def _dispatch(
        self, status: RecoveryRunStatus, *, kind: RecoveryDispatchKind
    ) -> None:
        self.dispatcher.dispatch(
            run_id=status.run_id,
            epoch=status.epoch,
            attempt_id=status.attempt_id,
            kind=kind,
        )

    def prepare(self, command: RecoveryPreparationCommand) -> RecoveryRunStatus:
        if not isinstance(command, RecoveryPreparationCommand):
            raise TypeError("command must be RecoveryPreparationCommand")
        if not isinstance(self.store, RecoveryPreparationStore):
            raise TypeError("store must implement RecoveryPreparationStore")
        status, created = self.store.admit_preparation(command)
        if created or (
            status.run_id == command.binding.run_id
            and status.binding == command.binding
            and status.state is RecoveryRunState.PENDING
            and status.phase is RecoveryRunPhase.QUEUED
        ):
            self._dispatch(status, kind=RecoveryDispatchKind.PREPARATION)
        return status

    def start(self, command: RecoveryStartCommand) -> RecoveryRunStatus:
        if not isinstance(command, RecoveryStartCommand):
            raise TypeError("command must be RecoveryStartCommand")
        if not isinstance(self.store, RecoveryPreparationStore):
            raise TypeError("store must implement RecoveryPreparationStore")
        status, transitioned = self.store.admit_prepared_start(command)
        if transitioned or (
            status.binding.confirmation_fingerprint
            == command.binding.confirmation_fingerprint
            and status.state is RecoveryRunState.PENDING
            and status.phase is RecoveryRunPhase.CONFIRMED
        ):
            self._dispatch(status, kind=RecoveryDispatchKind.RECOVERY)
        return status

    def status(self, run_id: str) -> RecoveryRunStatus:
        normalized = _require_non_empty(run_id, field="run_id")
        status = self.store.get_status(run_id=normalized)
        if status is None:
            raise RecoveryRunNotFound(run_id=normalized)
        return status

    def cancel(
        self,
        *,
        run_id: str,
        expected_epoch: int,
        requested_at: datetime,
        requested_by_actor_id: str,
        reason: str | None = None,
    ) -> RecoveryRunStatus:
        epoch = int(expected_epoch)
        if epoch < 1:
            raise ValueError("expected_epoch must be at least 1")
        normalized_run_id = _require_non_empty(run_id, field="run_id")
        normalized_at = _require_aware(requested_at, field="requested_at")
        requested_by = _require_non_empty(
            requested_by_actor_id,
            field="requested_by_actor_id",
        )
        normalized_reason = normalize_recovery_audit_reason(reason)
        current = self.status(normalized_run_id)
        if (
            current.state is RecoveryRunState.PENDING
            and current.phase is RecoveryRunPhase.PREPARED
        ):
            if not isinstance(self.store, RecoveryPreparationStore):
                raise TypeError("store must implement RecoveryPreparationStore")
            return self.store.cancel_prepared(
                run_id=normalized_run_id,
                expected_epoch=epoch,
                requested_at=normalized_at,
                requested_by_actor_id=requested_by,
                reason=normalized_reason,
            )
        return self.store.request_cancel(
            run_id=normalized_run_id,
            expected_epoch=epoch,
            requested_at=normalized_at,
            requested_by_actor_id=requested_by,
            reason=normalized_reason,
        )

    def resume(
        self,
        *,
        run_id: str,
        expected_epoch: int,
        requested_at: datetime,
        requested_by_actor_id: str,
        reason: str | None = None,
    ) -> RecoveryRunStatus:
        if not isinstance(self.store, RecoveryExplicitResumeStore):
            raise TypeError("store must implement RecoveryExplicitResumeStore")
        epoch = int(expected_epoch)
        if epoch < 1:
            raise ValueError("expected_epoch must be at least 1")
        resumed, transitioned = self.store.admit_explicit_resume(
            run_id=_require_non_empty(run_id, field="run_id"),
            expected_epoch=epoch,
            requested_at=_require_aware(requested_at, field="requested_at"),
            requested_by_actor_id=_require_non_empty(
                requested_by_actor_id,
                field="requested_by_actor_id",
            ),
            reason=normalize_recovery_audit_reason(reason),
        )
        kind = (
            RecoveryDispatchKind.PREPARATION
            if resumed.phase
            in {RecoveryRunPhase.QUEUED, RecoveryRunPhase.PREPARING}
            else RecoveryDispatchKind.RECOVERY
        )
        if transitioned or (
            resumed.state is RecoveryRunState.PENDING
            and resumed.phase in {
                RecoveryRunPhase.QUEUED,
                RecoveryRunPhase.CONFIRMED,
            }
        ):
            self._dispatch(resumed, kind=kind)
        return resumed


_RECOVERY_CONTROL_PLANE_RUNTIME_KEY = "kg.global_discovery_recovery.control_plane"


class RecoveryControlPlaneUnavailable(RuntimeError):
    """No edition-owned durable recovery control plane is composed."""

    code = "recovery_control_plane_unavailable"

    def __init__(self) -> None:
        super().__init__(self.code)


def register_recovery_control_plane(control: RecoveryControlPlane) -> None:
    """Register the edition-composed control plane in the active runtime."""

    if not isinstance(control, RecoveryControlPlane):
        raise TypeError("control must be RecoveryControlPlane")
    from okto_pulse.core.runtime_context import register_runtime_value

    register_runtime_value(_RECOVERY_CONTROL_PLANE_RUNTIME_KEY, control)


def reset_recovery_control_plane() -> None:
    """Remove the active recovery control plane during ordered teardown/tests."""

    from okto_pulse.core.runtime_context import reset_runtime_values

    reset_runtime_values(_RECOVERY_CONTROL_PLANE_RUNTIME_KEY)


def resolve_recovery_control_plane() -> RecoveryControlPlane:
    """Resolve the edition control plane, failing closed when not composed."""

    from okto_pulse.core.runtime_context import resolve_runtime_value

    control = resolve_runtime_value(_RECOVERY_CONTROL_PLANE_RUNTIME_KEY)
    if not isinstance(control, RecoveryControlPlane):
        raise RecoveryControlPlaneUnavailable()
    return control


@dataclass(frozen=True, slots=True)
class RecoveryCheckpoint:
    run_id: str
    epoch: int
    expected_progress_seq: int
    phase: RecoveryRunPhase
    heartbeat_at: datetime
    active_elapsed_ms: int
    counts: RecoveryProgressCounts

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "run_id", _require_non_empty(self.run_id, field="run_id")
        )
        epoch = int(self.epoch)
        if epoch < 1:
            raise ValueError("epoch must be at least 1")
        object.__setattr__(self, "epoch", epoch)
        object.__setattr__(
            self,
            "expected_progress_seq",
            _require_non_negative(
                self.expected_progress_seq,
                field="expected_progress_seq",
            ),
        )
        phase = RecoveryRunPhase(self.phase)
        if phase not in {RecoveryRunPhase.PREPARING, RecoveryRunPhase.CUTOVER}:
            raise ValueError("checkpoint phase must be preparing or cutover")
        object.__setattr__(self, "phase", phase)
        object.__setattr__(
            self,
            "heartbeat_at",
            _require_aware(self.heartbeat_at, field="heartbeat_at"),
        )
        object.__setattr__(
            self,
            "active_elapsed_ms",
            _require_non_negative(self.active_elapsed_ms, field="active_elapsed_ms"),
        )
        if not isinstance(self.counts, RecoveryProgressCounts):
            raise TypeError("counts must be RecoveryProgressCounts")


@dataclass(frozen=True, slots=True)
class RecoveryAttempt:
    run_id: str
    epoch: int
    progress_seq: int
    phase: RecoveryRunPhase
    heartbeat_at: datetime
    active_elapsed_ms: int
    counts: RecoveryProgressCounts
    attempt_budget_ms: int = DEFAULT_RECOVERY_ATTEMPT_BUDGET_MS
    cumulative_active_ms: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "run_id", _require_non_empty(self.run_id, field="run_id")
        )
        epoch = int(self.epoch)
        if epoch < 1:
            raise ValueError("epoch must be at least 1")
        object.__setattr__(self, "epoch", epoch)
        object.__setattr__(
            self,
            "progress_seq",
            _require_non_negative(self.progress_seq, field="progress_seq"),
        )
        phase = RecoveryRunPhase(self.phase)
        if phase not in {RecoveryRunPhase.PREPARING, RecoveryRunPhase.CUTOVER}:
            raise ValueError("attempt phase must be preparing or cutover")
        object.__setattr__(self, "phase", phase)
        object.__setattr__(
            self,
            "heartbeat_at",
            _require_aware(self.heartbeat_at, field="heartbeat_at"),
        )
        object.__setattr__(
            self,
            "active_elapsed_ms",
            _require_non_negative(self.active_elapsed_ms, field="active_elapsed_ms"),
        )
        attempt_budget_ms = _require_non_negative(
            self.attempt_budget_ms,
            field="attempt_budget_ms",
        )
        if attempt_budget_ms == 0 or attempt_budget_ms > DEFAULT_RECOVERY_ATTEMPT_BUDGET_MS:
            raise ValueError("attempt_budget_ms must be within 1..600000")
        object.__setattr__(self, "attempt_budget_ms", attempt_budget_ms)
        if self.active_elapsed_ms > attempt_budget_ms:
            raise ValueError("active_elapsed_ms exceeds attempt budget")
        cumulative_active_ms = (
            self.active_elapsed_ms
            if self.cumulative_active_ms is None
            else _require_non_negative(
                self.cumulative_active_ms,
                field="cumulative_active_ms",
            )
        )
        if cumulative_active_ms < self.active_elapsed_ms:
            raise ValueError("cumulative_active_ms cannot precede active_elapsed_ms")
        if cumulative_active_ms > MAX_RECOVERY_CUMULATIVE_BUDGET_MS:
            raise ValueError("cumulative_active_ms exceeds cumulative budget")
        object.__setattr__(self, "cumulative_active_ms", cumulative_active_ms)
        if not isinstance(self.counts, RecoveryProgressCounts):
            raise TypeError("counts must be RecoveryProgressCounts")

    def advance_checkpoint(self, checkpoint: RecoveryCheckpoint) -> RecoveryAttempt:
        """Return the next snapshot or raise a typed conflict without mutation."""

        if checkpoint.run_id != self.run_id:
            raise RecoveryCheckpointConflict(
                code="recovery_run_conflict",
                run_id=self.run_id,
                expected_epoch=checkpoint.epoch,
                actual_epoch=self.epoch,
                expected_progress_seq=checkpoint.expected_progress_seq,
                actual_progress_seq=self.progress_seq,
            )
        if checkpoint.epoch != self.epoch:
            raise RecoveryCheckpointConflict(
                code="recovery_epoch_conflict",
                run_id=self.run_id,
                expected_epoch=checkpoint.epoch,
                actual_epoch=self.epoch,
                expected_progress_seq=checkpoint.expected_progress_seq,
                actual_progress_seq=self.progress_seq,
            )
        if checkpoint.expected_progress_seq != self.progress_seq:
            raise RecoveryCheckpointConflict(
                code="recovery_progress_conflict",
                run_id=self.run_id,
                expected_epoch=checkpoint.epoch,
                actual_epoch=self.epoch,
                expected_progress_seq=checkpoint.expected_progress_seq,
                actual_progress_seq=self.progress_seq,
            )
        if checkpoint.active_elapsed_ms < self.active_elapsed_ms:
            raise ValueError("active_elapsed_ms cannot move backwards")
        if checkpoint.heartbeat_at < self.heartbeat_at:
            raise ValueError("heartbeat_at cannot move backwards")
        if checkpoint.active_elapsed_ms > self.attempt_budget_ms:
            raise ValueError("active_elapsed_ms exceeds attempt budget")
        _validate_progress_transition(self.counts, checkpoint.counts)
        assert self.cumulative_active_ms is not None
        previous_attempts_ms = self.cumulative_active_ms - self.active_elapsed_ms
        cumulative_active_ms = previous_attempts_ms + checkpoint.active_elapsed_ms
        if cumulative_active_ms > MAX_RECOVERY_CUMULATIVE_BUDGET_MS:
            raise ValueError("cumulative_active_ms exceeds cumulative budget")
        return replace(
            self,
            progress_seq=self.progress_seq + 1,
            phase=checkpoint.phase,
            heartbeat_at=checkpoint.heartbeat_at,
            active_elapsed_ms=checkpoint.active_elapsed_ms,
            cumulative_active_ms=cumulative_active_ms,
            counts=checkpoint.counts,
        )


__all__ = [
    "DEFAULT_RECOVERY_ATTEMPT_BUDGET_MS",
    "MAX_RECOVERY_CUMULATIVE_BUDGET_MS",
    "RECOVERY_HEARTBEAT_INTERVAL_MS",
    "RECOVERY_PREPARED_TTL_SECONDS",
    "RECOVERY_WORKER_LEASE_MS",
    "GLOBAL_RECOVERY_SLOT_ID",
    "GLOBAL_RECOVERY_STATUS_TOOL",
    "RecoveryAttempt",
    "RecoveryAuditReasonInvalid",
    "RecoveryBindingConflict",
    "RecoveryCheckpoint",
    "RecoveryCheckpointConflict",
    "RecoveryControlPlane",
    "RecoveryControlPlaneUnavailable",
    "RecoveryConfirmationState",
    "RecoveryDispatchClaimConflict",
    "RecoveryDispatchKind",
    "RecoveryExplicitResumeStore",
    "RecoveryInProgress",
    "RecoveryLeaseTakeoverDecision",
    "RecoveryLeaseTakeoverPolicy",
    "RecoveryProgressCounts",
    "RecoveryProgressInvariantViolation",
    "RecoveryPhaseIncoherent",
    "RecoveryPhysicalTruth",
    "RecoveryPreparationCommand",
    "RecoveryPreparationStore",
    "RecoveryPreparedResult",
    "RecoveryResumeDecision",
    "RecoveryResumePolicy",
    "RecoveryResumeRejected",
    "RecoveryRunBinding",
    "RecoveryRunNotFound",
    "RecoveryRunState",
    "RecoveryRunPhase",
    "RecoveryRunStatus",
    "RecoveryRunStore",
    "RecoveryStartCommand",
    "RecoverySlotOwnership",
    "RecoveryTerminalAttempt",
    "RecoveryTerminalOutcome",
    "RecoveryWorkerDispatcher",
    "RecoveryWorkerRunStore",
    "RecoveryWorkerResult",
    "RecoveryTransitionEvent",
    "RecoveryTransitionObserver",
    "normalize_recovery_audit_reason",
    "recovery_attempt_id",
    "register_recovery_control_plane",
    "reset_recovery_control_plane",
    "resolve_recovery_control_plane",
]
