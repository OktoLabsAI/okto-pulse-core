"""Safe write lifecycle for LadybugDB graphs (KG-01, FR6, contract api_1c9d19e1).

Every bulk write or shutdown-time operation against `graph.lbug` or
`discovery.lbug` MUST go through this wrapper AFTER the caller has
acquired the single-writer lock (FR5, KG-01.3). The wrapper is the
canonical place where checkpoint, flush, fsync and close/reopen probes
are applied in a deterministic order so KG-02 rebuild and normal writes
share the same correctness invariants.

The wrapper is pure-Python and takes the storage adapter as an injected
callable interface, so this module remains decoupled from the LadybugDB
runtime: tests pass a fake adapter, production passes the real one.

Contract surface (api_1c9d19e1):

    request   = {board_id, graph_type, operation, owner_token,
                 mutation_ref, required_steps}
    response  = {status, applied_steps, failed_step, health_state_after,
                 correlation_id}
    errors    = owner_token_required | safe_lifecycle_failed |
                boundary_violation

`health_state_after` is one of `healthy|at_risk|backpressure|recovery_needed`
(four states; the wrapper never returns `quarantined` — that's the
quarantine service's job in KG-01.4).

The lifecycle is the same for KG-01 normal writes and the KG-02 admin
rebuild lane, but the latter passes `admin_lane=True` to the lock at
acquire time. The wrapper does NOT acquire the lock — it only validates
the owner_token presented to it.
"""

from __future__ import annotations

import logging
import threading
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Iterable

from okto_pulse.core.kg.interfaces.graph_lifecycle import GraphLifecycleStepResult

logger = logging.getLogger("okto_pulse.kg.safe_write_lifecycle")


# --- Observability counter (OR or_a0162af4) -----------------------------------
#
# kg_safe_write_lifecycle_total labels (canonical):
#   board_id, operation, graph_type, outcome, failed_step_bucket
# outcome in {"applied", "blocked", "failed", "boundary_violation",
#             "owner_token_required"}
# failed_step_bucket in {"none", "checkpoint", "flush", "fsync",
#                        "close_reopen_probe", "boundary"}
# Bucket is "none" for applied/blocked, "boundary" for boundary_violation /
# owner_token_required, and the failing step name for failed.

_LIFECYCLE_COUNTER_LABELS = (
    "board_id",
    "operation",
    "graph_type",
    "outcome",
    "failed_step_bucket",
)

_LifecycleCounterKey = tuple[str, str, str, str, str]
_lifecycle_counter: dict[_LifecycleCounterKey, int] = {}
_lifecycle_counter_lock = threading.Lock()


def _bump_lifecycle_counter(
    *,
    board_id: str,
    operation: str,
    graph_type: str,
    outcome: str,
    failed_step_bucket: str,
) -> None:
    key: _LifecycleCounterKey = (
        board_id,
        operation,
        graph_type,
        outcome,
        failed_step_bucket,
    )
    with _lifecycle_counter_lock:
        _lifecycle_counter[key] = _lifecycle_counter.get(key, 0) + 1


def get_lifecycle_counter(
    board_id: str,
    outcome: str,
    *,
    operation: str | None = None,
    graph_type: str | None = None,
    failed_step_bucket: str | None = None,
) -> int:
    """Read kg_safe_write_lifecycle_total slice. Exact-match filters."""
    with _lifecycle_counter_lock:
        total = 0
        for (b, op, gt, out, fsb), value in _lifecycle_counter.items():
            if b != board_id or out != outcome:
                continue
            if operation is not None and op != operation:
                continue
            if graph_type is not None and gt != graph_type:
                continue
            if failed_step_bucket is not None and fsb != failed_step_bucket:
                continue
            total += value
        return total


def get_lifecycle_counter_samples() -> list[dict[str, Any]]:
    """Snapshot of every (label tuple, count). Shape matches the OR."""
    with _lifecycle_counter_lock:
        return [
            {
                "board_id": b,
                "operation": op,
                "graph_type": gt,
                "outcome": out,
                "failed_step_bucket": fsb,
                "count": value,
            }
            for (b, op, gt, out, fsb), value in _lifecycle_counter.items()
        ]


def get_lifecycle_counter_labels() -> tuple[str, ...]:
    return _LIFECYCLE_COUNTER_LABELS


def reset_lifecycle_counter() -> None:
    """Test hook."""
    with _lifecycle_counter_lock:
        _lifecycle_counter.clear()


# Canonical step set per contract. Order is fixed by FR6: checkpoint
# must precede flush, flush must precede fsync; close_reopen_probe is
# the tail-end validation that the on-disk state is readable.
STEP_CHECKPOINT = "checkpoint"
STEP_FLUSH = "flush"
STEP_FSYNC = "fsync"
STEP_CLOSE_REOPEN_PROBE = "close_reopen_probe"

CANONICAL_STEP_ORDER: tuple[str, ...] = (
    STEP_CHECKPOINT,
    STEP_FLUSH,
    STEP_FSYNC,
    STEP_CLOSE_REOPEN_PROBE,
)

# Default sequence applied when the caller passes an empty
# `required_steps`. Every bulk write benefits from this baseline; the
# caller opts in to a custom subset only when it knows better.
DEFAULT_REQUIRED_STEPS: tuple[str, ...] = CANONICAL_STEP_ORDER

CANONICAL_GRAPH_TYPES = frozenset({"board_graph", "global_discovery"})

# health_state_after per contract — quarantined is NOT in this set.
HEALTH_STATE_AFTER_VALUES = frozenset(
    {"healthy", "at_risk", "backpressure", "recovery_needed"}
)


class SafeWriteLifecycleStatus(str, Enum):
    """Three-valued status per contract api_1c9d19e1."""

    APPLIED = "applied"
    BLOCKED = "blocked"
    FAILED = "failed"


class SafeWriteLifecycleErrorCode(str, Enum):
    """Typed errors per contract api_1c9d19e1 response_errors."""

    OWNER_TOKEN_REQUIRED = "owner_token_required"
    SAFE_LIFECYCLE_FAILED = "safe_lifecycle_failed"
    BOUNDARY_VIOLATION = "boundary_violation"


class SafeWriteLifecycleError(Exception):
    """Raised when the lifecycle cannot even decide a status.

    The non-error path returns ``SafeWriteLifecycleResponse(status=FAILED,
    failed_step=...)`` with the same shape as ``APPLIED``. Exceptions are
    reserved for boundary violations (caller bug) and owner-token
    requirements (caller did not acquire the lock).
    """

    def __init__(
        self,
        code: SafeWriteLifecycleErrorCode,
        *,
        correlation_id: str,
        retryable: bool,
        reason: str,
    ) -> None:
        super().__init__(f"{code.value}: {reason}")
        self.code = code
        self.correlation_id = correlation_id
        self.retryable = retryable
        self.reason = reason


@dataclass(frozen=True, slots=True)
class SafeWriteLifecycleResponse:
    """Frozen contract-shaped response per api_1c9d19e1 success body."""

    status: SafeWriteLifecycleStatus
    applied_steps: tuple[str, ...]
    failed_step: str | None
    health_state_after: str
    correlation_id: str


LifecycleStepResult = GraphLifecycleStepResult


# A storage adapter is a callable that knows how to execute one of the
# canonical lifecycle steps on a (board_id, graph_type) pair. Tests
# wire fakes; production wires the LadybugDB driver. Keeping the
# adapter as a callable rather than a class keeps the wrapper testable
# without subclassing the entire driver.
StorageStepAdapter = Callable[[str, str, str], LifecycleStepResult]


@dataclass(frozen=True, slots=True)
class LockOwnerProbe:
    """Adapter signature used to validate the presented owner_token.

    The lifecycle wrapper does not own lock state; it asks the probe
    whether the (board_id, owner_token) pair is currently the active
    lock holder. Tests pass a fake; production passes a callable that
    consults ``KGSingleWriterLock.inspect``.
    """

    is_active_owner: Callable[[str, str], bool]


@dataclass(frozen=True, slots=True)
class HealthProbe:
    """Adapter that classifies the post-lifecycle health state.

    Returns one of `healthy|at_risk|backpressure|recovery_needed`
    (NEVER `quarantined`). When the caller does not provide a probe,
    the wrapper falls back to a conservative default (`at_risk`) per
    BR br_2a8cdfdc — never optimistically reports healthy.
    """

    classify: Callable[[str, str, SafeWriteLifecycleStatus, str | None], str]


def _conservative_health_classifier(
    board_id: str,
    graph_type: str,
    status: SafeWriteLifecycleStatus,
    failed_step: str | None,
) -> str:
    if status is SafeWriteLifecycleStatus.APPLIED:
        return "at_risk"  # conservative default until KG-01.5 sensors land
    if status is SafeWriteLifecycleStatus.BLOCKED:
        return "at_risk"
    return "recovery_needed"


class KGSafeWriteLifecycle:
    """Apply the safe write lifecycle around a LadybugDB mutation.

    The wrapper is stateless; it composes the supplied adapters and
    enforces the canonical step ordering. Typical usage:

        wrapper = KGSafeWriteLifecycle(
            step_adapter=ladybug_step_adapter,
            owner_probe=LockOwnerProbe(is_active_owner=single_writer_lock.is_owner),
            health_probe=HealthProbe(classify=health_classifier_callable),
        )
        response = wrapper.apply(
            board_id="b1",
            graph_type="board_graph",
            operation="bulk_consolidate",
            owner_token=acquisition.owner_token,
            mutation_ref="run-42",
            required_steps=["checkpoint", "flush", "fsync"],
        )
    """

    def __init__(
        self,
        *,
        step_adapter: StorageStepAdapter,
        owner_probe: LockOwnerProbe,
        health_probe: HealthProbe | None = None,
    ) -> None:
        self._step = step_adapter
        self._owner_probe = owner_probe
        self._health_probe = health_probe or HealthProbe(
            classify=_conservative_health_classifier
        )

    # --- public API --------------------------------------------------------

    def apply(
        self,
        *,
        board_id: str,
        graph_type: str,
        operation: str,
        owner_token: str | None,
        mutation_ref: str,
        required_steps: Iterable[str] | None = None,
    ) -> SafeWriteLifecycleResponse:
        """Run the lifecycle and return the contract-shaped response.

        Raises ``SafeWriteLifecycleError(owner_token_required)`` if the
        caller presented no token or a token that no longer matches an
        active lock holder. Raises ``boundary_violation`` for invalid
        graph_type or unknown step name. All other failures land as
        ``status=FAILED`` with the failing step.
        """
        # Local import to avoid a cycle between safe_write_lifecycle.py
        # and write_barrier.py (which is itself a sibling module under
        # okto_pulse.core.kg). The barrier is opt-in for callers; the
        # lifecycle wrapper always installs the guard so workers that
        # downstream require_write_token see an active guard during
        # step execution.
        from okto_pulse.core.kg.write_barrier import under_safe_write

        correlation_id = uuid.uuid4().hex

        if graph_type not in CANONICAL_GRAPH_TYPES:
            _bump_lifecycle_counter(
                board_id=board_id,
                operation=operation,
                graph_type=graph_type,
                outcome="boundary_violation",
                failed_step_bucket="boundary",
            )
            raise SafeWriteLifecycleError(
                SafeWriteLifecycleErrorCode.BOUNDARY_VIOLATION,
                correlation_id=correlation_id,
                retryable=False,
                reason=f"unknown_graph_type={graph_type}",
            )

        if not owner_token:
            _bump_lifecycle_counter(
                board_id=board_id,
                operation=operation,
                graph_type=graph_type,
                outcome="owner_token_required",
                failed_step_bucket="boundary",
            )
            raise SafeWriteLifecycleError(
                SafeWriteLifecycleErrorCode.OWNER_TOKEN_REQUIRED,
                correlation_id=correlation_id,
                retryable=False,
                reason="owner_token is empty",
            )
        if not self._owner_probe.is_active_owner(board_id, owner_token):
            _bump_lifecycle_counter(
                board_id=board_id,
                operation=operation,
                graph_type=graph_type,
                outcome="owner_token_required",
                failed_step_bucket="boundary",
            )
            raise SafeWriteLifecycleError(
                SafeWriteLifecycleErrorCode.OWNER_TOKEN_REQUIRED,
                correlation_id=correlation_id,
                retryable=False,
                reason="owner_token does not match active holder",
            )

        try:
            steps = self._validate_and_order_steps(required_steps, correlation_id)
        except SafeWriteLifecycleError:
            _bump_lifecycle_counter(
                board_id=board_id,
                operation=operation,
                graph_type=graph_type,
                outcome="boundary_violation",
                failed_step_bucket="boundary",
            )
            raise

        applied: list[str] = []
        with under_safe_write(board_id, owner_token, operation):
            for step in steps:
                try:
                    result = self._step(board_id, graph_type, step)
                except Exception as exc:
                    logger.error(
                        "kg.safe_lifecycle.step_exception board=%s graph=%s "
                        "step=%s mutation_ref=%s err=%s",
                        board_id, graph_type, step, mutation_ref, exc,
                    )
                    health = self._health_probe.classify(
                        board_id,
                        graph_type,
                        SafeWriteLifecycleStatus.FAILED,
                        step,
                    )
                    if health not in HEALTH_STATE_AFTER_VALUES:
                        health = "recovery_needed"
                    _bump_lifecycle_counter(
                        board_id=board_id,
                        operation=operation,
                        graph_type=graph_type,
                        outcome="failed",
                        failed_step_bucket=step,
                    )
                    return SafeWriteLifecycleResponse(
                        status=SafeWriteLifecycleStatus.FAILED,
                        applied_steps=tuple(applied),
                        failed_step=step,
                        health_state_after=health,
                        correlation_id=correlation_id,
                    )
                if not result.ok:
                    logger.warning(
                        "kg.safe_lifecycle.step_failed board=%s graph=%s "
                        "step=%s mutation_ref=%s detail=%s",
                        board_id, graph_type, step, mutation_ref, result.detail,
                    )
                    health = self._health_probe.classify(
                        board_id,
                        graph_type,
                        SafeWriteLifecycleStatus.FAILED,
                        step,
                    )
                    if health not in HEALTH_STATE_AFTER_VALUES:
                        health = "recovery_needed"
                    _bump_lifecycle_counter(
                        board_id=board_id,
                        operation=operation,
                        graph_type=graph_type,
                        outcome="failed",
                        failed_step_bucket=step,
                    )
                    return SafeWriteLifecycleResponse(
                        status=SafeWriteLifecycleStatus.FAILED,
                        applied_steps=tuple(applied),
                        failed_step=step,
                        health_state_after=health,
                        correlation_id=correlation_id,
                    )
                applied.append(step)

        health = self._health_probe.classify(
            board_id,
            graph_type,
            SafeWriteLifecycleStatus.APPLIED,
            None,
        )
        if health not in HEALTH_STATE_AFTER_VALUES:
            health = "at_risk"
        _bump_lifecycle_counter(
            board_id=board_id,
            operation=operation,
            graph_type=graph_type,
            outcome="applied",
            failed_step_bucket="none",
        )
        return SafeWriteLifecycleResponse(
            status=SafeWriteLifecycleStatus.APPLIED,
            applied_steps=tuple(applied),
            failed_step=None,
            health_state_after=health,
            correlation_id=correlation_id,
        )

    # --- internals ---------------------------------------------------------

    def _validate_and_order_steps(
        self,
        required_steps: Iterable[str] | None,
        correlation_id: str,
    ) -> tuple[str, ...]:
        if required_steps is None:
            return DEFAULT_REQUIRED_STEPS
        normalised = list(required_steps)
        if not normalised:
            return DEFAULT_REQUIRED_STEPS
        unknown = [s for s in normalised if s not in CANONICAL_STEP_ORDER]
        if unknown:
            raise SafeWriteLifecycleError(
                SafeWriteLifecycleErrorCode.BOUNDARY_VIOLATION,
                correlation_id=correlation_id,
                retryable=False,
                reason=f"unknown_steps={sorted(unknown)}",
            )
        # Always run in canonical order regardless of the order the
        # caller passed them in. Spec FR6 mandates this — checkpoint
        # before flush before fsync before close_reopen_probe.
        seen = set(normalised)
        return tuple(s for s in CANONICAL_STEP_ORDER if s in seen)


__all__ = [
    "CANONICAL_GRAPH_TYPES",
    "CANONICAL_STEP_ORDER",
    "DEFAULT_REQUIRED_STEPS",
    "HEALTH_STATE_AFTER_VALUES",
    "HealthProbe",
    "KGSafeWriteLifecycle",
    "LifecycleStepResult",
    "LockOwnerProbe",
    "SafeWriteLifecycleError",
    "SafeWriteLifecycleErrorCode",
    "SafeWriteLifecycleResponse",
    "SafeWriteLifecycleStatus",
    "STEP_CHECKPOINT",
    "STEP_CLOSE_REOPEN_PROBE",
    "STEP_FLUSH",
    "STEP_FSYNC",
    "StorageStepAdapter",
    "get_lifecycle_counter",
    "get_lifecycle_counter_labels",
    "get_lifecycle_counter_samples",
    "reset_lifecycle_counter",
]
