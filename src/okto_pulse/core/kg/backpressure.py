"""KG backpressure gate (Spec KG-01, FR4, contract api_a1ec2dcc).

Single entry point that every KG write intent MUST pass through BEFORE
the storage layer is touched. The gate decides — deterministically and
per board — whether an intent is accepted now, queued for later, or
rejected with retryable semantics.

The decision is one of three values, by contract:

    accepted            queued                 rejected_retryable

Semantics:

* `accepted` — there is headroom; the caller may proceed and write.
* `queued` — there is no immediate headroom but the gate accepted the
  intent into a bounded per-board FIFO. The caller MUST poll
  `consume_next` (or be drained by the writer worker) before issuing
  another idempotency-equivalent intent.
* `rejected_retryable` — there is no headroom AND no queue capacity, OR
  the caller's risk_state forbids queueing. The response carries a
  `retry_after_seconds` hint so the caller can back off.

Two errors short-circuit the decision (typed, never silently retried):

* `non_idempotent_queue_forbidden` — a non-idempotent intent
  (`idempotency_key=None`) cannot be queued. Either it fits the headroom
  budget immediately or it is rejected. The spec forbids silently delaying
  a non-idempotent operation because retrying it could mutate state twice.
* `backpressure_hard_limit` — the per-board hard limit on in-flight
  intents has been reached. This is a server-side protection, not a queue
  full condition. Always retryable with a longer backoff.

`risk_state` (one of the KG-01 health states) is consumed by the gate so
that a graph already in `recovery_needed`/`quarantined` rejects writes
without even consulting the queue — preventing further damage. `at_risk`
and `backpressure` allow queueing but bias the Retry-After upward.

The gate emits a single counter on every decision so observability tools
can slice by operation, decision class, reason and queue depth — per OR
or_748bf163 the canonical metric is:

    kg_backpressure_decision_total{board_id, operation, decision, reason, queue_depth_bucket}

Where `decision` is one of `accepted|queued|rejected_retryable|error|idempotent_hit`
and `queue_depth_bucket` is one of `0|1-3|4-15|16-63|64+` (snapshot of the
queue at decision time so historical samples stay joinable).

Idempotency: an intent with a previously-seen `idempotency_key` for the
same `(board_id, operation)` returns the prior decision verbatim (same
`correlation_id`) so retries don't duplicate writes or pollute the queue.
The idempotency cache is per-board and bounded; eviction is FIFO.

This module is dependency-free — no DB, no graph backend, no asyncio. The
KG-01.3 single-writer lock and KG-01.5 safe observability wire actual
writers and Prometheus emission around it.
"""

from __future__ import annotations

from okto_pulse.core.runtime_context import (
    register_runtime_value,
    reset_runtime_values,
    resolve_runtime_value,
    runtime_lock,
    runtime_state,
)

import logging
import threading
import time
import uuid
from collections import OrderedDict, deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

logger = logging.getLogger("okto_pulse.kg.backpressure")


# --- Contract surfaces ----------------------------------------------------------


class BackpressureDecision(str, Enum):
    """Three-valued decision per contract api_a1ec2dcc."""

    ACCEPTED = "accepted"
    QUEUED = "queued"
    REJECTED_RETRYABLE = "rejected_retryable"


class BackpressureErrorCode(str, Enum):
    """Typed errors that short-circuit the decision."""

    NON_IDEMPOTENT_QUEUE_FORBIDDEN = "non_idempotent_queue_forbidden"
    BACKPRESSURE_HARD_LIMIT = "backpressure_hard_limit"


class BackpressureError(Exception):
    """Raised when the gate cannot return a decision at all.

    Carries a typed `code` so callers map it to retryable/non-retryable
    behaviour without string parsing. `correlation_id` is always present
    so observability can join error events to subsequent retries.
    """

    def __init__(
        self,
        code: BackpressureErrorCode,
        *,
        correlation_id: str,
        retryable: bool,
        retry_after_seconds: int | None = None,
        reason: str | None = None,
    ) -> None:
        super().__init__(f"{code.value}: {reason or 'no_reason'}")
        self.code = code
        self.correlation_id = correlation_id
        self.retryable = retryable
        self.retry_after_seconds = retry_after_seconds
        self.reason = reason


@dataclass(frozen=True, slots=True)
class WriteIntent:
    """A single KG write intent submitted to the gate.

    `risk_state` MUST be one of the canonical health states
    (`healthy|at_risk|backpressure|recovery_needed|quarantined`). The
    gate uses it to short-circuit dangerous classes of writes without
    consulting the queue.
    """

    board_id: str
    operation: str
    idempotency_key: str | None
    risk_state: str


@dataclass(frozen=True, slots=True)
class BackpressureResponse:
    """Frozen contract-shaped response (api_a1ec2dcc, success body)."""

    decision: BackpressureDecision
    correlation_id: str
    retry_after_seconds: int | None = None
    reason: str | None = None
    # Position in the per-board queue when decision=queued; 0-indexed.
    # None for accepted/rejected. Not in the contract but exposed for
    # observability and worker-side fairness audits.
    queue_position: int | None = None


# --- Configuration --------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class BackpressureConfig:
    """Tunables for the gate.

    Defaults match KG-01 FR4 sizing for the MVP single-node deployment:

    * `accept_concurrency` — number of intents allowed "in flight" per
      board before queueing kicks in. Effectively the headroom of the
      downstream single-writer (KG-01.3).
    * `queue_capacity` — max queued intents per board. Reaching it
      yields `rejected_retryable` with reason="queue_full".
    * `hard_limit` — absolute cap on accepted+queued. A request beyond
      this raises `BackpressureError(backpressure_hard_limit)`. This is
      a defence against runaway producers and a different signal from
      a saturated queue.
    * `idempotency_cache_size` — bounded LRU per board for seen keys.
      Eviction is FIFO; legacy retries beyond this window are treated
      as fresh intents.
    * `base_retry_after_seconds` — Retry-After hint when the gate is
      lightly congested. Biased upward as risk_state degrades.
    """

    accept_concurrency: int = 8
    queue_capacity: int = 64
    hard_limit: int = 128
    idempotency_cache_size: int = 512
    base_retry_after_seconds: int = 2


# --- Internal per-board state --------------------------------------------------


@dataclass(slots=True)
class _BoardState:
    in_flight: int = 0
    queue: deque[BackpressureResponse] = field(default_factory=deque)
    idempotency: OrderedDict[tuple[str, str], BackpressureResponse] = field(
        default_factory=OrderedDict
    )


# --- Risk-state policy ---------------------------------------------------------

# KG-01 health states (FR1). The gate uses them BEFORE consulting the
# queue: `quarantined` and `recovery_needed` are hard rejects (writing
# would worsen corruption), `backpressure` and `at_risk` allow queueing
# but bias Retry-After upward, `healthy` is the fast path.
_RISK_STATE_HARD_REJECT = frozenset({"quarantined", "recovery_needed"})
_RISK_STATE_BIAS = {
    "healthy": 1.0,
    "at_risk": 2.0,
    "backpressure": 4.0,
    "recovery_needed": 8.0,  # never used (hard reject above)
    "quarantined": 8.0,      # never used (hard reject above)
}


# --- Decision counter (FR4 observability, OR or_748bf163) ----------------------

# OR or_748bf163 defines the canonical kg_backpressure_decision_total
# metric with these required labels: board_id, operation, decision,
# reason, queue_depth_bucket. We keep the in-memory representation as a
# dict keyed by the full label tuple so callers can query any slice and
# the KG-01.5 Prometheus emitter can read it without re-parsing.
#
# The "reason" label uses "n/a" when the decision did not carry a reason
# (typical for accepted). The "queue_depth_bucket" label is computed at
# decision time so historical samples remain joinable even after the
# queue drains.
_REQUIRED_COUNTER_LABELS = (
    "board_id",
    "operation",
    "decision",
    "reason",
    "queue_depth_bucket",
)

# Auditable static bucket boundaries — chosen to highlight the regimes
# operators care about: empty / shallow / mid / deep / saturated. Buckets
# are intentionally a small fixed set so dashboards can pin them.
_QUEUE_DEPTH_BUCKETS = (
    (0, "0"),
    (3, "1-3"),
    (15, "4-15"),
    (63, "16-63"),
)
_QUEUE_DEPTH_BUCKET_SATURATED = "64+"


def _queue_depth_bucket(depth: int) -> str:
    """Return the canonical bucket label for ``depth``."""
    for upper, label in _QUEUE_DEPTH_BUCKETS:
        if depth <= upper:
            return label
    return _QUEUE_DEPTH_BUCKET_SATURATED


_CounterKey = tuple[str, str, str, str, str]
_decision_counter = runtime_state("kg.backpressure.decision_counter", dict)
_decision_counter_lock = runtime_lock("kg.backpressure.decision_counter")


def get_decision_count(
    board_id: str,
    decision: str,
    *,
    operation: str | None = None,
    reason: str | None = None,
    queue_depth_bucket: str | None = None,
) -> int:
    """Read kg_backpressure_decision_total.

    Without optional filters returns the sum across operation/reason/bucket
    slices for ``(board_id, decision)``. Filter args narrow the count to
    a specific slice. Match is exact.
    """
    with _decision_counter_lock:
        total = 0
        for (b, op, dec, rsn, bucket), value in _decision_counter.items():
            if b != board_id or dec != decision:
                continue
            if operation is not None and op != operation:
                continue
            if reason is not None and rsn != reason:
                continue
            if queue_depth_bucket is not None and bucket != queue_depth_bucket:
                continue
            total += value
        return total


def get_decision_label_samples() -> list[dict[str, Any]]:
    """Return a snapshot of every (label tuple, count) seen so far.

    Each row is shaped like the OR contract:
    ``{"board_id": str, "operation": str, "decision": str, "reason": str,
       "queue_depth_bucket": str, "count": int}``. Used by tests and by
    the KG-01.5 emitter to translate to Prometheus samples.
    """
    with _decision_counter_lock:
        samples: list[dict[str, Any]] = []
        for (b, op, dec, rsn, bucket), value in _decision_counter.items():
            samples.append(
                {
                    "board_id": b,
                    "operation": op,
                    "decision": dec,
                    "reason": rsn,
                    "queue_depth_bucket": bucket,
                    "count": value,
                }
            )
        return samples


def get_required_counter_labels() -> tuple[str, ...]:
    """Expose the OR-required label tuple so emitters can validate shape."""
    return _REQUIRED_COUNTER_LABELS


def reset_decision_counter() -> None:
    """Test hook — clear all counter labels."""
    with _decision_counter_lock:
        _decision_counter.clear()


def _bump_counter(
    *,
    board_id: str,
    operation: str,
    decision: str,
    reason: str | None,
    queue_depth_bucket: str,
) -> None:
    """Increment the canonical counter slice by 1."""
    key: _CounterKey = (
        board_id,
        operation,
        decision,
        reason or "n/a",
        queue_depth_bucket,
    )
    with _decision_counter_lock:
        _decision_counter[key] = _decision_counter.get(key, 0) + 1


# --- The gate -------------------------------------------------------------------


class KGBackpressureGate:
    """Per-board bounded queue + decision rules.

    The gate is process-local and thread-safe; cross-process coordination
    is the single-writer lock's job (KG-01.3). Holding the gate's lock
    only serialises the decision logic — it does NOT pin the downstream
    writer.

    Typical lifecycle:

        gate = KGBackpressureGate()
        response = gate.submit_intent(WriteIntent(
            board_id="b1",
            operation="consolidate",
            idempotency_key="ix-42",
            risk_state="healthy",
        ))
        if response.decision is BackpressureDecision.ACCEPTED:
            try:
                do_write()
            finally:
                gate.release(response.correlation_id, "b1")
        elif response.decision is BackpressureDecision.QUEUED:
            # caller waits for worker to drain or polls consume_next
            ...
        else:  # REJECTED_RETRYABLE
            sleep(response.retry_after_seconds)
            retry()
    """

    def __init__(self, config: BackpressureConfig | None = None) -> None:
        self._config = config or BackpressureConfig()
        self._lock = threading.Lock()
        self._boards: dict[str, _BoardState] = {}
        # Tracks correlation_id -> board_id for release(). Bounded to
        # accept_concurrency * len(boards) at any time.
        self._active: dict[str, str] = {}

    # --- public API --------------------------------------------------------

    def submit_intent(self, intent: WriteIntent) -> BackpressureResponse:
        """Decide accepted/queued/rejected_retryable for ``intent``.

        Raises ``BackpressureError`` for the two typed errors. Callers
        MUST treat the error's ``correlation_id`` as the join key with
        subsequent retries — the contract requires it even on errors.
        """
        correlation_id = uuid.uuid4().hex

        if intent.risk_state in _RISK_STATE_HARD_REJECT:
            reason = f"risk_state.{intent.risk_state}"
            _bump_counter(
                board_id=intent.board_id,
                operation=intent.operation,
                decision="rejected_retryable",
                reason=reason,
                queue_depth_bucket=_queue_depth_bucket(0),
            )
            return BackpressureResponse(
                decision=BackpressureDecision.REJECTED_RETRYABLE,
                correlation_id=correlation_id,
                retry_after_seconds=self._risk_biased_retry_after(intent.risk_state),
                reason=reason,
            )

        with self._lock:
            state = self._boards.setdefault(intent.board_id, _BoardState())
            depth_at_decision = len(state.queue)
            bucket = _queue_depth_bucket(depth_at_decision)

            # Idempotency: previously seen key returns the prior decision
            # verbatim. Refresh LRU position so it stays alive.
            if intent.idempotency_key is not None:
                idem_key = (intent.operation, intent.idempotency_key)
                cached = state.idempotency.get(idem_key)
                if cached is not None:
                    state.idempotency.move_to_end(idem_key)
                    _bump_counter(
                        board_id=intent.board_id,
                        operation=intent.operation,
                        decision="idempotent_hit",
                        reason=cached.reason,
                        queue_depth_bucket=bucket,
                    )
                    return cached

            total_known = state.in_flight + len(state.queue)
            if total_known >= self._config.hard_limit:
                reason = (
                    f"in_flight={state.in_flight} "
                    f"queue={len(state.queue)} "
                    f"hard_limit={self._config.hard_limit}"
                )
                _bump_counter(
                    board_id=intent.board_id,
                    operation=intent.operation,
                    decision="error",
                    reason="backpressure_hard_limit",
                    queue_depth_bucket=bucket,
                )
                raise BackpressureError(
                    BackpressureErrorCode.BACKPRESSURE_HARD_LIMIT,
                    correlation_id=correlation_id,
                    retryable=True,
                    retry_after_seconds=self._risk_biased_retry_after(
                        intent.risk_state, multiplier=4
                    ),
                    reason=reason,
                )

            if state.in_flight < self._config.accept_concurrency:
                state.in_flight += 1
                self._active[correlation_id] = intent.board_id
                response = BackpressureResponse(
                    decision=BackpressureDecision.ACCEPTED,
                    correlation_id=correlation_id,
                    retry_after_seconds=None,
                    reason=None,
                )
                self._remember_idempotent(state, intent, response)
                _bump_counter(
                    board_id=intent.board_id,
                    operation=intent.operation,
                    decision="accepted",
                    reason=None,
                    queue_depth_bucket=bucket,
                )
                return response

            # No headroom. Queueing requires an idempotency_key — silently
            # delaying a non-idempotent write would let a retry duplicate
            # state mutation. Spec FR4 mandates the typed error.
            if intent.idempotency_key is None:
                _bump_counter(
                    board_id=intent.board_id,
                    operation=intent.operation,
                    decision="error",
                    reason="non_idempotent_queue_forbidden",
                    queue_depth_bucket=bucket,
                )
                raise BackpressureError(
                    BackpressureErrorCode.NON_IDEMPOTENT_QUEUE_FORBIDDEN,
                    correlation_id=correlation_id,
                    retryable=False,
                    retry_after_seconds=None,
                    reason=(
                        "in_flight=accept_concurrency and no idempotency_key — "
                        "provide an idempotency_key to enable queueing"
                    ),
                )

            if len(state.queue) >= self._config.queue_capacity:
                response = BackpressureResponse(
                    decision=BackpressureDecision.REJECTED_RETRYABLE,
                    correlation_id=correlation_id,
                    retry_after_seconds=self._risk_biased_retry_after(
                        intent.risk_state, multiplier=2
                    ),
                    reason="queue_full",
                )
                # Even rejected, the response is memoised so a retry with
                # the same key sees the same correlation_id and decision.
                self._remember_idempotent(state, intent, response)
                _bump_counter(
                    board_id=intent.board_id,
                    operation=intent.operation,
                    decision="rejected_retryable",
                    reason="queue_full",
                    queue_depth_bucket=bucket,
                )
                return response

            position = len(state.queue)
            response = BackpressureResponse(
                decision=BackpressureDecision.QUEUED,
                correlation_id=correlation_id,
                retry_after_seconds=self._risk_biased_retry_after(intent.risk_state),
                reason="queued_behind_in_flight",
                queue_position=position,
            )
            state.queue.append(response)
            self._remember_idempotent(state, intent, response)
            _bump_counter(
                board_id=intent.board_id,
                operation=intent.operation,
                decision="queued",
                reason="queued_behind_in_flight",
                queue_depth_bucket=bucket,
            )
            return response

    def release(
        self, correlation_id: str, board_id: str | None = None
    ) -> None:
        """Mark the in-flight intent as completed and drain one queued.

        Workers MUST call this in a ``finally`` after the write attempt
        regardless of outcome. The gate cannot infer completion from
        downstream signals.

        ``board_id`` is accepted for backward compatibility and used as a
        DEFENSIVE assert only — the source of truth is ``_active``. If
        the caller passes a board that does NOT match the one bound at
        accept time, the call is rejected silently (and the actual
        in_flight is NOT decremented). This prevents callers from
        leaking ``in_flight`` slots on the real board by misrouting a
        release. See validator feedback on val_bd6f656e.
        """
        with self._lock:
            real_board = self._active.pop(correlation_id, None)
            if real_board is None:
                # Unknown or already released — idempotent no-op.
                return
            if board_id is not None and board_id != real_board:
                # Defensive: caller misrouted; refuse to mutate the real
                # board's state AND refuse to mutate the wrong board's
                # state. Put the active binding back so a correct release
                # can still arrive.
                self._active[correlation_id] = real_board
                logger.warning(
                    "kg.backpressure.release.board_mismatch "
                    "correlation_id=%s passed_board=%s real_board=%s",
                    correlation_id, board_id, real_board,
                )
                return
            state = self._boards.get(real_board)
            if state is None:
                return
            if state.in_flight > 0:
                state.in_flight -= 1
            # Promote one queued intent to in_flight.
            while state.queue and state.in_flight < self._config.accept_concurrency:
                promoted = state.queue.popleft()
                state.in_flight += 1
                self._active[promoted.correlation_id] = real_board

    def consume_next(self, board_id: str) -> BackpressureResponse | None:
        """Pop the next queued intent for ``board_id`` (worker side).

        Returns None when the queue is empty OR when ``in_flight`` has
        already reached ``accept_concurrency``. The validator's repro on
        val_bd6f656e showed the previous implementation incremented
        ``in_flight`` past the admission cap, letting workers bypass the
        gate. ``consume_next`` MUST respect headroom — workers call
        ``release(correlation_id)`` first, and the natural drain inside
        ``release`` promotes a queued intent; explicit ``consume_next``
        is only useful when the worker wants to peek/pull without
        finishing a prior intent and still must not violate the cap.
        """
        with self._lock:
            state = self._boards.get(board_id)
            if state is None or not state.queue:
                return None
            if state.in_flight >= self._config.accept_concurrency:
                # Headroom exhausted — caller must release a prior intent
                # before pulling another. Returning None signals "try
                # again after a release".
                return None
            promoted = state.queue.popleft()
            state.in_flight += 1
            self._active[promoted.correlation_id] = board_id
            return promoted

    def snapshot(self, board_id: str) -> dict[str, int]:
        """Inspector for tests and the KG-01.5 health endpoint.

        Returns ``{"in_flight": N, "queue_depth": M}`` — never raises.
        """
        with self._lock:
            state = self._boards.get(board_id)
            if state is None:
                return {"in_flight": 0, "queue_depth": 0}
            return {"in_flight": state.in_flight, "queue_depth": len(state.queue)}

    # --- internals ---------------------------------------------------------

    def _remember_idempotent(
        self,
        state: _BoardState,
        intent: WriteIntent,
        response: BackpressureResponse,
    ) -> None:
        if intent.idempotency_key is None:
            return
        key = (intent.operation, intent.idempotency_key)
        state.idempotency[key] = response
        state.idempotency.move_to_end(key)
        while len(state.idempotency) > self._config.idempotency_cache_size:
            state.idempotency.popitem(last=False)

    def _risk_biased_retry_after(
        self, risk_state: str, *, multiplier: float = 1.0
    ) -> int:
        bias = _RISK_STATE_BIAS.get(risk_state, 2.0)
        seconds = self._config.base_retry_after_seconds * bias * multiplier
        return max(1, int(round(seconds)))


_default_gate_lock = runtime_lock("kg.backpressure.default_gate")
_RUNTIME_KEY = "kg.backpressure.default_gate"


def get_default_gate() -> KGBackpressureGate:
    """Singleton accessor used by service-layer callers.

    Tests should construct their own ``KGBackpressureGate`` rather than
    sharing this singleton.
    """
    with _default_gate_lock:
        gate = resolve_runtime_value(_RUNTIME_KEY)
        if gate is None:
            gate = KGBackpressureGate()
            register_runtime_value(_RUNTIME_KEY, gate)
        return gate


def reset_default_gate_for_tests() -> None:
    """Drop the module-level singleton (used by tests)."""
    with _default_gate_lock:
        reset_runtime_values(_RUNTIME_KEY)


__all__ = [
    "BackpressureConfig",
    "BackpressureDecision",
    "BackpressureError",
    "BackpressureErrorCode",
    "BackpressureResponse",
    "KGBackpressureGate",
    "WriteIntent",
    "get_decision_count",
    "get_decision_label_samples",
    "get_default_gate",
    "get_required_counter_labels",
    "reset_decision_counter",
    "reset_default_gate_for_tests",
]
