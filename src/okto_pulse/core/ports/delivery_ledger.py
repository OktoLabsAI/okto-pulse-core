"""Durable Global Discovery delivery ownership boundary.

The governed-deletion worker uses this port after the board graph mutation has
completed.  The concrete adapter must stage the delivery ledger row, optional
attempt-zero outbox event, and queue compare-and-delete in the caller-owned
relational transaction.  Implementations must never commit inside
``transfer_delivery_ownership``.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType
from datetime import datetime
from typing import Any, Mapping, Protocol, Sequence

from okto_pulse.core.runtime_context import (
    register_runtime_value,
    require_runtime_value,
    reset_runtime_values,
)


DELIVERY_WORK_KIND = "stale_reconcile"
DELIVERY_OUTBOX_EVENT_TYPE = "consolidation_committed"
DELIVERY_OUTBOX_REASON = "stale_demotion_parity"
DELIVERY_REDRIVE_OUTBOX_REASON = "delivery_debt_redrive"


class DeliveryState(str, Enum):
    """Durable states owned by the Global Discovery delivery ledger."""

    OUTBOX_PERSISTED = "outbox_persisted"
    DELIVERED = "delivered"
    DELIVERY_DEBT = "delivery_debt"


_INITIAL_TRANSFER_STATES = frozenset(
    {DeliveryState.OUTBOX_PERSISTED, DeliveryState.DELIVERY_DEBT}
)


class DeliveryTransferError(RuntimeError):
    """Base class for typed ownership-transfer conflicts."""


class DeliveryTransferClaimConflict(DeliveryTransferError):
    """The exact claimed queue identity no longer belongs to this worker."""


class DeliveryTransferReplayConflict(DeliveryTransferError):
    """A durable row exists under the same key with divergent immutable data."""


class DeliveryAttemptContractError(DeliveryTransferError):
    """A governed physical attempt does not match its derived envelope."""


class DeliveryAttemptMutationConflict(DeliveryTransferError):
    """A current ledger owner changed before its terminal result was staged."""


class DeliveryRedriveConflict(DeliveryTransferError):
    """A redrive would reuse or ambiguously replace a physical attempt key."""


def _required_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError(f"delivery_transfer_{field_name}_invalid")
    return value


def _non_negative_int(value: object, *, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"delivery_transfer_{field_name}_invalid")
    return value


def build_delivery_key(
    *,
    board_id: str,
    artifact_type: str,
    artifact_id: str,
    generation: int,
) -> str:
    """Return the stable logical identity shared by all delivery attempts."""

    board = _required_text(board_id, field_name="board_id")
    kind = _required_text(artifact_type, field_name="artifact_type")
    artifact = _required_text(artifact_id, field_name="artifact_id")
    generation_value = _non_negative_int(generation, field_name="generation")
    if generation_value < 1:
        raise ValueError("delivery_transfer_generation_invalid")
    return f"gd_parity:{board}:{kind}:{artifact}:{generation_value}"


def build_attempt_event_key(delivery_key: str, *, attempt: int) -> str:
    """Return the physical, never-reused outbox identity for one attempt."""

    logical_key = _required_text(delivery_key, field_name="delivery_key")
    if not logical_key.startswith("gd_parity:"):
        raise ValueError("delivery_transfer_delivery_key_invalid")
    attempt_value = _non_negative_int(attempt, field_name="attempt")
    return f"{logical_key}:attempt:{attempt_value}"


def _delivery_session_id(delivery_key: str) -> str:
    """Derive a stable UUID-shaped session id accepted by the existing outbox."""

    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"okto-pulse:{delivery_key}"))


@dataclass(frozen=True, slots=True)
class DeliveryAttemptEnvelope:
    """Exact immutable contract for one physical delivery attempt.

    Attempt zero is created only by the ownership-transfer transaction.
    Positive attempts are created only by the tick-owned redrive.  Every
    externally persisted field is derived here so consumers can reject a
    forged or mutated payload before touching the Global Discovery graph.
    """

    board_id: str
    artifact_type: str
    artifact_id: str
    generation: int
    delete_event_id: str
    attempt: int
    delivery_key: str = field(init=False)
    attempt_event_key: str = field(init=False)
    outbox_session_id: str = field(init=False)
    outbox_event_type: str = field(
        init=False,
        default=DELIVERY_OUTBOX_EVENT_TYPE,
    )
    reason: str = field(init=False)
    payload: Mapping[str, object] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        board_id = _required_text(self.board_id, field_name="board_id")
        artifact_type = _required_text(
            self.artifact_type,
            field_name="artifact_type",
        )
        artifact_id = _required_text(self.artifact_id, field_name="artifact_id")
        delete_event_id = _required_text(
            self.delete_event_id,
            field_name="delete_event_id",
        )
        generation = _non_negative_int(self.generation, field_name="generation")
        if generation < 1:
            raise ValueError("delivery_transfer_generation_invalid")
        attempt = _non_negative_int(self.attempt, field_name="attempt")
        delivery_key = build_delivery_key(
            board_id=board_id,
            artifact_type=artifact_type,
            artifact_id=artifact_id,
            generation=generation,
        )
        attempt_event_key = build_attempt_event_key(
            delivery_key,
            attempt=attempt,
        )
        session_id = _delivery_session_id(delivery_key)
        reason = (
            DELIVERY_OUTBOX_REASON
            if attempt == 0
            else DELIVERY_REDRIVE_OUTBOX_REASON
        )
        payload = MappingProxyType(
            {
                "event_id": attempt_event_key,
                "board_id": board_id,
                "session_id": session_id,
                "nodes_added": 0,
                "reason": reason,
                "delivery_key": delivery_key,
                "attempt": attempt,
                "artifact_type": artifact_type,
                "artifact_id": artifact_id,
                "generation": generation,
                "delete_event_id": delete_event_id,
            }
        )

        object.__setattr__(self, "board_id", board_id)
        object.__setattr__(self, "artifact_type", artifact_type)
        object.__setattr__(self, "artifact_id", artifact_id)
        object.__setattr__(self, "generation", generation)
        object.__setattr__(self, "delete_event_id", delete_event_id)
        object.__setattr__(self, "attempt", attempt)
        object.__setattr__(self, "delivery_key", delivery_key)
        object.__setattr__(self, "attempt_event_key", attempt_event_key)
        object.__setattr__(self, "outbox_session_id", session_id)
        object.__setattr__(self, "reason", reason)
        object.__setattr__(self, "payload", payload)


def is_governed_delivery_attempt(
    *,
    event_id: object,
    payload: object,
) -> bool:
    """Return whether an outbox row claims the governed delivery namespace."""

    if isinstance(event_id, str) and event_id.startswith("gd_parity:"):
        return True
    return isinstance(payload, Mapping) and "delivery_key" in payload


def parse_delivery_attempt_event(event: object) -> DeliveryAttemptEnvelope | None:
    """Validate and materialize a governed outbox attempt.

    Legacy outbox rows return ``None``.  A row which claims the governed
    namespace must match the fully derived physical key, session, event type,
    reason, identity and exact payload; partial markers fail closed.
    """

    event_id = getattr(event, "event_id", None)
    payload = getattr(event, "payload", None)
    if not is_governed_delivery_attempt(event_id=event_id, payload=payload):
        return None
    if not isinstance(payload, Mapping):
        raise DeliveryAttemptContractError("delivery_attempt_payload_invalid")
    try:
        envelope = DeliveryAttemptEnvelope(
            board_id=payload["board_id"],
            artifact_type=payload["artifact_type"],
            artifact_id=payload["artifact_id"],
            generation=payload["generation"],
            delete_event_id=payload["delete_event_id"],
            attempt=payload["attempt"],
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise DeliveryAttemptContractError(
            "delivery_attempt_payload_invalid"
        ) from exc

    persisted = (
        event_id,
        getattr(event, "board_id", None),
        getattr(event, "session_id", None),
        getattr(event, "event_type", DELIVERY_OUTBOX_EVENT_TYPE),
        dict(payload),
    )
    expected = (
        envelope.attempt_event_key,
        envelope.board_id,
        envelope.outbox_session_id,
        envelope.outbox_event_type,
        dict(envelope.payload),
    )
    if persisted != expected:
        raise DeliveryAttemptContractError("delivery_attempt_envelope_mismatch")
    return envelope


class DeliveryAttemptOutcome(str, Enum):
    """Terminal relational outcome of one verified physical attempt."""

    DELIVERED = "delivered"
    DELIVERY_DEBT = "delivery_debt"


@dataclass(frozen=True, slots=True)
class DeliveryAttemptResult:
    """Attempt result staged atomically with its outbox ACK or terminal DLQ."""

    envelope: DeliveryAttemptEnvelope
    outcome: DeliveryAttemptOutcome
    occurred_at: datetime
    error: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.envelope, DeliveryAttemptEnvelope):
            raise ValueError("delivery_attempt_result_envelope_invalid")
        try:
            outcome = DeliveryAttemptOutcome(self.outcome)
        except (TypeError, ValueError) as exc:
            raise ValueError("delivery_attempt_result_outcome_invalid") from exc
        if not isinstance(self.occurred_at, datetime):
            raise ValueError("delivery_attempt_result_occurred_at_invalid")
        if outcome is DeliveryAttemptOutcome.DELIVERED:
            if self.error is not None:
                raise ValueError("delivery_attempt_result_error_invalid")
        else:
            _required_text(self.error, field_name="attempt_result_error")
        object.__setattr__(self, "outcome", outcome)


@dataclass(frozen=True, slots=True)
class DeliveryMaintenanceReceipt:
    """Bounded progress made by one watchdog or redrive pass."""

    scanned: int
    transitioned: int = 0
    emitted: int = 0
    concurrency_lost: int = 0
    has_more: bool = False
    oldest_debt_age_seconds: float | None = None
    checkpoint_version: int = 0
    resume_board_id: str | None = None

    def __post_init__(self) -> None:
        for field_name in (
            "scanned",
            "transitioned",
            "emitted",
            "concurrency_lost",
            "checkpoint_version",
        ):
            _non_negative_int(getattr(self, field_name), field_name=field_name)
        if not isinstance(self.has_more, bool):
            raise ValueError("delivery_maintenance_has_more_invalid")
        if self.oldest_debt_age_seconds is not None:
            age = self.oldest_debt_age_seconds
            if (
                isinstance(age, bool)
                or not isinstance(age, (int, float))
                or age < 0
            ):
                raise ValueError(
                    "delivery_maintenance_oldest_debt_age_seconds_invalid"
                )
            object.__setattr__(self, "oldest_debt_age_seconds", float(age))
        if self.resume_board_id is not None:
            _required_text(
                self.resume_board_id,
                field_name="resume_board_id",
            )
        if self.has_more and self.resume_board_id is None:
            raise ValueError("delivery_maintenance_resume_board_id_required")


@dataclass(frozen=True, slots=True)
class DeliveryCircuitSnapshot:
    """Fail-closed signal used to select persisted delivery versus debt."""

    degraded: bool
    reason: str

    def __post_init__(self) -> None:
        if not isinstance(self.degraded, bool):
            raise ValueError("delivery_circuit_degraded_invalid")
        _required_text(self.reason, field_name="circuit_reason")


@dataclass(frozen=True, slots=True)
class DeliveryTransferRequest:
    """Immutable command for the three-effect ownership transfer.

    Logical and physical keys, the outbox session, and its payload are derived
    exclusively from the durable queue identity.  Callers therefore cannot
    supply a mismatched idempotency key or replay payload.
    """

    entry_id: str
    claim_token: str
    board_id: str
    artifact_type: str
    artifact_id: str
    generation: int
    delete_event_id: str
    target_state: DeliveryState
    reconcile_details: Mapping[str, object] = field(
        default_factory=dict,
        repr=False,
    )
    occurred_at: datetime | None = None
    work_kind: str = DELIVERY_WORK_KIND
    attempt: int = 0
    delivery_key: str = field(init=False)
    attempt_event_key: str = field(init=False)
    outbox_session_id: str = field(init=False)
    outbox_event_type: str = field(
        init=False,
        default=DELIVERY_OUTBOX_EVENT_TYPE,
    )
    payload: Mapping[str, object] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        entry_id = _required_text(self.entry_id, field_name="entry_id")
        claim_token = _required_text(self.claim_token, field_name="claim_token")
        board_id = _required_text(self.board_id, field_name="board_id")
        artifact_type = _required_text(
            self.artifact_type,
            field_name="artifact_type",
        )
        artifact_id = _required_text(self.artifact_id, field_name="artifact_id")
        delete_event_id = _required_text(
            self.delete_event_id,
            field_name="delete_event_id",
        )
        if self.work_kind != DELIVERY_WORK_KIND:
            raise ValueError("delivery_transfer_work_kind_invalid")
        generation = _non_negative_int(self.generation, field_name="generation")
        if generation < 1:
            raise ValueError("delivery_transfer_generation_invalid")
        attempt = _non_negative_int(self.attempt, field_name="attempt")
        if attempt != 0:
            raise ValueError("delivery_transfer_initial_attempt_invalid")
        try:
            target_state = DeliveryState(self.target_state)
        except (TypeError, ValueError) as exc:
            raise ValueError("delivery_transfer_target_state_invalid") from exc
        if target_state not in _INITIAL_TRANSFER_STATES:
            raise ValueError("delivery_transfer_target_state_invalid")
        if not isinstance(self.reconcile_details, Mapping):
            raise ValueError("delivery_transfer_reconcile_details_invalid")
        if self.occurred_at is not None and not isinstance(
            self.occurred_at,
            datetime,
        ):
            raise ValueError("delivery_transfer_occurred_at_invalid")

        envelope = DeliveryAttemptEnvelope(
            board_id=board_id,
            artifact_type=artifact_type,
            artifact_id=artifact_id,
            generation=generation,
            delete_event_id=delete_event_id,
            attempt=attempt,
        )

        # Re-assign validated values as well as derived fields.  This also
        # normalizes a string enum input without weakening frozen semantics.
        object.__setattr__(self, "entry_id", entry_id)
        object.__setattr__(self, "claim_token", claim_token)
        object.__setattr__(self, "board_id", board_id)
        object.__setattr__(self, "artifact_type", artifact_type)
        object.__setattr__(self, "artifact_id", artifact_id)
        object.__setattr__(self, "generation", generation)
        object.__setattr__(self, "delete_event_id", delete_event_id)
        object.__setattr__(self, "target_state", target_state)
        object.__setattr__(
            self,
            "reconcile_details",
            MappingProxyType(dict(self.reconcile_details)),
        )
        object.__setattr__(self, "attempt", attempt)
        object.__setattr__(self, "delivery_key", envelope.delivery_key)
        object.__setattr__(self, "attempt_event_key", envelope.attempt_event_key)
        object.__setattr__(self, "outbox_session_id", envelope.outbox_session_id)
        object.__setattr__(self, "payload", envelope.payload)


@dataclass(frozen=True, slots=True)
class DeliveryTransferReceipt:
    """Result of a transfer whose queue CAS matched exactly one row."""

    delivery_key: str
    state: DeliveryState
    attempt: int
    attempt_event_key: str | None
    replayed: bool = False

    def __post_init__(self) -> None:
        delivery_key = _required_text(
            self.delivery_key,
            field_name="delivery_key",
        )
        try:
            state = DeliveryState(self.state)
        except (TypeError, ValueError) as exc:
            raise ValueError("delivery_transfer_receipt_state_invalid") from exc
        if state not in _INITIAL_TRANSFER_STATES:
            raise ValueError("delivery_transfer_receipt_state_invalid")
        attempt = _non_negative_int(self.attempt, field_name="attempt")
        if attempt != 0:
            raise ValueError("delivery_transfer_receipt_attempt_invalid")
        if not isinstance(self.replayed, bool):
            raise ValueError("delivery_transfer_receipt_replayed_invalid")

        if state is DeliveryState.OUTBOX_PERSISTED:
            expected_event_key = build_attempt_event_key(
                delivery_key,
                attempt=attempt,
            )
            if self.attempt_event_key != expected_event_key:
                raise ValueError(
                    "delivery_transfer_receipt_attempt_event_key_invalid"
                )
        elif self.attempt_event_key is not None:
            raise ValueError("delivery_transfer_receipt_attempt_event_key_invalid")

        object.__setattr__(self, "delivery_key", delivery_key)
        object.__setattr__(self, "state", state)
        object.__setattr__(self, "attempt", attempt)


class DeliveryLedgerPort(Protocol):
    """Edition-owned persistence and circuit-state boundary."""

    async def read_circuit_snapshot(
        self,
        context: Any,
        *,
        board_id: str,
    ) -> DeliveryCircuitSnapshot: ...

    async def transfer_delivery_ownership(
        self,
        context: Any,
        request: DeliveryTransferRequest,
    ) -> DeliveryTransferReceipt:
        """Stage ledger, optional outbox, and queue CAS without committing."""
        ...

    async def apply_attempt_outcomes(
        self,
        context: Any,
        outcomes: Sequence[DeliveryAttemptResult],
    ) -> None:
        """Stage terminal ledger outcomes without committing."""
        ...

    async def reconcile_orphaned_attempts(
        self,
        context: Any,
        *,
        board_id: str,
        now: datetime,
        limit: int,
    ) -> DeliveryMaintenanceReceipt:
        """Repair bounded current-attempt/outbox drift without emitting."""
        ...

    async def redrive_delivery_debt(
        self,
        context: Any,
        *,
        now: datetime,
        limit: int,
    ) -> DeliveryMaintenanceReceipt:
        """Run one global fair debt page without committing.

        Implementations must select at most ``limit`` rows, serve one oldest
        due row per board before repeating a board, and advance their durable
        round-robin checkpoint in the same transaction as every emitted
        physical attempt.
        """
        ...


_RUNTIME_KEY = "ports.delivery_ledger.port"


def register_delivery_ledger_port(port: DeliveryLedgerPort) -> None:
    register_runtime_value(_RUNTIME_KEY, port)


def get_delivery_ledger_port() -> DeliveryLedgerPort:
    return require_runtime_value(
        _RUNTIME_KEY,
        "delivery_ledger_port_not_configured",
    )


def reset_delivery_ledger_port_for_tests() -> None:
    reset_runtime_values(_RUNTIME_KEY)


__all__ = [
    "DELIVERY_OUTBOX_EVENT_TYPE",
    "DELIVERY_OUTBOX_REASON",
    "DELIVERY_REDRIVE_OUTBOX_REASON",
    "DELIVERY_WORK_KIND",
    "DeliveryAttemptContractError",
    "DeliveryAttemptEnvelope",
    "DeliveryAttemptMutationConflict",
    "DeliveryAttemptOutcome",
    "DeliveryAttemptResult",
    "DeliveryCircuitSnapshot",
    "DeliveryLedgerPort",
    "DeliveryMaintenanceReceipt",
    "DeliveryRedriveConflict",
    "DeliveryState",
    "DeliveryTransferClaimConflict",
    "DeliveryTransferError",
    "DeliveryTransferReplayConflict",
    "DeliveryTransferRequest",
    "DeliveryTransferReceipt",
    "build_attempt_event_key",
    "build_delivery_key",
    "get_delivery_ledger_port",
    "is_governed_delivery_attempt",
    "parse_delivery_attempt_event",
    "register_delivery_ledger_port",
    "reset_delivery_ledger_port_for_tests",
]
