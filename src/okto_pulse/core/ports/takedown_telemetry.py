"""Durable governed-takedown telemetry contracts.

The delivery ledger is the current owner snapshot.  This port complements it
with an append-only state timeline so redrive can never erase an earlier debt
transition.  Core owns the state vocabulary, identity validation and SLO
policy; editions own persistence and aggregate queries.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from types import MappingProxyType
from typing import Any, Mapping, Protocol

from okto_pulse.core.runtime_context import (
    register_runtime_value,
    require_runtime_value,
    reset_runtime_values,
)


TAKEDOWN_NORMAL_SLO_SECONDS = 120.0
TAKEDOWN_RECOVERY_SLO_SECONDS = 26.0 * 60.0 * 60.0
TAKEDOWN_P95_WINDOW_SECONDS = 60 * 60
TAKEDOWN_SLO_RUNBOOK = (
    "okto_pulse_kg_global_outbox_dead_letter_reprocess",
    "okto_pulse_kg_stale_canonical_parity_list",
)


class TakedownState(str, Enum):
    INTENT_CREATED = "intent_created"
    GRAPH_DEMOTED = "graph_demoted"
    OUTBOX_PERSISTED = "outbox_persisted"
    DELIVERED = "delivered"
    DELIVERY_DEBT = "delivery_debt"


class TakedownSloEvaluationStatus(str, Enum):
    """Operational evidence state for one periodic SLO evaluation."""

    BREACHED = "breached"
    WITHIN_SLO = "within_slo"
    INSUFFICIENT_DATA = "insufficient_data"


class TakedownTransitionConflict(RuntimeError):
    """An idempotency key was replayed with divergent immutable data."""


TAKEDOWN_STATE_RANK = MappingProxyType(
    {
        TakedownState.INTENT_CREATED: 0,
        TakedownState.GRAPH_DEMOTED: 1,
        TakedownState.OUTBOX_PERSISTED: 2,
        TakedownState.DELIVERY_DEBT: 3,
        TakedownState.DELIVERED: 4,
    }
)


_ATTEMPT_STATES = frozenset(
    {
        TakedownState.OUTBOX_PERSISTED,
        TakedownState.DELIVERED,
        TakedownState.DELIVERY_DEBT,
    }
)


def _required_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError(f"takedown_telemetry_{field_name}_invalid")
    return value


def _optional_text(value: object, *, field_name: str) -> str | None:
    if value is None:
        return None
    return _required_text(value, field_name=field_name)


def _attempt(value: object | None, *, required: bool) -> int | None:
    if value is None and not required:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("takedown_telemetry_attempt_invalid")
    return value


@dataclass(frozen=True, slots=True)
class TakedownTransition:
    """One immutable, idempotently persisted state observation."""

    delete_event_id: str
    board_id: str
    artifact_type: str
    artifact_id: str
    generation: int
    state: TakedownState
    occurred_at: datetime
    delivery_key: str | None = None
    attempt: int | None = None
    last_error: str | None = None
    next_retry_at: datetime | None = None
    details: Mapping[str, object] = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        delete_event_id = _required_text(
            self.delete_event_id,
            field_name="delete_event_id",
        )
        board_id = _required_text(self.board_id, field_name="board_id")
        artifact_type = _required_text(
            self.artifact_type,
            field_name="artifact_type",
        )
        artifact_id = _required_text(self.artifact_id, field_name="artifact_id")
        if (
            isinstance(self.generation, bool)
            or not isinstance(self.generation, int)
            or self.generation < 1
        ):
            raise ValueError("takedown_telemetry_generation_invalid")
        try:
            state = TakedownState(self.state)
        except (TypeError, ValueError) as exc:
            raise ValueError("takedown_telemetry_state_invalid") from exc
        if not isinstance(self.occurred_at, datetime):
            raise ValueError("takedown_telemetry_occurred_at_invalid")
        delivery_key = _optional_text(
            self.delivery_key,
            field_name="delivery_key",
        )
        # New governed-delete writers know the deterministic delivery identity
        # before the asynchronous worker runs, so the intent transition may
        # carry it.  ``None`` remains valid for historical intent rows written
        # before that identity was persisted; every later state still requires
        # the key.
        if state is not TakedownState.INTENT_CREATED and delivery_key is None:
            raise ValueError("takedown_telemetry_delivery_key_required")
        if state not in _ATTEMPT_STATES and self.attempt is not None:
            raise ValueError("takedown_telemetry_attempt_invalid")
        attempt = _attempt(self.attempt, required=state in _ATTEMPT_STATES)
        last_error = _optional_text(self.last_error, field_name="last_error")
        if self.next_retry_at is not None and not isinstance(
            self.next_retry_at,
            datetime,
        ):
            raise ValueError("takedown_telemetry_next_retry_at_invalid")
        if not isinstance(self.details, Mapping):
            raise ValueError("takedown_telemetry_details_invalid")

        object.__setattr__(self, "delete_event_id", delete_event_id)
        object.__setattr__(self, "board_id", board_id)
        object.__setattr__(self, "artifact_type", artifact_type)
        object.__setattr__(self, "artifact_id", artifact_id)
        object.__setattr__(self, "state", state)
        object.__setattr__(self, "delivery_key", delivery_key)
        object.__setattr__(self, "attempt", attempt)
        object.__setattr__(self, "last_error", last_error)
        object.__setattr__(
            self,
            "details",
            MappingProxyType(dict(self.details)),
        )

    @property
    def transition_key(self) -> str:
        if self.state is TakedownState.INTENT_CREATED:
            return f"takedown:{self.delete_event_id}:intent_created"
        suffix = self.state.value
        if self.attempt is not None:
            suffix = f"{suffix}:attempt:{self.attempt}"
        return f"takedown:{self.delivery_key}:{suffix}"

    def to_dict(self) -> dict[str, object]:
        return {
            "state": self.state.value,
            "timestamp": self.occurred_at.isoformat(),
            "last_error": self.last_error,
            "next_retry": (
                self.next_retry_at.isoformat()
                if self.next_retry_at is not None
                else None
            ),
            "attempt": self.attempt,
            "delete_event_id": self.delete_event_id,
            "delivery_key": self.delivery_key,
            "artifact_type": self.artifact_type,
            "artifact_id": self.artifact_id,
            "generation": self.generation,
            "details": dict(self.details),
        }


@dataclass(frozen=True, slots=True)
class TakedownTelemetryQuery:
    """Board-scoped, exactly-one identity query with a controlled clock."""

    now: datetime
    board_id: str
    delete_event_id: str | None = None
    delivery_key: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.now, datetime):
            raise ValueError("takedown_telemetry_now_invalid")
        board_id = _required_text(self.board_id, field_name="board_id")
        delete_event_id = _optional_text(
            self.delete_event_id,
            field_name="delete_event_id",
        )
        delivery_key = _optional_text(
            self.delivery_key,
            field_name="delivery_key",
        )
        if (delete_event_id is None) == (delivery_key is None):
            raise ValueError("takedown_telemetry_identity_one_of_required")
        object.__setattr__(self, "board_id", board_id)
        object.__setattr__(self, "delete_event_id", delete_event_id)
        object.__setattr__(self, "delivery_key", delivery_key)


@dataclass(frozen=True, slots=True)
class TakedownAggregates:
    delivery_debt_backlog: int
    oldest_debt_age_seconds: float | None
    circuit_breaker_state: str
    circuit_breaker_reason: str
    p95_seconds_1h: float | None
    p95_sample_count: int = 0

    def __post_init__(self) -> None:
        if (
            isinstance(self.delivery_debt_backlog, bool)
            or not isinstance(self.delivery_debt_backlog, int)
            or self.delivery_debt_backlog < 0
        ):
            raise ValueError("takedown_telemetry_backlog_invalid")
        if (
            isinstance(self.p95_sample_count, bool)
            or not isinstance(self.p95_sample_count, int)
            or self.p95_sample_count < 0
        ):
            raise ValueError("takedown_telemetry_p95_sample_count_invalid")
        for name in ("oldest_debt_age_seconds", "p95_seconds_1h"):
            value = getattr(self, name)
            if value is not None and (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
                or value < 0
            ):
                raise ValueError(f"takedown_telemetry_{name}_invalid")
            if value is not None:
                object.__setattr__(self, name, float(value))
        _required_text(
            self.circuit_breaker_state,
            field_name="circuit_breaker_state",
        )
        _required_text(
            self.circuit_breaker_reason,
            field_name="circuit_breaker_reason",
        )

    @property
    def breach_reasons(self) -> tuple[str, ...]:
        reasons: list[str] = []
        if (
            self.p95_sample_count > 0
            and
            self.p95_seconds_1h is not None
            and self.p95_seconds_1h > TAKEDOWN_NORMAL_SLO_SECONDS
        ):
            reasons.append("takedown_p95_above_120_seconds")
        if (
            self.oldest_debt_age_seconds is not None
            and self.oldest_debt_age_seconds > TAKEDOWN_RECOVERY_SLO_SECONDS
        ):
            reasons.append("oldest_delivery_debt_above_26_hours")
        return tuple(reasons)

    def to_dict(self) -> dict[str, object]:
        return {
            "delivery_debt_backlog": self.delivery_debt_backlog,
            "oldest_debt_age_seconds": self.oldest_debt_age_seconds,
            "circuit_breaker_state": self.circuit_breaker_state,
            "circuit_breaker_reason": self.circuit_breaker_reason,
            "p95_seconds_1h": self.p95_seconds_1h,
            "p95_sample_count": self.p95_sample_count,
        }


@dataclass(frozen=True, slots=True)
class TakedownSloEvaluation:
    """Explicit monitor result; an empty sample window is not health proof."""

    board_id: str
    observed_at: datetime
    transaction_state: str
    aggregates: TakedownAggregates

    def __post_init__(self) -> None:
        _required_text(self.board_id, field_name="board_id")
        if not isinstance(self.observed_at, datetime):
            raise ValueError("takedown_telemetry_observed_at_invalid")
        _required_text(
            self.transaction_state,
            field_name="transaction_state",
        )
        if not isinstance(self.aggregates, TakedownAggregates):
            raise ValueError("takedown_telemetry_aggregates_invalid")

    @property
    def status(self) -> TakedownSloEvaluationStatus:
        if self.aggregates.breach_reasons:
            return TakedownSloEvaluationStatus.BREACHED
        if (
            self.aggregates.p95_sample_count == 0
            or self.aggregates.p95_seconds_1h is None
            or (
                self.aggregates.delivery_debt_backlog > 0
                and self.aggregates.oldest_debt_age_seconds is None
            )
        ):
            return TakedownSloEvaluationStatus.INSUFFICIENT_DATA
        return TakedownSloEvaluationStatus.WITHIN_SLO

    @property
    def breached(self) -> bool:
        return self.status is TakedownSloEvaluationStatus.BREACHED

    def to_dict(self) -> dict[str, object]:
        return {
            "evaluated": True,
            "board_id": self.board_id,
            "observed_at": self.observed_at.isoformat(),
            "transaction_state": self.transaction_state,
            "status": self.status.value,
            "breached": self.breached,
            "metrics": self.aggregates.to_dict(),
            "alert": build_takedown_slo_alert(self.aggregates),
        }


@dataclass(frozen=True, slots=True)
class TakedownTelemetrySnapshot:
    board_id: str
    delete_event_id: str
    delivery_key: str | None
    artifact_type: str
    artifact_id: str
    generation: int
    states: tuple[TakedownTransition, ...]
    aggregates: TakedownAggregates

    def __post_init__(self) -> None:
        _required_text(self.board_id, field_name="board_id")
        _required_text(self.delete_event_id, field_name="delete_event_id")
        _optional_text(self.delivery_key, field_name="delivery_key")
        _required_text(self.artifact_type, field_name="artifact_type")
        _required_text(self.artifact_id, field_name="artifact_id")
        if (
            isinstance(self.generation, bool)
            or not isinstance(self.generation, int)
            or self.generation < 1
        ):
            raise ValueError("takedown_telemetry_generation_invalid")
        if not isinstance(self.aggregates, TakedownAggregates):
            raise ValueError("takedown_telemetry_aggregates_invalid")
        if not isinstance(self.states, tuple) or not self.states:
            raise ValueError("takedown_telemetry_states_invalid")
        if any(not isinstance(item, TakedownTransition) for item in self.states):
            raise ValueError("takedown_telemetry_states_invalid")
        if any(
            (
                item.delete_event_id,
                item.board_id,
                item.artifact_type,
                item.artifact_id,
                item.generation,
            )
            != (
                self.delete_event_id,
                self.board_id,
                self.artifact_type,
                self.artifact_id,
                self.generation,
            )
            for item in self.states
        ):
            raise ValueError("takedown_telemetry_state_identity_mismatch")
        state_delivery_keys = {
            item.delivery_key
            for item in self.states
            if item.delivery_key is not None
        }
        if (
            len(state_delivery_keys) > 1
            or (
                state_delivery_keys
                and state_delivery_keys != {self.delivery_key}
            )
            or (not state_delivery_keys and self.delivery_key is not None)
        ):
            raise ValueError("takedown_telemetry_delivery_identity_mismatch")
        try:
            ordered_states = tuple(
                sorted(
                    self.states,
                    key=lambda item: (
                        item.occurred_at,
                        TAKEDOWN_STATE_RANK[item.state],
                        item.attempt if item.attempt is not None else -1,
                        item.transition_key,
                    ),
                )
            )
        except (KeyError, TypeError) as exc:
            raise ValueError("takedown_telemetry_states_order_invalid") from exc
        if self.states != ordered_states:
            raise ValueError("takedown_telemetry_states_order_invalid")

    def to_dict(self) -> dict[str, object]:
        breach_reasons = self.aggregates.breach_reasons
        return {
            "board_id": self.board_id,
            "delete_event_id": self.delete_event_id,
            "delivery_key": self.delivery_key,
            "artifact_type": self.artifact_type,
            "artifact_id": self.artifact_id,
            "generation": self.generation,
            "states": [item.to_dict() for item in self.states],
            "aggregates": self.aggregates.to_dict(),
            "slo": {
                "normal_threshold_seconds": TAKEDOWN_NORMAL_SLO_SECONDS,
                "recovery_threshold_seconds": TAKEDOWN_RECOVERY_SLO_SECONDS,
                "window_seconds": TAKEDOWN_P95_WINDOW_SECONDS,
                "breached": bool(breach_reasons),
                "breach_reasons": list(breach_reasons),
                "runbook": list(TAKEDOWN_SLO_RUNBOOK),
                "health_predicate": "delivered_state_and_evaluable_parity_probe",
            },
        }


def build_takedown_slo_alert(
    aggregates: TakedownAggregates,
) -> dict[str, object] | None:
    """Build the stable structured breach signal without emitting effects."""

    if not isinstance(aggregates, TakedownAggregates):
        raise ValueError("takedown_telemetry_aggregates_invalid")
    reasons = aggregates.breach_reasons
    if not reasons:
        return None
    return {
        "event": "kg.takedown.slo_breach",
        "severity": "critical",
        "reasons": list(reasons),
        "metrics": aggregates.to_dict(),
        "thresholds": {
            "normal_p95_seconds": TAKEDOWN_NORMAL_SLO_SECONDS,
            "recovery_oldest_debt_seconds": TAKEDOWN_RECOVERY_SLO_SECONDS,
            "p95_window_seconds": TAKEDOWN_P95_WINDOW_SECONDS,
        },
        "runbook": list(TAKEDOWN_SLO_RUNBOOK),
    }


class TakedownTelemetryReadPort(Protocol):
    async def query_takedown_telemetry(
        self,
        context: Any,
        query: TakedownTelemetryQuery,
    ) -> TakedownTelemetrySnapshot | None: ...

    async def evaluate_takedown_slo(
        self,
        context: Any,
        *,
        board_id: str,
        now: datetime,
        transaction_state: str,
    ) -> TakedownSloEvaluation: ...


_RUNTIME_KEY = "ports.takedown_telemetry.read"


def register_takedown_telemetry_read_port(
    port: TakedownTelemetryReadPort,
) -> None:
    register_runtime_value(_RUNTIME_KEY, port)


def get_takedown_telemetry_read_port() -> TakedownTelemetryReadPort:
    return require_runtime_value(
        _RUNTIME_KEY,
        "takedown_telemetry_read_port_not_configured",
    )


def reset_takedown_telemetry_read_port_for_tests() -> None:
    reset_runtime_values(_RUNTIME_KEY)


__all__ = [
    "TAKEDOWN_NORMAL_SLO_SECONDS",
    "TAKEDOWN_P95_WINDOW_SECONDS",
    "TAKEDOWN_RECOVERY_SLO_SECONDS",
    "TAKEDOWN_SLO_RUNBOOK",
    "TAKEDOWN_STATE_RANK",
    "TakedownAggregates",
    "TakedownState",
    "TakedownSloEvaluation",
    "TakedownSloEvaluationStatus",
    "TakedownTelemetryQuery",
    "TakedownTelemetryReadPort",
    "TakedownTelemetrySnapshot",
    "TakedownTransition",
    "TakedownTransitionConflict",
    "build_takedown_slo_alert",
    "get_takedown_telemetry_read_port",
    "register_takedown_telemetry_read_port",
    "reset_takedown_telemetry_read_port_for_tests",
]
