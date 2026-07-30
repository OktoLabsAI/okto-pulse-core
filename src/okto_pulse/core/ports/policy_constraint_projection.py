"""Edition-owned projection port for active guideline policy constraints."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, Protocol, runtime_checkable

from okto_pulse.core.domain.guideline_policy import (
    POLICY_BOARD_ID_MAX_LENGTH,
    POLICY_SQL_INTEGER_MAX,
)
from okto_pulse.core.events.types import PolicyConstraintChanged
from okto_pulse.core.runtime_context import (
    register_runtime_value,
    require_runtime_value,
    reset_runtime_values,
)


PolicyConstraintProjectionOperation = Literal[
    "adopt",
    "unlink",
    "retire",
    "rebuild",
]

POLICY_CONSTRAINT_RULE_REMOVED_REASON = "policy_constraint_rule_removed"
POLICY_CONSTRAINT_UNLINKED_REASON = "policy_constraint_unlinked"
POLICY_CONSTRAINT_GUIDELINE_RETIRED_REASON = (
    "policy_constraint_guideline_retired"
)
POLICY_CONSTRAINT_GUIDELINE_SUPERSEDED_REASON = (
    "policy_constraint_guideline_superseded"
)
POLICY_CONSTRAINT_REBUILD_NOT_ADOPTED_REASON = (
    "policy_constraint_rebuild_not_adopted"
)
POLICY_CONSTRAINT_PERMANENT_TOMBSTONE_REASONS = frozenset(
    {
        POLICY_CONSTRAINT_RULE_REMOVED_REASON,
        POLICY_CONSTRAINT_UNLINKED_REASON,
        POLICY_CONSTRAINT_GUIDELINE_RETIRED_REASON,
        POLICY_CONSTRAINT_GUIDELINE_SUPERSEDED_REASON,
        POLICY_CONSTRAINT_REBUILD_NOT_ADOPTED_REASON,
    }
)


@dataclass(frozen=True, slots=True)
class PolicyConstraintProjectionResult:
    """Bounded acknowledgement from the edition-owned materializer."""

    board_id: str
    operation: PolicyConstraintProjectionOperation
    event_id: str | None
    activated_count: int
    ended_count: int
    active_count: int
    unadopted_active_count: int
    node_ids: tuple[str, ...]
    replayed: bool

    def __post_init__(self) -> None:
        board_id = self.board_id.strip() if isinstance(self.board_id, str) else ""
        if not board_id or len(board_id) > POLICY_BOARD_ID_MAX_LENGTH:
            raise ValueError("policy_constraint_projection_board_id_invalid")
        if self.operation not in {"adopt", "unlink", "retire", "rebuild"}:
            raise ValueError("policy_constraint_projection_operation_invalid")
        event_id = (
            self.event_id.strip()
            if isinstance(self.event_id, str) and self.event_id.strip()
            else None
        )
        if (self.operation == "rebuild") != (event_id is None):
            raise ValueError("policy_constraint_projection_event_id_invalid")
        for name in (
            "activated_count",
            "ended_count",
            "active_count",
            "unadopted_active_count",
        ):
            value = getattr(self, name)
            if (
                not isinstance(value, int)
                or isinstance(value, bool)
                or value < 0
                or value > POLICY_SQL_INTEGER_MAX
            ):
                raise ValueError(f"policy_constraint_projection_{name}_invalid")
        if self.unadopted_active_count != 0:
            raise ValueError(
                "policy_constraint_projection_unadopted_count_invalid"
            )
        if not isinstance(self.node_ids, tuple | list) or any(
            not isinstance(node_id, str) or not node_id.strip()
            for node_id in self.node_ids
        ):
            raise ValueError("policy_constraint_projection_node_ids_invalid")
        node_ids = tuple(sorted({node_id.strip() for node_id in self.node_ids}))
        if len(node_ids) != len(self.node_ids):
            raise ValueError("policy_constraint_projection_node_ids_duplicate")
        if self.active_count != len(node_ids):
            raise ValueError("policy_constraint_projection_active_count_mismatch")
        if not isinstance(self.replayed, bool):
            raise ValueError("policy_constraint_projection_replayed_invalid")
        object.__setattr__(self, "board_id", board_id)
        object.__setattr__(self, "event_id", event_id)
        object.__setattr__(self, "node_ids", node_ids)


@runtime_checkable
class PolicyConstraintProjectionPort(Protocol):
    """Project immutable policy evidence in the caller-owned transaction."""

    async def apply(
        self,
        context: Any,
        *,
        event: PolicyConstraintChanged,
    ) -> PolicyConstraintProjectionResult: ...

    async def rebuild_board(
        self,
        context: Any,
        *,
        board_id: str,
    ) -> PolicyConstraintProjectionResult: ...


_RUNTIME_KEY = "ports.policy_constraint_projection"


def register_policy_constraint_projection_port(
    port: PolicyConstraintProjectionPort,
) -> None:
    if not isinstance(port, PolicyConstraintProjectionPort):
        raise TypeError("policy_constraint_projection_port_invalid")
    register_runtime_value(_RUNTIME_KEY, port)


def get_policy_constraint_projection_port() -> PolicyConstraintProjectionPort:
    port = require_runtime_value(
        _RUNTIME_KEY,
        "policy_constraint_projection_port_not_configured",
    )
    if not isinstance(port, PolicyConstraintProjectionPort):
        raise RuntimeError("policy_constraint_projection_port_invalid")
    return port


def reset_policy_constraint_projection_port_for_tests() -> None:
    reset_runtime_values(_RUNTIME_KEY)


__all__ = [
    "POLICY_CONSTRAINT_GUIDELINE_RETIRED_REASON",
    "POLICY_CONSTRAINT_GUIDELINE_SUPERSEDED_REASON",
    "POLICY_CONSTRAINT_PERMANENT_TOMBSTONE_REASONS",
    "POLICY_CONSTRAINT_REBUILD_NOT_ADOPTED_REASON",
    "POLICY_CONSTRAINT_RULE_REMOVED_REASON",
    "POLICY_CONSTRAINT_UNLINKED_REASON",
    "PolicyConstraintProjectionOperation",
    "PolicyConstraintProjectionPort",
    "PolicyConstraintProjectionResult",
    "get_policy_constraint_projection_port",
    "register_policy_constraint_projection_port",
    "reset_policy_constraint_projection_port_for_tests",
]
