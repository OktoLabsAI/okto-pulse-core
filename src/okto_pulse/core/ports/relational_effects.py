"""Dialect-neutral relational side-effect port.

Core handlers may request logical persistence effects through this port, but
the SQL dialect, SQLAlchemy metadata construction and concrete upsert mechanics
belong to an edition adapter.
"""

from __future__ import annotations

from okto_pulse.core.runtime_context import (
    register_runtime_value,
    reset_runtime_values,
    resolve_runtime_value,
)

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, Mapping, Protocol, Sequence, runtime_checkable


SPEC_DEPENDENCY_PROJECTION_QUEUE_CONTRACT = "spec-dependency-projection/v1"
SPEC_DEPENDENCY_PROJECTION_EVENT_TYPES = frozenset(
    {"spec.dependency_added", "spec.dependency_removed"}
)


@dataclass(frozen=True, slots=True)
class SpecDependencyProjectionQueueMetadata:
    """Durable ownership marker for one dependency projection fan-out."""

    mutation_event_id: str
    mutation_event_type: str
    dependency_id: str
    projection_owner_spec_id: str
    target_role: Literal["endpoint_bootstrap", "projection_owner"]

    def to_payload(self) -> dict[str, str]:
        return {
            "contract": SPEC_DEPENDENCY_PROJECTION_QUEUE_CONTRACT,
            "mutation_event_id": self.mutation_event_id,
            "mutation_event_type": self.mutation_event_type,
            "dependency_id": self.dependency_id,
            "projection_owner_spec_id": self.projection_owner_spec_id,
            "target_role": self.target_role,
        }


_SPEC_DEPENDENCY_PROJECTION_QUEUE_KEYS = frozenset(
    {
        "contract",
        "mutation_event_id",
        "mutation_event_type",
        "dependency_id",
        "projection_owner_spec_id",
        "target_role",
    }
)


def parse_spec_dependency_projection_queue_metadata(
    payload: object,
) -> SpecDependencyProjectionQueueMetadata | None:
    """Parse only the exact v1 ownership contract; drift is non-observable."""

    if not isinstance(payload, Mapping):
        return None
    if set(payload) != _SPEC_DEPENDENCY_PROJECTION_QUEUE_KEYS:
        return None
    if payload.get("contract") != SPEC_DEPENDENCY_PROJECTION_QUEUE_CONTRACT:
        return None
    values = {
        key: payload.get(key)
        for key in (
            "mutation_event_id",
            "mutation_event_type",
            "dependency_id",
            "projection_owner_spec_id",
            "target_role",
        )
    }
    if any(not isinstance(value, str) or not value for value in values.values()):
        return None
    event_type = str(values["mutation_event_type"])
    role = str(values["target_role"])
    if event_type not in SPEC_DEPENDENCY_PROJECTION_EVENT_TYPES or role not in {
        "endpoint_bootstrap",
        "projection_owner",
    }:
        return None
    return SpecDependencyProjectionQueueMetadata(
        mutation_event_id=str(values["mutation_event_id"]),
        mutation_event_type=event_type,
        dependency_id=str(values["dependency_id"]),
        projection_owner_spec_id=str(values["projection_owner_spec_id"]),
        target_role=role,
    )


class RelationalEffectsProviderMissing(RuntimeError):
    """Raised when a required relational effects provider is not registered."""

    code = "relational_effects_provider_missing"

    def __init__(self) -> None:
        super().__init__(
            "relational_effects_provider_missing: required provider not supplied"
        )


@dataclass(frozen=True, slots=True)
class ConsolidationQueueUpsert:
    board_id: str
    artifact_type: str
    artifact_id: str
    priority: str
    source: str
    triggered_by_event: str
    payload: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class KGTickRunUpsert:
    tick_id: str
    started_at: datetime
    completed_at: datetime
    nodes_recomputed: int
    duration_ms: float
    boards_processed: int
    boards_failed: int = 0
    error: str | None = None


@runtime_checkable
class RelationalEffectsPort(Protocol):
    """Logical relational operations required by core runtime handlers."""

    async def count_active_consolidation_queue(
        self,
        session: Any,
        *,
        board_id: str,
    ) -> int: ...

    async def upsert_consolidation_queue_unless_tombstoned(
        self,
        session: Any,
        upsert: ConsolidationQueueUpsert,
    ) -> bool:
        """Atomically admit legacy work unless fenced.

        Return ``True`` only when a row is inserted or a terminal row is
        reopened. Active-row coalescing and tombstone suppression return
        ``False`` without mutating the queue.
        """
        ...

    async def list_board_ids(self, session: Any) -> Sequence[str]: ...

    async def is_global_recovery_active(self, session: Any) -> bool:
        """Return durable global-recovery activity without mutating it."""
        ...

    async def fence_kg_tick_publication(self, session: Any) -> bool:
        """Fence the caller transaction against recovery admission.

        The implementation must acquire a transaction-scoped writer fence
        before returning whether recovery is active. Callers must keep the
        transaction short and publish the tick event before commit/rollback.
        """
        ...

    async def read_latest_kg_tick_completed_at(
        self,
        session: Any,
    ) -> datetime | None: ...

    async def upsert_kg_tick_run(
        self,
        session: Any,
        upsert: KGTickRunUpsert,
    ) -> None: ...


_RUNTIME_KEY = "ports.relational_effects.port"


def register_relational_effects_port(port: RelationalEffectsPort) -> None:
    """Register the edition-owned relational effects port."""

    register_runtime_value(_RUNTIME_KEY, port)


def get_relational_effects_port() -> RelationalEffectsPort:
    """Return the registered relational effects port, fail-closed if absent."""

    port = resolve_runtime_value(_RUNTIME_KEY)
    if port is None:
        raise RelationalEffectsProviderMissing()
    return port


def reset_relational_effects_port_for_tests() -> None:
    """Drop the registered port for test isolation."""

    reset_runtime_values(_RUNTIME_KEY)


__all__ = [
    "ConsolidationQueueUpsert",
    "KGTickRunUpsert",
    "RelationalEffectsPort",
    "RelationalEffectsProviderMissing",
    "SPEC_DEPENDENCY_PROJECTION_EVENT_TYPES",
    "SPEC_DEPENDENCY_PROJECTION_QUEUE_CONTRACT",
    "SpecDependencyProjectionQueueMetadata",
    "get_relational_effects_port",
    "parse_spec_dependency_projection_queue_metadata",
    "register_relational_effects_port",
    "reset_relational_effects_port_for_tests",
]
