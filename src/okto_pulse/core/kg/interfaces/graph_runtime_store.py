"""Logical graph runtime capability port.

The core asks this port about graph availability, purge and storage footprint
without learning how an edition stores the graph. Local file paths and backend
file names belong to adapters.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Protocol, runtime_checkable

from okto_pulse.core.kg.interfaces.storage_ref import StorageRef


class GraphRuntimeObservationState(str, Enum):
    """Non-opening diagnosis returned by an edition-owned graph runtime.

    The error state intentionally does not claim that storage presence was
    confirmed: a stat deadline can expire before the adapter knows whether the
    path exists. ``reason_code`` on :class:`GraphRuntimeState` carries that
    distinction without expanding this deliberately small state machine.
    """

    PRESENT_READABLE_CANDIDATE = "present_readable_candidate"
    CONFIRMED_ABSENT = "confirmed_absent"
    PRESENT_UNREADABLE_OR_ERROR = "present_unreadable_or_error"
    PROVIDER_UNAVAILABLE = "provider_unavailable"


@dataclass(frozen=True)
class GraphRuntimeState:
    board_id: str
    storage_ref: StorageRef
    exists: bool
    status: str
    backend: str | None = None
    schema_version: str | None = None
    locked: bool = False
    quarantined: bool = False
    unavailable_reason: str | None = None
    details: dict[str, Any] = field(default_factory=dict)
    # Additive v0.3.0 diagnosis. Legacy adapters may omit these fields; Core
    # treats a legacy negative ``exists`` result as provider-unavailable rather
    # than upgrading it to a confirmed absence.
    state: GraphRuntimeObservationState | None = None
    generation: str | None = None
    reason_code: str | None = None
    observed_at: datetime | None = None

    @classmethod
    def from_observation(
        cls,
        *,
        board_id: str,
        storage_ref: StorageRef,
        state: GraphRuntimeObservationState,
        generation: str | None,
        reason_code: str,
        observed_at: datetime,
        backend: str | None = None,
        schema_version: str | None = None,
        locked: bool = False,
        quarantined: bool = False,
        details: dict[str, Any] | None = None,
    ) -> "GraphRuntimeState":
        """Build a four-state observation with binary compatibility fields."""

        normalized = GraphRuntimeObservationState(state)
        if normalized is GraphRuntimeObservationState.PRESENT_READABLE_CANDIDATE:
            exists = True
            status = "available"
            unavailable_reason = None
        elif normalized is GraphRuntimeObservationState.CONFIRMED_ABSENT:
            exists = False
            status = "absent"
            unavailable_reason = None
        else:
            exists = False
            status = "unavailable"
            unavailable_reason = reason_code
        return cls(
            board_id=board_id,
            storage_ref=storage_ref,
            exists=exists,
            status=status,
            backend=backend,
            schema_version=schema_version,
            locked=locked,
            quarantined=quarantined,
            unavailable_reason=unavailable_reason,
            details=dict(details or {}),
            state=normalized,
            generation=generation,
            reason_code=reason_code,
            observed_at=observed_at,
        )

    @property
    def normalized_state(self) -> GraphRuntimeObservationState:
        """Return a fail-closed diagnosis for new and legacy adapters.

        Legacy positive existence is sufficient to say that storage is a
        readable candidate. Legacy absence is not proof that a non-opening stat
        succeeded, so it remains unavailable until the Community adapter is
        migrated to the explicit four-state contract.
        """

        if self.state is not None:
            return GraphRuntimeObservationState(self.state)
        if self.exists:
            return GraphRuntimeObservationState.PRESENT_READABLE_CANDIDATE
        return GraphRuntimeObservationState.PROVIDER_UNAVAILABLE


@dataclass(frozen=True)
class GraphStorageFootprint:
    board_id: str
    storage_ref: StorageRef
    status: str
    source: str
    total_bytes: int | None = None
    primary_bytes: int | None = None
    sidecar_bytes: int | None = None
    configured_max_bytes: int | None = None
    percentage: float | None = None
    unavailable_reason: str | None = None


@dataclass(frozen=True)
class GraphPurgeResult:
    board_id: str
    removed: bool
    not_found: bool
    status: str
    reason: str
    backend: str | None = None
    error_code: str | None = None


@runtime_checkable
class GraphRuntimeStore(Protocol):
    def graph_state(
        self,
        board_id: str,
        *,
        generation: str | None = None,
    ) -> GraphRuntimeState:
        """Return the logical graph state for a board."""
        ...

    def exists(self, board_id: str) -> bool:
        """Return whether a board graph is present for runtime use."""
        ...

    def purge_board_graph(self, board_id: str, *, reason: str) -> GraphPurgeResult:
        """Purge board graph data through the edition-owned runtime."""
        ...

    def erase_board_graph(self, board_id: str, *, reason: str) -> GraphPurgeResult:
        """Irreversibly erase board graph data, including recovery copies."""
        ...

    def footprint(self, board_id: str) -> GraphStorageFootprint:
        """Return a logical storage footprint projection if the adapter has one."""
        ...


__all__ = [
    "GraphPurgeResult",
    "GraphRuntimeObservationState",
    "GraphRuntimeState",
    "GraphRuntimeStore",
    "GraphStorageFootprint",
]
