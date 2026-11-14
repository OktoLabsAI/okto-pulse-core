"""Semantic lifecycle port for edition-owned graph storage."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from okto_pulse.core.kg.interfaces.storage_ref import StorageRef


@dataclass(frozen=True)
class GraphHandle:
    """Typed result of making a board graph available."""

    board_id: str
    storage_ref: StorageRef
    opened: bool
    status: str
    locked: bool
    quarantined: bool
    # Opening a writer does not establish availability of independent readers.
    # Older providers must explicitly opt in before activation can report ready.
    readers_prepared: bool = False


@dataclass(frozen=True)
class RebuildReport:
    """Structured outcome of a connection-level rebuild."""

    board_id: str
    status: str  # "rebuilt" | "skipped" | "failed"
    steps: tuple[str, ...] = ()
    reason: str | None = None


@runtime_checkable
class PreparedGraphReaderObservation(Protocol):
    """Optional non-mutating observation of an already prepared Board reader.

    The provider pins one existing participant and revalidates its exact route.
    It must never cold-open, initialize, checkpoint or repair storage. None
    means no prepared participant exists; temporary admission contention raises
    GraphLockContention or GraphQueryTimeout, never None. Other refusal/route
    failures remain distinct and must not trigger an automatic retry. The
    returned availability is an observation, not a reservation or write proof.
    """

    def observe_prepared_reader(self, board_id: str) -> GraphHandle | None: ...


@dataclass(frozen=True)
class PurgeReport:
    """Structured outcome of a quarantine-then-clear purge."""

    board_id: str
    status: str  # "purged" | "noop"
    reason: str
    affected_storage_refs: tuple[StorageRef, ...] = ()
    quarantined: bool = False
    quarantine_ref: str | None = None


@dataclass(frozen=True)
class GraphLifecycleStepResult:
    """Typed result for one durability step in the safe-write policy."""

    ok: bool
    detail: str | None = None


@runtime_checkable
class GraphLifecycle(Protocol):
    async def open(self, board_id: str) -> GraphHandle:
        """Ensure the board's graph exists/openable; returns a GraphHandle."""
        ...

    async def close(self, board_id: str | None = None) -> None:
        """Release active runtime resources for one board or all boards."""
        ...

    async def rebuild(self, board_id: str) -> RebuildReport:
        """Recycle the board's graph handle (release + re-ensure).

        Runtime-session rebuild primitive; the full data-rebuild orchestration
        (kg rebuild) consumes this primitive rather than duplicating it.
        """
        ...

    async def purge(self, board_id: str, *, reason: str) -> PurgeReport:
        """Quarantine-then-clear the board's graph storage."""
        ...

    def apply_step(
        self,
        board_id: str,
        graph_type: str,
        step: str,
    ) -> GraphLifecycleStepResult:
        """Apply one named durability step without exposing driver primitives."""
        ...
