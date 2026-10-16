"""Pure rebuild source and ingestion contracts.

Concrete source enumeration and ingestion adapters live outside the core
application boundary. This module carries only structural Protocols, DTO aliases
and structured errors shared by core orchestration and edition adapters.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
import math
from typing import Any, Literal, Protocol, overload


BoardSourceRow = dict[str, Any]
BoardSourceSnapshotCause = Literal[
    "db_missing",
    "table_missing",
    "realm_incomplete",
]
RebuildSourceResolver = Callable[[Any], Sequence[Mapping[str, Any]]]
RebuildStepAdapter = Callable[[Any], Any]


@dataclass(frozen=True, slots=True)
class BoardSourceSnapshot(Sequence[BoardSourceRow]):
    """One explicit, completeness-qualified board-source census.

    The sequence surface is a temporary compatibility bridge for complete
    snapshots only.  An incomplete census is deliberately non-iterable so a
    legacy consumer cannot silently reinterpret source unavailability as an
    authoritative empty board.
    """

    rows: tuple[BoardSourceRow, ...] = ()
    complete: bool = True
    cause: BoardSourceSnapshotCause | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "rows", tuple(self.rows))
        if self.complete and self.cause is not None:
            raise ValueError("complete source snapshot cannot have a cause")
        if not self.complete and self.cause is None:
            raise ValueError("incomplete source snapshot requires a cause")

    @overload
    def __getitem__(self, index: int) -> BoardSourceRow: ...

    @overload
    def __getitem__(self, index: slice) -> tuple[BoardSourceRow, ...]: ...

    def __getitem__(
        self,
        index: int | slice,
    ) -> BoardSourceRow | tuple[BoardSourceRow, ...]:
        self._require_complete()
        return self.rows[index]

    def __len__(self) -> int:
        self._require_complete()
        return len(self.rows)

    def _require_complete(self) -> None:
        if not self.complete:
            raise SourceUnavailableError(
                "board source snapshot is incomplete",
                cause_type=str(self.cause or "unknown"),
            )


@dataclass(slots=True)
class SourceReadError(Exception):
    """Structured source-read failure raised by edition adapters."""

    code: str
    message: str
    cause_type: str | None = None

    def __str__(self) -> str:
        detail = f"{self.code}: {self.message}"
        if self.cause_type:
            return f"{detail} ({self.cause_type})"
        return detail


class SourceUnavailableError(SourceReadError):
    def __init__(self, message: str, *, cause_type: str | None = None) -> None:
        super().__init__("source_unavailable", message, cause_type)


class SourceReadFailure(SourceReadError):
    def __init__(self, message: str, *, cause_type: str | None = None) -> None:
        super().__init__("read_error", message, cause_type)


class InvalidArtifactTypeError(SourceReadError):
    def __init__(self, message: str, *, cause_type: str | None = None) -> None:
        super().__init__("invalid_artifact_type", message, cause_type)


@dataclass(frozen=True, slots=True)
class SourceObservationBudget:
    """Aggregate relational read limits; no pagination or partial source census."""

    max_rows: int = 10000
    max_bytes: int = 4 * 1024 * 1024
    timeout_seconds: float = 0.35

    def __post_init__(self) -> None:
        if type(self.max_rows) is not int or not 1 <= self.max_rows <= 10000:
            raise ValueError("invalid_source_observation_rows")
        if type(self.max_bytes) is not int or not 1024 <= self.max_bytes <= 16 * 1024 * 1024:
            raise ValueError("invalid_source_observation_bytes")
        if (type(self.timeout_seconds) not in (int, float)
                or not math.isfinite(self.timeout_seconds)
                or not 0 < self.timeout_seconds <= 5):
            raise ValueError("invalid_source_observation_timeout")


class BoardSourceReader(Protocol):
    """Reads raw SDLC source rows for a board."""

    def fetch(
        self, board_id: str, *, observation_budget: SourceObservationBudget | None = None
    ) -> BoardSourceSnapshot:
        """Return a complete snapshot or refuse; never ignore an observation budget."""


class RebuildIngestionPort(Protocol):
    """Factory for the synchronous rebuild step adapter."""

    def build_step_adapter(
        self,
        source_resolver: RebuildSourceResolver,
    ) -> RebuildStepAdapter:
        ...


RebuildStepAdapterFactory = RebuildIngestionPort


__all__ = [
    "SourceObservationBudget",
    "BoardSourceReader",
    "BoardSourceRow",
    "BoardSourceSnapshot",
    "BoardSourceSnapshotCause",
    "InvalidArtifactTypeError",
    "RebuildIngestionPort",
    "RebuildSourceResolver",
    "RebuildStepAdapter",
    "RebuildStepAdapterFactory",
    "SourceReadError",
    "SourceReadFailure",
    "SourceUnavailableError",
]
