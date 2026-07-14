"""Pure rebuild source and ingestion contracts.

Concrete source enumeration and ingestion adapters live outside the core
application boundary. This module carries only structural Protocols, DTO aliases
and structured errors shared by core orchestration and edition adapters.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol


BoardSourceRow = dict[str, Any]
RebuildSourceResolver = Callable[[Any], Sequence[Mapping[str, Any]]]
RebuildStepAdapter = Callable[[Any], Any]


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


class BoardSourceReader(Protocol):
    """Reads raw SDLC source rows for a board."""

    def fetch(self, board_id: str) -> list[BoardSourceRow]:
        """Return raw source rows for ``board_id``."""


class RebuildIngestionPort(Protocol):
    """Factory for the synchronous rebuild step adapter."""

    def build_step_adapter(
        self,
        source_resolver: RebuildSourceResolver,
    ) -> RebuildStepAdapter:
        ...


RebuildStepAdapterFactory = RebuildIngestionPort


__all__ = [
    "BoardSourceReader",
    "BoardSourceRow",
    "InvalidArtifactTypeError",
    "RebuildIngestionPort",
    "RebuildSourceResolver",
    "RebuildStepAdapter",
    "RebuildStepAdapterFactory",
    "SourceReadError",
    "SourceReadFailure",
    "SourceUnavailableError",
]
