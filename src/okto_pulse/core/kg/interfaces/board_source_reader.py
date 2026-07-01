"""Board source reader port for KG source enumeration."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


BoardSourceRow = dict[str, Any]


@dataclass(frozen=True, slots=True)
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


__all__ = [
    "BoardSourceReader",
    "BoardSourceRow",
    "InvalidArtifactTypeError",
    "SourceReadError",
    "SourceReadFailure",
    "SourceUnavailableError",
]
