"""Content ingestion port for MCP tools.

Core tools may consume inline content directly. Concrete source resolution
(local files, HTTP URLs, object stores) belongs to runtime adapters and is
reached only through ``ContentIngestionResolver``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable


@dataclass(frozen=True)
class IngestedBinaryContent:
    data: bytes
    mime_type: str | None = None
    source: str = "inline"


@dataclass(frozen=True)
class IngestedTextContent:
    text: str
    mime_type: str | None = "text/plain"
    source: str = "inline"


class ContentIngestionError(ValueError):
    """Structured, non-leaky ingestion failure."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


@runtime_checkable
class ContentIngestionResolver(Protocol):
    async def resolve_text(
        self, reference: str, *, max_bytes: int
    ) -> IngestedTextContent:
        """Resolve a text content reference supplied by a runtime adapter."""

    async def resolve_binary(
        self, reference: str, *, max_bytes: int
    ) -> IngestedBinaryContent:
        """Resolve a binary content reference supplied by a runtime adapter."""


__all__ = [
    "ContentIngestionError",
    "ContentIngestionResolver",
    "IngestedBinaryContent",
    "IngestedTextContent",
]
