"""MCP instruction provider contracts.

The core MCP server consumes this provider surface instead of knowing
deployment-owned prompt paths. Editions can register a provider at composition
time while core keeps a bundled fallback.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable


@runtime_checkable
class McpInstructionProvider(Protocol):
    """A source for the command-catalog session instructions text."""

    @property
    def provider_id(self) -> str: ...

    def load_instructions(self) -> str:
        """Return instructions text, or an empty string when unavailable."""
        ...


@dataclass(frozen=True)
class StaticMcpInstructionProvider:
    """Instruction provider backed by an in-memory string."""

    provider_id: str
    content: str

    def load_instructions(self) -> str:
        return self.content


__all__ = [
    "McpInstructionProvider",
    "StaticMcpInstructionProvider",
]
