"""MCP instruction provider contracts.

The core MCP server consumes this provider surface instead of knowing
deployment-owned prompt paths. Editions can register a provider at composition
time while core keeps a bundled fallback.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
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
class StaticFileMcpInstructionProvider:
    """Instruction provider backed by a file confined to ``base_dir``."""

    provider_id: str
    base_dir: Path
    relative_path: str

    def load_instructions(self) -> str:
        base = self.base_dir.resolve()
        candidate = (base / self.relative_path).resolve()
        try:
            candidate.relative_to(base)
        except ValueError:
            return ""
        return candidate.read_text(encoding="utf-8") if candidate.exists() else ""


@dataclass(frozen=True)
class StaticMcpInstructionProvider:
    """Instruction provider backed by an in-memory string."""

    provider_id: str
    content: str

    def load_instructions(self) -> str:
        return self.content


__all__ = [
    "McpInstructionProvider",
    "StaticFileMcpInstructionProvider",
    "StaticMcpInstructionProvider",
]
