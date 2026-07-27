"""MCP replay trace sink port.

The core owns the MCP trace record semantics, but concrete persistence belongs
to an edition adapter. Community can persist JSONL locally; SaaS can route the
same records elsewhere.
"""

from __future__ import annotations

from collections.abc import Awaitable, Mapping
from typing import Any, Protocol, runtime_checkable

McpTraceRecord = Mapping[str, Any]
TraceWriteResult = Awaitable[None] | None


@runtime_checkable
class McpTraceSink(Protocol):
    """Persistence boundary for per-call MCP replay trace records."""

    def write_trace(
        self,
        session_id: str,
        record: McpTraceRecord,
    ) -> TraceWriteResult:
        """Persist one already JSON-safe MCP trace record."""
        ...


__all__ = ["McpTraceRecord", "McpTraceSink", "TraceWriteResult"]
