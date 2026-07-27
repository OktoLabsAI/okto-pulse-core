"""Public errors and protocol for edition-owned traceability reads."""

from __future__ import annotations

from typing import Any, Protocol


class TraceabilityReadError(Exception):
    """Transport-neutral contextual error for traceability reads."""

    def __init__(self, code: str, message: str, *, status_code: int = 400) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code


class TraceabilityReadPort(Protocol):
    async def build_traceability_report(
        self,
        context: object,
        board_id: str,
        *,
        ideation_id: str = "",
        spec_id: str = "",
        include_artifacts: bool = True,
    ) -> dict[str, Any]: ...

    async def build_lineage_graph(
        self,
        context: object,
        board_id: str,
        *,
        entity_type: str,
        entity_id: str,
        include_artifacts: bool = True,
    ) -> dict[str, Any]: ...


__all__ = ["TraceabilityReadError", "TraceabilityReadPort"]
