"""Public errors and protocol for edition-owned traceability reads."""

from __future__ import annotations

from typing import Any, Protocol, TypedDict


class CodeTraceabilityReportSummary(TypedDict):
    evidence_total: int
    evidence_linked: int
    targets_total: int
    targets_resolved: int
    targets_outdated: int
    high_overlaps: int


class TraceabilityReport(TypedDict, total=False):
    """Open report envelope with a stable Code Traceability extension."""

    board_id: str
    filters: dict[str, Any]
    summary: dict[str, int]
    ideations: list[dict[str, Any]]
    orphan_specs: list[dict[str, Any]]
    code_traceability: CodeTraceabilityReportSummary


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
    ) -> TraceabilityReport: ...

    async def build_lineage_graph(
        self,
        context: object,
        board_id: str,
        *,
        entity_type: str,
        entity_id: str,
        include_artifacts: bool = True,
    ) -> dict[str, Any]: ...


__all__ = [
    "CodeTraceabilityReportSummary",
    "TraceabilityReadError",
    "TraceabilityReadPort",
    "TraceabilityReport",
]
