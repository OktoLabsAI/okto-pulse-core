"""Public errors and protocol for edition-owned traceability reads."""

from __future__ import annotations

from typing import Any, Literal, Protocol, TypedDict, cast


LineageGraphView = Literal["lineage", "dependency"]
LineageGraphDependencyScope = Literal["selected", "lineage"]


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


def validate_lineage_graph_view(view: object) -> LineageGraphView:
    """Return one supported view or fail before dispatching to an adapter."""

    if view not in ("lineage", "dependency"):
        raise TraceabilityReadError(
            "invalid_lineage_graph_view",
            "Lineage graph view must be 'lineage' or 'dependency'.",
            status_code=400,
        )
    return cast(LineageGraphView, view)


def validate_lineage_graph_dependency_scope(
    scope: object,
) -> LineageGraphDependencyScope:
    """Return one supported dependency scope before adapter dispatch."""

    if scope not in ("selected", "lineage"):
        raise TraceabilityReadError(
            "invalid_lineage_graph_dependency_scope",
            "Lineage graph dependency scope must be 'selected' or 'lineage'.",
            status_code=400,
        )
    return cast(LineageGraphDependencyScope, scope)


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
        view: LineageGraphView = "lineage",
        dependency_scope: LineageGraphDependencyScope = "selected",
    ) -> dict[str, Any]: ...


__all__ = [
    "CodeTraceabilityReportSummary",
    "LineageGraphDependencyScope",
    "LineageGraphView",
    "TraceabilityReadError",
    "TraceabilityReadPort",
    "TraceabilityReport",
    "validate_lineage_graph_dependency_scope",
    "validate_lineage_graph_view",
]
