"""Traceability application facade over an edition-owned read adapter."""

from __future__ import annotations

from typing import Any

from okto_pulse.core.ports.relational_services import resolve_traceability_adapter
from okto_pulse.core.ports.traceability import TraceabilityReadError
from okto_pulse.core.services.analytics_service import spec_coverage_summary


async def build_traceability_report(
    context: Any,
    board_id: str,
    *,
    ideation_id: str = "",
    spec_id: str = "",
    include_artifacts: bool = True,
) -> dict[str, Any]:
    return await resolve_traceability_adapter().build_traceability_report(
        context,
        board_id,
        ideation_id=ideation_id,
        spec_id=spec_id,
        include_artifacts=include_artifacts,
    )


async def resolve_root_ideation_id(
    context: Any,
    board_id: str,
    *,
    entity_type: str,
    entity_id: str,
):
    return await resolve_traceability_adapter().resolve_root_ideation_id(
        context,
        board_id,
        entity_type=entity_type,
        entity_id=entity_id,
    )


async def resolve_lineage_root(
    context: Any,
    board_id: str,
    *,
    entity_type: str,
    entity_id: str,
):
    return await resolve_traceability_adapter().resolve_lineage_root(
        context,
        board_id,
        entity_type=entity_type,
        entity_id=entity_id,
    )


async def build_lineage_graph(
    context: Any,
    board_id: str,
    *,
    entity_type: str,
    entity_id: str,
    include_artifacts: bool = True,
) -> dict[str, Any]:
    return await resolve_traceability_adapter().build_lineage_graph(
        context,
        board_id,
        entity_type=entity_type,
        entity_id=entity_id,
        include_artifacts=include_artifacts,
    )


__all__ = [
    "TraceabilityReadError",
    "build_lineage_graph",
    "build_traceability_report",
    "resolve_lineage_root",
    "resolve_root_ideation_id",
    "spec_coverage_summary",
]
