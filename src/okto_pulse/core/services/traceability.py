"""Traceability application facade over an edition-owned read adapter."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from okto_pulse.core.domain.code_traceability import (
    CodeInvestigationHeadState,
    CodeTraceabilityContext,
    CodeTraceabilityContractError,
    CodeTraceabilityLifecycleStatus,
    CodeTraceabilitySubjectType,
    ImplementationTargetResolutionState,
    ImplementationTargetRole,
    TargetOverlapSeverity,
)

from okto_pulse.core.ports.relational_services import resolve_traceability_adapter
from okto_pulse.core.ports.traceability import (
    CodeTraceabilityReportSummary,
    LineageGraphDependencyScope,
    LineageGraphView,
    TraceabilityReadError,
    validate_lineage_graph_dependency_scope,
    validate_lineage_graph_view,
)
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


def project_code_traceability_report(
    contexts: Iterable[CodeTraceabilityContext],
) -> CodeTraceabilityReportSummary:
    """Aggregate the §19 report from structured Pulse projections only.

    Editions materialize bounded ``SUMMARY`` contexts; Core owns all counting
    and currentness semantics.  No source repository, provider or filesystem is
    consulted by this projection.
    """

    values = tuple(contexts)
    board_ids = {item.board_id for item in values}
    if len(board_ids) > 1:
        raise CodeTraceabilityContractError(
            "code_traceability_report_board_scope_mismatch"
        )
    report_collections = {
        "heads",
        "evidence",
        "evidence_links",
        "targets",
        "resolutions",
        "overlaps",
    }
    omitted_collections = tuple(
        sorted(
            {
                omitted.collection
                for context in values
                for omitted in context.omitted_content_manifest
                if omitted.collection in report_collections
            }
        )
    )
    if omitted_collections:
        raise CodeTraceabilityContractError(
            "code_traceability_report_incomplete",
            details={"collections": list(omitted_collections)},
        )

    evidence = {
        item.id: item
        for context in values
        for item in context.evidence
        if item.lifecycle_status is CodeTraceabilityLifecycleStatus.ACTIVE
    }
    linked_evidence_ids = {
        link.evidence_id
        for context in values
        for link in context.evidence_links
        if link.evidence_id in evidence
    }
    targets = {
        item.id: item
        for context in values
        for item in context.targets
        if item.lifecycle_status is CodeTraceabilityLifecycleStatus.ACTIVE
    }
    resolutions = {
        item.id: item
        for context in values
        for item in context.resolutions
    }
    heads = {
        item.source_ref: item
        for context in values
        for item in context.heads
    }
    card_versions = {
        context.subject_id: context.subject_version
        for context in values
        if context.subject_type is CodeTraceabilitySubjectType.CARD
    }

    resolved = 0
    outdated = 0
    for target in targets.values():
        resolution = (
            resolutions.get(target.current_resolution_id)
            if target.current_resolution_id is not None
            else None
        )
        if resolution is None:
            continue
        head = heads.get(target.source_ref)
        expected_card_version = card_versions.get(target.card_id)
        current = (
            resolution.target_id == target.id
            and resolution.target_revision == target.revision
            and expected_card_version is not None
            and resolution.subject_version == expected_card_version
            and resolution.source_ref == target.source_ref
            and head is not None
            and head.state is CodeInvestigationHeadState.CURRENT
            and head.current_receipt_id == resolution.investigation_receipt_id
        )
        if not current:
            outdated += 1
            continue
        accepted_state = resolution.state in {
            ImplementationTargetResolutionState.RESOLVED,
            ImplementationTargetResolutionState.MOVED,
        } or (
            target.role is ImplementationTargetRole.CREATE
            and resolution.state is ImplementationTargetResolutionState.MISSING
            and resolution.reason_code == "missing_expected"
        )
        if accepted_state:
            resolved += 1

    high_overlaps = {
        (
            item.target_a_id,
            item.target_b_id,
            item.resolution_a_id,
            item.resolution_b_id,
        )
        for context in values
        for item in context.overlaps
        if item.severity is TargetOverlapSeverity.HIGH
        and item.target_a_id in targets
        and item.target_b_id in targets
    }
    return {
        "evidence_total": len(evidence),
        "evidence_linked": len(linked_evidence_ids),
        "targets_total": len(targets),
        "targets_resolved": resolved,
        "targets_outdated": outdated,
        "high_overlaps": len(high_overlaps),
    }


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
    view: LineageGraphView = "lineage",
    dependency_scope: LineageGraphDependencyScope = "selected",
) -> dict[str, Any]:
    normalized_view = validate_lineage_graph_view(view)
    normalized_dependency_scope = validate_lineage_graph_dependency_scope(
        dependency_scope
    )
    if normalized_view != "dependency" and normalized_dependency_scope != "selected":
        raise TraceabilityReadError(
            "dependency_scope_requires_dependency_view",
            "Lineage dependency scope is available only for dependency view.",
            status_code=400,
        )
    adapter = resolve_traceability_adapter()
    if normalized_view == "dependency":
        dependency_kwargs = (
            {"dependency_scope": normalized_dependency_scope}
            if normalized_dependency_scope != "selected"
            else {}
        )
        return await adapter.build_lineage_graph(
            context,
            board_id,
            entity_type=entity_type,
            entity_id=entity_id,
            include_artifacts=include_artifacts,
            view=normalized_view,
            **dependency_kwargs,
        )
    return await adapter.build_lineage_graph(
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
    "project_code_traceability_report",
    "resolve_lineage_root",
    "resolve_root_ideation_id",
    "spec_coverage_summary",
]
