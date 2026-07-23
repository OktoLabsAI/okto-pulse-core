"""Shared REST/MCP projections for selective Knowledge propagation v2."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast

from okto_pulse.core.domain.knowledge_selection import (
    KnowledgeAssignmentState,
    KnowledgeSelectionState,
    KnowledgeTargetType,
)
from okto_pulse.core.models.knowledge_propagation import (
    CardCreateKnowledgeMutationResponse,
    DeriveSpecKnowledgeMutationResponse,
    KnowledgeAssignmentTechnicalProjection,
    KnowledgeMutationAssignmentResponse,
    KnowledgeMutationResponse,
    KnowledgeRefreshItemResponse,
    KnowledgeRefreshResponse,
    KnowledgeTechnicalReadResponse,
)


def _result_parts(result: Any) -> tuple[Any, Any]:
    receipt = getattr(result, "receipt", None)
    result_v2 = getattr(result, "result_v2", None)
    if receipt is None or result_v2 is None:
        raise TypeError("knowledge_propagation_use_case_result_invalid")
    return receipt, result_v2


def _selection_state(result_v2: Any) -> KnowledgeSelectionState:
    state = result_v2.selection_state
    if not isinstance(state, KnowledgeSelectionState):
        raise ValueError("knowledge_propagation_selection_state_missing")
    return state


def project_knowledge_propagation_error(error: Exception) -> dict[str, object]:
    """Project the transport-neutral error envelope shared by REST and MCP."""

    code = str(getattr(error, "code", "knowledge_propagation_failed"))
    detail = str(getattr(error, "detail", str(error)))
    return {
        "error": code,
        "code": code,
        "detail": detail,
        "details": dict(getattr(error, "details", {}) or {}),
        "retryable": (
            code == "knowledge_creation_race"
            or bool(getattr(error, "retryable", False))
        ),
    }


def project_knowledge_mutation_response(result: Any) -> KnowledgeMutationResponse:
    receipt, result_v2 = _result_parts(result)
    return KnowledgeMutationResponse(
        target_type=cast(KnowledgeTargetType, result_v2.target.target_type).value,
        target_id=result_v2.target.target_id,
        operation_id=result_v2.operation_id,
        revision=result_v2.revision,
        replayed=receipt.replayed,
        selection_state=_selection_state(result_v2),
        assignments=[
            KnowledgeMutationAssignmentResponse(
                root_knowledge_id=assignment.revision_stamp.root_id,
                source_knowledge_id=assignment.source_knowledge_id,
                mode=assignment.mode,
                state=assignment.state,
                stale=assignment.state is KnowledgeAssignmentState.STALE,
            )
            for assignment in result_v2.assignments
        ],
    )


def project_derive_spec_response(
    result: Any,
) -> DeriveSpecKnowledgeMutationResponse:
    projected = project_knowledge_mutation_response(result)
    if projected.target_type != KnowledgeTargetType.SPEC.value:
        raise ValueError("knowledge_propagation_target_type_invalid")
    return DeriveSpecKnowledgeMutationResponse(
        **projected.model_dump(),
        spec_id=projected.target_id,
    )


def project_card_create_response(
    result: Any,
) -> CardCreateKnowledgeMutationResponse:
    receipt, result_v2 = _result_parts(result)
    creation_result = result_v2.creation_result
    card = creation_result.get("card") if creation_result else None
    if not isinstance(card, Mapping):
        raise ValueError("knowledge_propagation_creation_result_missing")
    return CardCreateKnowledgeMutationResponse(
        card=dict(card),
        operation_id=result_v2.operation_id,
        revision=result_v2.revision,
        replayed=receipt.replayed,
        selection_state=_selection_state(result_v2),
        assignments=[
            KnowledgeMutationAssignmentResponse(
                root_knowledge_id=assignment.revision_stamp.root_id,
                source_knowledge_id=assignment.source_knowledge_id,
                mode=assignment.mode,
                state=assignment.state,
                stale=assignment.state is KnowledgeAssignmentState.STALE,
            )
            for assignment in result_v2.assignments
        ],
    )


def project_refresh_response(result: Any) -> KnowledgeRefreshResponse:
    receipt, result_v2 = _result_parts(result)
    refreshed_roots = set(result_v2.refreshed_knowledge_ids)
    by_root = {
        assignment.revision_stamp.root_id: assignment
        for assignment in result_v2.assignments
        if assignment.revision_stamp.root_id in refreshed_roots
    }
    if set(by_root) != refreshed_roots:
        raise ValueError("knowledge_propagation_refresh_result_incomplete")
    return KnowledgeRefreshResponse(
        operation_id=result_v2.operation_id,
        revision=result_v2.revision,
        replayed=receipt.replayed,
        refreshed=[
            KnowledgeRefreshItemResponse(
                root_knowledge_id=root_id,
                source_revision=cast(
                    str,
                    by_root[root_id].revision_stamp.source_revision,
                ),
                source_content_sha256=cast(
                    str,
                    by_root[root_id].revision_stamp.source_content_sha256,
                ),
            )
            for root_id in sorted(refreshed_roots)
        ],
    )


def project_technical_read_response(
    read_result: Any,
) -> KnowledgeTechnicalReadResponse:
    assignments = [
        KnowledgeAssignmentTechnicalProjection(
            root_knowledge_id=item.assignment.revision_stamp.root_id,
            mode=item.assignment.mode,
            origin_class=item.assignment.origin_class.value,
            state=item.state.value,
            stale=item.state is KnowledgeAssignmentState.STALE,
        )
        for item in read_result.resolved_assignments
    ]
    return KnowledgeTechnicalReadResponse(
        revision=read_result.scope_revision,
        selection_state=read_result.selection_state,
        assignments=assignments,
    )


__all__ = [
    "project_card_create_response",
    "project_derive_spec_response",
    "project_knowledge_propagation_error",
    "project_knowledge_mutation_response",
    "project_refresh_response",
    "project_technical_read_response",
]
