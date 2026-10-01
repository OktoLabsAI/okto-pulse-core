from dataclasses import replace

import pytest

from okto_pulse.core.domain.code_traceability import (
    CODE_TRACEABILITY_CONTEXT_COLLECTION_LIMITS,
    CodeEvidenceBaselinePresence, CodeEvidenceBaselineProvenance,
    CodeEvidenceSourceRole, CodeEvidenceSpecLink, CodeEvidenceSpecRelationType,
    CodeTraceabilityContext, CodeTraceabilityOmittedContent,
    CodeTraceabilityProjectionProfile, CodeTraceabilitySubjectType,
    DeliveryContext, ContextualInvestigationOutcomeV2,
    DirectSpecDeliveryContextProvenance, SpecEntityType,
    SpecDeliveryContextProvenance, build_source_context_summary_v2,
    source_context_evidence_item_v2,
)
from okto_pulse.core.models.schemas import CodeTraceabilitySettings
from okto_pulse.core.services.code_traceability_gate import CodeTraceabilityGateEvaluator
from test_code_traceability_application import NOW
from test_code_traceability_gate import _accepted, _evidence


async def _native_evidence():
    accepted, _, _, _ = await _accepted(
        subject_type=CodeTraceabilitySubjectType.REFINEMENT,
        subject_id="refinement-1", subject_version=3,
    )
    return _evidence(accepted.receipt, evidence_id="native-1", parent_version=3)


def _baseline(evidence) -> CodeEvidenceBaselineProvenance:
    dirty = evidence.workspace_state.declared_dirty
    return CodeEvidenceBaselineProvenance(
        presence=(
            CodeEvidenceBaselinePresence.PREEXISTING_WORKTREE
            if dirty
            else CodeEvidenceBaselinePresence.COMMITTED_SNAPSHOT
        ),
        workspace_state_id=evidence.workspace_state.workspace_state_id,
        provenance_note=("Present before delivery began." if dirty else None),
    )



def _contextual_evidence(
    evidence,
    *,
    evidence_id: str,
    source_role: CodeEvidenceSourceRole,
):
    return replace(
        evidence,
        id=evidence_id,
        source_role=source_role,
        relevance_summary="Authored source context.",
        scope_relation="same bounded delivery scope",
        source_origin="accepted repository baseline",
        interpretation_limit=(
            "Context only; it does not prove delivered behavior."
            if source_role
            in {
                CodeEvidenceSourceRole.EXISTING_SCAFFOLD,
                CodeEvidenceSourceRole.REFERENCE_PATTERN,
            }
            else None
        ),
        baseline_provenance=_baseline(evidence),
        context_contract_version=2,
    )



@pytest.mark.asyncio
async def test_contextual_coverage_excludes_context_only_evidence() -> None:
    native = await _native_evidence()
    current = _contextual_evidence(
        native,
        evidence_id="current-implementation",
        source_role=CodeEvidenceSourceRole.CURRENT_IMPLEMENTATION,
    )
    context_only = _contextual_evidence(
        native,
        evidence_id="existing-scaffold",
        source_role=CodeEvidenceSourceRole.EXISTING_SCAFFOLD,
    )
    provenance = SpecDeliveryContextProvenance(
        value=DeliveryContext.HYBRID,
        inherited_value=DeliveryContext.HYBRID,
        source_refinement_id="refinement-1",
        source_refinement_version=3,
    )
    summary = build_source_context_summary_v2(
        delivery_context=DeliveryContext.HYBRID,
        delivery_context_provenance=provenance,
        current_investigation_outcomes=(
            ContextualInvestigationOutcomeV2.EVIDENCE_APPLICABLE,
        ),
        evidence=(current, context_only),
    )
    link = CodeEvidenceSpecLink(
        id="link-current",
        board_id="board-1",
        spec_id="spec-1",
        evidence_id=current.id,
        entity_type=SpecEntityType.TECHNICAL_REQUIREMENT,
        entity_id="tr-1",
        relation_type=CodeEvidenceSpecRelationType.SUPPORTS,
        rationale="The implemented baseline supports this requirement.",
        evidence_content_sha256=current.content_sha256,
        source_refinement_version=3,
        spec_version=4,
        created_by="user-1",
        created_at=NOW,
    )
    context = CodeTraceabilityContext(
        board_id="board-1",
        subject_type=CodeTraceabilitySubjectType.SPEC,
        subject_id="spec-1",
        subject_version=4,
        profile=CodeTraceabilityProjectionProfile.DETAIL,
        evidence=(current, context_only),
        evidence_links=(link,),
        source_refinement_id="refinement-1",
        source_refinement_snapshot_id="snapshot-3",
        source_refinement_version=3,
        source_context=summary,
        source_context_items=(
            source_context_evidence_item_v2(current),
            source_context_evidence_item_v2(context_only),
        ),
    )
    payload = (
        CodeTraceabilityGateEvaluator()
        .project(
            context,
            CodeTraceabilitySettings(mode="advisory"),
        )
        .as_dict()
    )

    assert payload["contextual_evidence_coverage"] == {
        "total": 1,
        "linked": 1,
        "dispositioned": 0,
        "pending": 0,
        "pending_ids": [],
        "coverage_pct": 100.0,
        "projection_complete": True,
    }
    assert payload["coverage"]["total"] == 2
    assert payload["coverage"]["coverage_pct"] == 50.0



def test_contextual_coverage_greenfield_absence_is_not_applicable() -> None:
    provenance = DirectSpecDeliveryContextProvenance(
        value=DeliveryContext.GREENFIELD,
        source_spec_id="spec-greenfield",
        source_spec_version=1,
    )
    summary = build_source_context_summary_v2(
        delivery_context=DeliveryContext.GREENFIELD,
        delivery_context_provenance=provenance,
        current_investigation_outcomes=(
            ContextualInvestigationOutcomeV2.NO_RELEVANT_EXISTING_IMPLEMENTATION,
        ),
        evidence=(),
    )
    context = CodeTraceabilityContext(
        board_id="board-1",
        subject_type=CodeTraceabilitySubjectType.SPEC,
        subject_id="spec-greenfield",
        subject_version=1,
        profile=CodeTraceabilityProjectionProfile.SUMMARY,
        source_context=summary,
    )
    payload = (
        CodeTraceabilityGateEvaluator()
        .project(
            context,
            CodeTraceabilitySettings(mode="advisory"),
        )
        .as_dict()
    )

    coverage = payload["contextual_evidence_coverage"]
    assert coverage["total"] == 0
    assert coverage["coverage_pct"] is None
    assert coverage["projection_complete"] is True



@pytest.mark.asyncio
async def test_contextual_coverage_partial_and_incomplete_are_null() -> None:
    native = await _native_evidence()
    current = _contextual_evidence(
        native,
        evidence_id="current-implementation",
        source_role=CodeEvidenceSourceRole.CURRENT_IMPLEMENTATION,
    )
    provenance = SpecDeliveryContextProvenance(
        value=DeliveryContext.HYBRID,
        inherited_value=DeliveryContext.HYBRID,
        source_refinement_id="refinement-1",
        source_refinement_version=3,
    )

    def payload_for(
        *,
        outcomes: tuple[ContextualInvestigationOutcomeV2, ...],
        incomplete: bool = False,
    ) -> dict[str, object]:
        evidence = (current,)
        summary = build_source_context_summary_v2(
            delivery_context=DeliveryContext.HYBRID,
            delivery_context_provenance=provenance,
            current_investigation_outcomes=outcomes,
            evidence=evidence,
        )
        omitted = (
            (
                CodeTraceabilityOmittedContent(
                    collection="evidence",
                    hard_limit=CODE_TRACEABILITY_CONTEXT_COLLECTION_LIMITS["evidence"],
                    included_count=len(evidence),
                ),
            )
            if incomplete
            else ()
        )
        context = CodeTraceabilityContext(
            board_id="board-1",
            subject_type=CodeTraceabilitySubjectType.SPEC,
            subject_id="spec-1",
            subject_version=4,
            profile=CodeTraceabilityProjectionProfile.DETAIL,
            evidence=evidence,
            omitted_content_manifest=omitted,
            source_refinement_id="refinement-1",
            source_refinement_snapshot_id="snapshot-3",
            source_refinement_version=3,
            source_context=summary,
            source_context_items=tuple(
                source_context_evidence_item_v2(item) for item in evidence
            ),
        )
        return (
            CodeTraceabilityGateEvaluator()
            .project(
                context,
                CodeTraceabilitySettings(mode="advisory"),
            )
            .as_dict()["contextual_evidence_coverage"]
        )

    partial = payload_for(
        outcomes=(ContextualInvestigationOutcomeV2.PARTIAL,),
    )
    incomplete = payload_for(
        outcomes=(ContextualInvestigationOutcomeV2.EVIDENCE_APPLICABLE,),
        incomplete=True,
    )

    assert partial["coverage_pct"] is None
    assert incomplete["coverage_pct"] is None
    assert incomplete["projection_complete"] is False

