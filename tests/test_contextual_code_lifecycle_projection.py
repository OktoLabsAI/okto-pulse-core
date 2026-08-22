from __future__ import annotations

from dataclasses import replace

import pytest

from okto_pulse.core.domain.code_traceability import (
    CodeEvidenceSourceRole,
    CodeInvestigationReceiptCurrentness,
    CodeTraceabilityContext,
    CodeTraceabilityContextScope,
    CodeTraceabilityContractError,
    CodeTraceabilityLifecycleStatus,
    CodeTraceabilityProjectionProfile,
    CodeTraceabilitySubjectType,
    ContextualInvestigationOutcomeV2,
    DeliveryContext,
    RefinementDeliveryContextProvenance,
    SpecDeliveryContextProvenance,
    aggregate_current_contextual_investigation_outcome_v2,
    build_source_context_summary_v2,
    code_investigation_receipt_currentness,
)
from okto_pulse.core.models.schemas import CodeTraceabilitySettings
from okto_pulse.core.services.code_traceability_gate import (
    CodeTraceabilityGateEvaluator,
    CodeTraceabilityGatePhase,
)
from test_code_traceability_gate import _evidence
from test_contextual_code_investigation_outcomes import (
    _gate_context,
    _submit_contextual_receipt,
)


@pytest.mark.parametrize(
    ("outcomes", "expected"),
    (
        ((), None),
        ((None,), None),
        (
            (ContextualInvestigationOutcomeV2.NO_RELEVANT_EXISTING_IMPLEMENTATION,),
            ContextualInvestigationOutcomeV2.NO_RELEVANT_EXISTING_IMPLEMENTATION,
        ),
        (
            (
                ContextualInvestigationOutcomeV2.NO_RELEVANT_EXISTING_IMPLEMENTATION,
                ContextualInvestigationOutcomeV2.EVIDENCE_APPLICABLE,
            ),
            ContextualInvestigationOutcomeV2.EVIDENCE_APPLICABLE,
        ),
        (
            (
                ContextualInvestigationOutcomeV2.EVIDENCE_APPLICABLE,
                ContextualInvestigationOutcomeV2.PARTIAL,
            ),
            ContextualInvestigationOutcomeV2.PARTIAL,
        ),
        (
            (
                ContextualInvestigationOutcomeV2.PARTIAL,
                ContextualInvestigationOutcomeV2.UNAVAILABLE,
            ),
            ContextualInvestigationOutcomeV2.UNAVAILABLE,
        ),
    ),
)
def test_current_outcome_aggregation_has_closed_precedence(
    outcomes: tuple[ContextualInvestigationOutcomeV2 | None, ...],
    expected: ContextualInvestigationOutcomeV2 | None,
) -> None:
    assert aggregate_current_contextual_investigation_outcome_v2(outcomes) is expected


def test_refinement_and_spec_provenance_are_subject_typed() -> None:
    refinement_provenance = RefinementDeliveryContextProvenance(
        value=DeliveryContext.GREENFIELD,
        source_refinement_id="refinement-1",
        source_refinement_version=3,
    )
    summary = build_source_context_summary_v2(
        delivery_context=DeliveryContext.GREENFIELD,
        delivery_context_provenance=refinement_provenance,
        current_investigation_outcomes=(None,),
        evidence=(),
    )
    CodeTraceabilityContext(
        board_id="board-1",
        subject_type=CodeTraceabilitySubjectType.REFINEMENT,
        subject_id="refinement-1",
        subject_version=3,
        profile=CodeTraceabilityProjectionProfile.SUMMARY,
        source_context=summary,
    )

    with pytest.raises(
        CodeTraceabilityContractError,
        match="code_traceability_context_source_context_provenance_invalid",
    ):
        CodeTraceabilityContext(
            board_id="board-1",
            subject_type=CodeTraceabilitySubjectType.SPEC,
            subject_id="spec-1",
            subject_version=4,
            profile=CodeTraceabilityProjectionProfile.SUMMARY,
            source_context=summary,
        )


@pytest.mark.asyncio
async def test_source_context_counts_only_active_factual_roles() -> None:
    submitted, _service, _store, _clock = await _submit_contextual_receipt(
        outcome=ContextualInvestigationOutcomeV2.EVIDENCE_APPLICABLE,
        delivery_context=DeliveryContext.HYBRID,
    )
    receipt = submitted.receipt
    current = _evidence(receipt, evidence_id="current", parent_version=3)
    scaffold = replace(
        current,
        id="scaffold",
        source_role=CodeEvidenceSourceRole.EXISTING_SCAFFOLD,
        interpretation_limit="Scaffold only; it does not prove delivered behavior.",
    )
    constraint = replace(
        current,
        id="constraint",
        source_role=CodeEvidenceSourceRole.EXISTING_CONSTRAINT,
    )
    pattern = replace(
        current,
        id="pattern",
        source_role=CodeEvidenceSourceRole.REFERENCE_PATTERN,
        interpretation_limit="Pattern only; it does not prove delivered behavior.",
    )
    legacy = replace(
        current,
        id="legacy",
        source_role=CodeEvidenceSourceRole.UNCATEGORIZED_LEGACY,
        relevance_summary=None,
        scope_relation=None,
        source_origin=None,
        interpretation_limit=None,
        baseline_provenance=None,
        context_contract_version=None,
    )
    revoked = replace(
        current,
        id="revoked",
        lifecycle_status=CodeTraceabilityLifecycleStatus.REVOKED,
        revocation_reason="Superseded by a newer observation.",
    )
    provenance = RefinementDeliveryContextProvenance(
        value=DeliveryContext.HYBRID,
        source_refinement_id="refinement-1",
        source_refinement_version=3,
    )

    summary = build_source_context_summary_v2(
        delivery_context=DeliveryContext.HYBRID,
        delivery_context_provenance=provenance,
        current_investigation_outcomes=(receipt.contextual_outcome,),
        evidence=(current, scaffold, constraint, pattern, legacy, revoked),
    )

    assert summary.role_counts.current_implementation_count == 1
    assert summary.role_counts.existing_scaffold_count == 1
    assert summary.role_counts.existing_constraint_count == 1
    assert summary.role_counts.reference_pattern_count == 1
    assert summary.role_counts.uncategorized_legacy_count == 1
    assert summary.classification_state.classified_count == 4
    assert summary.items_not_current_implementation_count == 4
    assert summary.technical_details_available is True


def _source_context_for_subject(
    subject_type: CodeTraceabilitySubjectType,
):
    if subject_type is CodeTraceabilitySubjectType.REFINEMENT:
        provenance = RefinementDeliveryContextProvenance(
            value=DeliveryContext.GREENFIELD,
            source_refinement_id="refinement-1",
            source_refinement_version=3,
        )
    else:
        provenance = SpecDeliveryContextProvenance(
            value=DeliveryContext.GREENFIELD,
            inherited_value=DeliveryContext.GREENFIELD,
            source_refinement_id="refinement-1",
            source_refinement_version=3,
        )
    return build_source_context_summary_v2(
        delivery_context=DeliveryContext.GREENFIELD,
        delivery_context_provenance=provenance,
        current_investigation_outcomes=(
            ContextualInvestigationOutcomeV2.NO_RELEVANT_EXISTING_IMPLEMENTATION,
        ),
        evidence=(),
    )


@pytest.mark.parametrize(
    ("subject_type", "subject_id", "subject_version"),
    (
        (CodeTraceabilitySubjectType.REFINEMENT, "refinement-1", 3),
        (CodeTraceabilitySubjectType.SPEC, "spec-1", 4),
        (CodeTraceabilitySubjectType.CARD, "card-1", 5),
    ),
)
def test_source_context_projection_is_profile_stable_for_every_subject(
    subject_type: CodeTraceabilitySubjectType,
    subject_id: str,
    subject_version: int,
) -> None:
    source_context = _source_context_for_subject(subject_type)
    evaluator = CodeTraceabilityGateEvaluator()
    settings = CodeTraceabilitySettings(mode="advisory")
    projected: list[dict[str, object] | None] = []
    for profile, context_scope in (
        (
            CodeTraceabilityProjectionProfile.SUMMARY,
            CodeTraceabilityContextScope.DEFAULT,
        ),
        (
            CodeTraceabilityProjectionProfile.DETAIL,
            CodeTraceabilityContextScope.DEFAULT,
        ),
        (
            CodeTraceabilityProjectionProfile.FULL,
            CodeTraceabilityContextScope.DEFAULT,
        ),
        (
            CodeTraceabilityProjectionProfile.FULL,
            CodeTraceabilityContextScope.GATE,
        ),
    ):
        lineage = (
            {}
            if subject_type is CodeTraceabilitySubjectType.REFINEMENT
            else {
                "source_refinement_id": "refinement-1",
                "source_refinement_snapshot_id": "snapshot-1",
                "source_refinement_version": 3,
            }
        )
        context = CodeTraceabilityContext(
            board_id="board-1",
            subject_type=subject_type,
            subject_id=subject_id,
            subject_version=subject_version,
            profile=profile,
            context_scope=context_scope,
            source_context=source_context,
            **lineage,
        )
        projected.append(
            evaluator.project(context, settings).as_dict()["source_context"]
        )

    assert all(item == projected[0] for item in projected)
    assert projected[0] is not None
    assert projected[0]["delivery_context"] == "greenfield"
    assert projected[0]["investigation_outcome"] == (
        "no_relevant_existing_implementation"
    )


def test_legacy_source_context_remains_explicit_without_inference() -> None:
    summary = build_source_context_summary_v2(
        delivery_context=None,
        delivery_context_provenance=None,
        current_investigation_outcomes=(None,),
        evidence=(),
    )

    assert summary.delivery_context is None
    assert summary.delivery_context_provenance is None
    assert summary.investigation_outcome is None
    assert summary.evidence_applicable is None


@pytest.mark.asyncio
async def test_v2_receipt_with_changed_inherited_context_is_outdated() -> None:
    submitted, _service, store, clock = await _submit_contextual_receipt(
        outcome=ContextualInvestigationOutcomeV2.EVIDENCE_APPLICABLE,
        delivery_context=DeliveryContext.GREENFIELD,
    )
    receipt = submitted.receipt
    evidence = _evidence(receipt, evidence_id="current", parent_version=3)
    provenance = RefinementDeliveryContextProvenance(
        value=DeliveryContext.BROWNFIELD,
        source_refinement_id="refinement-1",
        source_refinement_version=3,
    )
    changed_context = build_source_context_summary_v2(
        delivery_context=DeliveryContext.BROWNFIELD,
        delivery_context_provenance=provenance,
        current_investigation_outcomes=(receipt.contextual_outcome,),
        evidence=(evidence,),
    )
    context = replace(
        _gate_context(receipt, store, evidence=(evidence,)),
        source_context=changed_context,
    )
    assert (
        code_investigation_receipt_currentness(
            receipt,
            head=store.heads[(receipt.board_id, receipt.source_ref)],
            at=clock.value,
            expected_delivery_context=DeliveryContext.BROWNFIELD,
        )
        is CodeInvestigationReceiptCurrentness.OUTDATED
    )

    evaluation = CodeTraceabilityGateEvaluator(clock=clock).evaluate(
        context,
        CodeTraceabilitySettings(mode="blocking", evidence_attestation="required"),
        phases=(CodeTraceabilityGatePhase.REFINEMENT_EVIDENCE,),
        referenced_evidence_ids=(evidence.id,),
    )

    assert evaluation.allowed is False
    assert evaluation.receipt_currentness[receipt.id] == (
        CodeInvestigationReceiptCurrentness.OUTDATED.value
    )
    assert evaluation.blockers[0].code == "code_evidence_receipt_mismatch"


@pytest.mark.asyncio
async def test_card_currentness_uses_effective_spec_provenance() -> None:
    submitted, _service, store, clock = await _submit_contextual_receipt(
        outcome=ContextualInvestigationOutcomeV2.EVIDENCE_APPLICABLE,
        delivery_context=DeliveryContext.GREENFIELD,
    )
    receipt = replace(
        submitted.receipt,
        subject_type=CodeTraceabilitySubjectType.CARD,
        subject_id="card-1",
        subject_version=5,
    )
    spec_provenance = SpecDeliveryContextProvenance(
        value=DeliveryContext.HYBRID,
        inherited_value=DeliveryContext.GREENFIELD,
        source_refinement_id="refinement-1",
        source_refinement_version=3,
        override_reason="The Spec adds a pre-existing integration constraint.",
    )
    source_context = build_source_context_summary_v2(
        delivery_context=DeliveryContext.HYBRID,
        delivery_context_provenance=spec_provenance,
        current_investigation_outcomes=(receipt.contextual_outcome,),
        evidence=(),
    )
    context = CodeTraceabilityContext(
        board_id="board-1",
        subject_type=CodeTraceabilitySubjectType.CARD,
        subject_id="card-1",
        subject_version=5,
        profile=CodeTraceabilityProjectionProfile.FULL,
        heads=(store.heads[(receipt.board_id, receipt.source_ref)],),
        receipts=(receipt,),
        source_refinement_id="refinement-1",
        source_refinement_snapshot_id="snapshot-1",
        source_refinement_version=3,
        source_context=source_context,
    )

    status = CodeTraceabilityGateEvaluator(clock=clock)._receipt_policy_status(
        context,
        CodeTraceabilitySettings(),
        receipt,
        subject_type=CodeTraceabilitySubjectType.CARD,
        subject_id="card-1",
        subject_version=5,
        source_ref=receipt.source_ref,
    )

    assert status == ("outdated", "code_evidence_receipt_mismatch")
