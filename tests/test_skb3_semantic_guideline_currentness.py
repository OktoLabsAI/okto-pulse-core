"""SK-B3 I2 tests for normative semantic assessment currentness."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone

import pytest

from okto_pulse.core.domain.guideline_policy import (
    BoardGuidelineBinding,
    GuidelineEnforcement,
    GuidelineMetric,
    GuidelineMetricDirection,
    GuidelineRevision,
    PolicyCurrentness,
    PolicyEntityType,
    PolicySubjectRef,
    PolicySubjectSnapshot,
)
from okto_pulse.core.domain.guideline_semantic_assessment import (
    SemanticAssessmentAssessor,
    SemanticAssessmentContractError,
    SemanticGuidelineAssessmentContext,
    SemanticGuidelineAssessmentSubmission,
    SemanticMetricAssessment,
    record_semantic_guideline_assessment,
)
from okto_pulse.core.domain.guideline_semantic_currentness import (
    SemanticAssessmentCurrentnessReason,
    assess_semantic_assessment_currentness,
    semantic_assessment_current_snapshot_from_context,
)
from okto_pulse.core.domain.quality_assessment import (
    EvidenceRef,
    FindingAnchorType,
    UnboundFindingAnchor,
)


NOW = datetime(2026, 7, 30, 20, tzinfo=timezone.utc)
DIGEST_A = "a" * 64
DIGEST_B = "b" * 64
DIGEST_C = "c" * 64
DIGEST_D = "d" * 64


def _context_and_receipt():
    metric = GuidelineMetric(
        metric_id="metric-segregation",
        code="architecture.segregation",
        title="Segregation",
        description="Measure separation of domain and infrastructure.",
        evaluation_rubric="Inspect dependencies and score from 0 to 100.",
        target_entity_types=(PolicyEntityType.SPEC,),
        direction=GuidelineMetricDirection.MINIMUM,
        default_threshold=70,
    )
    revision = GuidelineRevision(
        revision_id="revision-1",
        guideline_id="guideline-1",
        revision_number=1,
        semantic_version="1.0.0",
        title="Hexagonal architecture",
        content="Keep domain rules independent from infrastructure.",
        metrics=(metric,),
        created_by="author-1",
        created_at=NOW,
    )
    binding = BoardGuidelineBinding(
        binding_id="binding-1",
        board_id="board-1",
        guideline_id=revision.guideline_id,
        revision_id=revision.revision_id,
        semantic_version=revision.semantic_version,
        revision_digest=revision.revision_digest,
        priority=0,
        binding_revision=2,
        adopted_by="owner-1",
        adopted_at=NOW,
        enforcement=GuidelineEnforcement.BLOCKING,
        minimum_confidence=80,
        metric_threshold_overrides={},
    )
    snapshot = PolicySubjectSnapshot(
        subject=PolicySubjectRef(
            board_id="board-1",
            entity_type=PolicyEntityType.SPEC,
            subject_id="spec-1",
            subject_version=4,
        ),
        content_digest=DIGEST_A,
        last_semantic_editor_id="editor-1",
        captured_at=NOW,
    )
    context = SemanticGuidelineAssessmentContext(
        subject_snapshot=snapshot,
        binding=binding,
        revision=revision,
        policy_set_digest=DIGEST_B,
        binding_head_digest=DIGEST_C,
    )
    submission = SemanticGuidelineAssessmentSubmission(
        subject=snapshot.subject,
        binding_id=binding.binding_id,
        expected_binding_revision=binding.binding_revision,
        guideline_revision_id=revision.revision_id,
        idempotency_key="assessment:spec-1:v4",
        confidence=90,
        assessor=SemanticAssessmentAssessor(
            agent_id="reviewer-1",
            model_id="model-a",
        ),
        metric_results=(
            SemanticMetricAssessment(
                metric_id=metric.metric_id,
                score=85,
                rationale="Dependency evidence shows an isolated domain.",
                evidence_refs=(
                    EvidenceRef(
                        source_type="spec",
                        source_id="spec-1",
                        source_version=4,
                        content_hash=DIGEST_A,
                    ),
                ),
                pinpoints=(
                    UnboundFindingAnchor(
                        anchor_type=FindingAnchorType.STRUCTURED_CHILD,
                        anchor_ref="technical_requirements.tr_hexagonal",
                        excerpt_hash=DIGEST_D,
                    ),
                ),
            ),
        ),
    )
    result = record_semantic_guideline_assessment(
        submission,
        context,
        receipt_id="receipt-1",
        recorded_at=NOW,
    )
    return context, result.receipt


def test_exact_normative_snapshot_is_current() -> None:
    context, receipt = _context_and_receipt()
    current = semantic_assessment_current_snapshot_from_context(context)

    assessment = assess_semantic_assessment_currentness(receipt, current)

    assert assessment.currentness is PolicyCurrentness.CURRENT
    assert assessment.reasons == ()
    assert assessment.is_current


def test_model_or_assessor_change_alone_does_not_stale_receipt() -> None:
    context, receipt = _context_and_receipt()
    current = semantic_assessment_current_snapshot_from_context(context)
    audit_only_change = replace(
        receipt,
        assessor=SemanticAssessmentAssessor(
            agent_id="reviewer-2",
            model_id="model-b",
        ),
        assessor_independent=True,
        receipt_digest=None,
    )

    assessment = assess_semantic_assessment_currentness(
        audit_only_change,
        current,
    )

    assert assessment.currentness is PolicyCurrentness.CURRENT
    assert assessment.reasons == ()


def test_all_normative_fence_changes_have_ordered_specific_reasons() -> None:
    context, receipt = _context_and_receipt()
    current = semantic_assessment_current_snapshot_from_context(context)
    changed = replace(
        current,
        subject=replace(current.subject, subject_version=5),
        subject_content_digest=DIGEST_D,
        guideline_revision_id="revision-2",
        guideline_revision_digest=DIGEST_D,
        binding_revision=3,
        binding_configuration_digest=DIGEST_D,
        policy_set_digest=DIGEST_D,
        binding_head_digest=DIGEST_D,
        input_digest=DIGEST_D,
    )

    assessment = assess_semantic_assessment_currentness(receipt, changed)

    assert assessment.currentness is PolicyCurrentness.STALE
    assert assessment.reasons == (
        SemanticAssessmentCurrentnessReason.SUBJECT_VERSION_CHANGED,
        SemanticAssessmentCurrentnessReason.SUBJECT_CONTENT_CHANGED,
        SemanticAssessmentCurrentnessReason.GUIDELINE_REVISION_CHANGED,
        (
            SemanticAssessmentCurrentnessReason
            .GUIDELINE_REVISION_DIGEST_CHANGED
        ),
        SemanticAssessmentCurrentnessReason.BINDING_REVISION_CHANGED,
        (
            SemanticAssessmentCurrentnessReason
            .BINDING_CONFIGURATION_CHANGED
        ),
        SemanticAssessmentCurrentnessReason.POLICY_SET_CHANGED,
        SemanticAssessmentCurrentnessReason.BINDING_HEAD_CHANGED,
        SemanticAssessmentCurrentnessReason.INPUT_DIGEST_CHANGED,
    )


def test_missing_current_snapshot_is_fail_closed_stale() -> None:
    _, receipt = _context_and_receipt()

    assessment = assess_semantic_assessment_currentness(receipt, None)

    assert assessment.currentness is PolicyCurrentness.STALE
    assert assessment.reasons == (
        SemanticAssessmentCurrentnessReason.CURRENT_SNAPSHOT_MISSING,
    )


def test_cross_subject_or_binding_comparison_is_rejected() -> None:
    context, receipt = _context_and_receipt()
    current = semantic_assessment_current_snapshot_from_context(context)

    with pytest.raises(
        SemanticAssessmentContractError,
        match="semantic_currentness_subject_scope_mismatch",
    ):
        assess_semantic_assessment_currentness(
            receipt,
            replace(
                current,
                subject=replace(current.subject, subject_id="spec-2"),
            ),
        )
    with pytest.raises(
        SemanticAssessmentContractError,
        match="semantic_currentness_binding_scope_mismatch",
    ):
        assess_semantic_assessment_currentness(
            receipt,
            replace(current, binding_id="binding-2"),
        )
