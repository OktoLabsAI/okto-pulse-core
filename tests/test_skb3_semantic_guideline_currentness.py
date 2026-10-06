"""SK-B3 I2 tests for normative semantic assessment currentness."""

from __future__ import annotations

from dataclasses import replace

import pytest

from okto_pulse.core.domain.guideline_policy import PolicyCurrentness
from okto_pulse.core.domain.guideline_semantic_assessment import SemanticAssessmentContractError
from okto_pulse.core.domain.guideline_semantic_currentness import (
    SemanticAssessmentCurrentnessReason,
    assess_native_semantic_assessment_fences,
    native_semantic_assessment_snapshot,
)
from test_native_semantic_history_currentness import _fixture

DIGEST_D = "d" * 64


def _snapshot_and_receipt():
    receipt, subject, binding, revision = _fixture()
    return native_semantic_assessment_snapshot(subject=subject, binding=binding, revision=revision), receipt


def test_exact_normative_snapshot_is_current() -> None:
    current, receipt = _snapshot_and_receipt()

    assessment = assess_native_semantic_assessment_fences(receipt, current)

    assert assessment.currentness is PolicyCurrentness.CURRENT
    assert assessment.reasons == ()
    assert assessment.is_current


def test_assessor_change_alone_does_not_stale_receipt() -> None:
    current, receipt = _snapshot_and_receipt()
    audit_only_change = replace(receipt, assessment_assessor_id="reviewer-2")

    assessment = assess_native_semantic_assessment_fences(
        audit_only_change,
        current,
    )

    assert assessment.currentness is PolicyCurrentness.CURRENT
    assert assessment.reasons == ()


def test_all_normative_fence_changes_have_ordered_specific_reasons() -> None:
    current, receipt = _snapshot_and_receipt()
    changed = replace(
        current,
        subject=replace(current.subject, subject_version=5),
        subject_content_digest=DIGEST_D,
        guideline_revision_id="revision-2",
        guideline_revision_digest=DIGEST_D,
        binding_revision=current.binding_revision + 1,
        binding_configuration_digest=DIGEST_D,
    )

    assessment = assess_native_semantic_assessment_fences(receipt, changed)

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
    )


def test_missing_current_snapshot_is_fail_closed_stale() -> None:
    _, receipt = _snapshot_and_receipt()

    assessment = assess_native_semantic_assessment_fences(receipt, None)

    assert assessment.currentness is PolicyCurrentness.STALE
    assert assessment.reasons == (
        SemanticAssessmentCurrentnessReason.CURRENT_SNAPSHOT_MISSING,
    )


def test_cross_subject_or_binding_comparison_is_rejected() -> None:
    current, receipt = _snapshot_and_receipt()

    with pytest.raises(
        SemanticAssessmentContractError,
        match="semantic_currentness_subject_scope_mismatch",
    ):
        assess_native_semantic_assessment_fences(
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
        assess_native_semantic_assessment_fences(
            receipt,
            replace(current, binding_id="binding-2"),
        )
