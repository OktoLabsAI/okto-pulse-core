"""Native history uses edition authority without predecessor receipt conversion."""

from dataclasses import replace

import pytest

from okto_pulse.core.domain.guideline_policy import PolicyCurrentness, PolicyEntityType, PolicySubjectSnapshot
from okto_pulse.core.domain.guideline_semantic_assessment import SemanticAssessmentContractError, SemanticMetricOutcome
from okto_pulse.core.domain.guideline_semantic_currentness import assess_native_semantic_assessment_currentness
from test_skb31_semantic_guideline_v2_findings import _receipt, _result, _pinpoint, _subject, NOW
from test_skb3_semantic_guideline_application import _binding, _revision


def _fixture(edition=None):
    reference = replace(_subject(), subject_edition=edition,
                        entity_type=PolicyEntityType.SPEC if edition else PolicyEntityType.CARD)
    receipt = _receipt(_result("pass", outcome=SemanticMetricOutcome.PASS,
                               pinpoints=(_pinpoint("warning"),), subject=reference), subject=reference)
    revision = _revision()
    binding = _binding(revision)
    receipt = replace(receipt, binding_revision=binding.binding_revision,
                      binding_configuration_digest=binding.configuration_digest,
                      guideline_revision_digest=revision.revision_digest)
    subject = PolicySubjectSnapshot(subject=reference, content_digest=receipt.subject_content_digest,
                                    last_semantic_editor_id="author", captured_at=NOW)
    return receipt, subject, binding, revision


def test_human_edition_dominates_technical_drift_but_reopening_preserves_previous():
    receipt, subject, _, _ = _fixture(edition=2)
    changed = replace(subject, subject=replace(subject.subject, subject_version=9), content_digest="f" * 64)
    assert assess_native_semantic_assessment_currentness(receipt, subject=changed).is_current
    reopened = replace(changed, subject=replace(changed.subject, subject_edition=3))
    state = assess_native_semantic_assessment_currentness(receipt, subject=reopened)
    assert state.currentness is PolicyCurrentness.STALE
    assert [reason.value for reason in state.reasons] == ["subject_edition_changed"]
    assert receipt.subject.subject_edition == 2


def test_uneditioned_history_reports_each_native_fence_without_global_legacy_digests():
    receipt, subject, binding, revision = _fixture()
    assert assess_native_semantic_assessment_currentness(
        receipt, subject=subject, binding=binding, revision=revision,
    ).is_current
    changed = replace(subject, subject=replace(subject.subject, subject_version=9), content_digest="f" * 64)
    state = assess_native_semantic_assessment_currentness(
        receipt, subject=changed,
        binding=replace(binding, binding_revision=binding.binding_revision + 1),
        revision=revision,
    )
    assert [reason.value for reason in state.reasons] == [
        "subject_version_changed", "subject_content_changed", "binding_revision_changed",
    ]


def test_missing_authority_is_unavailable_and_cross_board_snapshot_is_rejected():
    receipt, subject, binding, revision = _fixture()
    for snapshot in (None, subject):
        state = assess_native_semantic_assessment_currentness(receipt, subject=snapshot)
        assert [reason.value for reason in state.reasons] == ["current_snapshot_missing"]
    with pytest.raises(SemanticAssessmentContractError, match="subject_scope_mismatch"):
        assess_native_semantic_assessment_currentness(
            receipt, subject=replace(subject, subject=replace(subject.subject, board_id="other-board")),
            binding=binding, revision=revision,
        )
