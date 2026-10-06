"""SK-B B07 contracts for immutable receipts and honest currentness."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone

import pytest

from okto_pulse.core.domain.guideline_compliance import PolicyCursorCodec
from okto_pulse.core.domain.guideline_semantic_projection import (
    SEMANTIC_FINDING_ORDERING,
    SEMANTIC_GUIDELINE_KEYSET_CONTRACT_VERSION,
    SEMANTIC_ASSESSMENT_ORDERING,
    SemanticFindingPageCursor,
    SemanticGuidelineProjection,
    SemanticAssessmentPageCursor,
)
from okto_pulse.core.domain.guideline_policy import (
    AdoptedGuidelineRevisionRef,
    GuidelineEnforcement,
    PolicyComplianceFinding,
    PolicyComplianceReasonCode,
    PolicyComplianceReceipt,
    PolicyComplianceRuleResult,
    PolicyComplianceState,
    PolicyCurrentness,
    PolicyEntityType,
    PolicyEvaluationOutcome,
    PolicySubjectRef,
    GuidelinePolicyContractError,
)
from okto_pulse.core.ports.guideline_policy import (
    GuidelinePolicyCursorConflict,
    SemanticFindingListQuery,
    SemanticAssessmentListQuery,
)


NOW = datetime(2026, 7, 29, 14, tzinfo=timezone.utc)


def _subject(*, version: int = 4) -> PolicySubjectRef:
    return PolicySubjectRef(
        board_id="board-1",
        entity_type=PolicyEntityType.SPEC,
        subject_id="spec-1",
        subject_version=version,
    )


def _finding(
    *,
    finding_id: str = "finding-1",
    enforcement: GuidelineEnforcement = GuidelineEnforcement.BLOCKING,
    waiver_id: str | None = None,
) -> PolicyComplianceFinding:
    return PolicyComplianceFinding(
        finding_id=finding_id,
        receipt_id="receipt-1",
        subject=_subject(),
        guideline_id="guideline-1",
        revision_id="revision-1",
        rule_id="rule-1",
        outcome=PolicyEvaluationOutcome.FAIL,
        enforcement=enforcement,
        message="The executable rule failed.",
        created_at=NOW,
        evidence_refs=("predicate:abc:fail",),
        waiver_id=waiver_id,
    )


def _receipt() -> PolicyComplianceReceipt:
    finding = _finding()
    return PolicyComplianceReceipt(
        receipt_id="receipt-1",
        subject=_subject(),
        subject_content_digest="a" * 64,
        input_digest="b" * 64,
        policy_set_digest="c" * 64,
        binding_head_digest="d" * 64,
        catalog_version="guideline-predicate-catalog/v1",
        ruleset_version="guideline-ruleset/v1",
        adopted_revisions=(
            AdoptedGuidelineRevisionRef(
                binding_id="binding-1",
                binding_revision=2,
                guideline_id="guideline-1",
                revision_id="revision-1",
                semantic_version="1.1.0",
                revision_digest="e" * 64,
            ),
        ),
        outcome=PolicyEvaluationOutcome.FAIL,
        state=PolicyComplianceState.BLOCKED,
        currentness=PolicyCurrentness.CURRENT,
        findings=(finding,),
        evaluator_version="policy-evaluator/v1",
        evaluated_by="agent-1",
        evaluated_at=NOW,
        rule_results=(
            PolicyComplianceRuleResult(
                guideline_id=finding.guideline_id,
                revision_id=finding.revision_id,
                rule_id=finding.rule_id,
                outcome=finding.outcome,
                enforcement=finding.enforcement,
            ),
        ),
    )


def test_policy_keyset_cursor_is_bound_to_filter_and_projection() -> None:
    first = SemanticAssessmentListQuery(
        board_id="board-1",
        entity_type=PolicyEntityType.SPEC,
        projection=SemanticGuidelineProjection.SUMMARY,
    )
    cursor = SemanticAssessmentPageCursor(
        at=NOW,
        item_id="receipt-1",
        filter_digest=first.filter_digest,
        projection_digest=first.projection_digest,
    )
    second = SemanticAssessmentListQuery(
        board_id="board-1",
        entity_type=PolicyEntityType.SPEC,
        projection=SemanticGuidelineProjection.SUMMARY,
        cursor=cursor,
    )

    assert second.cursor is cursor
    assert cursor.schema_version == SEMANTIC_GUIDELINE_KEYSET_CONTRACT_VERSION
    assert cursor.ordering == SEMANTIC_ASSESSMENT_ORDERING
    with pytest.raises(
        GuidelinePolicyCursorConflict,
        match="semantic_assessment_cursor_context_mismatch",
    ):
        SemanticAssessmentListQuery(
            board_id="board-1",
            entity_type=PolicyEntityType.SPEC,
            projection=SemanticGuidelineProjection.DETAIL,
            cursor=cursor,
        )


def test_finding_keyset_cursor_rejects_filter_drift() -> None:
    first = SemanticFindingListQuery(
        board_id="board-1",
        guideline_id="guideline-1",
        projection=SemanticGuidelineProjection.SUMMARY,
    )
    cursor = SemanticFindingPageCursor(
        at=NOW,
        item_id="finding-1",
        filter_digest=first.filter_digest,
        projection_digest=first.projection_digest,
    )

    assert cursor.ordering == SEMANTIC_FINDING_ORDERING
    with pytest.raises(
        GuidelinePolicyCursorConflict,
        match="semantic_finding_cursor_context_mismatch",
    ):
        SemanticFindingListQuery(
            board_id="board-1",
            guideline_id="guideline-2",
            projection=SemanticGuidelineProjection.SUMMARY,
            cursor=cursor,
        )


def test_policy_cursor_codec_is_opaque_tamper_evident_and_kind_bound() -> None:
    query = SemanticAssessmentListQuery(board_id="board-1")
    cursor = SemanticAssessmentPageCursor(
        at=NOW,
        item_id="receipt-1",
        filter_digest=query.filter_digest,
        projection_digest=query.projection_digest,
    )
    codec = PolicyCursorCodec(b"policy-cursor-test-key-32-bytes!!")
    token = codec.encode(cursor)

    assert "receipt-1" not in token
    assert codec.decode(token, expected_kind="semantic_assessment") == cursor
    with pytest.raises(GuidelinePolicyContractError, match="invalid_cursor"):
        codec.decode(token, expected_kind="semantic_finding")
    replacement = "A" if token[-1] != "A" else "B"
    with pytest.raises(GuidelinePolicyContractError, match="invalid_cursor"):
        codec.decode(token[:-1] + replacement, expected_kind="semantic_assessment")
    payload, signature = token.split(".")
    with pytest.raises(GuidelinePolicyContractError, match="invalid_cursor"):
        codec.decode(f"{payload}!!!!.{signature}", expected_kind="semantic_assessment")
    with pytest.raises(GuidelinePolicyContractError, match="invalid_cursor"):
        codec.decode(f"{payload}.{signature}!!!!", expected_kind="semantic_assessment")


def test_rule_rows_and_findings_have_exact_bidirectional_integrity() -> None:
    with pytest.raises(
        GuidelinePolicyContractError,
        match="policy_rule_result_outcome_invalid",
    ):
        replace(
            _receipt().rule_results[0],
            outcome=PolicyEvaluationOutcome.NOT_APPLICABLE,
        )
    with pytest.raises(
        GuidelinePolicyContractError,
        match="policy_finding_outcome_invalid",
    ):
        replace(_finding(), outcome=PolicyEvaluationOutcome.PASS)
    with pytest.raises(
        GuidelinePolicyContractError,
        match="policy_receipt_findings_incomplete",
    ):
        replace(_receipt(), findings=())
    with pytest.raises(
        GuidelinePolicyContractError,
        match="policy_receipt_duplicate_finding_for_rule",
    ):
        replace(
            _receipt(),
            findings=(
                _finding(),
                _finding(finding_id="finding-2"),
            ),
        )


def _error_receipt(
    *,
    enforcement: GuidelineEnforcement,
    state: PolicyComplianceState,
    reason: PolicyComplianceReasonCode,
) -> PolicyComplianceReceipt:
    finding = replace(
        _finding(enforcement=enforcement),
        outcome=PolicyEvaluationOutcome.ERROR,
    )
    return PolicyComplianceReceipt(
        receipt_id="receipt-1",
        subject=_subject(),
        subject_content_digest="a" * 64,
        input_digest="b" * 64,
        policy_set_digest="c" * 64,
        binding_head_digest="d" * 64,
        catalog_version="guideline-predicate-catalog/v1",
        ruleset_version="guideline-ruleset/v1",
        adopted_revisions=(
            AdoptedGuidelineRevisionRef(
                binding_id="binding-1",
                binding_revision=2,
                guideline_id="guideline-1",
                revision_id="revision-1",
                semantic_version="1.1.0",
                revision_digest="e" * 64,
            ),
        ),
        outcome=PolicyEvaluationOutcome.ERROR,
        state=state,
        currentness=PolicyCurrentness.CURRENT,
        findings=(finding,),
        evaluator_version="policy-evaluator/v1",
        evaluated_by="agent-1",
        evaluated_at=NOW,
        rule_results=(
            PolicyComplianceRuleResult(
                guideline_id=finding.guideline_id,
                revision_id=finding.revision_id,
                rule_id=finding.rule_id,
                outcome=finding.outcome,
                enforcement=finding.enforcement,
            ),
        ),
        reason_codes=(reason,),
    )


def test_unavailable_blocking_fails_closed_and_advisory_degrades() -> None:
    blocked = _error_receipt(
        enforcement=GuidelineEnforcement.BLOCKING,
        state=PolicyComplianceState.BLOCKED,
        reason=PolicyComplianceReasonCode.POLICY_EVALUATION_UNAVAILABLE,
    )
    advisory = _error_receipt(
        enforcement=GuidelineEnforcement.ADVISORY,
        state=PolicyComplianceState.READY,
        reason=PolicyComplianceReasonCode.POLICY_EVALUATION_DEGRADED,
    )

    assert blocked.blocking_rule_count == 1
    assert blocked.error_rule_count == 1
    assert advisory.blocking_rule_count == 0
    assert advisory.error_rule_count == 1
    with pytest.raises(
        GuidelinePolicyContractError,
        match="policy_receipt_state_outcome_inconsistent",
    ):
        _error_receipt(
            enforcement=GuidelineEnforcement.BLOCKING,
            state=PolicyComplianceState.READY,
            reason=(PolicyComplianceReasonCode.POLICY_EVALUATION_UNAVAILABLE),
        )
    with pytest.raises(
        GuidelinePolicyContractError,
        match="policy_receipt_reason_codes_inconsistent",
    ):
        replace(blocked, reason_codes=())
