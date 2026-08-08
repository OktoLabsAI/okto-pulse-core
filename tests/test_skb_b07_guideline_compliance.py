"""SK-B B07 contracts for immutable receipts and honest currentness."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone

import pytest

from okto_pulse.core.domain.guideline_compliance import (
    POLICY_FINDING_ORDERING,
    POLICY_KEYSET_CONTRACT_VERSION,
    POLICY_RECEIPT_ORDERING,
    PolicyComplianceCurrentSnapshot,
    PolicyCurrentnessReason,
    PolicyCursorCodec,
    PolicyFindingPageCursor,
    PolicyProjection,
    PolicyReceiptPageCursor,
    assess_policy_receipt_currentness,
    policy_finding_severity_rank,
    project_policy_compliance_finding,
    project_policy_compliance_receipt,
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
    PolicyComplianceFindingListQuery,
    PolicyComplianceReceiptListQuery,
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


def _current() -> PolicyComplianceCurrentSnapshot:
    receipt = _receipt()
    return PolicyComplianceCurrentSnapshot(
        subject=receipt.subject,
        subject_content_digest=receipt.subject_content_digest,
        input_digest=receipt.input_digest,
        policy_set_digest=receipt.policy_set_digest,
        binding_head_digest=receipt.binding_head_digest,
        catalog_version=receipt.catalog_version,
        ruleset_version=receipt.ruleset_version,
    )


def test_currentness_is_complete_and_missing_snapshot_fails_closed() -> None:
    receipt = _receipt()

    assert (
        assess_policy_receipt_currentness(
            receipt,
            _current(),
        ).currentness
        is PolicyCurrentness.CURRENT
    )
    missing = assess_policy_receipt_currentness(receipt, None)

    assert missing.currentness is PolicyCurrentness.STALE
    assert missing.reasons == (PolicyCurrentnessReason.CURRENT_SNAPSHOT_MISSING,)


@pytest.mark.parametrize(
    ("change", "reason"),
    (
        (
            lambda current: replace(
                current,
                subject=replace(current.subject, subject_version=5),
            ),
            PolicyCurrentnessReason.SUBJECT_VERSION_CHANGED,
        ),
        (
            lambda current: replace(current, policy_set_digest="f" * 64),
            PolicyCurrentnessReason.POLICY_SET_CHANGED,
        ),
        (
            lambda current: replace(
                current,
                catalog_version="guideline-predicate-catalog/v2",
            ),
            PolicyCurrentnessReason.CATALOG_VERSION_CHANGED,
        ),
        (
            lambda current: replace(
                current,
                ruleset_version="guideline-ruleset/v2",
            ),
            PolicyCurrentnessReason.RULESET_VERSION_CHANGED,
        ),
        (
            lambda current: replace(
                current,
                binding_head_digest="f" * 64,
            ),
            PolicyCurrentnessReason.BINDING_HEAD_CHANGED,
        ),
    ),
    ids=(
        "subject-version",
        "policy-set",
        "catalog-version",
        "ruleset-version",
        "binding-head",
    ),
)
def test_each_normative_fence_has_one_exact_stale_reason(
    change,
    reason: PolicyCurrentnessReason,
) -> None:
    assessment = assess_policy_receipt_currentness(
        _receipt(),
        change(_current()),
    )

    assert assessment.currentness is PolicyCurrentness.STALE
    assert assessment.reasons == (reason,)


def test_currentness_reports_all_independent_fences_and_suppresses_only_input() -> None:
    assessment = assess_policy_receipt_currentness(
        _receipt(),
        replace(
            _current(),
            policy_set_digest="f" * 64,
            binding_head_digest="e" * 64,
            input_digest="0" * 64,
        ),
    )
    version_assessment = assess_policy_receipt_currentness(
        _receipt(),
        replace(
            _current(),
            subject=replace(_current().subject, subject_version=5),
            subject_content_digest="f" * 64,
            input_digest="0" * 64,
        ),
    )

    assert assessment.reasons == (
        PolicyCurrentnessReason.POLICY_SET_CHANGED,
        PolicyCurrentnessReason.BINDING_HEAD_CHANGED,
    )
    assert version_assessment.reasons == (
        PolicyCurrentnessReason.SUBJECT_VERSION_CHANGED,
        PolicyCurrentnessReason.SUBJECT_CONTENT_CHANGED,
    )


def test_summary_is_slim_and_detail_preserves_frozen_evidence() -> None:
    summary = project_policy_compliance_receipt(
        _receipt(),
        projection=PolicyProjection.SUMMARY,
        current=_current(),
    )
    detail = project_policy_compliance_receipt(
        _receipt(),
        projection=PolicyProjection.DETAIL,
        current=_current(),
    )

    assert summary.finding_count == 1
    assert summary.blocking_finding_count == 1
    assert summary.adopted_revisions is None
    assert summary.input_digest is None
    assert detail.adopted_revisions == _receipt().adopted_revisions
    assert detail.input_digest == "b" * 64


def test_finding_projection_and_severity_are_deterministic() -> None:
    blocking = _finding()
    waived = _finding(finding_id="finding-2", waiver_id="waiver-1")
    advisory = _finding(
        finding_id="finding-3",
        enforcement=GuidelineEnforcement.ADVISORY,
    )

    assert [
        policy_finding_severity_rank(item) for item in (blocking, waived, advisory)
    ] == [50, 40, 20]
    summary = project_policy_compliance_finding(
        blocking,
        projection=PolicyProjection.SUMMARY,
    )
    detail = project_policy_compliance_finding(
        blocking,
        projection=PolicyProjection.DETAIL,
    )
    assert summary.message is None
    assert summary.evidence_refs is None
    assert detail.message == blocking.message
    assert detail.evidence_refs == blocking.evidence_refs


def test_policy_keyset_cursor_is_bound_to_filter_and_projection() -> None:
    first = PolicyComplianceReceiptListQuery(
        board_id="board-1",
        entity_type=PolicyEntityType.SPEC,
        projection=PolicyProjection.SUMMARY,
    )
    cursor = PolicyReceiptPageCursor(
        evaluated_at=NOW,
        item_id="receipt-1",
        filter_digest=first.filter_digest,
        projection_digest=first.projection_digest,
    )
    second = PolicyComplianceReceiptListQuery(
        board_id="board-1",
        entity_type=PolicyEntityType.SPEC,
        projection=PolicyProjection.SUMMARY,
        cursor=cursor,
    )

    assert second.cursor is cursor
    assert cursor.schema_version == POLICY_KEYSET_CONTRACT_VERSION
    assert cursor.ordering == POLICY_RECEIPT_ORDERING
    with pytest.raises(
        GuidelinePolicyCursorConflict,
        match="policy_receipt_cursor_context_mismatch",
    ):
        PolicyComplianceReceiptListQuery(
            board_id="board-1",
            entity_type=PolicyEntityType.SPEC,
            projection=PolicyProjection.DETAIL,
            cursor=cursor,
        )


def test_finding_keyset_cursor_rejects_filter_drift() -> None:
    first = PolicyComplianceFindingListQuery(
        board_id="board-1",
        guideline_id="guideline-1",
        projection=PolicyProjection.SUMMARY,
    )
    cursor = PolicyFindingPageCursor(
        severity_rank=50,
        rule_id="rule-1",
        item_id="finding-1",
        filter_digest=first.filter_digest,
        projection_digest=first.projection_digest,
    )

    assert cursor.ordering == POLICY_FINDING_ORDERING
    with pytest.raises(
        GuidelinePolicyCursorConflict,
        match="policy_finding_cursor_context_mismatch",
    ):
        PolicyComplianceFindingListQuery(
            board_id="board-1",
            guideline_id="guideline-2",
            projection=PolicyProjection.SUMMARY,
            cursor=cursor,
        )


def test_policy_cursor_codec_is_opaque_tamper_evident_and_kind_bound() -> None:
    query = PolicyComplianceReceiptListQuery(board_id="board-1")
    cursor = PolicyReceiptPageCursor(
        evaluated_at=NOW,
        item_id="receipt-1",
        filter_digest=query.filter_digest,
        projection_digest=query.projection_digest,
    )
    codec = PolicyCursorCodec(b"policy-cursor-test-key-32-bytes!!")
    token = codec.encode(cursor)

    assert "receipt-1" not in token
    assert codec.decode(token, expected_kind="receipt") == cursor
    with pytest.raises(GuidelinePolicyContractError, match="invalid_cursor"):
        codec.decode(token, expected_kind="finding")
    replacement = "A" if token[-1] != "A" else "B"
    with pytest.raises(GuidelinePolicyContractError, match="invalid_cursor"):
        codec.decode(token[:-1] + replacement, expected_kind="receipt")
    payload, signature = token.split(".")
    with pytest.raises(GuidelinePolicyContractError, match="invalid_cursor"):
        codec.decode(f"{payload}!!!!.{signature}", expected_kind="receipt")
    with pytest.raises(GuidelinePolicyContractError, match="invalid_cursor"):
        codec.decode(f"{payload}.{signature}!!!!", expected_kind="receipt")


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
