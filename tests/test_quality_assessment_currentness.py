from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone

import pytest

from okto_pulse.core.domain.quality_assessment import (
    AssessmentCurrentness,
    AssessmentDigestSet,
    AssessmentKind,
    AssessmentOrigin,
    AssessmentOutcome,
    AssessmentReceipt,
    AssessmentReceiptState,
    AssessmentScale,
    AssessmentScaleKind,
    AssessmentSource,
    AssessmentStaleReason,
    AssessmentSubjectRef,
    AssessmentSubjectType,
    AssessmentVersionSet,
    QualityPage,
    QualityAssessmentContractError,
    ScoreDirection,
    evaluate_assessment_currentness,
    project_assessment_receipt_view,
)
from okto_pulse.core.domain.quality_canonicalization import canonical_sha256


def _digests(seed: str = "base") -> AssessmentDigestSet:
    return AssessmentDigestSet(
        content_digest=canonical_sha256([seed, "content"]),
        clarification_digest=canonical_sha256([seed, "clarification"]),
        ruleset_digest=canonical_sha256([seed, "ruleset"]),
        taxonomy_digest=canonical_sha256([seed, "taxonomy"]),
        policy_digest=canonical_sha256([seed, "policy"]),
    )


def _receipt() -> AssessmentReceipt:
    subject = AssessmentSubjectRef(
        board_id="b1",
        subject_type=AssessmentSubjectType.REFINEMENT,
        subject_id="r1",
        subject_version=7,
    )
    digests = _digests()
    return AssessmentReceipt(
        id="qar_1",
        subject=subject,
        assessment_kind=AssessmentKind.AMBIGUITY,
        origin=AssessmentOrigin.HUMAN_OR_AGENT,
        source=AssessmentSource.NATIVE,
        channel="mcp",
        outcome=AssessmentOutcome.RECORDED,
        scale=AssessmentScale(
            AssessmentScaleKind.AMBIGUITY_SCORE,
            1,
            5,
            ScoreDirection.LOWER_BETTER,
        ),
        score=2,
        justification="Ambiguity score is supported by the recorded findings.",
        digests=digests,
        versions=AssessmentVersionSet(
            "rules/v1",
            "taxonomy/v1",
            "analyzer/v1",
            "policy/v1",
        ),
        run_identity_digest=canonical_sha256(
            [subject.subject_version, digests.input_digest]
        ),
        authority_digest=canonical_sha256("authority"),
        idempotency_key="idem-1",
        request_digest=canonical_sha256("request"),
        created_by="agent",
        created_at=datetime(2026, 7, 27, tzinfo=timezone.utc),
    )


def test_receipt_preserves_explicit_scale_and_analyzer_versions() -> None:
    receipt = _receipt()

    assert receipt.scale.kind is AssessmentScaleKind.AMBIGUITY_SCORE
    assert receipt.versions.analyzer_version == "analyzer/v1"


def test_scale_kind_is_a_closed_explicit_contract() -> None:
    with pytest.raises(
        QualityAssessmentContractError,
        match="assessment_scale_kind_invalid",
    ):
        AssessmentScale(
            "ambiguity_score",  # type: ignore[arg-type]
            1,
            5,
            ScoreDirection.LOWER_BETTER,
        )


@pytest.mark.parametrize(
    ("field_name", "value", "code"),
    [
        (
            "source",
            AssessmentSource.LEGACY_MIGRATION,
            "assessment_source_origin_mismatch",
        ),
        (
            "outcome",
            AssessmentOutcome.ADVISORY,
            "assessment_outcome_kind_mismatch",
        ),
        (
            "request_digest",
            "not-a-sha256",
            "assessment_request_digest_invalid",
        ),
        (
            "predecessor_receipt_id",
            "qar_1",
            "assessment_predecessor_self_reference",
        ),
    ],
)
def test_receipt_rejects_invalid_immutable_provenance(
    field_name: str,
    value: object,
    code: str,
) -> None:
    with pytest.raises(QualityAssessmentContractError, match=code):
        replace(_receipt(), **{field_name: value})


def test_receipt_is_current_only_when_version_and_all_digests_match() -> None:
    receipt = _receipt()
    result = evaluate_assessment_currentness(
        receipt,
        current_subject=receipt.subject,
        current_digests=receipt.digests,
    )
    assert result == AssessmentCurrentness(current=True)


def test_explicit_input_digest_is_normalized_to_lowercase() -> None:
    baseline = _digests()
    normalized = AssessmentDigestSet(
        content_digest=baseline.content_digest,
        clarification_digest=baseline.clarification_digest,
        ruleset_digest=baseline.ruleset_digest,
        taxonomy_digest=baseline.taxonomy_digest,
        policy_digest=baseline.policy_digest,
        input_digest=(baseline.input_digest or "").upper(),
    )

    assert normalized.input_digest == baseline.input_digest


@pytest.mark.parametrize(
    ("field_name", "reason"),
    [
        ("content_digest", AssessmentStaleReason.CONTENT_CHANGED),
        ("clarification_digest", AssessmentStaleReason.CLARIFICATION_CHANGED),
        ("ruleset_digest", AssessmentStaleReason.RULESET_CHANGED),
        ("taxonomy_digest", AssessmentStaleReason.TAXONOMY_CHANGED),
        ("policy_digest", AssessmentStaleReason.POLICY_CHANGED),
    ],
)
def test_currentness_uses_the_closed_digest_reason_set(field_name, reason) -> None:
    receipt = _receipt()
    changed = replace(
        receipt.digests,
        **{field_name: canonical_sha256(["changed", field_name])},
        input_digest=None,
    )
    result = evaluate_assessment_currentness(
        receipt,
        current_subject=receipt.subject,
        current_digests=changed,
    )
    assert result.current is False
    assert result.stale_reasons == (reason,)


def test_version_binding_wins_even_when_text_and_digests_are_identical() -> None:
    receipt = _receipt()
    current_subject = replace(receipt.subject, subject_version=8)
    result = evaluate_assessment_currentness(
        receipt,
        current_subject=current_subject,
        current_digests=receipt.digests,
    )
    assert result.stale_reasons == (
        AssessmentStaleReason.SUBJECT_VERSION_CHANGED,
    )


def test_done_to_draft_spec_reopen_makes_the_previous_receipt_stale() -> None:
    receipt = replace(
        _receipt(),
        subject=AssessmentSubjectRef(
            board_id="b1",
            subject_type=AssessmentSubjectType.SPEC,
            subject_id="spec-reopened",
            subject_version=7,
        ),
    )
    reopened_subject = replace(receipt.subject, subject_version=8)

    result = evaluate_assessment_currentness(
        receipt,
        current_subject=reopened_subject,
        current_digests=receipt.digests,
    )

    assert result.current is False
    assert result.stale_reasons == (
        AssessmentStaleReason.SUBJECT_VERSION_CHANGED,
    )


def test_multiple_stale_reasons_are_deterministically_ordered() -> None:
    receipt = _receipt()
    current_subject = replace(receipt.subject, subject_version=8)
    changed = replace(
        receipt.digests,
        content_digest=canonical_sha256("changed-content"),
        policy_digest=canonical_sha256("changed-policy"),
        input_digest=None,
    )
    result = evaluate_assessment_currentness(
        receipt,
        current_subject=current_subject,
        current_digests=changed,
    )
    assert result.stale_reasons == (
        AssessmentStaleReason.SUBJECT_VERSION_CHANGED,
        AssessmentStaleReason.CONTENT_CHANGED,
        AssessmentStaleReason.POLICY_CHANGED,
    )


def test_threshold_enable_architecture_and_mockup_are_not_digest_inputs() -> None:
    receipt = _receipt()
    # These readiness-only values are intentionally absent from the function.
    assert evaluate_assessment_currentness(
        receipt,
        current_subject=receipt.subject,
        current_digests=receipt.digests,
    ).current


def test_currentness_rejects_a_different_subject() -> None:
    receipt = _receipt()
    with pytest.raises(
        QualityAssessmentContractError,
        match="assessment_subject_mismatch",
    ):
        evaluate_assessment_currentness(
            receipt,
            current_subject=replace(receipt.subject, subject_id="other"),
            current_digests=receipt.digests,
        )


def test_fresh_but_non_head_receipt_is_superseded_not_current() -> None:
    receipt = _receipt()
    view = project_assessment_receipt_view(
        receipt,
        head_receipt_id="qar_newer",
        current_subject=receipt.subject,
        current_digests=receipt.digests,
    )
    assert view.freshness.current is True
    assert view.is_head is False
    assert view.state is AssessmentReceiptState.SUPERSEDED


def test_head_with_changed_input_is_stale() -> None:
    receipt = _receipt()
    changed = replace(
        receipt.digests,
        clarification_digest=canonical_sha256("new-qa"),
        input_digest=None,
    )
    view = project_assessment_receipt_view(
        receipt,
        head_receipt_id=receipt.id,
        current_subject=receipt.subject,
        current_digests=changed,
    )
    assert view.state is AssessmentReceiptState.STALE
    assert view.freshness.stale_reasons == (
        AssessmentStaleReason.CLARIFICATION_CHANGED,
    )


def test_quality_page_rejects_more_items_than_limit() -> None:
    with pytest.raises(
        QualityAssessmentContractError,
        match="quality_page_item_count_exceeds_limit",
    ):
        QualityPage(("a", "b"), total_filtered=2, total_overall=2, offset=0, limit=1)


def test_quality_page_rejects_items_past_filtered_total() -> None:
    with pytest.raises(
        QualityAssessmentContractError,
        match="quality_page_items_exceed_filtered_total",
    ):
        QualityPage(("a", "b"), total_filtered=2, total_overall=3, offset=1, limit=2)


def test_quality_page_rejects_underfilled_server_page() -> None:
    with pytest.raises(
        QualityAssessmentContractError,
        match="quality_page_underfilled",
    ):
        QualityPage(("a",), total_filtered=3, total_overall=3, offset=0, limit=2)


def test_quality_page_allows_empty_page_beyond_end() -> None:
    page = QualityPage((), total_filtered=3, total_overall=5, offset=10, limit=2)
    assert page.items == ()
