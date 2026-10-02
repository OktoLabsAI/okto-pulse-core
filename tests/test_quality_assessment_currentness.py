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
        subject_edition=1,
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


@pytest.mark.parametrize("head,edition,expected", [
    ("qar_1", 1, AssessmentReceiptState.CURRENT),
    ("qar_new", 1, AssessmentReceiptState.PREVIOUS),
    ("qar_1", 2, AssessmentReceiptState.PREVIOUS),
    ("qar_new", 2, AssessmentReceiptState.PREVIOUS),
])
def test_native_receipt_view_preserves_current_and_previous(head, edition, expected):
    original = _receipt()
    receipt = replace(original, subject=replace(original.subject, subject_edition=1))
    view = project_assessment_receipt_view(
        receipt, head_receipt_id=head,
        current_subject=replace(receipt.subject, subject_edition=edition, subject_version=8),
        current_digests=_digests("changed"),
    )
    assert view.state is expected
    assert view.receipt == receipt
    with pytest.raises(QualityAssessmentContractError, match="assessment_receipt_state_mismatch"):
        replace(view, state=AssessmentReceiptState.SUPERSEDED)


@pytest.mark.parametrize("origin,source", [("legacy_import", "native"), ("semantic_writer", "native"), ("human_or_agent", "legacy_migration")])
def test_projection_refuses_removed_identity_even_with_current_edition(origin, source):
    from okto_pulse.core.services.quality_projection_currentness import (
        QualityProjectionCurrentnessError,
        evaluate_quality_projection_currentness,
    )
    subject = {"id": "r1", "version": 7, "edition": 1}
    with pytest.raises(QualityProjectionCurrentnessError):
        evaluate_quality_projection_currentness(
            board_id="b1", subject_type="refinement", subject_id="r1",
            assessed_subject_version=7, assessed_subject_edition=1,
            assessed_digests=_digests(), assessment_kind="ambiguity",
            origin=origin, source=source, current_subject=subject,
        )
    assert subject == {"id": "r1", "version": 7, "edition": 1}


@pytest.mark.parametrize("field", ["assessed", "current"])
@pytest.mark.parametrize("edition", [None, 0, -1, True, "1", 1.0])
def test_projection_requires_explicit_positive_integer_editions(field, edition):
    from okto_pulse.core.services.quality_projection_currentness import (
        QualityProjectionCurrentnessError, evaluate_quality_projection_currentness,
    )
    subject = {"id": "r1", "version": 7, "edition": edition if field == "current" else 1}
    with pytest.raises(QualityProjectionCurrentnessError, match="quality_projection_edition_required"):
        evaluate_quality_projection_currentness(
            board_id="b1", subject_type="refinement", subject_id="r1",
            assessed_subject_version=7,
            assessed_subject_edition=edition if field == "assessed" else 1,
            assessed_digests=_digests(), assessment_kind="ambiguity",
            origin="human_or_agent", source="native", current_subject=subject,
        )
    assert subject["edition"] == (edition if field == "current" else 1)


@pytest.mark.parametrize("assessed_edition,current_edition,current", [(1, 1, True), (1, 2, False)])
def test_projection_uses_native_edition_without_technical_drift_fallback(assessed_edition, current_edition, current):
    from okto_pulse.core.services.quality_projection_currentness import evaluate_quality_projection_currentness
    result = evaluate_quality_projection_currentness(
        board_id="b1", subject_type="refinement", subject_id="r1",
        assessed_subject_version=7, assessed_subject_edition=assessed_edition,
        assessed_digests=_digests(), assessment_kind="ambiguity",
        origin="human_or_agent", source="native",
        current_subject={"id": "r1", "version": 20, "edition": current_edition},
    )
    assert result.current is current
    assert result.stale_reasons == (() if current else (AssessmentStaleReason.SUBJECT_EDITION_CHANGED,))


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
            "legacy_migration",
            "assessment_source_invalid",
        ),
        (
            "origin",
            "legacy_import",
            "assessment_origin_invalid",
        ),
        (
            "origin",
            "semantic_writer",
            "assessment_origin_invalid",
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


def test_receipt_is_current_in_its_edition() -> None:
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
def test_technical_digest_changes_do_not_invalidate_same_edition(field_name, reason) -> None:
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
    assert result.current is True
    assert result.stale_reasons == ()


def test_technical_version_change_preserves_current_edition() -> None:
    receipt = _receipt()
    current_subject = replace(receipt.subject, subject_version=8)
    result = evaluate_assessment_currentness(
        receipt,
        current_subject=current_subject,
        current_digests=receipt.digests,
    )
    assert result.current is True
    assert result.stale_reasons == ()


def test_new_edition_makes_the_previous_receipt_non_current() -> None:
    receipt = replace(
        _receipt(),
        subject=AssessmentSubjectRef(
            subject_edition=1,
            board_id="b1",
            subject_type=AssessmentSubjectType.SPEC,
            subject_id="spec-reopened",
            subject_version=7,
        ),
    )
    reopened_subject = replace(receipt.subject, subject_version=8, subject_edition=2)

    result = evaluate_assessment_currentness(
        receipt,
        current_subject=reopened_subject,
        current_digests=receipt.digests,
    )

    assert result.current is False
    assert result.stale_reasons == (
        AssessmentStaleReason.SUBJECT_EDITION_CHANGED,
    )


def test_combined_technical_changes_preserve_current_edition() -> None:
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
    assert result.current is True
    assert result.stale_reasons == ()


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


def test_non_head_receipt_is_previous() -> None:
    receipt = _receipt()
    view = project_assessment_receipt_view(
        receipt,
        head_receipt_id="qar_newer",
        current_subject=receipt.subject,
        current_digests=receipt.digests,
    )
    assert view.freshness.current is True
    assert view.is_head is False
    assert view.state is AssessmentReceiptState.PREVIOUS


def test_head_with_changed_technical_input_remains_current() -> None:
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
    assert view.state is AssessmentReceiptState.CURRENT
    assert view.freshness.stale_reasons == ()


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


@pytest.mark.parametrize("edition", [None, 0, -1, True, "1", 1.0])
def test_subject_ref_refuses_non_native_edition(edition):
    with pytest.raises(QualityAssessmentContractError, match="assessment_subject_edition_invalid"):
        replace(_receipt().subject, subject_edition=edition)
