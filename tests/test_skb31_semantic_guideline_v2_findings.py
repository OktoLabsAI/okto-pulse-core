"""SK-B3.1 v2 finding cardinality and immutable waiver fences."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone

from okto_pulse.core.domain.guideline_policy import (
    GuidelineMetricDirection,
    PolicyEntityType,
    PolicySubjectRef,
)
from okto_pulse.core.domain.guideline_semantic_assessment import (
    SemanticMetricOutcome,
    SemanticThresholdSource,
)
from okto_pulse.core.domain.guideline_semantic_findings_v2 import (
    SEMANTIC_WAIVER_FINDING_IDENTITY_MISMATCH,
    SemanticAssessmentReceiptProjectionV2,
    SemanticMetricWaiverAnchorV2,
    SemanticWaiverApplicability,
    evaluate_semantic_waiver_applicability_v2,
    project_semantic_metric_findings_v2,
)
from okto_pulse.core.domain.guideline_semantic_v2 import (
    AnchorSnapshot,
    SemanticAnchorAvailability,
    SemanticMetricResultV2,
    SemanticPinpointKind,
    SemanticPinpointV2,
)
from okto_pulse.core.domain.quality_assessment import (
    EvidenceRef,
    FindingAnchorType,
    FindingSeverity,
    UnboundFindingAnchor,
)


NOW = datetime(2026, 8, 9, 3, tzinfo=timezone.utc)
DIGEST_A = "a" * 64
DIGEST_B = "b" * 64
DIGEST_C = "c" * 64


def _subject(version: int = 4) -> PolicySubjectRef:
    return PolicySubjectRef(
        board_id="board-1",
        entity_type=PolicyEntityType.CARD,
        subject_id="card-1",
        subject_version=version,
    )


def _pinpoint(key: str) -> SemanticPinpointV2:
    return SemanticPinpointV2(
        pinpoint_key=key,
        kind=SemanticPinpointKind.ISSUE,
        title=f"Issue {key}",
        detail=f"Detail {key}",
        severity=FindingSeverity.HIGH,
        remediation=f"Remediate {key}",
        anchor=UnboundFindingAnchor(
            anchor_type=FindingAnchorType.STRUCTURED_CHILD,
            anchor_ref=f"technical_requirements.{key}",
        ),
        anchor_snapshot=AnchorSnapshot(
            label=f"Requirement {key}",
            excerpt=f"Excerpt {key}",
            source_version="4",
            availability_at_seal=SemanticAnchorAvailability.AVAILABLE,
        ),
    )


def _result(
    metric_id: str,
    *,
    outcome: SemanticMetricOutcome,
    pinpoints: tuple[SemanticPinpointV2, ...],
    receipt_id: str = "receipt-1",
    subject: PolicySubjectRef | None = None,
) -> SemanticMetricResultV2:
    score = 95 if outcome is SemanticMetricOutcome.PASS else 40
    return SemanticMetricResultV2(
        metric_result_id=f"result-{metric_id}",
        receipt_id=receipt_id,
        subject=subject or _subject(),
        binding_id="binding-1",
        guideline_id="guideline-1",
        revision_id="revision-1",
        metric_id=metric_id,
        metric_code=f"architecture.{metric_id}",
        metric_definition_digest=DIGEST_A,
        score=score,
        direction=GuidelineMetricDirection.MINIMUM,
        default_threshold=80,
        effective_threshold=80,
        threshold_source=SemanticThresholdSource.DEFAULT,
        outcome=outcome,
        rationale=f"Rationale {metric_id}",
        evidence_refs=(
            EvidenceRef(
                source_type="card",
                source_id="card-1",
                source_version=4,
                content_hash=DIGEST_B,
            ),
        ),
        pinpoints=pinpoints,
    )


def _receipt(
    *results: SemanticMetricResultV2,
    receipt_id: str = "receipt-1",
    subject: PolicySubjectRef | None = None,
) -> SemanticAssessmentReceiptProjectionV2:
    return SemanticAssessmentReceiptProjectionV2(
        receipt_id=receipt_id,
        receipt_digest=DIGEST_C,
        subject=subject or _subject(),
        subject_content_digest=DIGEST_A,
        guideline_id="guideline-1",
        guideline_revision_id="revision-1",
        guideline_revision_digest=DIGEST_B,
        binding_id="binding-1",
        binding_revision=2,
        binding_configuration_digest=DIGEST_C,
        assessment_assessor_id="agent-1",
        confidence=91,
        metric_results=results,
        recorded_at=NOW,
    )


def test_pass_creates_zero_findings_and_each_fail_creates_exactly_one() -> None:
    pass_result = _result(
        "pass",
        outcome=SemanticMetricOutcome.PASS,
        pinpoints=(_pinpoint("warning"),),
    )
    fail_one = _result(
        "fail-one",
        outcome=SemanticMetricOutcome.FAIL,
        pinpoints=(_pinpoint("only"),),
    )
    fail_many = _result(
        "fail-many",
        outcome=SemanticMetricOutcome.FAIL,
        pinpoints=(_pinpoint("second"), _pinpoint("first")),
    )

    findings = project_semantic_metric_findings_v2(
        _receipt(pass_result, fail_one, fail_many)
    )

    assert len(findings) == 2
    assert {item.metric_id for item in findings} == {"fail-one", "fail-many"}
    fail_many_finding = next(item for item in findings if item.metric_id == "fail-many")
    assert [item.pinpoint_key for item in fail_many_finding.pinpoints] == [
        "first",
        "second",
    ]


def test_finding_identity_does_not_depend_on_pinpoint_input_order() -> None:
    first = _pinpoint("first")
    second = _pinpoint("second")
    finding_ab = project_semantic_metric_findings_v2(
        _receipt(
            _result(
                "failed",
                outcome=SemanticMetricOutcome.FAIL,
                pinpoints=(first, second),
            )
        )
    )[0]
    finding_ba = project_semantic_metric_findings_v2(
        _receipt(
            _result(
                "failed",
                outcome=SemanticMetricOutcome.FAIL,
                pinpoints=(second, first),
            )
        )
    )[0]

    assert finding_ab.finding_id == finding_ba.finding_id
    assert finding_ab.finding_digest == finding_ba.finding_digest


def test_waiver_applies_only_to_exact_immutable_finding() -> None:
    finding_a = project_semantic_metric_findings_v2(
        _receipt(
            _result(
                "failed",
                outcome=SemanticMetricOutcome.FAIL,
                pinpoints=(_pinpoint("issue"),),
            )
        )
    )[0]
    anchor = SemanticMetricWaiverAnchorV2.from_finding(finding_a)

    assert (
        evaluate_semantic_waiver_applicability_v2(anchor, finding_a).status
        is SemanticWaiverApplicability.APPLICABLE
    )

    subject_b = _subject(version=5)
    result_b = _result(
        "failed",
        outcome=SemanticMetricOutcome.FAIL,
        pinpoints=(_pinpoint("issue"),),
        receipt_id="receipt-2",
        subject=subject_b,
    )
    finding_b = project_semantic_metric_findings_v2(
        _receipt(result_b, receipt_id="receipt-2", subject=subject_b)
    )[0]
    applicability = evaluate_semantic_waiver_applicability_v2(anchor, finding_b)

    assert finding_b.finding_id != finding_a.finding_id
    assert applicability.status is SemanticWaiverApplicability.NOT_APPLICABLE
    assert applicability.reason_code == SEMANTIC_WAIVER_FINDING_IDENTITY_MISMATCH


def test_changed_semantic_pinpoint_creates_new_finding_identity() -> None:
    original = _pinpoint("issue")
    changed = replace(original, detail="A new reassessment detail")

    finding_original = project_semantic_metric_findings_v2(
        _receipt(
            _result(
                "failed",
                outcome=SemanticMetricOutcome.FAIL,
                pinpoints=(original,),
            )
        )
    )[0]
    finding_changed = project_semantic_metric_findings_v2(
        _receipt(
            _result(
                "failed",
                outcome=SemanticMetricOutcome.FAIL,
                pinpoints=(changed,),
            )
        )
    )[0]

    assert finding_original.finding_id != finding_changed.finding_id
    assert finding_original.finding_digest != finding_changed.finding_digest
