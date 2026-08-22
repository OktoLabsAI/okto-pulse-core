"""SK-B3.1 Core v2 semantic pinpoint and canonical digest contracts."""

from __future__ import annotations

from dataclasses import FrozenInstanceError, replace

import pytest

from okto_pulse.core.domain.guideline_policy import (
    GuidelineMetricDirection,
    PolicyEntityType,
    PolicySubjectRef,
)
from okto_pulse.core.domain.guideline_semantic_assessment import (
    SemanticAssessmentAssessor,
    SemanticAssessmentContractError,
    SemanticMetricOutcome,
    SemanticThresholdSource,
)
from okto_pulse.core.domain.guideline_semantic_v2 import (
    AnchorSnapshot,
    SemanticAnchorAvailability,
    SemanticAssessmentRequestV2,
    SemanticMetricAssessmentV2,
    SemanticMetricResultV2,
    SemanticPinpointKind,
    SemanticPinpointV2,
    semantic_assessment_request_digest_v2,
    semantic_finding_digest_v2,
    semantic_metric_result_digest_v2,
    semantic_receipt_digest_v2,
)
from okto_pulse.core.domain.quality_assessment import (
    EvidenceRef,
    FindingAnchorType,
    FindingSeverity,
    UnboundFindingAnchor,
)


DIGEST_A = "a" * 64
DIGEST_B = "b" * 64


def _subject() -> PolicySubjectRef:
    return PolicySubjectRef(
        board_id="board-1",
        entity_type=PolicyEntityType.SPEC,
        subject_id="spec-1",
        subject_version=7,
    )


def _anchor(ref: str = "technical_requirements.tr_domain") -> UnboundFindingAnchor:
    return UnboundFindingAnchor(
        anchor_type=FindingAnchorType.STRUCTURED_CHILD,
        anchor_ref=ref,
        excerpt_hash=DIGEST_B,
    )


def _snapshot(**overrides: object) -> AnchorSnapshot:
    values: dict[str, object] = {
        "label": "Domain contract",
        "excerpt": "Core declares policy; Community supplies mechanism.",
        "source_version": "7",
        "availability_at_seal": SemanticAnchorAvailability.AVAILABLE,
    }
    values.update(overrides)
    return AnchorSnapshot(**values)


def _pinpoint(
    key: str = "domain-contract",
    *,
    kind: SemanticPinpointKind = SemanticPinpointKind.ISSUE,
    severity: FindingSeverity | None = FindingSeverity.HIGH,
    title: str = "Concrete mechanism crosses the Core boundary",
) -> SemanticPinpointV2:
    return SemanticPinpointV2(
        pinpoint_key=key,
        kind=kind,
        title=title,
        detail="The requirement imports a concrete persistence concern.",
        severity=severity,
        remediation="Introduce a public Core port and register its adapter.",
        anchor=_anchor(f"technical_requirements.{key}"),
        anchor_snapshot=_snapshot(label=title),
    )


def _evidence() -> EvidenceRef:
    return EvidenceRef(
        source_type="spec",
        source_id="spec-1",
        source_version=7,
        content_hash=DIGEST_A,
    )


def _assessment(*pinpoints: SemanticPinpointV2) -> SemanticMetricAssessmentV2:
    return SemanticMetricAssessmentV2(
        metric_id="metric-1",
        score=35,
        rationale="The referenced requirement crosses the architectural boundary.",
        evidence_refs=(_evidence(),),
        pinpoints=pinpoints or (_pinpoint(),),
    )


def _request(*pinpoints: SemanticPinpointV2) -> SemanticAssessmentRequestV2:
    return SemanticAssessmentRequestV2(
        subject=_subject(),
        binding_id="binding-1",
        expected_binding_revision=3,
        guideline_revision_id="revision-1",
        idempotency_key="request-1",
        confidence=96,
        assessor=SemanticAssessmentAssessor(
            agent_id="agent-1",
            model_id="model-1",
        ),
        metric_results=(_assessment(*pinpoints),),
    )


def _result(*pinpoints: SemanticPinpointV2) -> SemanticMetricResultV2:
    return SemanticMetricResultV2(
        metric_result_id="metric-result-1",
        receipt_id="receipt-1",
        subject=_subject(),
        binding_id="binding-1",
        guideline_id="guideline-1",
        revision_id="revision-1",
        metric_id="metric-1",
        metric_code="architecture.segregation",
        metric_definition_digest=DIGEST_A,
        score=35,
        direction=GuidelineMetricDirection.MINIMUM,
        default_threshold=80,
        effective_threshold=80,
        threshold_source=SemanticThresholdSource.DEFAULT,
        outcome=SemanticMetricOutcome.FAIL,
        rationale="The referenced requirement crosses the architectural boundary.",
        evidence_refs=(_evidence(),),
        pinpoints=pinpoints or (_pinpoint(),),
    )


def test_value_objects_are_frozen_and_normalize_utf8_nfc() -> None:
    pinpoint = _pinpoint(title="Cafe\u0301 boundary")

    assert pinpoint.title == "Café boundary"
    assert pinpoint.anchor_snapshot.label == "Café boundary"
    assert pinpoint.blocking_for(SemanticMetricOutcome.FAIL) is True
    assert pinpoint.blocking_for(SemanticMetricOutcome.PASS) is False
    with pytest.raises(FrozenInstanceError):
        pinpoint.title = "changed"  # type: ignore[misc]


@pytest.mark.parametrize(
    ("kind", "severity", "expected_code"),
    [
        (
            SemanticPinpointKind.ISSUE,
            None,
            "semantic_pinpoint_v2_issue_severity_required",
        ),
        ("warning", FindingSeverity.LOW, "semantic_pinpoint_v2_kind_invalid"),
    ],
)
def test_invalid_pinpoint_combinations_return_stable_codes(
    kind: object,
    severity: FindingSeverity | None,
    expected_code: str,
) -> None:
    with pytest.raises(SemanticAssessmentContractError) as exc_info:
        _pinpoint(kind=kind, severity=severity)  # type: ignore[arg-type]

    assert exc_info.value.code == expected_code


def test_inaccessible_snapshot_forbids_sealed_excerpt() -> None:
    with pytest.raises(SemanticAssessmentContractError) as exc_info:
        _snapshot(
            availability_at_seal=SemanticAnchorAvailability.INACCESSIBLE,
            excerpt="secret",
        )

    assert (
        exc_info.value.code == "semantic_anchor_snapshot_inaccessible_excerpt_forbidden"
    )


def test_failed_result_requires_at_least_one_issue() -> None:
    evidence = _pinpoint(
        kind=SemanticPinpointKind.EVIDENCE,
        severity=None,
    )

    with pytest.raises(SemanticAssessmentContractError) as exc_info:
        _result(evidence)

    assert exc_info.value.code == "semantic_pinpoints_v2_failed_issue_required"


def test_v2_request_and_result_digests_are_order_independent() -> None:
    first = _pinpoint("a")
    second = _pinpoint("b", title="Second issue")

    request_ab = semantic_assessment_request_digest_v2(_request(first, second))
    request_ba = semantic_assessment_request_digest_v2(_request(second, first))
    result_ab = semantic_metric_result_digest_v2(_result(first, second))
    result_ba = semantic_metric_result_digest_v2(_result(second, first))

    assert request_ab == request_ba
    assert result_ab == result_ba
    assert len(request_ab) == len(result_ab) == 64
    assert request_ab != result_ab


@pytest.mark.parametrize(
    "changed",
    [
        _pinpoint(title="Changed title"),
        replace(_pinpoint(), detail="Changed detail"),
        replace(_pinpoint(), remediation="Changed remediation"),
        replace(_pinpoint(), anchor=_anchor("technical_requirements.changed")),
        replace(_pinpoint(), anchor_snapshot=_snapshot(label="Changed label")),
    ],
)
def test_each_semantic_change_changes_v2_request_digest(
    changed: SemanticPinpointV2,
) -> None:
    assert semantic_assessment_request_digest_v2(
        _request(changed)
    ) != semantic_assessment_request_digest_v2(_request(_pinpoint()))


def test_receipt_and_finding_have_independent_v2_namespaces() -> None:
    payload = {
        "contract_version": 2,
        "receipt_id": "receipt-1",
        "metric_results": [
            _pinpoint().digest_payload(outcome=SemanticMetricOutcome.FAIL)
        ],
    }

    receipt = semantic_receipt_digest_v2(payload)
    finding = semantic_finding_digest_v2(payload)

    assert len(receipt) == len(finding) == 64
    assert receipt != finding


def test_v2_golden_vectors_are_fixed() -> None:
    assert semantic_assessment_request_digest_v2(_request(_pinpoint())) == (
        "451e7cfd3e6f3442542ae8917a06652c6ab4ef8478721b88b146cbfc7f51a85c"
    )
    assert semantic_metric_result_digest_v2(_result(_pinpoint())) == (
        "a235390ae74906b5cee8fd53502c5c2b432aad5729c4dac8ba345e11604cde4e"
    )
