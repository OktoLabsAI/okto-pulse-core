"""SK-B3.1 authorized anchor sealing and application atomicity."""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest

from okto_pulse.core.application.use_cases.semantic_guideline_v2 import (
    SealSemanticGuidelineAssessmentV2Command,
    SealSemanticGuidelineAssessmentV2UseCase,
)
from okto_pulse.core.domain.guideline_policy import (
    PolicyEntityType,
    PolicySubjectRef,
)
from okto_pulse.core.domain.guideline_semantic_assessment import (
    SemanticAssessmentAssessor,
)
from okto_pulse.core.domain.guideline_semantic_v2 import (
    AnchorSnapshot,
    SemanticAnchorAvailability,
    SemanticAssessmentDraftV2,
    SemanticMetricAssessmentDraftV2,
    SemanticPinpointDraftV2,
    SemanticPinpointKind,
    semantic_assessment_request_digest_v2,
)
from okto_pulse.core.domain.quality_assessment import (
    EvidenceRef,
    FindingAnchorType,
    FindingSeverity,
    UnboundFindingAnchor,
)
from okto_pulse.core.domain.quality_canonicalization import canonical_sha256
from okto_pulse.core.inbound.guideline_policy_error import (
    guideline_policy_http_status,
    project_guideline_policy_error,
)
from okto_pulse.core.ports.semantic_subject_projection import (
    SemanticAssessmentV2CapabilitySnapshot,
    SemanticAssessmentV2PersistenceResult,
    SemanticAssessmentV2WriterUnavailable,
    SemanticPinpointProjectionV1,
    SemanticPinpointProjectionV2,
    SemanticSubjectProjectionError,
    SemanticSubjectProjectionFailure,
    SemanticSubjectProjectionRequest,
)


DIGEST = "a" * 64


def _subject() -> PolicySubjectRef:
    return PolicySubjectRef(
        board_id="board-1",
        entity_type=PolicyEntityType.SPEC,
        subject_id="spec-1",
        subject_version=3,
    )


def _pinpoint(anchor_type: FindingAnchorType) -> SemanticPinpointDraftV2:
    excerpt = f"Excerpt for {anchor_type.value}"
    return SemanticPinpointDraftV2(
        pinpoint_key=f"pinpoint-{anchor_type.value}",
        kind=SemanticPinpointKind.ISSUE,
        title=f"Review {anchor_type.value}",
        detail="The semantic issue is located in this exact element.",
        severity=FindingSeverity.HIGH,
        remediation="Update the referenced element.",
        anchor=UnboundFindingAnchor(
            anchor_type=anchor_type,
            anchor_ref=(
                None
                if anchor_type is FindingAnchorType.WHOLE_ARTIFACT
                else f"stable-{anchor_type.value}-id"
            ),
            excerpt_hash=canonical_sha256(excerpt),
        ),
    )


def _draft(*pinpoints: SemanticPinpointDraftV2) -> SemanticAssessmentDraftV2:
    return SemanticAssessmentDraftV2(
        subject=_subject(),
        binding_id="binding-1",
        expected_binding_revision=2,
        guideline_revision_id="revision-1",
        idempotency_key="request-1",
        confidence=95,
        assessor=SemanticAssessmentAssessor(
            agent_id="actor-1",
            model_id="model-1",
        ),
        metric_results=(
            SemanticMetricAssessmentDraftV2(
                metric_id="metric-1",
                score=45,
                rationale="The supplied anchors identify the issue.",
                evidence_refs=(
                    EvidenceRef(
                        source_type="spec",
                        source_id="spec-1",
                        source_version=3,
                        content_hash=DIGEST,
                    ),
                ),
                pinpoints=pinpoints,
            ),
        ),
    )


@dataclass
class ProjectionSpy:
    failure_for: FindingAnchorType | None = None
    calls: list[SemanticSubjectProjectionRequest] = field(default_factory=list)

    async def resolve_semantic_anchor(
        self,
        request: SemanticSubjectProjectionRequest,
    ) -> AnchorSnapshot:
        self.calls.append(request)
        if request.anchor.anchor_type is self.failure_for:
            raise SemanticSubjectProjectionError(
                SemanticSubjectProjectionFailure.FORBIDDEN
            )
        return AnchorSnapshot(
            label=f"Label for {request.anchor.anchor_type.value}",
            excerpt=f"Excerpt for {request.anchor.anchor_type.value}",
            source_version=str(request.subject.subject_version),
            availability_at_seal=SemanticAnchorAvailability.AVAILABLE,
        )


@dataclass
class PersistenceSpy:
    calls: list[object] = field(default_factory=list)

    async def save_semantic_assessment_v2(
        self,
        request,
    ) -> SemanticAssessmentV2PersistenceResult:
        self.calls.append(request)
        return SemanticAssessmentV2PersistenceResult(
            receipt_id="receipt-1",
            request_digest=semantic_assessment_request_digest_v2(request),
        )


@pytest.mark.asyncio
async def test_seals_all_four_authorized_anchor_types_before_one_write() -> None:
    projection = ProjectionSpy()
    persistence = PersistenceSpy()
    pinpoints = tuple(_pinpoint(item) for item in FindingAnchorType)
    use_case = SealSemanticGuidelineAssessmentV2UseCase(
        subject_projection=projection,
        persistence=persistence,
    )

    result = await use_case.execute(
        SealSemanticGuidelineAssessmentV2Command(
            board_id="board-1",
            actor_id="actor-1",
            draft=_draft(*pinpoints),
        )
    )

    assert [call.anchor.anchor_type for call in projection.calls] == sorted(
        FindingAnchorType,
        key=lambda item: f"pinpoint-{item.value}",
    )
    assert len(persistence.calls) == 1
    sealed = result.request.metric_results[0].pinpoints
    assert {item.anchor.anchor_type for item in sealed} == set(FindingAnchorType)
    assert all(
        item.anchor_snapshot.availability_at_seal
        is SemanticAnchorAvailability.AVAILABLE
        for item in sealed
    )


@pytest.mark.asyncio
async def test_forbidden_anchor_aborts_before_persistence() -> None:
    projection = ProjectionSpy(failure_for=FindingAnchorType.QA)
    persistence = PersistenceSpy()
    use_case = SealSemanticGuidelineAssessmentV2UseCase(
        subject_projection=projection,
        persistence=persistence,
    )

    with pytest.raises(SemanticSubjectProjectionError) as exc_info:
        await use_case.execute(
            SealSemanticGuidelineAssessmentV2Command(
                board_id="board-1",
                actor_id="actor-1",
                draft=_draft(
                    _pinpoint(FindingAnchorType.FIELD),
                    _pinpoint(FindingAnchorType.QA),
                ),
            )
        )

    assert exc_info.value.reason is SemanticSubjectProjectionFailure.FORBIDDEN
    assert persistence.calls == []


@pytest.mark.asyncio
async def test_excerpt_digest_mismatch_is_malformed_and_atomic() -> None:
    projection = ProjectionSpy()
    persistence = PersistenceSpy()
    pinpoint = _pinpoint(FindingAnchorType.FIELD)
    pinpoint = SemanticPinpointDraftV2(
        pinpoint_key=pinpoint.pinpoint_key,
        kind=pinpoint.kind,
        title=pinpoint.title,
        detail=pinpoint.detail,
        severity=pinpoint.severity,
        remediation=pinpoint.remediation,
        anchor=UnboundFindingAnchor(
            anchor_type=pinpoint.anchor.anchor_type,
            anchor_ref=pinpoint.anchor.anchor_ref,
            excerpt_hash="b" * 64,
        ),
    )
    use_case = SealSemanticGuidelineAssessmentV2UseCase(
        subject_projection=projection,
        persistence=persistence,
    )

    with pytest.raises(SemanticSubjectProjectionError) as exc_info:
        await use_case.execute(
            SealSemanticGuidelineAssessmentV2Command(
                board_id="board-1",
                actor_id="actor-1",
                draft=_draft(pinpoint),
            )
        )

    assert exc_info.value.reason is SemanticSubjectProjectionFailure.MALFORMED
    assert persistence.calls == []


@pytest.mark.asyncio
async def test_missing_adapters_fail_closed() -> None:
    with pytest.raises(TypeError, match="semantic_subject_projection_adapter_missing"):
        SealSemanticGuidelineAssessmentV2UseCase(
            subject_projection=object(),  # type: ignore[arg-type]
            persistence=PersistenceSpy(),
        )
    with pytest.raises(
        TypeError,
        match="semantic_assessment_v2_persistence_adapter_missing",
    ):
        SealSemanticGuidelineAssessmentV2UseCase(
            subject_projection=ProjectionSpy(),
            persistence=object(),  # type: ignore[arg-type]
        )


@pytest.mark.asyncio
async def test_projection_contracts_discriminate_v1_and_v2_losslessly() -> None:
    projection = ProjectionSpy()
    persistence = PersistenceSpy()
    result = await SealSemanticGuidelineAssessmentV2UseCase(
        subject_projection=projection,
        persistence=persistence,
    ).execute(
        SealSemanticGuidelineAssessmentV2Command(
            board_id="board-1",
            actor_id="actor-1",
            draft=_draft(_pinpoint(FindingAnchorType.FIELD)),
        )
    )
    sealed = result.request.metric_results[0].pinpoints[0]
    v1 = SemanticPinpointProjectionV1(
        anchor_type="field",
        anchor_ref="stable-field-id",
        excerpt_hash=DIGEST,
    )
    v2 = SemanticPinpointProjectionV2.from_domain(sealed, blocking=True)

    assert v1.contract_version == 1
    assert v2.contract_version == 2
    assert v2.pinpoint_key == sealed.pinpoint_key
    assert v2.label == sealed.anchor_snapshot.label
    assert v2.blocking is True


@pytest.mark.parametrize(
    ("overrides", "expected_active", "expected_reason"),
    (
        ({}, False, "unsupported_contract_version"),
        ({"writer_requested": True}, False, "v2_writer_not_ready"),
        (
            {
                "readers_ready": True,
                "storage_ready": True,
                "triggers_ready": True,
                "rest_transport_ready": True,
                "mcp_transport_ready": True,
                "writer_requested": True,
            },
            True,
            None,
        ),
    ),
)
def test_v2_writer_capability_requires_every_readers_first_prerequisite(
    overrides: dict[str, bool],
    expected_active: bool,
    expected_reason: str | None,
) -> None:
    values = {
        "readers_ready": False,
        "storage_ready": False,
        "triggers_ready": False,
        "rest_transport_ready": False,
        "mcp_transport_ready": False,
        "writer_requested": False,
    }
    values.update(overrides)

    snapshot = SemanticAssessmentV2CapabilitySnapshot(**values)

    assert snapshot.writer_active is expected_active
    assert snapshot.reason_code == expected_reason


@pytest.mark.parametrize(
    ("writer_requested", "expected_code", "expected_retryable"),
    (
        (False, "unsupported_contract_version", False),
        (True, "v2_writer_not_ready", True),
    ),
)
def test_v2_writer_gate_projects_one_bounded_rest_mcp_error(
    writer_requested: bool,
    expected_code: str,
    expected_retryable: bool,
) -> None:
    snapshot = SemanticAssessmentV2CapabilitySnapshot(
        readers_ready=False,
        storage_ready=True,
        triggers_ready=True,
        rest_transport_ready=True,
        mcp_transport_ready=True,
        writer_requested=writer_requested,
    )
    error = SemanticAssessmentV2WriterUnavailable(snapshot)

    projection = project_guideline_policy_error(error)

    assert guideline_policy_http_status(error) == 503
    assert projection["code"] == expected_code
    assert projection["retryable"] is expected_retryable
    assert projection["details"] == {
        "capability_state": (
            "disabled" if not writer_requested else "readers_not_ready"
        ),
        "mcp_transport_ready": "true",
        "readers_ready": "false",
        "reason_code": expected_code,
        "rest_transport_ready": "true",
        "storage_ready": "true",
        "triggers_ready": "true",
        "writer_requested": str(writer_requested).lower(),
    }
