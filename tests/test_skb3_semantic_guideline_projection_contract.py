"""I5 closed projections and signed semantic keyset contracts."""

from __future__ import annotations

from dataclasses import asdict, fields
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from okto_pulse.core.application.use_cases.base import ActorContext
from okto_pulse.core.application.use_cases.policy_governance import WAIVER_READ
from okto_pulse.core.application.use_cases.semantic_guideline_governance import (
    GetSemanticMetricWaiverCommand,
    GetSemanticMetricWaiverUseCase,
    ListSemanticMetricWaiversCommand,
    ListSemanticMetricWaiversUseCase,
)
from okto_pulse.core.domain.guideline_compliance import (
    POLICY_CURSOR_TOKEN_MAX_LENGTH,
    PolicyCursorCodec,
)
from okto_pulse.core.domain.guideline_policy import (
    BoardGuidelineBinding,
    GuidelineEnforcement,
    GuidelineMetric,
    GuidelineMetricDirection,
    GuidelinePolicyContractError,
    GuidelineRevision,
    PolicyCurrentness,
    PolicyEntityType,
    PolicySubjectRef,
    PolicySubjectSnapshot,
)
from okto_pulse.core.domain.guideline_semantic_assessment import (
    SemanticAssessmentAssessor,
    SemanticGuidelineAssessmentContext,
    SemanticGuidelineAssessmentSubmission,
    SemanticMetricAssessment,
    record_semantic_guideline_assessment,
    semantic_binding_head_digest_v1,
    semantic_policy_set_digest_v1,
)
from okto_pulse.core.domain.guideline_semantic_currentness import (
    assess_semantic_assessment_currentness,
    semantic_assessment_current_snapshot_from_context,
)
from okto_pulse.core.domain.guideline_semantic_exceptions import (
    SemanticExceptionActorKind,
    SemanticMetricWaiverAnchor,
    SemanticMetricWaiverEventType,
    SemanticMetricWaiverExpireReason,
    SemanticMetricWaiverStatus,
    SemanticPolicySkipScope,
    create_semantic_policy_skip,
    request_semantic_metric_waiver,
    transition_semantic_metric_waiver,
)
from okto_pulse.core.domain.guideline_semantic_findings import (
    project_semantic_metric_findings,
)
from okto_pulse.core.domain.guideline_semantic_projection import (
    SemanticAssessmentPage,
    SemanticAssessmentPageCursor,
    SemanticAssessmentSummary,
    SemanticFindingPageCursor,
    SemanticGuidelineProjection,
    SemanticSkipPageCursor,
    SemanticWaiverPageCursor,
    project_semantic_assessment,
    project_semantic_finding,
    project_semantic_skip,
    project_semantic_waiver,
)
from okto_pulse.core.domain.quality_assessment import (
    EvidenceRef,
    FindingAnchorType,
    UnboundFindingAnchor,
)
from okto_pulse.core.ports.guideline_policy import (
    GuidelinePolicyInvalidCursor,
    SemanticAssessmentListQuery,
    SemanticFindingListQuery,
    SemanticSkipListQuery,
    SemanticWaiverListQuery,
)


NOW = datetime(2026, 7, 31, 12, tzinfo=timezone.utc)
DIGEST = "a" * 64
SIGNING_KEY = b"s" * 32


def _evidence() -> EvidenceRef:
    return EvidenceRef(
        source_type="spec",
        source_id="spec-1",
        source_version=4,
        content_hash=DIGEST,
    )


def _semantic_evidence():
    metric = GuidelineMetric(
        metric_id="segregation",
        code="architecture.segregation",
        title="Segregation",
        description="Measure technical/business isolation.",
        evaluation_rubric="0 is coupled; 100 is isolated.",
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
        content="Use ports and adapters.",
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
        binding_revision=3,
        adopted_by="owner-1",
        adopted_at=NOW,
        enforcement=GuidelineEnforcement.BLOCKING,
        minimum_confidence=80,
    )
    subject = PolicySubjectSnapshot(
        subject=PolicySubjectRef(
            board_id="board-1",
            entity_type=PolicyEntityType.SPEC,
            subject_id="spec-1",
            subject_version=4,
        ),
        content_digest=DIGEST,
        last_semantic_editor_id="author-1",
        captured_at=NOW,
    )
    context = SemanticGuidelineAssessmentContext(
        subject_snapshot=subject,
        binding=binding,
        revision=revision,
        policy_set_digest=semantic_policy_set_digest_v1(
            (binding,),
            (revision,),
        ),
        binding_head_digest=semantic_binding_head_digest_v1((binding,)),
    )
    submission = SemanticGuidelineAssessmentSubmission(
        subject=subject.subject,
        binding_id=binding.binding_id,
        expected_binding_revision=binding.binding_revision,
        guideline_revision_id=revision.revision_id,
        idempotency_key="assessment-key",
        confidence=90,
        assessor=SemanticAssessmentAssessor(
            agent_id="agent-1",
            model_id="model-1",
        ),
        metric_results=(
            SemanticMetricAssessment(
                metric_id=metric.metric_id,
                score=20,
                rationale="The domain imports an infrastructure adapter.",
                evidence_refs=(_evidence(),),
                pinpoints=(
                    UnboundFindingAnchor(
                        anchor_type=FindingAnchorType.FIELD,
                        anchor_ref="technical_requirements.architecture",
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
    current_snapshot = semantic_assessment_current_snapshot_from_context(context)
    currentness = assess_semantic_assessment_currentness(
        result.receipt,
        current_snapshot,
    )
    finding = project_semantic_metric_findings(result.receipt)[0]
    waiver = request_semantic_metric_waiver(
        waiver_id="waiver-1",
        event_id="waiver-event-1",
        anchor=SemanticMetricWaiverAnchor.from_finding(
            finding,
            assessment_assessor_id=result.receipt.assessor.agent_id,
        ),
        justification="Temporary migration exception.",
        evidence_refs=(_evidence(),),
        requested_by="requester-1",
        requested_at=NOW + timedelta(minutes=1),
        expires_at=NOW + timedelta(days=1),
        idempotency_key="waiver-key",
    ).waiver
    skip = create_semantic_policy_skip(
        skip_id="skip-1",
        event_id="skip-event-1",
        scope=SemanticPolicySkipScope.from_authority(
            subject_snapshot=subject,
            binding=binding,
            revision=revision,
        ),
        reason="Human exception.",
        actor_id="human-1",
        actor_kind=SemanticExceptionActorKind.HUMAN,
        occurred_at=NOW,
        idempotency_key="skip-key",
    ).skip
    return result.receipt, current_snapshot, currentness, finding, waiver, skip


def _field_names(value: object) -> set[str]:
    return {field.name for field in fields(value)}


def test_summary_detail_full_allowlists_are_structurally_closed() -> None:
    receipt, current, currentness, finding, waiver, skip = _semantic_evidence()

    assessment_summary = project_semantic_assessment(
        receipt,
        currentness=currentness,
        projection=SemanticGuidelineProjection.SUMMARY,
    )
    assessment_detail = project_semantic_assessment(
        receipt,
        currentness=currentness,
        projection=SemanticGuidelineProjection.DETAIL,
    )
    assessment_full = project_semantic_assessment(
        receipt,
        currentness=currentness,
        projection=SemanticGuidelineProjection.FULL,
    )
    assert {
        "metric_results",
        "rationale",
        "evidence_refs",
        "pinpoints",
        "receipt_digest",
        "idempotency_key",
    }.isdisjoint(_field_names(assessment_summary))
    metric_detail = assessment_detail.metric_results[0]
    assert {
        "rationale",
        "evidence_refs",
        "pinpoints",
    } <= _field_names(metric_detail)
    assert "metric_definition_digest" not in _field_names(metric_detail)
    assert {
        "receipt_digest",
        "input_digest",
        "request_digest",
        "idempotency_key",
    } <= _field_names(assessment_full)
    assert "metric_definition_digest" in _field_names(assessment_full.metric_results[0])

    finding_summary = project_semantic_finding(
        finding,
        currentness=currentness,
        projection=SemanticGuidelineProjection.SUMMARY,
    )
    finding_detail = project_semantic_finding(
        finding,
        currentness=currentness,
        projection=SemanticGuidelineProjection.DETAIL,
    )
    finding_full = project_semantic_finding(
        finding,
        currentness=currentness,
        projection=SemanticGuidelineProjection.FULL,
    )
    assert {"rationale", "evidence_refs", "pinpoints"}.isdisjoint(
        _field_names(finding_summary)
    )
    assert {"rationale", "evidence_refs", "pinpoints"} <= _field_names(finding_detail)
    assert "finding_digest" in _field_names(finding_full)

    waiver_summary = project_semantic_waiver(
        waiver,
        currentness=currentness,
        projection=SemanticGuidelineProjection.SUMMARY,
    )
    waiver_detail = project_semantic_waiver(
        waiver,
        currentness=currentness,
        projection=SemanticGuidelineProjection.DETAIL,
    )
    waiver_full = project_semantic_waiver(
        waiver,
        currentness=currentness,
        projection=SemanticGuidelineProjection.FULL,
    )
    assert {"justification", "evidence_refs", "scope_digest"}.isdisjoint(
        _field_names(waiver_summary)
    )
    assert {"justification", "evidence_refs"} <= _field_names(waiver_detail)
    assert {
        "scope_digest",
        "head_digest",
        "last_event_idempotency_key",
    } <= _field_names(waiver_full)
    assert waiver_full.last_event_idempotency_key == "waiver-key"

    skip_summary = project_semantic_skip(
        skip,
        current=current,
        projection=SemanticGuidelineProjection.SUMMARY,
    )
    assert skip_summary.currentness is PolicyCurrentness.CURRENT
    assert skip_summary.currentness_reasons == ()
    assert {"reason", "scope_digest", "idempotency_key"}.isdisjoint(
        _field_names(skip_summary)
    )


def test_waiver_collection_projection_is_expiry_aware_without_mutating_head() -> None:
    _, _, currentness, _, requested, _ = _semantic_evidence()
    approved = transition_semantic_metric_waiver(
        requested,
        event_id="waiver-event-approve",
        expected_waiver_revision=1,
        event_type=SemanticMetricWaiverEventType.APPROVE,
        actor_id="reviewer-2",
        occurred_at=NOW + timedelta(minutes=2),
        reason="The exception is independently bounded and reviewed.",
        evidence_refs=(_evidence(),),
        idempotency_key="waiver-approve-key",
    ).waiver

    projected = project_semantic_waiver(
        approved,
        currentness=currentness,
        projection=SemanticGuidelineProjection.DETAIL,
        evaluated_at=NOW + timedelta(days=2),
    )

    assert approved.status is SemanticMetricWaiverStatus.APPROVED
    assert approved.expire_reason is None
    assert projected.status is SemanticMetricWaiverStatus.EXPIRED
    assert projected.expire_reason is SemanticMetricWaiverExpireReason.SCHEDULED_EXPIRY
    assert projected.currentness is PolicyCurrentness.CURRENT
    assert projected.last_event_type is SemanticMetricWaiverEventType.APPROVE


@pytest.mark.asyncio
async def test_expired_status_filter_scans_approved_heads_as_of_evaluated_at() -> None:
    receipt, current, _, _, requested, _ = _semantic_evidence()
    expired_head = transition_semantic_metric_waiver(
        requested,
        event_id="waiver-event-expired-head-approve",
        expected_waiver_revision=1,
        event_type=SemanticMetricWaiverEventType.APPROVE,
        actor_id="reviewer-2",
        occurred_at=NOW + timedelta(minutes=2),
        reason="The first bounded exception is approved.",
        evidence_refs=(_evidence(),),
        idempotency_key="waiver-expired-head-approve",
    ).waiver
    future_request = request_semantic_metric_waiver(
        waiver_id="waiver-future",
        event_id="waiver-event-future-request",
        anchor=requested.anchor,
        justification="A second independently bounded migration exception.",
        evidence_refs=(_evidence(),),
        requested_by="requester-2",
        requested_at=NOW + timedelta(minutes=5),
        expires_at=NOW + timedelta(days=5),
        idempotency_key="waiver-future-request",
    )
    future_head = transition_semantic_metric_waiver(
        future_request.waiver,
        event_id="waiver-event-future-approve",
        expected_waiver_revision=1,
        event_type=SemanticMetricWaiverEventType.APPROVE,
        actor_id="reviewer-2",
        occurred_at=NOW + timedelta(minutes=6),
        reason="The second bounded exception is approved.",
        evidence_refs=(_evidence(),),
        idempotency_key="waiver-future-approve",
    ).waiver

    class Port:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        async def list_board_semantic_waivers(self, **kwargs):
            self.calls.append(kwargs)
            after = kwargs["after"]
            if after is None:
                return (
                    (future_head,),
                    (future_head.requested_at, future_head.waiver_id),
                )
            return ((expired_head,), None)

        async def get_semantic_assessment_receipt(self, **_kwargs):
            return receipt

        async def resolve_semantic_assessment_current_snapshot(
            self,
            **_kwargs,
        ):
            return current

    class Boards:
        async def get(self, board_id: str):
            return SimpleNamespace(
                id=board_id,
                owner_id="owner-1",
                realm_id="local",
            )

    port = Port()
    uow = SimpleNamespace(
        boards=Boards(),
        services=SimpleNamespace(
            guidelines=SimpleNamespace(
                semantic_policy_persistence=lambda: port,
            )
        ),
    )
    result = await ListSemanticMetricWaiversUseCase().execute(
        ListSemanticMetricWaiversCommand(
            SemanticWaiverListQuery(
                board_id="board-1",
                evaluated_at=NOW + timedelta(days=2),
                status=SemanticMetricWaiverStatus.EXPIRED,
                limit=1,
            )
        ),
        actor=ActorContext(
            "reader-1",
            "mcp",
            board_id="board-1",
            permissions=(WAIVER_READ, "guidelines.read"),
        ),
        uow=uow,
    )

    assert tuple(item.waiver_id for item in result.page.items) == ("waiver-1",)
    assert result.page.items[0].status is SemanticMetricWaiverStatus.EXPIRED
    assert result.page.has_more is False
    assert len(port.calls) == 2
    assert all(call["status"] is None for call in port.calls)


@pytest.mark.asyncio
async def test_singular_waiver_projects_expiry_at_required_snapshot() -> None:
    receipt, current, _, _, requested, _ = _semantic_evidence()
    approved = transition_semantic_metric_waiver(
        requested,
        event_id="waiver-event-singular-approve",
        expected_waiver_revision=1,
        event_type=SemanticMetricWaiverEventType.APPROVE,
        actor_id="reviewer-2",
        occurred_at=NOW + timedelta(minutes=2),
        reason="The bounded exception is independently approved.",
        evidence_refs=(_evidence(),),
        idempotency_key="waiver-singular-approve",
    ).waiver

    class Port:
        async def get_semantic_waiver(self, **_kwargs):
            return approved

        async def get_semantic_assessment_receipt(self, **_kwargs):
            return receipt

        async def resolve_semantic_assessment_current_snapshot(
            self,
            **_kwargs,
        ):
            return current

    class Boards:
        async def get(self, board_id: str):
            return SimpleNamespace(
                id=board_id,
                owner_id="owner-1",
                realm_id="local",
            )

    uow = SimpleNamespace(
        boards=Boards(),
        services=SimpleNamespace(
            guidelines=SimpleNamespace(
                semantic_policy_persistence=lambda: Port(),
            )
        ),
    )
    evaluated_at = NOW + timedelta(days=2)
    result = await GetSemanticMetricWaiverUseCase().execute(
        GetSemanticMetricWaiverCommand(
            board_id="board-1",
            waiver_id="waiver-1",
            evaluated_at=evaluated_at,
        ),
        actor=ActorContext(
            "reader-1",
            "mcp",
            board_id="board-1",
            permissions=(WAIVER_READ, "guidelines.read"),
        ),
        uow=uow,
    )

    assert result.waiver.status is SemanticMetricWaiverStatus.EXPIRED
    assert (
        result.waiver.expire_reason
        is SemanticMetricWaiverExpireReason.SCHEDULED_EXPIRY
    )
    with pytest.raises(
        ValueError,
        match="semantic_waiver_evaluated_at_required",
    ):
        GetSemanticMetricWaiverCommand(
            board_id="board-1",
            waiver_id="waiver-1",
            evaluated_at=None,  # type: ignore[arg-type]
        )


def test_projection_and_page_reject_cross_profile_construction() -> None:
    receipt, _current, currentness, *_ = _semantic_evidence()
    summary = project_semantic_assessment(
        receipt,
        currentness=currentness,
        projection=SemanticGuidelineProjection.SUMMARY,
    )
    with pytest.raises(
        GuidelinePolicyContractError,
        match="semantic_assessment_summary_projection_invalid",
    ):
        SemanticAssessmentSummary(
            **{
                **asdict(summary),
                "projection": SemanticGuidelineProjection.FULL,
            }
        )
    with pytest.raises(
        GuidelinePolicyContractError,
        match="semantic_page_projection_mismatch",
    ):
        SemanticAssessmentPage(
            items=(summary,),
            limit=1,
            next_cursor=None,
            has_more=False,
            projection=SemanticGuidelineProjection.DETAIL,
        )


def _semantic_cursors():
    common = {
        "at": NOW,
        "item_id": "item-1",
        "filter_digest": "b" * 64,
        "projection_digest": "c" * 64,
    }
    return (
        ("semantic_assessment", SemanticAssessmentPageCursor(**common)),
        ("semantic_finding", SemanticFindingPageCursor(**common)),
        ("semantic_waiver", SemanticWaiverPageCursor(**common)),
        ("semantic_skip", SemanticSkipPageCursor(**common)),
    )


@pytest.mark.parametrize(("kind", "cursor"), _semantic_cursors())
def test_semantic_cursor_round_trip_and_cross_kind_rejection(
    kind: str,
    cursor: object,
) -> None:
    codec = PolicyCursorCodec(SIGNING_KEY)
    token = codec.encode(cursor)
    assert codec.decode(token, expected_kind=kind) == cursor
    other_kind = next(
        candidate for candidate, _ in _semantic_cursors() if candidate != kind
    )
    with pytest.raises(GuidelinePolicyContractError, match="invalid_cursor"):
        codec.decode(token, expected_kind=other_kind)
    with pytest.raises(GuidelinePolicyContractError, match="invalid_cursor"):
        codec.decode(token, expected_kind=kind.removeprefix("semantic_"))


def test_semantic_cursor_tamper_and_oversize_fail_closed() -> None:
    codec = PolicyCursorCodec(SIGNING_KEY)
    token = codec.encode(_semantic_cursors()[0][1])
    encoded, signature = token.split(".", 1)
    replacement = "A" if encoded[-1] != "A" else "B"
    tampered = f"{encoded[:-1]}{replacement}.{signature}"
    with pytest.raises(GuidelinePolicyContractError, match="invalid_cursor"):
        codec.decode(tampered, expected_kind="semantic_assessment")
    with pytest.raises(GuidelinePolicyContractError, match="invalid_cursor"):
        codec.decode(
            "x" * (POLICY_CURSOR_TOKEN_MAX_LENGTH + 1),
            expected_kind="semantic_assessment",
        )


def test_query_cursor_is_scoped_to_board_filters_and_projection() -> None:
    query = SemanticAssessmentListQuery(
        board_id="board-1",
        guideline_id="guideline-1",
        currentness=PolicyCurrentness.CURRENT,
        projection=SemanticGuidelineProjection.SUMMARY,
    )
    cursor = SemanticAssessmentPageCursor(
        at=NOW,
        item_id="receipt-1",
        filter_digest=query.filter_digest,
        projection_digest=query.projection_digest,
    )
    SemanticAssessmentListQuery(
        board_id="board-1",
        guideline_id="guideline-1",
        currentness=PolicyCurrentness.CURRENT,
        projection=SemanticGuidelineProjection.SUMMARY,
        cursor=cursor,
    )
    for changed in (
        {"board_id": "board-2"},
        {"guideline_id": "guideline-2"},
        {"currentness": PolicyCurrentness.STALE},
        {"projection": SemanticGuidelineProjection.FULL},
    ):
        values = {
            "board_id": "board-1",
            "guideline_id": "guideline-1",
            "currentness": PolicyCurrentness.CURRENT,
            "projection": SemanticGuidelineProjection.SUMMARY,
            "cursor": cursor,
            **changed,
        }
        with pytest.raises(
            GuidelinePolicyInvalidCursor,
            match="semantic_assessment_cursor_context_mismatch",
        ):
            SemanticAssessmentListQuery(**values)


def test_all_query_profiles_have_distinct_digests_and_limit_is_200() -> None:
    factories = (
        lambda profile: SemanticAssessmentListQuery(
            board_id="board-1",
            projection=profile,
            limit=200,
        ),
        lambda profile: SemanticFindingListQuery(
            board_id="board-1",
            projection=profile,
            limit=200,
        ),
        lambda profile: SemanticWaiverListQuery(
            board_id="board-1",
            evaluated_at=NOW,
            projection=profile,
            limit=200,
        ),
        lambda profile: SemanticSkipListQuery(
            board_id="board-1",
            projection=profile,
            limit=200,
        ),
    )
    for factory in factories:
        digests = {
            factory(profile).projection_digest
            for profile in SemanticGuidelineProjection
        }
        assert len(digests) == 3
        with pytest.raises(ValueError, match="guideline_page_limit_invalid"):
            factory(SemanticGuidelineProjection.SUMMARY).__class__(
                **{
                    **{
                        field.name: getattr(
                            factory(SemanticGuidelineProjection.SUMMARY),
                            field.name,
                        )
                        for field in fields(
                            factory(SemanticGuidelineProjection.SUMMARY)
                        )
                        if field.init
                    },
                    "limit": 201,
                }
            )
