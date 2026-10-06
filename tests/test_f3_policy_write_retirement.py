"""Core rejects new Sprint authority independently of an edition's storage."""

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.application.use_cases.base import ActorContext
from okto_pulse.core.application.use_cases import policy_governance as policy
from okto_pulse.core.application.use_cases import semantic_guideline_governance as governance
from okto_pulse.core.domain.guideline_policy import PolicyEntityType, PolicySubjectRef, PolicySubjectSnapshot
from okto_pulse.core.domain.guideline_semantic_exceptions import SemanticMetricWaiverEventType
from okto_pulse.core.domain.quality_assessment import FindingAnchorType, UnboundFindingAnchor
from okto_pulse.core.ports.guideline_policy import GuidelinePolicySubjectConflict
from test_skb3_semantic_guideline_application import _Port, _Uow, _binding, _submission, NOW, DIGEST


def _retired_fixture():
    port = _Port()
    subject = PolicySubjectRef(board_id="board-1", entity_type=PolicyEntityType.SPRINT,
                               subject_id="historical", subject_version=4)
    snapshot = PolicySubjectSnapshot(subject=subject, content_digest=DIGEST,
                                    last_semantic_editor_id="author-1", captured_at=NOW)
    port.revision = replace(port.revision, revision_digest=None, metrics=tuple(
        replace(metric, target_entity_types=(PolicyEntityType.SPRINT,)) for metric in port.revision.metrics
    ))
    port.binding = _binding(port.revision)
    port.binding_heads = (port.binding,)
    port._subject_snapshot = lambda: snapshot
    port.finding = SimpleNamespace(subject=subject)
    port.waiver = SimpleNamespace(anchor=SimpleNamespace(subject=subject))
    port.get_semantic_skip_event_by_idempotency = AsyncMock(return_value=None)
    port.get_semantic_skip = AsyncMock(return_value=SimpleNamespace(scope=SimpleNamespace(subject=subject)))
    port.save_semantic_policy_skip_mutation = AsyncMock()
    submission = replace(_submission(), subject=subject,
                         assessor=replace(_submission().assessor, agent_id="owner-1"), metric_results=tuple(
        replace(metric, pinpoints=(replace(metric.pinpoints[0], anchor=UnboundFindingAnchor(anchor_type=FindingAnchorType.WHOLE_ARTIFACT)),))
        for metric in _submission().metric_results
    ))
    actor = ActorContext("owner-1", "rest", actor_kind="human", board_id="board-1", permissions=(
        policy.ASSESSMENTS_RECORD, policy.ADOPTION_MANAGE, policy.WAIVER_REQUEST,
        policy.WAIVER_REVIEW, policy.WAIVER_REVOKE, policy.WAIVER_REVALIDATE, "guidelines.read",
        "spec.validation.submit", "guidelines.delete", "spec.entity.edit_fields",
    ))
    return port, _Uow(port), submission, actor


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["assessment", "request", "review", "revoke", "revalidate", "skip_create", "skip_revoke"])
async def test_new_sprint_policy_writes_are_refused_before_commit(operation):
    port, uow, submission, actor = _retired_fixture()
    evidence = submission.metric_results[0].evidence_refs
    common = dict(board_id="board-1", idempotency_key="new-operation")
    from okto_pulse.core.application.use_cases.semantic_guideline_v2 import (
        SealSemanticGuidelineAssessmentV2Command, SealSemanticGuidelineAssessmentV2UseCase,
    )
    from test_skb31_semantic_guideline_v2_application import _draft, _pinpoint
    draft = replace(_draft(_pinpoint(FindingAnchorType.WHOLE_ARTIFACT)),
                    subject=submission.subject, assessor=submission.assessor)
    cases = {
        "assessment": (SealSemanticGuidelineAssessmentV2UseCase(),
                       SealSemanticGuidelineAssessmentV2Command(board_id="board-1", actor_id=actor.actor_id, draft=draft)),
        "request": (governance.RequestSemanticMetricWaiverUseCase(), governance.RequestSemanticMetricWaiverCommand(
            **common, metric_result_id="metric", finding_id="finding", receipt_id="receipt",
            justification="New exception", evidence_refs=evidence, expires_at=None)),
        "review": (governance.ReviewSemanticMetricWaiverUseCase(), governance.ReviewSemanticMetricWaiverCommand(
            **common, waiver_id="waiver", decision=SemanticMetricWaiverEventType.APPROVE, reason="Review",
            evidence_refs=evidence, expected_waiver_revision=1)),
        "revoke": (governance.RevokeSemanticMetricWaiverUseCase(), governance.RevokeSemanticMetricWaiverCommand(
            **common, waiver_id="waiver", reason="Revoke", evidence_refs=evidence, expected_waiver_revision=2)),
        "revalidate": (governance.RevalidateSemanticMetricWaiverUseCase(), governance.RevalidateSemanticMetricWaiverCommand(
            **common, waiver_id="waiver", expected_waiver_revision=2, evaluated_at=NOW)),
        "skip_create": (governance.CreateSemanticPolicySkipUseCase(), governance.CreateSemanticPolicySkipCommand(
            **common, entity_type=PolicyEntityType.SPRINT, subject_id="historical", expected_subject_version=4,
            binding_id=port.binding.binding_id, reason="New skip")),
        "skip_revoke": (governance.RevokeSemanticPolicySkipUseCase(), governance.RevokeSemanticPolicySkipCommand(
            **common, skip_id="skip", expected_skip_revision=1, reason="Revoke")),
    }
    use_case, command = cases[operation]
    with pytest.raises(GuidelinePolicySubjectConflict, match="semantic_policy_subject_type_retired"):
        await use_case.execute(command, actor=actor, uow=uow)
    assert uow.commit_count == 0
    assert port.saved is None and port.waiver_save_count == 0
    port.save_semantic_policy_skip_mutation.assert_not_awaited()


@pytest.mark.asyncio
async def test_native_writer_refuses_sprint_without_uow_before_resolving_adapters():
    from okto_pulse.core.application.use_cases.semantic_guideline_v2 import (
        SealSemanticGuidelineAssessmentV2Command, SealSemanticGuidelineAssessmentV2UseCase,
    )
    from test_skb31_semantic_guideline_v2_application import _draft, _pinpoint

    _, _, submission, actor = _retired_fixture()
    draft = replace(_draft(_pinpoint(FindingAnchorType.WHOLE_ARTIFACT)),
                    subject=submission.subject, assessor=submission.assessor)
    command = SealSemanticGuidelineAssessmentV2Command(
        board_id="board-1", actor_id=actor.actor_id, draft=draft,
    )
    with pytest.raises(GuidelinePolicySubjectConflict, match="semantic_policy_subject_type_retired"):
        await SealSemanticGuidelineAssessmentV2UseCase().execute(command)
