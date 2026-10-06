"""Native waivers retain exact evidence identity and locked edition authority."""

from dataclasses import replace
from datetime import timedelta

import pytest

from okto_pulse.core.application.use_cases.base import ActorContext
from okto_pulse.core.application.use_cases.policy_governance import WAIVER_REQUEST
from okto_pulse.core.application.use_cases.semantic_guideline_governance import (
    RequestSemanticMetricWaiverCommand, RequestSemanticMetricWaiverUseCase,
    _evaluate_semantic_waiver_revalidation,
)
from okto_pulse.core.domain.guideline_policy import PolicyCurrentness
from okto_pulse.core.domain.guideline_semantic_assessment import SemanticAssessmentContractError, SemanticMetricOutcome
from okto_pulse.core.domain.guideline_semantic_currentness import SemanticAssessmentCurrentness, SemanticAssessmentCurrentnessReason
from okto_pulse.core.domain.guideline_semantic_findings_v2 import project_semantic_metric_findings_v2
from okto_pulse.core.domain.guideline_semantic_exceptions import (
    SemanticMetricWaiverEventType, transition_semantic_metric_waiver,
)
from okto_pulse.core.domain.guideline_semantic_projection import project_semantic_waiver, SemanticGuidelineProjection
from test_skb31_semantic_guideline_v2_findings import _receipt, _result, _pinpoint, NOW
from test_skb3_semantic_guideline_application import _Port, _Uow


class NativeReader:
    def __init__(self):
        self.metric = _result("failed", outcome=SemanticMetricOutcome.FAIL, pinpoints=(_pinpoint("boundary"),))
        self.receipt = _receipt(self.metric)
        self.finding = project_semantic_metric_findings_v2(self.receipt)[0]
        self.stale = False
        self.locks = []

    async def get_semantic_assessment_v2(self, **kwargs):
        return self.receipt

    async def get_semantic_finding_v2(self, **kwargs):
        return self.finding

    async def get_semantic_metric_result_v2(self, **kwargs):
        return self.metric

    async def get_semantic_assessment_v2_currentness(self, receipt, *, lock=False):
        self.locks.append(lock)
        return SemanticAssessmentCurrentness(receipt_id=receipt.receipt_id,
            currentness=PolicyCurrentness.STALE if self.stale else PolicyCurrentness.CURRENT,
            reasons=(SemanticAssessmentCurrentnessReason.SUBJECT_EDITION_CHANGED,) if self.stale else ())

    async def list_semantic_findings_v2(self, **kwargs):
        raise AssertionError("Request must resolve the exact finding")

    async def list_semantic_assessment_v2_receipts(self, **kwargs):
        raise AssertionError("Request must resolve the exact receipt")

    async def get_current_semantic_assessment_v2(self, **kwargs):
        raise AssertionError("Request must preserve the supplied receipt identity")


@pytest.mark.asyncio
async def test_native_waiver_request_checks_identity_and_locks_currentness():
    reader = NativeReader()
    storage = _Port()
    uow = _Uow(storage)
    uow.semantic_assessment_v2_reader = reader
    command = RequestSemanticMetricWaiverCommand(
        board_id=reader.receipt.subject.board_id, finding_id=reader.finding.finding_id,
        metric_result_id=reader.metric.metric_result_id, receipt_id=reader.receipt.receipt_id,
        justification="Bounded human-reviewed exception", evidence_refs=reader.metric.evidence_refs,
        expires_at=NOW + timedelta(days=1), idempotency_key="native-waiver",
    )
    actor = ActorContext("requester", "mcp", board_id=command.board_id,
                         permissions=(WAIVER_REQUEST, "guidelines.read"))
    result = await RequestSemanticMetricWaiverUseCase(clock=lambda: NOW).execute(command, actor=actor, uow=uow)
    waiver = result.mutation.waiver
    assert waiver.anchor.matches_finding(reader.finding)
    assert waiver.anchor.assessment_assessor_id == reader.receipt.assessment_assessor_id
    assert reader.locks == [True]
    projected = project_semantic_waiver(waiver,
        currentness=await reader.get_semantic_assessment_v2_currentness(reader.receipt),
        projection=SemanticGuidelineProjection.DETAIL,
    )
    assert projected.subject_edition is None
    assert projected.lifecycle_state.value == "current"
    approved = transition_semantic_metric_waiver(waiver,
        event_id="approve", expected_waiver_revision=1,
        event_type=SemanticMetricWaiverEventType.APPROVE, actor_id="reviewer",
        occurred_at=NOW + timedelta(minutes=1), reason="Independent approval",
        evidence_refs=reader.metric.evidence_refs, idempotency_key="approve-native",
    ).waiver
    state = await _evaluate_semantic_waiver_revalidation(reader=reader, waiver=approved, evaluated_at=NOW + timedelta(minutes=2))
    assert state[0].value == "approved" and reader.locks[-1] is True
    reader.stale = True
    state = await _evaluate_semantic_waiver_revalidation(reader=reader, waiver=approved, evaluated_at=NOW + timedelta(minutes=2))
    assert state[0].value == "anchor_stale"
    with pytest.raises(SemanticAssessmentContractError, match="semantic_waiver_anchor_stale"):
        await RequestSemanticMetricWaiverUseCase(clock=lambda: NOW).execute(replace(command, idempotency_key="stale"), actor=actor, uow=uow)
    assert storage.waiver_save_count == 1
