from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    PermissionDeniedError,
)
from okto_pulse.core.application.use_cases.policy_governance import (
    ADOPTION_MANAGE,
    ASSESSMENTS_RECORD,
    WAIVER_REVALIDATE,
)
from okto_pulse.core.application.use_cases.semantic_guideline_governance import (
    ListSemanticPolicySkipsCommand,
    ListSemanticPolicySkipsUseCase,
    RevalidateSemanticMetricWaiverCommand,
    RevalidateSemanticMetricWaiverUseCase,
)
from okto_pulse.core.domain.guideline_policy import (
    BoardGuidelineBinding,
    GuidelineEnforcement,
    GuidelineMetric,
    GuidelineMetricDirection,
    GuidelineRevision,
    PolicyEntityType,
    PolicySubjectRef,
    PolicySubjectSnapshot,
)
from okto_pulse.core.domain.guideline_semantic_assessment import (
    SemanticAssessmentAssessor,
)
from okto_pulse.core.domain.guideline_semantic_v2 import (
    SemanticAssessmentDraftV2, SemanticMetricAssessmentDraftV2,
    SemanticPinpointDraftV2, SemanticPinpointKind,
)
from okto_pulse.core.domain.guideline_semantic_currentness import (
    NativeSemanticAssessmentCurrentSnapshot,
    native_semantic_assessment_snapshot,
)
from okto_pulse.core.domain.guideline_semantic_exceptions import (
    SemanticMetricWaiverAnchor,
    SemanticMetricWaiverEventType,
    SemanticMetricWaiverRevalidationReason,
    SemanticMetricWaiverRevalidationStatus,
    request_semantic_metric_waiver,
    transition_semantic_metric_waiver,
)
from okto_pulse.core.domain.quality_assessment import (
    EvidenceRef,
    FindingAnchorType,
    UnboundFindingAnchor,
)
from okto_pulse.core.ports.guideline_policy import (
    GuidelinePolicyIdempotencyConflict,
    SemanticSkipListQuery,
)


NOW = datetime(2026, 7, 30, 12, tzinfo=timezone.utc)
DIGEST = "a" * 64


def _metric() -> GuidelineMetric:
    return GuidelineMetric(
        metric_id="segregation",
        code="architecture.segregation",
        title="Segregation",
        description="Measures separation from business rules.",
        evaluation_rubric="0 is coupled; 100 is fully isolated.",
        target_entity_types=(PolicyEntityType.SPEC,),
        direction=GuidelineMetricDirection.MINIMUM,
        default_threshold=70,
    )


def _revision() -> GuidelineRevision:
    return GuidelineRevision(
        revision_id="revision-1",
        guideline_id="guideline-1",
        revision_number=1,
        semantic_version="1.0.0",
        title="Hexagonal architecture",
        content="Use ports and adapters.",
        metrics=(_metric(),),
        created_by="author-1",
        created_at=NOW,
    )


def _binding(
    revision: GuidelineRevision,
    board_id: str = "board-1",
) -> BoardGuidelineBinding:
    return BoardGuidelineBinding(
        binding_id="binding-1",
        board_id=board_id,
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
        metric_threshold_overrides={"architecture.segregation": 75},
    )


def _submission(
    board_id: str = "board-1",
    score: int = 82,
) -> SemanticAssessmentDraftV2:
    return SemanticAssessmentDraftV2(
        subject=PolicySubjectRef(
            board_id=board_id,
            entity_type=PolicyEntityType.SPEC,
            subject_id="spec-1",
            subject_version=4,
            subject_edition=1,
        ),
        binding_id="binding-1",
        expected_binding_revision=3,
        guideline_revision_id="revision-1",
        idempotency_key="assessment-key",
        confidence=90,
        assessor=SemanticAssessmentAssessor(
            agent_id="agent-1",
            model_id="model-1",
        ),
        metric_results=(
            SemanticMetricAssessmentDraftV2(
                metric_id="segregation",
                score=score,
                rationale="Domain behavior depends only on declared ports.",
                evidence_refs=(
                    EvidenceRef(
                        source_type="spec",
                        source_id="spec-1",
                        source_version=4,
                        content_hash=DIGEST,
                    ),
                ),
                pinpoints=(
                    SemanticPinpointDraftV2(
                        pinpoint_key="architecture",
                        kind=SemanticPinpointKind.EVIDENCE,
                        title="Architecture boundary",
                        detail="Domain behavior depends only on declared ports.",
                        severity=None, remediation=None,
                        anchor=UnboundFindingAnchor(
                            anchor_type=FindingAnchorType.FIELD,
                            anchor_ref="technical_requirements.architecture",
                        ),
                    ),
                ),
            ),
        ),
    )


class _BoardRepo:
    async def get(self, board_id: str):
        return SimpleNamespace(
            id=board_id,
            owner_id="owner-1",
            realm_id="local",
        )


class _Port:
    def __init__(self, board_id: str = "board-1") -> None:
        self.board_id = board_id
        self.revision = _revision()
        self.binding = _binding(self.revision, board_id)
        self.binding_heads = (self.binding,)
        self.saved = None
        self.replay = None
        self.waiver = None
        self.waiver_replay = None
        self.last_waiver_mutation = None
        self.finding = None
        self.waiver_save_count = 0
        self.lock_values: list[bool] = []
        self.skip_list_count = 0


    async def resolve_policy_subject_snapshot(self, *, lock=False, **_kwargs):
        self.lock_values.append(lock)
        return self._subject_snapshot()

    def _subject_snapshot(self) -> PolicySubjectSnapshot:
        return PolicySubjectSnapshot(
            subject=_submission(self.board_id).subject,
            content_digest=DIGEST,
            last_semantic_editor_id="author-1",
            captured_at=NOW,
        )

    def _current_snapshot(self) -> NativeSemanticAssessmentCurrentSnapshot:
        return native_semantic_assessment_snapshot(
            subject=self._subject_snapshot(), binding=self.binding, revision=self.revision,
        )

    async def list_bindings(self, **_kwargs):
        return (self.binding,)

    async def get_revision(self, **_kwargs):
        return self.revision


    async def get_semantic_waiver_by_idempotency(self, **_kwargs):
        return self.waiver_replay

    async def get_semantic_waiver(self, **_kwargs):
        return self.waiver




    async def resolve_semantic_assessment_current_snapshot(
        self,
        *,
        lock=False,
        **_kwargs,
    ):
        self.lock_values.append(lock)
        return self._current_snapshot()

    async def get_semantic_assessment_v2(self, **kwargs):
        return None if self.saved is None else self.saved.receipt

    async def get_semantic_finding_v2(self, **kwargs):
        return self.finding

    async def get_semantic_metric_result_v2(self, **kwargs):
        return None if self.saved is None else self.saved.receipt.metric_results[0]

    async def get_semantic_assessment_v2_currentness(self, receipt, *, lock=False):
        from okto_pulse.core.domain.guideline_semantic_currentness import assess_native_semantic_assessment_currentness
        self.lock_values.append(lock)
        return assess_native_semantic_assessment_currentness(receipt,
            subject=self._subject_snapshot(), binding=self.binding, revision=self.revision)

    async def list_semantic_assessment_v2_receipts(self, **kwargs):
        raise AssertionError("Unexpected native assessment list")

    async def get_current_semantic_assessment_v2(self, **kwargs):
        raise AssertionError("Unexpected current assessment read")

    async def list_semantic_findings_v2(self, **kwargs):
        raise AssertionError("Unexpected native finding list")

    async def save_semantic_metric_waiver_mutation(self, *, mutation):
        self.waiver_save_count += 1
        self.last_waiver_mutation = mutation
        self.waiver = mutation.waiver
        return mutation

    async def list_semantic_policy_skips(self, **_kwargs):
        self.skip_list_count += 1
        return (), None


class _Uow:
    def __init__(self, port: _Port) -> None:
        self._board_id = port.board_id
        self.boards = _BoardRepo()
        self.semantic_assessment_v2_reader = port
        self.services = SimpleNamespace(
            guidelines=SimpleNamespace(
                policy_persistence=lambda: port,
                semantic_policy_persistence=lambda: port,
            ),
            specs=SimpleNamespace(
                get_spec=self._get_spec,
            ),
        )
        self.commit_count = 0
        self.rollback_count = 0

    async def _get_spec(self, spec_id: str):
        return SimpleNamespace(
            id=spec_id,
            board_id=self._board_id,
            status=SimpleNamespace(value="approved"),
            edition=1,
        )

    async def commit(self) -> None:
        self.commit_count += 1

    async def rollback(self) -> None:
        self.rollback_count += 1


def _actor(board_id: str = "board-1") -> ActorContext:
    return ActorContext(
        "agent-1",
        "mcp",
        board_id=board_id,
        permissions=(ASSESSMENTS_RECORD, "guidelines.read"),
    )










async def _seed_approved_semantic_waiver(
    port: _Port,
    uow: _Uow,
):
    from test_skb31_semantic_guideline_v2_findings import _receipt, _result, _pinpoint
    from okto_pulse.core.domain.guideline_semantic_assessment import SemanticMetricOutcome
    from okto_pulse.core.domain.guideline_semantic_findings_v2 import project_semantic_metric_findings_v2
    reference = port._subject_snapshot().subject
    receipt = replace(_receipt(_result("failed", outcome=SemanticMetricOutcome.FAIL,
        pinpoints=(_pinpoint("boundary"),), subject=reference), subject=reference),
        recorded_at=NOW, binding_revision=port.binding.binding_revision,
        binding_configuration_digest=port.binding.configuration_digest,
        guideline_revision_digest=port.revision.revision_digest)
    port.saved = SimpleNamespace(receipt=receipt)
    finding = project_semantic_metric_findings_v2(receipt)[0]
    evidence = receipt.metric_results[0].evidence_refs
    requested = request_semantic_metric_waiver(
        waiver_id="waiver-1",
        event_id="waiver-event-request",
        anchor=SemanticMetricWaiverAnchor.from_finding(
            finding,
            assessment_assessor_id=receipt.assessment_assessor_id,
        ),
        justification="A bounded policy exception is required.",
        evidence_refs=evidence,
        requested_by="requester-1",
        requested_at=NOW + timedelta(minutes=1),
        expires_at=NOW + timedelta(days=1),
        idempotency_key="waiver-request",
    )
    approved = transition_semantic_metric_waiver(
        requested.waiver,
        event_id="waiver-event-approve",
        expected_waiver_revision=1,
        event_type=SemanticMetricWaiverEventType.APPROVE,
        actor_id="reviewer-2",
        occurred_at=NOW + timedelta(minutes=2),
        reason="The exception is independently reviewed.",
        evidence_refs=evidence,
        idempotency_key="waiver-approve",
    )
    port.finding = finding
    port.waiver = approved.waiver
    return approved


def _revalidator() -> ActorContext:
    return ActorContext(
        "revalidator-3",
        "mcp",
        board_id="board-1",
        permissions=(WAIVER_REVALIDATE, "spec.validation.submit"),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("source", "actor_kind"),
    (
        ("mcp", "agent"),
        ("rest", "agent"),
        ("rest", "unknown"),
    ),
)
async def test_semantic_skip_reads_require_an_explicit_human_rest_session(
    source: str,
    actor_kind: str,
) -> None:
    port = _Port()
    with pytest.raises(PermissionDeniedError, match="human_session_required"):
        await ListSemanticPolicySkipsUseCase().execute(
            ListSemanticPolicySkipsCommand(
                SemanticSkipListQuery(board_id="board-1"),
            ),
            actor=ActorContext(
                "owner-1",
                source,
                actor_kind=actor_kind,
                board_id="board-1",
                permissions=(ADOPTION_MANAGE,),
            ),
            uow=_Uow(port),
        )
    assert port.skip_list_count == 0


@pytest.mark.asyncio
async def test_semantic_skip_reads_accept_an_explicit_human_rest_session() -> None:
    port = _Port()
    result = await ListSemanticPolicySkipsUseCase().execute(
        ListSemanticPolicySkipsCommand(
            SemanticSkipListQuery(board_id="board-1"),
        ),
        actor=ActorContext(
            "owner-1",
            "rest",
            actor_kind="human",
            board_id="board-1",
            permissions=(ADOPTION_MANAGE, "spec.entity.edit_fields"),
        ),
        uow=_Uow(port),
    )

    assert result.page.items == ()
    assert port.skip_list_count == 1


@pytest.mark.asyncio
async def test_revalidation_use_case_is_append_only_replay_safe_and_atomic() -> None:
    port = _Port()
    uow = _Uow(port)
    await _seed_approved_semantic_waiver(port, uow)
    command = RevalidateSemanticMetricWaiverCommand(
        board_id="board-1",
        waiver_id="waiver-1",
        expected_waiver_revision=2,
        evaluated_at=NOW + timedelta(minutes=3),
        idempotency_key="waiver-revalidate-current",
    )

    result = await RevalidateSemanticMetricWaiverUseCase(
        clock=lambda: NOW + timedelta(minutes=3),
    ).execute(
        command,
        actor=_revalidator(),
        uow=uow,
    )

    assert result.waiver_id == "waiver-1"
    assert result.waiver_revision == 3
    assert result.status is SemanticMetricWaiverRevalidationStatus.APPROVED
    assert result.current is True
    assert result.reason_code is SemanticMetricWaiverRevalidationReason.CURRENT
    assert result.replayed is False
    assert port.waiver_save_count == 1
    assert port.last_waiver_mutation.event.predecessor_event_id == (
        "waiver-event-approve"
    )
    assert port.lock_values[-1] is True
    assert uow.commit_count == 1

    port.waiver_replay = port.last_waiver_mutation
    replay_uow = _Uow(port)
    replay = await RevalidateSemanticMetricWaiverUseCase().execute(
        command,
        actor=_revalidator(),
        uow=replay_uow,
    )
    assert replay.replayed is True
    assert port.waiver_save_count == 1
    assert replay_uow.commit_count == 0

    conflict_uow = _Uow(port)
    with pytest.raises(
        GuidelinePolicyIdempotencyConflict,
        match="semantic_waiver_idempotency_conflict",
    ):
        await RevalidateSemanticMetricWaiverUseCase().execute(
            RevalidateSemanticMetricWaiverCommand(
                board_id="board-1",
                waiver_id="waiver-1",
                expected_waiver_revision=2,
                evaluated_at=NOW + timedelta(minutes=4),
                idempotency_key="waiver-revalidate-current",
            ),
            actor=_revalidator(),
            uow=conflict_uow,
        )
    assert port.waiver_save_count == 1
    assert (conflict_uow.commit_count, conflict_uow.rollback_count) == (0, 0)


@pytest.mark.asyncio
async def test_revalidation_missing_anchor_appends_stale_instead_of_throwing() -> None:
    port = _Port()
    uow = _Uow(port)
    await _seed_approved_semantic_waiver(port, uow)
    port.finding = None

    result = await RevalidateSemanticMetricWaiverUseCase(
        clock=lambda: NOW + timedelta(minutes=3),
    ).execute(
        RevalidateSemanticMetricWaiverCommand(
            board_id="board-1",
            waiver_id="waiver-1",
            expected_waiver_revision=2,
            evaluated_at=NOW + timedelta(minutes=3),
            idempotency_key="waiver-revalidate-missing-anchor",
        ),
        actor=_revalidator(),
        uow=uow,
    )

    assert result.status is SemanticMetricWaiverRevalidationStatus.ANCHOR_STALE
    assert result.current is False
    assert result.reason_code is SemanticMetricWaiverRevalidationReason.ANCHOR_MISSING
    assert port.waiver.status.value == "approved"
    assert port.waiver.waiver_revision == 3
    assert port.waiver_save_count == 1
