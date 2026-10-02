"""Native public history preserves authorization, projection bounds and keysets."""

from dataclasses import replace
from types import SimpleNamespace

import pytest

from okto_pulse.core.application.use_cases.base import ActorContext, EntityNotFoundError, PermissionDeniedError
from okto_pulse.core.application.use_cases.policy_governance import ASSESSMENTS_READ
from okto_pulse.core.application.use_cases.semantic_guideline_governance import (
    GetSemanticGuidelineAssessmentCommand, GetSemanticGuidelineAssessmentUseCase,
    ListSemanticGuidelineAssessmentsCommand, ListSemanticGuidelineAssessmentsUseCase,
)
from okto_pulse.core.domain.guideline_policy import PolicyCurrentness
from okto_pulse.core.domain.guideline_semantic_currentness import SemanticAssessmentCurrentness, SemanticAssessmentCurrentnessReason
from okto_pulse.core.domain.guideline_semantic_projection import SemanticGuidelineProjection
from okto_pulse.core.ports.guideline_policy import SemanticAssessmentListQuery, GuidelinePolicyInvalidCursor
from test_native_semantic_history_currentness import _fixture


class _History:
    def __init__(self):
        template, *_ = _fixture(edition=2)
        self.items = []
        self.calls = []
        for index in range(205):
            identity = f"receipt-{205-index:03d}"
            metrics = tuple(replace(item, receipt_id=identity, metric_result_id=f"{identity}-metric")
                            for item in template.metric_results)
            self.items.append(replace(template, receipt_id=identity, metric_results=metrics))

    async def get_semantic_assessment_v2(self, *, board_id, receipt_id):
        self.calls.append(("get", board_id))
        return next((item for item in self.items if item.subject.board_id == board_id
                     and item.receipt_id == receipt_id), None)

    async def list_semantic_assessment_v2_receipts(self, *, board_id, after, limit, **filters):
        self.calls.append(("list", board_id, after, limit))
        items = [item for item in self.items if item.subject.board_id == board_id
                 and (after is None or (item.recorded_at, item.receipt_id) < after)]
        page = items[:limit]
        cursor = (page[-1].recorded_at, page[-1].receipt_id) if len(items) > limit else None
        return tuple(page), cursor

    async def get_semantic_assessment_v2_currentness(self, receipt):
        previous = int(receipt.receipt_id.split("-")[-1]) <= 5
        return SemanticAssessmentCurrentness(
            receipt_id=receipt.receipt_id,
            currentness=PolicyCurrentness.STALE if previous else PolicyCurrentness.CURRENT,
            reasons=(SemanticAssessmentCurrentnessReason.SUBJECT_EDITION_CHANGED,) if previous else (),
        )

    async def get_current_semantic_assessment_v2(self, **kwargs):
        raise AssertionError("History must not collapse to the newest current receipt")


class _Boards:
    async def get(self, board_id):
        return SimpleNamespace(id=board_id, owner_id="reader", realm_id="local")


def _context():
    reader = _History()
    return reader, SimpleNamespace(boards=_Boards(), semantic_assessment_v2_reader=reader), ActorContext(
        "reader", "rest", board_id="board-1", permissions=(ASSESSMENTS_READ, "guidelines.read"),
    )


@pytest.mark.asyncio
async def test_native_filtered_history_scans_beyond_200_and_binds_cursor_to_query():
    reader, uow, actor = _context()
    query = SemanticAssessmentListQuery(board_id="board-1", limit=3, currentness=PolicyCurrentness.STALE)
    use_case = ListSemanticGuidelineAssessmentsUseCase()
    first = (await use_case.execute(ListSemanticGuidelineAssessmentsCommand(query), actor=actor, uow=uow)).page
    assert [item.receipt_id for item in first.items] == ["receipt-005", "receipt-004", "receipt-003"]
    assert first.has_more and first.next_cursor is not None
    assert all(not hasattr(item, "metric_results") for item in first.items)
    assert all(item.lifecycle_state.value == "previous" for item in first.items)
    assert len([call for call in reader.calls if call[0] == "list"]) == 2
    second = (await use_case.execute(ListSemanticGuidelineAssessmentsCommand(
        replace(query, cursor=first.next_cursor)), actor=actor, uow=uow)).page
    assert [item.receipt_id for item in second.items] == ["receipt-002", "receipt-001"]
    assert not second.has_more and second.next_cursor is None
    for overrides in ({"board_id": "other-board"}, {"projection": SemanticGuidelineProjection.FULL}):
        with pytest.raises(GuidelinePolicyInvalidCursor):
            replace(query, cursor=first.next_cursor, **overrides)


@pytest.mark.asyncio
async def test_native_get_preserves_sealed_details_and_refuses_cross_board_read():
    reader, uow, actor = _context()
    result = await GetSemanticGuidelineAssessmentUseCase().execute(
        GetSemanticGuidelineAssessmentCommand("board-1", "receipt-005"), actor=actor, uow=uow,
    )
    assert result.assessment.receipt_digest == reader.items[-5].receipt_digest
    assert result.assessment.request_digest == reader.items[-5].request_digest
    assert result.assessment.metric_results[0].pinpoints[0].anchor_snapshot.label
    with pytest.raises(EntityNotFoundError):
        await GetSemanticGuidelineAssessmentUseCase().execute(
            GetSemanticGuidelineAssessmentCommand("other-board", "receipt-005"),
            actor=ActorContext("reader", "rest", board_id="other-board",
                               permissions=(ASSESSMENTS_READ, "guidelines.read")), uow=uow,
        )


@pytest.mark.asyncio
async def test_native_history_permission_denial_precedes_reader_access():
    reader, uow, actor = _context()
    with pytest.raises(PermissionDeniedError):
        await ListSemanticGuidelineAssessmentsUseCase().execute(
            ListSemanticGuidelineAssessmentsCommand(SemanticAssessmentListQuery(board_id="board-1")),
            actor=ActorContext("reader", "rest", board_id="board-1", permissions=()), uow=uow,
        )
    assert reader.calls == []
