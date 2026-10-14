from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.application.use_cases.base import ActorContext
from okto_pulse.core.application.use_cases.validation_cycle import ValidationCycleReadUseCases
from okto_pulse.core.domain.realm import RealmScope
from okto_pulse.core.domain.workspace_revisions import WorkspaceRevisions, WorkspaceRevisionsQuery
from okto_pulse.core.ports.validation_cycle import ValidationCycleReadAccessDenied


def query():
    return WorkspaceRevisionsQuery(board_id="board", subjects=[
        {"subject_type": "spec", "subject_id": "spec"},
    ])


@pytest.mark.parametrize("subjects", [
    [], [{"subject_type": "ideation", "subject_id": "x"}],
    [{"subject_type": "spec", "subject_id": "x"}] * 2,
    [{"subject_type": "spec", "subject_id": str(index)} for index in range(51)],
    [{"subject_type": "spec_collection", "subject_id": "another-board"}],
    [{"subject_type": "card_collection", "subject_id": "another-board"}],
])
def test_workspace_revision_query_has_closed_bounded_population(subjects):
    with pytest.raises(ValueError):
        WorkspaceRevisionsQuery(board_id="board", subjects=subjects)


@pytest.mark.parametrize("kind", ["spec_collection", "card_collection"])
def test_workspace_collection_query_uses_its_authorized_board_identity(kind):
    result = WorkspaceRevisionsQuery(board_id="board", subjects=[
        {"subject_type": kind, "subject_id": "board"},
    ])
    assert result.subjects[0].subject_id == result.board_id


@pytest.mark.asyncio
async def test_workspace_revisions_are_scoped_to_authenticated_board_before_read():
    reader = SimpleNamespace(get_workspace_revisions=AsyncMock())
    actor = ActorContext("agent", "mcp", board_id="another", realm_scope=RealmScope.local())
    with pytest.raises(ValidationCycleReadAccessDenied):
        await ValidationCycleReadUseCases(reader=reader).get_workspace_revisions(query(), actor=actor)
    reader.get_workspace_revisions.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("board,subject", [("other", "spec"), ("board", "other")])
async def test_workspace_revisions_reject_wrong_adapter_population(board, subject):
    result = WorkspaceRevisions(board_id=board,
        authority={"incarnation": "a" * 32, "revision": 1},
        subjects=[{"subject_type": "spec", "subject_id": subject, "token": None}])
    reader = SimpleNamespace(get_workspace_revisions=AsyncMock(return_value=result))
    with pytest.raises(RuntimeError, match="scope_mismatch"):
        await ValidationCycleReadUseCases(reader=reader).get_workspace_revisions(
            query(), actor=ActorContext("user", "rest", realm_scope=RealmScope.local()),
        )
