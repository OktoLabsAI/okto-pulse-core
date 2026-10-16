from types import SimpleNamespace
from unittest.mock import AsyncMock
import pytest
from okto_pulse.core.application.use_cases.base import ActorContext, PermissionDeniedError
from okto_pulse.core.application.use_cases.lineage_query import LineageCommand, LineageUseCase

FLAGS = ['board.read','spec.entity.read','card.entity.read','ideation.entity.read',
    'refinement.entity.read','story.entity.read','amendment.revision.read']


def setup(flags):
    actor = ActorContext('agent','mcp',actor_kind='agent',realm_id='local',board_id='board',permissions=flags)
    board = SimpleNamespace(id='board',realm_id='local',owner_id='owner',settings={'kg_query_timeout_ms':700})
    operation = AsyncMock(return_value={})
    uow = SimpleNamespace(boards=SimpleNamespace(get=AsyncMock(return_value=board)),
        services=SimpleNamespace(analytics=SimpleNamespace(lineage=operation)))
    return actor,uow,operation


@pytest.mark.asyncio
@pytest.mark.parametrize('missing',FLAGS)
async def test_all_read_families_are_authorized_before_aggregation(missing):
    actor,uow,operation = setup([flag for flag in FLAGS if flag != missing])
    with pytest.raises(PermissionDeniedError):
        await LineageUseCase().execute(LineageCommand('board','spec:spec'),actor=actor,uow=uow)
    operation.assert_not_called()


@pytest.mark.asyncio
async def test_source_lineage_does_not_borrow_code_or_graph_grants_and_obeys_policy():
    actor,uow,operation = setup(FLAGS)
    await LineageUseCase().execute(LineageCommand('board','spec:spec',timeout_ms=30000),actor=actor,uow=uow)
    assert operation.call_args.args[0].actor_scope_ref == 'local:agent:mcp:agent'
    assert operation.call_args.kwargs == {'timeout_ms':700}
