from types import SimpleNamespace
from unittest.mock import AsyncMock
import pytest
from okto_pulse.core.application.use_cases.base import ActorContext, PermissionDeniedError
from okto_pulse.core.application.use_cases.decision_impact import DecisionImpactCommand, DecisionImpactUseCase

FLAGS = ['board.read','spec.entity.read','card.entity.read','spec.tests.read',
    'spec.integration_requirements.read','spec.observability_requirements.read','kg.query.related_context']


def setup(flags):
    actor = ActorContext('agent','mcp',actor_kind='agent',realm_id='local',board_id='board',permissions=flags)
    board = SimpleNamespace(id='board',realm_id='local',owner_id='owner',settings={'kg_query_timeout_ms':700})
    operation = AsyncMock(return_value={})
    uow = SimpleNamespace(boards=SimpleNamespace(get=AsyncMock(return_value=board)),
        services=SimpleNamespace(analytics=SimpleNamespace(decision_impact=operation)))
    return actor,uow,operation


@pytest.mark.asyncio
@pytest.mark.parametrize('missing',FLAGS)
async def test_each_scope_grant_precedes_graph_or_source_reads(missing):
    actor,uow,operation = setup([flag for flag in FLAGS if flag != missing])
    with pytest.raises(PermissionDeniedError):
        await DecisionImpactUseCase().execute(DecisionImpactCommand('board','spec','decision'),actor=actor,uow=uow)
    operation.assert_not_called()


@pytest.mark.asyncio
async def test_no_code_evidence_grant_is_needed_or_used_and_policy_caps_deadline():
    actor,uow,operation = setup(FLAGS)
    await DecisionImpactUseCase().execute(DecisionImpactCommand('board','spec','decision',timeout_ms=30000),actor=actor,uow=uow)
    query = operation.call_args.args[0]
    assert query.source_query().read_delivery is False
    assert query.actor_scope_ref == 'local:agent:mcp:agent'
    assert operation.call_args.kwargs == {'timeout_ms':700}
