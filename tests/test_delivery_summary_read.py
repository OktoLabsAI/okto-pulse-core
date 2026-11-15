from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from pydantic import ValidationError

from okto_pulse.core.application.use_cases.base import ActorContext, PermissionDeniedError
from okto_pulse.core.application.use_cases.delivery_evidence import GetDeliverySummaryUseCase
from okto_pulse.core.models.delivery_summary import DeliverySummaryQuery
from okto_pulse.core.ports.delivery_readiness import ReadinessActor, ReadinessAccessDenied, ReadinessReadError
from okto_pulse.core.services.delivery_readiness import DeliveryReadinessService
from test_delivery_summary import card, facts, population

ACTOR = ReadinessActor('reader', 'human', 'local', 'board')


@pytest.mark.parametrize('changes', [dict(limit=True), dict(limit=0), dict(limit=21),
    dict(limit='10'), dict(cursor='token'), dict(group='other'), dict(extra=True)])
def test_summary_query_is_closed_and_continuation_requires_a_group(changes):
    with pytest.raises(ValidationError):
        DeliverySummaryQuery(board_id='board', spec_id='spec', **changes)


@pytest.mark.asyncio
async def test_summary_service_loads_once_per_page_and_binds_exact_actor_population_and_group():
    value = population([card(str(i), 'in_progress') for i in range(7)],
        [facts(str(i), 'in_progress') for i in range(7)])
    reader = SimpleNamespace(load_delivery_summary=AsyncMock(return_value=value), load=AsyncMock())
    codec = SimpleNamespace(encode_summary=Mock(return_value='signed'), decode_summary=Mock(return_value=5))
    service = DeliveryReadinessService(reader, codec)
    query = DeliverySummaryQuery(board_id='board', spec_id='spec', limit=7)
    first = await service.delivery_summary(query, actor=ACTOR)
    assert len(first['groups'][1]['items']) == 5
    binding, offset = codec.encode_summary.call_args.args
    assert (binding.actor_id, binding.actor_kind, binding.realm_id) == ('reader', 'human', 'local')
    assert binding.population_id == first['population_id'] == value.population_id
    assert binding.group == 'in_progress' and binding.limit == 7 and offset == 5
    second = await service.delivery_summary(query.model_copy(update=dict(group='in_progress', cursor='signed')), actor=ACTOR)
    assert len(second['groups'][1]['items']) == 2 and not second['groups'][1]['has_more']
    assert codec.decode_summary.call_args.args == ('signed', binding)
    assert reader.load_delivery_summary.await_count == 2
    reader.load.assert_not_awaited()


@pytest.mark.asyncio
async def test_denial_on_a_continuation_is_not_hidden_by_cursor_errors():
    reader = SimpleNamespace(load_delivery_summary=AsyncMock(side_effect=ReadinessAccessDenied('card.entity.read')))
    codec = Mock()
    with pytest.raises(ReadinessAccessDenied):
        await DeliveryReadinessService(reader, codec).delivery_summary(
            DeliverySummaryQuery(board_id='board', spec_id='spec', group='pending', cursor='forged'), actor=ACTOR)
    codec.decode_summary.assert_not_called()


@pytest.mark.asyncio
async def test_missing_adapter_and_foreign_adapter_scope_fail_closed():
    query = DeliverySummaryQuery(board_id='foreign', spec_id='spec')
    with pytest.raises(ReadinessReadError, match='readiness_assessment_unavailable'):
        await DeliveryReadinessService(object(), Mock()).delivery_summary(query, actor=ACTOR)
    with pytest.raises(ReadinessReadError, match='readiness_scope_mismatch'):
        await DeliveryReadinessService(SimpleNamespace(load_delivery_summary=AsyncMock(return_value=population())), Mock()).delivery_summary(query, actor=ACTOR)


@pytest.mark.asyncio
async def test_summary_use_case_authorizes_without_committing_or_loading_evidence():
    service = SimpleNamespace(delivery_summary=AsyncMock(return_value={'complete': False, 'task_count': None}))
    uow = SimpleNamespace(services=SimpleNamespace(delivery_readiness=service), commit=AsyncMock())
    actor = ActorContext(actor_id='reader', actor_kind='human', source='rest', realm_id='local',
        board_id='board', permissions=['code_traceability.evidence.read'])
    query = DeliverySummaryQuery(board_id='board', spec_id='spec')
    assert await GetDeliverySummaryUseCase().execute(query, actor=actor, uow=uow) == {'complete': False, 'task_count': None}
    assert service.delivery_summary.await_args.args == (query,)
    assert service.delivery_summary.await_args.kwargs['actor'] == ACTOR
    uow.commit.assert_not_awaited()
    service.delivery_summary.reset_mock()
    denied = ActorContext(actor_id='reader', actor_kind='human', source='rest', realm_id='local',
        board_id='board', permissions=[])
    with pytest.raises(PermissionDeniedError):
        await GetDeliverySummaryUseCase().execute(query, actor=denied, uow=uow)
    service.delivery_summary.assert_not_awaited()
