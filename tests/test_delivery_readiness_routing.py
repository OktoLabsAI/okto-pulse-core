"""P07 closed query and existing-use-case dispatch contracts."""
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

from okto_pulse.core.application.use_cases.base import ActorContext
from okto_pulse.core.application.use_cases.delivery_evidence import GetDeliveryEvidenceUseCase
from okto_pulse.core.models.delivery_evidence import DeliveryEvidenceReadQuery
from okto_pulse.core.services.delivery_readiness import DeliveryReadinessService
from test_delivery_obligation_assessment import snapshot
from test_delivery_readiness_shared_evaluation import prepared
from test_delivery_readiness_paging import Cursor


@pytest.mark.parametrize('patch', [
    {'record_id': 'record'}, {'edition': 1}, {'issue_id': 'issue'}, {'limit': True},
    {'limit': 21}, {'phase': 'test'}, {'snapshot_id': 'invalid'}, {'unknown': True},
])
def test_S05_readiness_rejects_ambiguous_or_non_strict_input(patch):
    with pytest.raises(ValidationError):
        DeliveryEvidenceReadQuery(board_id='board', spec_id='spec', view='readiness', **patch)


@pytest.mark.parametrize('view', ['progress', 'ledger', 'resume'])
@pytest.mark.parametrize('patch', [{'phase': 'verification'}, {'issue_id': 'issue'}, {'snapshot_id': 'a' * 64}])
def test_readiness_controls_are_exclusive_to_readiness(view, patch):
    with pytest.raises(ValidationError, match='readiness_query_invalid'):
        DeliveryEvidenceReadQuery(board_id='board', spec_id='spec', card_id='ui', view=view, **patch)


@pytest.mark.asyncio
@pytest.mark.parametrize('card_id', [None, 'ui', 'test-card-1'])
async def test_S01_S02_use_case_dispatches_readiness_before_history_and_keeps_nulls(card_id):
    loader = SimpleNamespace(load=AsyncMock(return_value=prepared(snapshot())))
    history = SimpleNamespace(progress_history=AsyncMock(), projection=AsyncMock(), card_resume=AsyncMock())
    uow = SimpleNamespace(services=SimpleNamespace(delivery_readiness=DeliveryReadinessService(loader, Cursor()),
        delivery_evidence=history), commit=AsyncMock())
    actor = ActorContext(actor_id='reader', actor_kind='agent', source='mcp', board_id='board',
        realm_id='local', permissions=['code_traceability.evidence.read'])
    query = DeliveryEvidenceReadQuery(board_id='board', spec_id='spec', card_id=card_id, view='readiness')
    result = await GetDeliveryEvidenceUseCase().execute(query, actor=actor, uow=uow)
    assert result['contract_version'] == 'delivery-readiness/v1'
    assert result['scope']['card_id'] == card_id
    assert result['lifecycle']['qualification_is_current'] is None
    assert result['page']['next_cursor'] is None
    assert result['implementation']['percent'] == (None if card_id == 'test-card-1' else 100)
    passed_actor = loader.load.await_args.kwargs['actor']
    assert passed_actor.realm_id == 'local' and passed_actor.board_id == 'board'
    history.progress_history.assert_not_awaited()
    history.projection.assert_not_awaited()
    history.card_resume.assert_not_awaited()
    uow.commit.assert_not_awaited()
