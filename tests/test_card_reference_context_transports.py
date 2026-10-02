"""Existing REST-use-case/MCP reads re-evaluate the same persisted source."""
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from okto_pulse.core.domain.architecture_adoption import ArchitectureAdoptionScope

from mcp_runtime_testing import register_mcp_test_runtime
from sqlalchemy_test_models import Board, Card, Spec
from sqlalchemy_test_unit_of_work import SQLAlchemyUnitOfWorkFactory
from okto_pulse.core.application.use_cases.base import ActorContext
from okto_pulse.core.application.use_cases.card_crud import GetCardCommand, GetCardUseCase, UpdateCardCommand, UpdateCardUseCase
from okto_pulse.core.models.schemas import CardUpdate
from okto_pulse.core.infra.database import get_session_factory
from okto_pulse.core.mcp import server


@pytest.mark.parametrize('profile,scope', [('summary', 'all'), ('full', 'all'), ('full', 'gate')])
async def test_real_card_and_mcp_read_follow_source_correction_without_waiting_for_graph(monkeypatch, profile, scope):
    suffix = uuid4().hex
    board_id, spec_id, card_id = [f'{kind}-{suffix}' for kind in ('board', 'spec', 'card')]
    factory = get_session_factory()
    async with factory() as session:
        session.add(Board(id=board_id, name='Board', owner_id='owner'))
        session.add(Spec(architecture_adoption=ArchitectureAdoptionScope(board_id=board_id, spec_id=spec_id, adopted_in_edition=1, actor_id='owner', inherited_resource_ids=()).model_dump(mode="json"), id=spec_id, board_id=board_id, title='Spec', created_by='owner', test_scenarios=[]))
        session.add(Card(id=card_id, board_id=board_id, spec_id=spec_id, title='Card', created_by='owner',
                         test_scenario_ids=['missing']))
        await session.commit()
    register_mcp_test_runtime(factory)
    actor = ActorContext('owner', 'mcp', board_id=board_id, permissions=['*'])
    async with SQLAlchemyUnitOfWorkFactory(factory)(actor=actor) as uow:
        result = await GetCardUseCase().execute(GetCardCommand(card_id), actor=actor, uow=uow)
        reference = result.card.scenario_reference_context
        assert reference.status == 'available' and reference.finding_count == 1
        assert reference.findings[0].target_ref == f'spec:{spec_id}:test_scenario:missing'
    async with SQLAlchemyUnitOfWorkFactory(factory)(actor=actor) as uow:
        updated = await UpdateCardUseCase().execute(UpdateCardCommand(card_id, CardUpdate(title='Updated')),
            actor=actor, uow=uow)
        assert updated.card.scenario_reference_context == reference
    monkeypatch.setattr(server, '_get_agent_ctx', AsyncMock(return_value=SimpleNamespace(
        agent_id='owner', agent_name='owner', board_id=board_id, permissions=['*'])))
    monkeypatch.setattr(server, 'check_permission', lambda *args: None)
    monkeypatch.setattr(server, '_mcp_code_traceability_projection', AsyncMock(return_value={}))
    tool = await server.mcp.get_tool('okto_pulse_get_task_context')
    async def context():
        return json.loads(await tool.fn(board_id=board_id, card_id=card_id, profile=profile, context_scope=scope))
    before = await context()
    assert before['scenario_reference_context'] == reference.model_dump(mode='json')
    async with factory() as session:
        spec = await session.get(Spec, spec_id)
        spec.test_scenarios = [{'id': 'missing', 'title': 'Now present', 'status': 'not_run'}]
        await session.commit()
    after = (await context())['scenario_reference_context']
    assert after['status'] == 'available' and after['finding_count'] == 0 and after['findings'] == []
    assert after['source_fingerprint'] != before['scenario_reference_context']['source_fingerprint']
    update_tool = await server.mcp.get_tool('okto_pulse_update_card')
    mutation = json.loads(await update_tool.fn(board_id=board_id, card_id=card_id, test_scenario_ids=['missing']))
    assert mutation.get('success'), mutation
    assert mutation['scenario_reference_context']['status'] == 'available'
    assert mutation['scenario_reference_context']['finding_count'] == 0
