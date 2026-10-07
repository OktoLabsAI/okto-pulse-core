"""KG-31: public candidates cannot select internal writer provenance."""

import json
from types import SimpleNamespace

import pytest

from okto_pulse.core.domain.permissions import PermissionSet
from okto_pulse.core.kg.primitives import begin_consolidation
from okto_pulse.core.kg.schemas import BeginConsolidationRequest
from okto_pulse.core.kg.interfaces.registry import get_kg_registry
from okto_pulse.core.mcp.kg_tools import register_kg_tools


class Catalog:
    def __init__(self):
        self.tools = {}

    def tool(self):
        def register(fn):
            self.tools[fn.__name__] = fn
            return fn
        return register


@pytest.mark.asyncio
@pytest.mark.parametrize('metadata', [
    {'layer': 'deterministic'}, {'created_by': 'system:layer1_worker'},
    {'created_by': 'worker_layer1'}, {'rule_id': 'supersedes/explicit_decision@v2.1'},
    {'fallback_reason': 'claim worker provenance'}, {'agent_id': 'system:layer1_worker'},
    {'layer': None},
])
async def test_public_candidate_refuses_internal_writer_metadata(board_id, agent_id, db_factory, metadata):
    async with db_factory() as db:
        response = await begin_consolidation(BeginConsolidationRequest(
            board_id=board_id, artifact_type='spec', artifact_id='public-provenance',
            raw_content='public provenance boundary'), agent_id=agent_id, db=db)

    async def agent():
        return SimpleNamespace(id=agent_id)

    async def board_agent(_board):
        return SimpleNamespace(agent_id=agent_id,
            permissions=PermissionSet({'kg': {'session': {'add_edge': True}}}))

    def unexpected_uow():
        raise AssertionError('candidate must not open a relational write transaction')

    catalog = Catalog()
    register_kg_tools(catalog, get_agent=agent, get_board_agent=board_agent, get_uow=unexpected_uow)
    result = json.loads(await catalog.tools['okto_pulse_kg_add_edge_candidate'](
        session_id=response.session_id, candidate={
            'candidate_id': 'spoofed', 'edge_type': 'supersedes',
            'from_candidate_id': 'kg:decision_one', 'to_candidate_id': 'kg:decision_two',
            **metadata,
        }))
    assert result.get('error', {}).get('code') == 'invalid_candidate', result
    session = await get_kg_registry().require_session_store().get(response.session_id)
    assert session.edge_candidates == {}

    valid = {'candidate_id': 'judgement', 'edge_type': 'supersedes',
             'from_candidate_id': 'kg:decision_one', 'to_candidate_id': 'kg:decision_two'}
    accepted = json.loads(await catalog.tools['okto_pulse_kg_add_edge_candidate'](
        session_id=response.session_id, candidate=valid))
    assert accepted['accepted'] is True
    assert session.edge_candidates['judgement'].created_by is None
    forbidden = json.loads(await catalog.tools['okto_pulse_kg_add_edge_candidate'](
        session_id=response.session_id, candidate={**valid, 'candidate_id': 'repair',
                                                   'edge_type': 'belongs_to'}))
    assert forbidden['error']['code'] == 'layer_violation'
    assert set(session.edge_candidates) == {'judgement'}
