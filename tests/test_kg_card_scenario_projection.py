"""KG G2/G4: authoritative Card/Spec links become one typed observed relation."""
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.ports.consolidation import ConsolidationProjectionInputs
from okto_pulse.core.ports.deterministic_projection import DeterministicProjectionSource, make_deterministic_projection_planner
from test_deterministic_projection_planner import spec


@pytest.mark.asyncio
@pytest.mark.parametrize('card_type', ['normal', 'test', 'bug'])
@pytest.mark.parametrize('link_origin', ['card', 'spec', 'both'])
async def test_current_card_and_scenario_sources_converge_to_one_support(card_type, link_origin):
    card = SimpleNamespace(id='card-one', board_id='board', spec_id='spec-one',
        title='Linked card', description='', status='done', card_type=card_type,
        test_scenario_ids=['ts_one'] if link_origin in {'card', 'both'} else [],
        observed_behavior='Observed', expected_behavior='Expected', steps_to_reproduce='Repeat',
        conclusions=[{'summary': 'Completed'}])
    parent = spec(test_scenarios=[{'id': 'ts_one', 'title': 'Scenario',
        'linked_task_ids': ['card-one'] if link_origin in {'spec', 'both'} else []}])
    async def load(_context, *, artifact_type, artifact_id):
        return {('card', 'card-one'): card, ('spec', 'spec-one'): parent}.get((artifact_type, artifact_id))
    port = SimpleNamespace(load_artifact=load, list_artifacts=AsyncMock(return_value=()),
        load_projection_inputs=AsyncMock(return_value=ConsolidationProjectionInputs()),
        latest_card_transitions=AsyncMock(return_value=()))
    document = await make_deterministic_projection_planner(port).prepare(None,
        DeterministicProjectionSource('board', 'card', 'card-one'))
    projection = json.loads(document.document)['projection']
    nodes = {node['candidate_id']: node for node in projection['nodes']}
    edges = [edge for edge in projection['edges'] if edge['edge_type'] == 'supports']
    assert len(edges) == 1
    edge = edges[0]
    source_node = nodes[edge['from_candidate_id']]
    assert (source_node['node_type'], source_node['source_artifact_ref']) == (
        'Bug' if card_type == 'bug' else 'Entity', 'card:card-one')
    assert edge['to_candidate_id'] == 'kgref:TestScenario:spec:spec-one:test_scenario:ts_one'


@pytest.mark.asyncio
@pytest.mark.parametrize('card_type', ['normal', 'test', 'bug'])
@pytest.mark.parametrize('status,archived', [('cancelled', False), ('done', True)])
async def test_retired_card_plans_empty_owned_set_without_reviving_root(card_type, status, archived):
    from okto_pulse.core.application.processors.consolidation import _prepare_deterministic_projection
    card = SimpleNamespace(id='card-one', board_id='board', card_type=card_type,
        status=status, archived=archived)
    entry = SimpleNamespace(board_id='board', artifact_type='card', artifact_id='card-one')
    port = SimpleNamespace(load_artifact=AsyncMock(return_value=card))
    result = await _prepare_deterministic_projection(None, entry, persistence=port)
    # The cleanup must remain a projection, never the old True/ACK-only shortcut.
    assert result is not True
    result, source = result
    assert source is card
    assert result.nodes == []
    assert result.edges == []
    intent, = result.relational_projection_active_set_intents
    assert (intent.owner_type, intent.owner_id, intent.namespace) == ('card', 'card-one', 'card_scenarios')
    assert intent.active_refs == ()
    assert intent.active_edges == ()


@pytest.mark.asyncio
@pytest.mark.parametrize('damage', ['card_scope', 'incomplete_card', 'missing_parent',
                                    'parent_scope', 'duplicate_target', 'missing_target'])
async def test_incomplete_or_ambiguous_sources_do_not_authorize_empty_replacement(damage):
    from okto_pulse.core.application.processors.card_scenario_projection import prepare_card_scenario_projection
    card = SimpleNamespace(id='card-one', board_id='board', spec_id='spec-one', test_scenario_ids=['ts_one'])
    parent = spec(test_scenarios=[{'id': 'ts_one', 'linked_task_ids': ['card-one']}])
    result = SimpleNamespace(nodes=[SimpleNamespace(candidate_id='root', node_type='Entity',
        source_artifact_ref='card:card-one')], edges=[], relational_projection_active_set_intents=(), content_hash='before')
    if damage == 'card_scope':
        card.board_id = 'foreign'
    elif damage == 'incomplete_card':
        del card.test_scenario_ids
    elif damage == 'missing_parent':
        parent = None
    elif damage == 'parent_scope':
        parent.board_id = 'foreign'
    elif damage == 'duplicate_target':
        parent.test_scenarios *= 2
    elif damage == 'missing_target':
        parent.test_scenarios = []
    with pytest.raises(ValueError, match='card_scenario_'):
        await prepare_card_scenario_projection(None, board_id='board', card=card, result=result,
            persistence=SimpleNamespace(load_artifact=AsyncMock(return_value=parent)))
    assert result.edges == []
    assert result.relational_projection_active_set_intents == ()
    assert result.content_hash == 'before'
