"""G2/G4: every declared Spec child link is projected from its Card owner."""
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.ports.consolidation import ConsolidationProjectionInputs
from okto_pulse.core.ports.deterministic_projection import DeterministicProjectionSource, make_deterministic_projection_planner
from test_deterministic_projection_planner import spec


CHILDREN = (
    ('functional_requirements', 'fr', 'Requirement'),
    ('technical_requirements', 'tr', 'Constraint'),
    ('acceptance_criteria', 'ac', 'Criterion'),
    ('business_rules', 'business_rule', 'Constraint'),
    ('api_contracts', 'api_contract', 'APIContract'),
    ('integration_requirements', 'integration_requirement', 'Requirement'),
    ('observability_requirements', 'observability_requirement', 'Constraint'),
    ('decisions', 'decision', 'Decision'),
    ('test_scenarios', 'test_scenario', 'TestScenario'),
)


@pytest.mark.asyncio
@pytest.mark.parametrize('card_type', ['normal', 'test', 'bug'])
async def test_every_declared_child_has_one_typed_card_support(card_type):
    card = SimpleNamespace(id='card-one', board_id='board', spec_id='spec-one', title='Linked card',
        description='', status='done', card_type=card_type, test_scenario_ids=['child-test_scenario'],
        observed_behavior='Observed', expected_behavior='Expected', steps_to_reproduce='Repeat',
        conclusions=[{'summary': 'Completed'}])
    parent = spec(**{field: [{'id': 'child-' + section, 'title': section, 'text': section,
        'status': 'active', 'linked_task_ids': ['card-one']}] for field, section, _kind in CHILDREN})
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
    assert {edge['to_candidate_id'] for edge in edges} == {
        f'kgref:{kind}:spec:spec-one:{section}:child-{section}' for _field, section, kind in CHILDREN}
    assert len(edges) == len(CHILDREN)
    for edge in edges:
        root = nodes[edge['from_candidate_id']]
        assert (root['node_type'], root['source_artifact_ref']) == (
            'Bug' if card_type == 'bug' else 'Entity', 'card:card-one')


@pytest.mark.parametrize('damage', ['missing_collection', 'invalid_collection', 'duplicate_id', 'missing_id'])
def test_incomplete_child_source_cannot_authorize_any_replacement(damage):
    from okto_pulse.core.application.processors.card_child_projection import prepare_card_child_projection
    from okto_pulse.core.application.processors.deterministic_kg import WorkerResult
    parent = spec(**{field: [] for field, _section, _kind in CHILDREN})
    parent.functional_requirements = [{'id': 'fr', 'linked_task_ids': ['card-one']}]
    if damage == 'missing_collection':
        del parent.decisions
    elif damage == 'invalid_collection':
        parent.decisions = {'unexpected': []}
    elif damage == 'duplicate_id':
        parent.functional_requirements *= 2
    else:
        parent.functional_requirements[0].pop('id')
    result = WorkerResult(nodes=[SimpleNamespace(candidate_id='root', node_type='Entity', source_artifact_ref='card:card-one')],
        content_hash='retained')
    with pytest.raises(ValueError, match='card_child_'):
        prepare_card_child_projection(card=SimpleNamespace(id='card-one'), parent=parent, result=result)
    assert result.edges == [] and result.relational_projection_active_set_intents == ()
    assert result.content_hash == 'retained'


def test_registry_and_old_new_consumers_cover_all_declared_child_collections():
    from okto_pulse.core.domain.delivery_inventory import COLLECTIONS
    from okto_pulse.core.ports.card_projection import CARD_CHILD_FAMILIES, CARD_PROJECTION_FIELDS, spec_linked_card_ids
    assert {family.field for family in CARD_CHILD_FAMILIES} == {field for _prefix, field in COLLECTIONS}
    assert CARD_PROJECTION_FIELDS == {field for field, _section, _kind in CHILDREN}
    for field in CARD_PROJECTION_FIELDS:
        assert spec_linked_card_ids({field: [{'id': 'old', 'linked_task_ids': ['removed']}]},
            {field: [{'id': 'new', 'linked_task_ids': ['added']}]}) == ['added', 'removed']
