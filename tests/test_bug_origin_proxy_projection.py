"""KG G5: origin associations are scoped inference, never confirmed cause."""
import json

import pytest

from sqlalchemy_test_models import Board, Card, Spec
from okto_pulse.core.ports.deterministic_projection import (
    DeterministicProjectionSource, make_deterministic_projection_planner,
)
from okto_pulse.community.adapters.sqlalchemy_consolidation import CommunitySqlAlchemyConsolidationPersistence


@pytest.mark.asyncio
async def test_bug_proxy_uses_only_children_assigned_to_its_origin(db_factory):
    async with db_factory() as db:
        db.add(Board(id='proxy-board', name='Scoped proxy', owner_id='owner'))
        collections = {
            'functional_requirements': ('fr', 'Requirement'),
            'technical_requirements': ('tr', 'Constraint'),
            'acceptance_criteria': ('ac', 'Criterion'),
            'business_rules': ('business_rule', 'Constraint'),
            'integration_requirements': ('integration_requirement', 'Requirement'),
            'observability_requirements': ('observability_requirement', 'Constraint'),
        }
        values = {field: [
            {'id': section + '-linked', 'description': 'Associated with origin', 'linked_task_ids': ['origin-card']},
            {'id': section + '-other', 'description': 'Unrelated', 'linked_task_ids': ['unrelated-card']},
        ] for field, (section, _) in collections.items()}
        db.add(Spec(id='proxy-spec', board_id='proxy-board', title='Scoped requirements',
            status='done', created_by='owner', **values))
        for identity, kind, origin in [('origin-card', 'normal', None), ('bug-card', 'bug', 'origin-card')]:
            db.add(Card(id=identity, board_id='proxy-board', spec_id='proxy-spec',
                title=identity, description='Projection fixture', status='done', card_type=kind,
                origin_task_id=origin, created_by='owner', test_scenario_ids=[],
                conclusions=[{'summary': 'Durable fixture'}]))
        await db.commit()
        planner = make_deterministic_projection_planner(CommunitySqlAlchemyConsolidationPersistence())
        result = await planner.prepare(db, DeterministicProjectionSource('proxy-board', 'card', 'bug-card'))
        projection = json.loads(result.document)['projection']
        edges = [edge for edge in projection['edges'] if edge['edge_type'] == 'violates']
        assert {edge['to_candidate_id'] for edge in edges} == {
            f'kgref:{kind}:spec:proxy-spec:{section}:{section}-linked' for section, kind in collections.values()}
        assert len(edges) == 6
        roots = {node['candidate_id']: node for node in projection['nodes']}
        for edge in edges:
            assert roots[edge['from_candidate_id']]['source_artifact_ref'] == 'card:bug-card'
            assert edge['confidence'] == 0.8
            assert edge['fallback_reason'] == 'inferred_origin_proxy:card:origin-card'
        # A changed origin with the same targets must replace its provenance,
        # not retain the earlier edge merely because both endpoints still exist.
        db.add(Card(id='replacement-origin', board_id='proxy-board', spec_id='proxy-spec',
            title='Replacement', status='done', card_type='normal', created_by='owner',
            test_scenario_ids=[], conclusions=[{'summary': 'Completed'}]))
        bug = await db.get(Card, 'bug-card')
        bug.origin_task_id = 'replacement-origin'
        spec = await db.get(Spec, 'proxy-spec')
        for field in collections:
            setattr(spec, field, [{**item, 'linked_task_ids': ['replacement-origin']}
                if item['id'].endswith('-linked') else item for item in getattr(spec, field)])
        await db.commit()
        changed = await planner.prepare(db, DeterministicProjectionSource('proxy-board', 'card', 'bug-card'))
        replacements = [edge for edge in json.loads(changed.document)['projection']['edges']
            if edge['edge_type'] == 'violates']
        assert len(replacements) == 6
        assert {edge['rule_id'] for edge in replacements}.isdisjoint(edge['rule_id'] for edge in edges)
        assert {edge['fallback_reason'] for edge in replacements} == {'inferred_origin_proxy:card:replacement-origin'}
        bug.origin_task_id = None
        await db.commit()
        absent = await planner.prepare(db, DeterministicProjectionSource('proxy-board', 'card', 'bug-card'))
        assert not [edge for edge in json.loads(absent.document)['projection']['edges'] if edge['edge_type'] == 'violates']


@pytest.mark.asyncio
async def test_unavailable_origin_does_not_authorize_an_empty_proxy_snapshot():
    from types import SimpleNamespace
    from okto_pulse.core.application.processors.bug_origin_proxy_projection import prepare_bug_origin_proxy
    from okto_pulse.core.application.processors.deterministic_kg import DeterministicWorker
    card = SimpleNamespace(id='bug', card_type='bug', origin_task_id='origin')
    result = DeterministicWorker().process_card({'id': 'bug', 'card_type': 'bug', 'origin_task_id': 'origin'})
    async def unavailable(*args, **kwargs):
        raise ConnectionError('source unavailable')
    with pytest.raises(ConnectionError, match='source unavailable'):
        await prepare_bug_origin_proxy(None, board_id='board', card=card, result=result,
            persistence=SimpleNamespace(load_artifact=unavailable))
    assert not result.relational_projection_active_set_intents
