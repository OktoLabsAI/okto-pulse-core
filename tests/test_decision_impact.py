from dataclasses import replace
from datetime import datetime, timezone, timedelta
from types import SimpleNamespace
import pytest

from okto_pulse.core.application.use_cases.base import EntityNotFoundError
from okto_pulse.core.domain.delivery_evidence import DeliveryScope
from okto_pulse.core.domain.delivery_inventory import COLLECTIONS
from okto_pulse.core.ports.decision_impact import DecisionImpactQuery
from okto_pulse.core.ports.spec_coverage_query import SpecCoverageSnapshot, SpecCoverageGraphFacts
from okto_pulse.core.services.decision_impact import build_decision_impact_scope, project_decision_impact

QUERY = DecisionImpactQuery('board', 'spec', 'decision', 'actor')


def source():
    fields = {field: [] for _, field in COLLECTIONS}
    fields.update(decisions=[{'id': 'decision', 'title': 'Choice', 'status': 'active', 'linked_requirements': ['fr']}],
        functional_requirements=[{'id': 'fr', 'text': 'Changed requirement', 'linked_task_ids': ['card']}],
        acceptance_criteria=[{'id': 'unrelated', 'text': 'A different condition'}])
    spec = SimpleNamespace(id='spec', board_id='board', edition=1, version=2, title='Spec', status='in_progress',
        test_scenarios=[{'id': 'scenario', 'title': 'Unrelated scenario via same Card',
            'linked_task_ids': ['card'], 'linked_criteria': ['unrelated'], 'status': 'ready'}], **fields)
    card = SimpleNamespace(id='card', spec_id='spec', board_id='board', title='Shared Card',
        status='done', card_type='normal', archived=False, test_scenario_ids=['scenario'])
    return SpecCoverageSnapshot(DeliveryScope('board','spec',1), 'actor', 'source', datetime(2026,10,1,tzinfo=timezone.utc),
        spec, (card,), True, None, 'restricted')


def project(snapshot=None, query=QUERY, graph=None):
    snapshot = snapshot or source()
    scope = build_decision_impact_scope(snapshot)
    graph = graph if graph is not None else SpecCoverageGraphFacts(scope.nodes, scope.relations, 'generation')
    return project_decision_impact(query, snapshot, scope, graph)


def test_kg57_shared_card_does_not_confirm_that_a_scenario_tests_this_requirement():
    result = project()
    rows = {row['target_ref']: row for row in result['items']}
    assert rows['spec:spec:fr:fr']['certainty'] == 'confirmed_link'
    assert rows['spec:spec:fr:fr']['reach'] == 'direct'
    assert rows['card:card']['certainty'] == 'confirmed_link'
    scenario = rows['spec:spec:test_scenario:scenario']
    assert scenario['certainty'] == 'potential'
    assert scenario['interpretation'] == 'potential_shared_card_reach'
    assert [step['direction'] for step in scenario['path']] == ['outgoing','incoming','outgoing']
    assert all(step['source_confirmed'] for step in scenario['path'])
    assert result['completeness']['truncated']
    assert result['frontier_refs'] == ['spec:spec:ac:unrelated']
    assert result['authority'] == 'informational'


def test_expanding_horizon_follows_real_paths_without_promoting_potential_to_confirmed():
    result = project(query=replace(QUERY,max_depth=5))
    target = next(row for row in result['items'] if row['target_ref'] == 'spec:spec:ac:unrelated')
    assert target['certainty'] == 'potential'
    assert len(target['path']) == 4
    assert result['scope']['max_depth'] == 5
    assert result['projection_freshness']['state'] == 'unknown'
    assert result['completeness']['complete_for_scope'] is False


def test_legacy_cooccurrence_never_becomes_an_initial_impact_scope():
    snapshot = source()
    snapshot.spec.decisions[0]['linked_requirements'] = []
    scope = build_decision_impact_scope(snapshot)
    stale = ('Decision','spec:spec:decision:decision','derives_from','Requirement','spec:spec:fr:fr',
        'derives_from/cooccurrence@v2.0','deterministic','worker_layer1')
    result = project(snapshot,graph=SpecCoverageGraphFacts(scope.nodes,(*scope.relations,stale),'generation'))
    assert result['items'] == []
    assert result['counts']['observed_targets'] == 0


def test_superseded_decision_is_navigable_history_not_current_impact():
    snapshot = source()
    snapshot.spec.decisions[0]['status'] = 'superseded'
    snapshot.spec.decisions.append({'id':'replacement','title':'Current choice','status':'active',
        'supersedes_decision_id':'decision','linked_requirements':['fr']})
    result = project(snapshot)
    assert result['current_decision'] is False
    assert result['items'] == []
    assert [row['status'] for row in result['history']] == ['superseded','active']
    assert result['history'][1]['supersedes_ref'] == 'spec:spec:decision:decision'
    current = project(snapshot,replace(QUERY,decision_id='replacement'))
    assert current['current_decision'] is True and current['items']


def test_missing_history_predecessor_is_unavailable_without_inventing_a_record():
    snapshot = source(); snapshot.spec.decisions[0]['supersedes_decision_id'] = 'missing'
    result = project(snapshot)
    row = next(item for item in result['history'] if item['subject_ref'].endswith(':missing'))
    assert row['status'] == 'unavailable' and row['title'] == ''


def test_missing_graph_uses_source_paths_and_never_infers_a_checkpoint():
    result = project(graph=SpecCoverageGraphFacts(state='unavailable'))
    assert result['items'][0]['path']
    assert all(not step['graph_observed'] for row in result['items'] for step in row['path'])
    assert result['projection_freshness']['state'] == 'unavailable'
    assert result['data_source'] == 'relational'


def test_whole_scope_counts_do_not_depend_on_page_and_clock_does_not_invalidate():
    snapshot = source(); request = replace(QUERY,limit=1)
    first = project(snapshot,request)
    second = project(replace(snapshot,checked_at=snapshot.checked_at+timedelta(seconds=10)),
        replace(request,cursor=first['next_cursor']))
    assert first['counts'] == second['counts']
    assert first['items'][0]['target_ref'] != second['items'][0]['target_ref']


@pytest.mark.parametrize('change',['generation','source','actor','horizon'])
def test_cursor_binds_source_generation_actor_and_horizon(change):
    snapshot = source(); request = replace(QUERY,limit=1)
    first = project(snapshot,request)
    request = replace(request,cursor=first['next_cursor'])
    scope = build_decision_impact_scope(snapshot)
    graph = SpecCoverageGraphFacts(scope.nodes,scope.relations,'generation')
    if change == 'generation': graph = replace(graph,generation='new')
    elif change == 'source': snapshot = replace(snapshot,source_revision='new')
    elif change == 'actor': snapshot = replace(snapshot,actor_scope_ref='new'); request = replace(request,actor_scope_ref='new')
    else: request = replace(request,max_depth=4)
    with pytest.raises(ValueError,match='cursor_stale'):
        project(snapshot,request,graph)


def test_foreign_endpoint_and_proof_payload_are_refused():
    snapshot = source(); scope = build_decision_impact_scope(snapshot)
    with pytest.raises(ValueError,match='outside_scope'):
        project(snapshot,graph=SpecCoverageGraphFacts((*scope.nodes,('Requirement','spec:other:fr:private')),()))
    with pytest.raises(ValueError,match='proof_outside_scope'):
        project(replace(snapshot,delivery=object()))


def test_unknown_decision_is_not_an_empty_success():
    with pytest.raises(EntityNotFoundError):
        project(query=replace(QUERY,decision_id='unknown'))


@pytest.mark.parametrize('cursor',['', 'invalid', 'decision-impact-v1:'+'a'*64+':0'])
def test_invalid_cursors_fail_closed(cursor):
    with pytest.raises(ValueError,match='cursor_invalid'):
        project(query=replace(QUERY,cursor=cursor))


def test_history_and_path_payloads_remain_bounded(monkeypatch):
    import okto_pulse.core.services.decision_impact as module
    monkeypatch.setattr(module,'MAX_SPEC_COVERAGE_BYTES',100)
    with pytest.raises(ValueError,match='summary_payload_bound'):
        project()
