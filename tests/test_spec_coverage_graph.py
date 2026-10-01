from dataclasses import replace

import pytest

from okto_pulse.core.ports.spec_coverage_query import SpecCoverageGraphFacts
from okto_pulse.core.services.spec_coverage_graph import build_spec_coverage_graph_scope
from okto_pulse.core.services.spec_coverage_query import project_spec_coverage
from test_spec_coverage_query import source, QUERY

QUERY = replace(QUERY, read_graph=True)


def observed(**changes):
    snapshot = source()
    scope = build_spec_coverage_graph_scope(snapshot)
    graph = replace(SpecCoverageGraphFacts(scope.nodes, scope.relations, generation='one'), **changes)
    return replace(snapshot, graph=graph, graph_scope=scope)


def test_matching_observations_are_not_a_full_projection_checkpoint_or_delivery_proof():
    result = project_spec_coverage(QUERY, observed())
    assert result['projection_freshness']['state'] == 'unknown'
    assert result['completeness']['complete_for_scope'] is False
    assert result['structure']['graph']['missing_nodes'] == 0
    assert result['structure']['graph']['missing_relations'] == 0
    assert result['delivery']['counts']['verification_proven'] == 1
    assert any(row['kind'] == 'structure_relation' for row in result['items'])


def test_missing_source_nodes_are_visible_even_with_an_empty_graph():
    result = project_spec_coverage(QUERY, observed(nodes=(), relations=()))
    assert result['projection_freshness']['state'] == 'incomplete'
    assert result['structure']['graph']['observed_nodes'] == 0
    assert result['structure']['graph']['missing_nodes'] == result['structure']['graph']['expected_nodes']
    assert all(row['observation'] == 'not_found_in_projection'
        for row in result['items'] if row['kind'].startswith('structure_'))
    assert result['delivery']['counts']['verification_proven'] == 1


def test_same_endpoints_with_wrong_writer_remain_graph_only_and_missing_expected():
    snapshot = observed()
    edge = snapshot.graph.relations[0]
    wrong = (*edge[:-1], 'untrusted')
    snapshot = replace(snapshot, graph=replace(snapshot.graph, relations=(wrong,)))
    result = project_spec_coverage(QUERY, snapshot)
    assert result['structure']['graph']['missing_relations'] > 0
    assert any(row.get('observation') == 'graph_only' for row in result['items'])


def test_graph_observation_never_upgrades_a_linked_test_card_to_passing():
    snapshot = observed()
    snapshot = replace(snapshot, delivery=replace(snapshot.delivery, implementations=(), tests=()))
    result = project_spec_coverage(QUERY, snapshot)
    assert result['structure']['summary']['scenario_task_linkage_pct'] == 100
    assert result['delivery']['counts']['verification_proven'] == 0
    assert all(row['verification'] == 'missing' for row in result['items'] if row['kind'] == 'delivery')


@pytest.mark.parametrize('change', ['generation', 'edge', 'node'])
def test_graph_change_invalidates_whole_view_cursor(change):
    snapshot = observed()
    request = replace(QUERY, limit=1)
    cursor = project_spec_coverage(request, snapshot)['next_cursor']
    graph = replace(snapshot.graph, **{'generation': {'generation': 'two'},
        'edge': {'relations': ()}, 'node': {'nodes': (), 'relations': ()}}[change])
    with pytest.raises(ValueError, match='cursor_stale'):
        project_spec_coverage(replace(request, cursor=cursor), replace(snapshot, graph=graph))


def test_foreign_identity_or_edge_is_rejected_before_aggregation():
    snapshot = observed()
    with pytest.raises(ValueError, match='identity_invalid'):
        project_spec_coverage(QUERY, replace(snapshot, graph=replace(snapshot.graph,
            nodes=snapshot.graph.nodes + (('Requirement', 'spec:foreign:fr:secret'),))))
    edge = snapshot.graph.relations[0]
    with pytest.raises(ValueError, match='endpoint_outside_scope'):
        project_spec_coverage(QUERY, replace(snapshot, graph=replace(snapshot.graph,
            relations=(('Requirement', 'spec:foreign:fr:secret', *edge[2:]),))))


def test_no_graph_grant_never_exposes_observation_or_counts():
    result = project_spec_coverage(replace(QUERY, read_graph=False), source())
    assert result['structure']['graph']['state'] == 'restricted'
    assert result['structure']['graph']['expected_nodes'] is None
    with pytest.raises(ValueError, match='outside_authority'):
        project_spec_coverage(replace(QUERY, read_graph=False), observed())


def test_source_link_changes_use_existing_projector_and_invalidate_old_scope():
    snapshot = observed()
    snapshot.spec.test_scenarios[0]['linked_criteria'] = []
    with pytest.raises(ValueError, match='scope_mismatch'):
        project_spec_coverage(QUERY, snapshot)


def test_graph_unavailable_preserves_relational_result_without_fake_zero():
    result = project_spec_coverage(QUERY, observed(nodes=(), relations=(), state='unavailable'))
    assert result['structure']['graph']['observed_nodes'] is None
    assert result['projection_freshness']['state'] == 'unavailable'
    assert result['delivery']['counts']['verification_proven'] == 1
