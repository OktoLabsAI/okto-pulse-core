from dataclasses import replace
from datetime import datetime, timedelta, timezone
import pytest
from okto_pulse.core.ports.lineage_query import LineageNode, LineageRelation, LineageSnapshot, LineageQuery
from okto_pulse.core.services.lineage_query import project_lineage

QUERY = LineageQuery('board', 'spec:0', 'actor')


def chain():
    return LineageSnapshot('board', 'spec:0', 'actor', 'revision', datetime(2026,10,1,tzinfo=timezone.utc),
        tuple(LineageNode(f'spec:{i}', 'spec', f'Spec {i}', 'validated') for i in range(6)),
        tuple(LineageRelation(f'spec:{i}', f'spec:{i+1}', 'precedes', f'dependency:{i}') for i in range(5)), True)


def test_kg62_three_hops_is_partial_and_expansion_retains_actual_direction():
    first = project_lineage(QUERY, chain())
    assert first['completeness']['complete_for_scope'] is False
    assert first['frontier_refs'] == ['spec:4']
    assert first['counts']['reached_targets'] == 3
    full = project_lineage(replace(QUERY, max_depth=8), chain())
    assert full['completeness']['complete_for_scope'] is True
    assert full['counts']['reached_targets'] == 5
    assert len(full['items'][-1]['path']) == 5
    assert all(step['direction'] == 'outgoing' for step in full['items'][-1]['path'])
    assert full['projection_freshness']['state'] == 'unknown'


def test_incoming_path_and_visited_set_prevent_repetition_in_a_cycle():
    snapshot = chain()
    snapshot = replace(snapshot, subject_ref='spec:3', relations=(*snapshot.relations,
        LineageRelation('spec:5','spec:0','derived_from','source:cycle')))
    result = project_lineage(replace(QUERY,subject_ref='spec:3',max_depth=8),snapshot)
    assert len(result['items']) == 5
    assert len({item['subject_ref'] for item in result['items']}) == 5
    assert any(step['direction'] == 'incoming' for item in result['items'] for step in item['path'])


def test_kg61_partial_amendment_does_not_supersede_the_original_spec():
    snapshot = replace(chain(), nodes=(*chain().nodes, LineageNode('amendment_hotfix_revision:a','amendment_hotfix_revision','Partial revision','draft')),
        relations=(*chain().relations, LineageRelation('amendment_hotfix_revision:a','spec:0','amendment_of','amendment_hotfix_revision:a:original_spec_id'),
            LineageRelation('spec:1','amendment_hotfix_revision:a','derived_from','amendment_hotfix_revision:a:revision_spec_id')))
    result = project_lineage(QUERY,snapshot)
    amendment = next(row for row in result['items'] if row['entity_type'] == 'amendment_hotfix_revision')
    assert amendment['path'][0]['relation'] == 'amendment_of'
    assert not any(step['relation'] == 'supersedes' for row in result['items'] for step in row['path'])
    with pytest.raises(ValueError,match='amendment_source_invalid'):
        project_lineage(QUERY,replace(snapshot,relations=(LineageRelation('spec:1','spec:0','amendment_of','bad'),)))


def test_source_unavailable_never_becomes_complete_because_queue_is_empty():
    result = project_lineage(replace(QUERY,max_depth=8),replace(chain(),source_complete=False,
        limitations=('source_endpoint_unavailable',)))
    assert result['completeness']['complete_for_scope'] is False
    assert 'source_endpoint_unavailable' in result['completeness']['limitations']


def test_kg61_amendment_writer_does_not_project_whole_spec_supersedence():
    from okto_pulse.core.application.processors.deterministic_kg import DeterministicWorker
    result = DeterministicWorker().process_amendment({'id':'partial','board_id':'board',
        'original_spec_id':'original','revision_spec_id':'revision','origin_bug_id':'bug',
        'affected_task_ids':['affected'],'status':'done','lineage_state':'complete'})
    assert result.nodes
    assert all(node.source_artifact_ref != 'spec:original' for node in result.nodes)
    assert all(edge.edge_type != 'supersedes' for edge in result.edges)


def test_pagination_binds_revision_scope_and_actor_but_not_observation_clock():
    query = replace(QUERY,limit=1)
    first = project_lineage(query,chain())
    request = replace(query,cursor=first['next_cursor'])
    second = project_lineage(request,replace(chain(),checked_at=chain().checked_at+timedelta(seconds=1)))
    assert first['counts'] == second['counts']
    assert first['items'][0]['subject_ref'] != second['items'][0]['subject_ref']
    for changed_query, changed_source in [
        (request, replace(chain(),source_revision='new')),
        (replace(request,actor_scope_ref='other'),replace(chain(),actor_scope_ref='other')),
        (replace(request,max_depth=4),chain()),
    ]:
        with pytest.raises(ValueError,match='cursor_stale'):
            project_lineage(changed_query,changed_source)


@pytest.mark.parametrize('cursor',['', 'bad', 'lineage-v1:'+'a'*64+':0'])
def test_malformed_cursor_is_not_accepted(cursor):
    with pytest.raises(ValueError,match='cursor_invalid'):
        project_lineage(replace(QUERY,cursor=cursor),chain())


def test_foreign_snapshot_and_missing_endpoints_fail_closed():
    with pytest.raises(ValueError,match='scope_mismatch'):
        project_lineage(QUERY,replace(chain(),board_id='other'))
    with pytest.raises(ValueError,match='relation_invalid'):
        project_lineage(QUERY,replace(chain(),relations=(LineageRelation('spec:0','spec:foreign','precedes','dep'),)))


def test_large_rows_cannot_bypass_serialized_payload_bound(monkeypatch):
    import okto_pulse.core.services.lineage_query as module
    monkeypatch.setattr(module,'MAX_LINEAGE_BYTES',10)
    with pytest.raises(ValueError,match='summary_payload_bound'):
        project_lineage(QUERY,chain())
