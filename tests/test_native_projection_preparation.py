"""Native preparation, metadata and terminal cleanup survive retirement removal."""
from datetime import datetime
from types import SimpleNamespace

import pytest

from okto_pulse.core.application.processors import consolidation as live
from native_projection_test_support import spec, persistence, prepare_projection


@pytest.mark.asyncio
@pytest.mark.parametrize('collection,section,rule', [
    ('integration_requirements', 'integration_requirement', 'ir_requirement'),
    ('observability_requirements', 'observability_requirement', 'or_requirement'),
])
async def test_declared_requirement_links_reach_live_admission(monkeypatch, collection, section, rule):
    artifact = spec(technical_requirements=[{'id': 'tr-one', 'text': 'Technical condition'}],
        **{collection: [{'id': 'source-one', 'title': 'Declared source', 'status': 'active',
                        'linked_requirements': ['fr-one', 'tr-one']}]})
    port = persistence(artifact)
    expected = {(f'spec:spec-one:{section}:source-one', f'spec:spec-one:{target}:{identity}')
        for target, identity in [('fr', 'fr-one'), ('tr', 'tr-one')]}
    projection = await prepare_projection(None, port, 'board', 'spec', 'spec-one')
    refs = {node['candidate_id']: node['source_artifact_ref'] for node in projection['nodes']}
    assert {(refs[edge['from_candidate_id']], refs[edge['to_candidate_id']])
        for edge in projection['edges'] if edge['rule_id'] == f'derives_from/{rule}@v2.1'} == expected

    class Captured(Exception):
        pass

    async def begin(request, **kwargs):
        edges = [edge for intent in kwargs['relational_projection_active_set_intents']
            for edge in intent.active_edges if edge.rule_id == f'derives_from/{rule}@v2.1']
        assert {(refs[edge.from_candidate_id], refs[edge.to_candidate_id]) for edge in edges} == expected
        raise Captured

    monkeypatch.setattr(live, 'get_consolidation_persistence_port', lambda: port)
    monkeypatch.setattr(live, 'begin_consolidation', begin)
    with pytest.raises(Captured):
        await live._process_queue_entry(None, SimpleNamespace(board_id='board',
            artifact_type='spec', artifact_id='spec-one', work_kind='consolidate'))

    # A subsequent read must use the changed native source.
    getattr(artifact, collection)[0]['linked_requirements'] = ['fr-one']
    changed = await prepare_projection(None, port, 'board', 'spec', 'spec-one')
    assert len([edge for edge in changed['edges']
        if edge['rule_id'] == f'derives_from/{rule}@v2.1']) == 1


@pytest.mark.asyncio
async def test_live_admission_preserves_source_dates_and_root_metadata(monkeypatch):
    port = persistence(spec(created_at=datetime(2001, 1, 2), updated_at=datetime(2002, 1, 3)))

    class Captured(Exception):
        pass

    async def begin(request, **kwargs):
        assert {'spec:spec-one', 'spec:spec-one:fr:fr-one'} <= {
            node.source_artifact_ref for node in request.deterministic_candidates}
        assert {node.source_artifact_ref: node._source_projection_metadata.graph_attributes()
            for node in request.deterministic_candidates if node._source_projection_metadata is not None
        } == {'spec:spec-one': {
            'source_created_at': '2001-01-02T00:00:00+00:00',
            'source_updated_at': '2002-01-03T00:00:00+00:00',
            'source_status': 'done', 'severity': None, 'resolved_at': None}}
        raise Captured

    monkeypatch.setattr(live, 'get_consolidation_persistence_port', lambda: port)
    monkeypatch.setattr(live, 'begin_consolidation', begin)
    with pytest.raises(Captured):
        await live._process_queue_entry(None, SimpleNamespace(board_id='board',
            artifact_type='spec', artifact_id='spec-one', work_kind='consolidate'))


@pytest.mark.asyncio
@pytest.mark.parametrize('status', ['cancelled', 'archived'])
async def test_terminal_refinement_cleans_only_its_owned_set_without_reviving_root(status, monkeypatch):
    port = persistence(spec(id='refinement-one', status=status))
    projection = await prepare_projection(None, port, 'board', 'refinement', 'refinement-one')
    assert projection['nodes'] == projection['edges'] == []
    assert projection['relational_projection_active_set_intents'] == ({
        'owner_type': 'refinement', 'owner_id': 'refinement-one', 'namespace': 'rdl',
        'active_refs': (), 'active_edges': ()},)
    port.load_projection_inputs.assert_not_awaited()

    class Captured(Exception):
        pass

    async def begin(request, **kwargs):
        assert request.raw_content == projection['raw_content']
        assert request.deterministic_candidates == []
        intent, = kwargs['relational_projection_active_set_intents']
        assert (intent.owner_type, intent.owner_id, intent.namespace) == (
            'refinement', 'refinement-one', 'rdl')
        assert intent.active_refs == intent.active_edges == ()
        raise Captured

    monkeypatch.setattr(live, 'get_consolidation_persistence_port', lambda: port)
    monkeypatch.setattr(live, 'begin_consolidation', begin)
    with pytest.raises(Captured):
        await live._process_queue_entry(None, SimpleNamespace(board_id='board',
            artifact_type='refinement', artifact_id='refinement-one', work_kind='consolidate'))


@pytest.mark.asyncio
async def test_cancelled_spec_is_explicitly_skipped_without_loading_projection_inputs():
    port = persistence(spec(status='cancelled'))
    assert await live._prepare_deterministic_projection(None, SimpleNamespace(
        board_id='board', artifact_type='spec', artifact_id='spec-one'), persistence=port) is True
    port.load_projection_inputs.assert_not_awaited()
