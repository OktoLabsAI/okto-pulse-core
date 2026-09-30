from dataclasses import replace
from types import SimpleNamespace

import pytest

from okto_pulse.core.application import learning_candidates as app
from okto_pulse.core.kg.interfaces.graph_errors import GraphUnavailable
from okto_pulse.core.ports.kg_cognitive_source import CognitiveSourceRecord, CognitiveSourceUnavailable


def record(identity='learning', **payload):
    return CognitiveSourceRecord(board_id='board', node_type='Learning', node_id=identity,
        generation=0, payload=dict(content='Lesson ' + identity, context='Context', graph_layer='canonical',
            maturity_status='canonical_eligible', created_by_agent='author', created_at='2026-09-29T00:00:00Z',
            source_artifact_ref='bug:origin') | payload, evidence_refs=('bug:origin',))


def hit(row, score):
    return dict(node_id=row.node_id, node_type='Learning', generation=row.generation,
        vector_score=score, **{key: row.payload.get(key) for key in ('content', 'context', 'source_artifact_ref')})


class Store:
    def __init__(self, rows):
        self.rows = {row.node_id: (row,) for row in rows}
        self.calls = []
    async def read_bounded_history_in_context(self, context, *, board_id, node_id, generation, max_records):
        self.calls.append((board_id, node_id, generation, max_records))
        rows = self.rows.get(node_id, ())
        if len(rows) > max_records:
            raise CognitiveSourceUnavailable('cognitive_source_history_limit', board_id=board_id, node_id=node_id)
        return rows


@pytest.fixture
def runtime(monkeypatch):
    rows = [record(str(index)) for index in range(5)]
    store = Store(rows)
    page = dict(mode='vector', ranking='cosine', hits=[hit(row, score) for row, score in zip(rows, [.59, .7, .85, .95, .99])],
        snapshot='12', vector_regime='exact', complete=True, achieved_k=5, requested_k=20)
    calls = []
    def search(board, request): calls.append((board, request)); return page
    registry = SimpleNamespace(ranked_graph_search=SimpleNamespace(search=search),
        embedding_provider=SimpleNamespace(encode=lambda query: [1.0] + [0.0] * 383))
    monkeypatch.setattr(app, 'get_kg_registry', lambda: registry)
    monkeypatch.setattr(app, 'require_cognitive_source_store', lambda: store)
    return rows, store, page, registry, calls


@pytest.mark.asyncio
async def test_ranked_suggestions_are_explicit_bounded_and_source_qualified(runtime):
    rows, store, _, _, calls = runtime
    page = await app.learning_capture_candidates(None, board_id='board', query='lesson')
    assert page['status'] == 'available' and page['exhaustive'] is False
    assert page['applicability'] == 'not_assessed'
    assert [item['learning_id'] for item in page['items']] == ['4', '3', '2']
    assert [item['suggestion'] for item in page['items']] == ['reuse', 'reuse', 'review_replacement']
    assert page['items'][0]['fingerprint'] == rows[4].record_fingerprint
    assert [call[-1] for call in store.calls] == [200, 199, 198]
    board, request = calls[0]
    assert board == 'board' and request.mode == 'vector' and request.limit == 20
    assert request.timeout_seconds == 15 and request.graph_layer == 'canonical'
    assert not request.include_superseded and not request.include_code_traceability


@pytest.mark.asyncio
async def test_stale_projection_and_pending_heads_are_omitted_with_limits(runtime):
    rows, store, _, _, _ = runtime
    store.rows['4'] = (record('4', content='Changed source'),)
    store.rows['3'] = (record('3', maturity_status='working'),)
    page = await app.learning_capture_candidates(None, board_id='board', query='lesson')
    assert [item['learning_id'] for item in page['items']] == ['2', '1']
    assert page['limitations'] == ['projection_differs_from_source', 'source_not_eligible']
    assert page['items'][-1]['suggestion'] == 'related'


@pytest.mark.asyncio
async def test_budget_exhaustion_is_not_empty_complete_history(runtime):
    rows, store, _, _, _ = runtime
    store.rows['4'] = (rows[4],) * 201
    page = await app.learning_capture_candidates(None, board_id='board', query='lesson')
    assert not page['items'] and page['limitations'] == ['history_limit']
    assert len(store.calls) == 1 and page['exhaustive'] is False


@pytest.mark.asyncio
@pytest.mark.parametrize('damage', ['rank', 'scope', 'duplicate', 'score', 'generation', 'head_scope', 'count'])
async def test_invalid_provider_evidence_is_not_a_suggestion(runtime, damage):
    rows, store, page, _, _ = runtime
    if damage == 'rank': page['ranking'] = 'rrf_v1_union'
    elif damage == 'scope': page['hits'][0]['node_type'] = 'Decision'
    elif damage == 'duplicate': page['hits'][0] = page['hits'][1]
    elif damage == 'score': page['hits'][0]['vector_score'] = float('nan')
    elif damage == 'generation': page['hits'][0]['generation'] = True
    elif damage == 'count': page['achieved_k'] = 99
    else: store.rows['4'] = (replace(rows[4], board_id='foreign', record_fingerprint=''),)
    with pytest.raises(ValueError, match='candidate_result_invalid'):
        await app.learning_capture_candidates(None, board_id='board', query='lesson')


@pytest.mark.asyncio
@pytest.mark.parametrize('missing', ['search', 'embedder', 'history', 'failure'])
async def test_unavailable_search_does_not_mean_no_similar_learning(runtime, monkeypatch, missing):
    _, _, _, registry, calls = runtime
    if missing == 'search': registry.ranked_graph_search = None
    elif missing == 'embedder': registry.embedding_provider = None
    elif missing == 'history': monkeypatch.setattr(app, 'require_cognitive_source_store', lambda: object())
    else:
        def fail(*args): raise GraphUnavailable('SECRET native details')
        registry.ranked_graph_search.search = fail
    page = await app.learning_capture_candidates(None, board_id='board', query='lesson')
    assert page['status'] == 'unavailable' and page['limitation'] and not page['items']
    assert 'SECRET' not in str(page) and not calls
