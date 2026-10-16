"""Bounded suggestions, never an admission or an automatic memory rewrite."""
import math

from okto_pulse.core.kg.blocking_io import run_blocking_graph_io
from okto_pulse.core.kg.interfaces.graph_errors import GraphError
from okto_pulse.core.kg.interfaces.ranked_graph_search import RankedGraphQuery
from okto_pulse.core.kg.interfaces.registry import get_kg_registry
from okto_pulse.core.ports.kg_cognitive_source import (
    BoundedCognitiveHistoryReader, CognitiveSourceUnavailable,
    latest_cognitive_source_records, require_cognitive_source_store,
)


async def learning_capture_candidates(context, *, board_id, query):
    if type(query) is not str or not query.strip() or len(query) > 4096:
        raise ValueError('learning_capture_candidate_query_invalid')
    base = dict(contract_version='learning-candidates/v1', status='unavailable', items=[],
        exhaustive=False, similarity_floor=0.60, candidate_window=20,
        applicability='not_assessed', data_source='graph_and_cognitive_source')
    registry = get_kg_registry()
    search, embedder = registry.ranked_graph_search, registry.embedding_provider
    if search is None or embedder is None:
        return {**base, 'limitation': 'search_capability_unavailable'}
    store = require_cognitive_source_store()
    if not isinstance(store, BoundedCognitiveHistoryReader):
        return {**base, 'limitation': 'history_capability_unavailable'}

    def retrieve():
        vector = tuple(embedder.encode(query))
        return search.search(board_id, RankedGraphQuery('Learning', query, mode='vector',
            vector=vector, limit=20, candidate_limit=20, min_confidence=0,
            graph_layer='canonical', timeout_seconds=15, max_filter_rows=10_000))
    try:
        page = await run_blocking_graph_io(retrieve, task_name='learning_candidates')
    except GraphError:
        # An unavailable search is explicit and does not prevent independent
        # authorship. Never turn a provider failure into "no similar Learning".
        return {**base, 'limitation': 'search_unavailable'}
    if (type(page) is not dict or page.get('mode') != 'vector' or page.get('ranking') != 'cosine'
            or page.get('complete') is not True or type(page.get('hits')) is not list
            or len(page['hits']) > 20 or type(page.get('snapshot')) is not str
            or not page['snapshot'] or type(page.get('vector_regime')) is not str
            or not page['vector_regime'] or type(page.get('achieved_k')) is not int
            or page['achieved_k'] != len(page['hits']) or page.get('requested_k') != 20):
        raise ValueError('learning_capture_candidate_result_invalid')
    seen, ranked = set(), []
    for hit in page['hits']:
        if type(hit) is not dict:
            raise ValueError('learning_capture_candidate_result_invalid')
        identity, generation, score = hit.get('node_id'), hit.get('generation'), hit.get('vector_score')
        if (type(identity) is not str or not identity.strip() or len(identity) > 4096
                or type(generation) is not int or generation < 0 or (identity, generation) in seen
                or hit.get('node_type') != 'Learning' or type(score) not in (int, float)
                or not math.isfinite(score) or not -1.000000001 <= score <= 1.000000001):
            raise ValueError('learning_capture_candidate_result_invalid')
        seen.add((identity, generation))
        if score >= 0.60:
            ranked.append(hit)
    ranked.sort(key=lambda hit: (-hit['vector_score'], hit['node_id'], hit['generation']))
    items, limitations, remaining = [], set(), 200
    for hit in ranked:
        if remaining == 0:
            limitations.add('history_limit')
            break
        try:
            history = await store.read_bounded_history_in_context(context, board_id=board_id,
                node_id=hit['node_id'], generation=hit['generation'], max_records=remaining)
        except CognitiveSourceUnavailable as exc:
            if exc.failure_reason != 'cognitive_source_history_limit':
                raise
            limitations.add('history_limit')
            break
        if (type(history) is not tuple or len(history) > remaining
                or any((row.board_id, row.node_type, row.node_id, row.generation)
                    != (board_id, 'Learning', hit['node_id'], hit['generation']) for row in history)
                or any(a.source_revision >= b.source_revision for a, b in zip(history, history[1:]))):
            raise ValueError('learning_capture_candidate_result_invalid')
        latest_cognitive_source_records(history)
        remaining -= len(history)
        if not history:
            limitations.add('source_unavailable')
            continue
        head = history[-1]
        payload = head.payload
        if ('capture_format' in payload or payload.get('graph_layer') != 'canonical'
                or payload.get('maturity_status') != 'canonical_eligible'
                or payload.get('superseded_by') or payload.get('revocation_reason')
                or any(type(payload.get(key)) is not str or not payload[key].strip()
                    for key in ('content', 'context', 'created_by_agent', 'created_at'))):
            limitations.add('source_not_eligible')
            continue
        if any(hit.get(key) != payload.get(key) for key in ('content', 'context', 'source_artifact_ref')):
            limitations.add('projection_differs_from_source')
            continue
        if any(len(payload[key]) > 65536 for key in ('content', 'context')):
            limitations.add('source_payload_limit')
            continue
        score = min(1.0, hit['vector_score'])
        items.append(dict(learning_id=head.node_id, generation=head.generation,
            source_revision=head.source_revision, fingerprint=head.record_fingerprint,
            content=payload['content'], context=payload['context'], similarity=score,
            suggestion='reuse' if score >= 0.95 else 'review_replacement' if score >= 0.85 else 'related'))
        if len(items) == 3:
            break
    return {**base, 'status': 'available', 'items': items, 'limitation': None,
        'limitations': sorted(limitations), 'graph_snapshot': page['snapshot'],
        'vector_regime': page['vector_regime']}
