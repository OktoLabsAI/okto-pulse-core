"""KG6.5: source confidence must constrain every natural retrieval path."""

from types import SimpleNamespace
import pytest

from kg_schema_testing import bootstrap_board_graph, open_board_connection
from okto_pulse.core.kg.interfaces.registry import get_kg_registry
from okto_pulse.core.kg.tier_power import execute_natural_query, TierPowerError, _filter_natural_source_confidence


@pytest.mark.parametrize('minimum,expected', [(0.0, {'low', 'high'}), (0.8, {'high'}), (1.0, set())])
def test_literal_and_topic_fallback_respect_source_confidence(monkeypatch, minimum, expected):
    board = f'natural-source-confidence-{minimum}'
    bootstrap_board_graph(board)
    with open_board_connection(board) as (_db, connection):
        for identity, confidence in [('low', 0.1), ('high', 0.9)]:
            connection.execute(
                "CREATE (:Bug {id: $id, title: 'confidence needle', content: 'confidence needle', "
                "graph_layer: 'canonical', source_confidence: $confidence, relevance_score: 0.5})",
                {'id': identity, 'confidence': confidence},
            )
    monkeypatch.setattr(get_kg_registry(), 'embedding_provider', SimpleNamespace(encode=lambda query: None))
    result = execute_natural_query(board, 'confidence needle', min_confidence=minimum)
    assert {row['node_id'] for row in result['nodes']} == expected


def test_vector_similarity_cannot_substitute_for_native_source_confidence(monkeypatch):
    board = 'vector-source-confidence'
    bootstrap_board_graph(board)
    with open_board_connection(board) as (_db, connection):
        for identity, confidence in [('low', 0.1), ('high', 0.9)]:
            connection.execute(
                "CREATE (:Decision {id: $id, title: 'different title', content: 'different content', "
                "graph_layer: 'canonical', source_confidence: $confidence, relevance_score: 0.5})",
                {'id': identity, 'confidence': confidence},
            )
    registry = get_kg_registry()
    monkeypatch.setattr(registry, 'embedding_provider', SimpleNamespace(encode=lambda query: [1.0] * 384))

    def vector_search(**kwargs):
        if kwargs['node_type'] != 'Decision':
            return []
        return [dict(node_id=identity, node_type='Decision', title='different title', similarity=score)
                for identity, score in [('low', 0.99), ('high', 0.4)]]

    # Retrieval is isolated; final confidence and layer qualification read the real graph.
    monkeypatch.setattr(registry, 'graph_store', SimpleNamespace(vector_search=vector_search))
    result = execute_natural_query(board, 'unmatched query', min_confidence=0.8)
    assert [(row['node_id'], row['similarity']) for row in result['nodes']] == [('high', 0.4)]


@pytest.mark.parametrize('minimum', [-1, 2, True, None, '0.5', float('nan'), float('inf')])
def test_invalid_confidence_is_refused_before_any_provider(monkeypatch, minimum):
    from okto_pulse.core.kg.interfaces import registry

    def unexpected():
        raise AssertionError('provider must not be resolved')

    monkeypatch.setattr(registry, 'get_kg_registry', unexpected)
    with pytest.raises(TierPowerError) as failure:
        execute_natural_query('missing', 'query', min_confidence=minimum)
    assert failure.value.code == 'invalid_param'


def test_confidence_reader_failure_is_not_a_successful_empty_result(monkeypatch):
    from okto_pulse.core.kg.interfaces import registry

    def refused(*args, **kwargs):
        raise RuntimeError('source reader unavailable')

    monkeypatch.setattr(registry, 'get_kg_registry', lambda: SimpleNamespace(
        cypher_executor=SimpleNamespace(execute_read_only=refused)))
    with pytest.raises(TierPowerError) as failure:
        _filter_natural_source_confidence('b', [dict(node_type='Decision', node_id='n')], 0.8)
    assert failure.value.code == 'query_confidence_unavailable'
