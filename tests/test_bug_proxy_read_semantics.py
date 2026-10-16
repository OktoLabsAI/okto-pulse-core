"""Curated reads must retain the distinction between association and cause."""
from types import SimpleNamespace

import pytest

from okto_pulse.core.kg import kg_service
from okto_pulse.core.ports.card_projection import bug_origin_proxy_read_metadata


def test_constraint_explanation_keeps_origin_proxy_classification(monkeypatch):
    rule = 'violates/bug_origin_proxy_tr/origin@v2.1'
    def read(board_id, constraint_id):
        assert (board_id, constraint_id) == ('board', 'constraint')
        return ([['constraint', 'Constraint', 'Content', 'Reason', 'spec:one:tr:a', 1]], [], [
            ['bug-a', 'Different cause A', 0.8, rule, 'deterministic', 'worker_layer1', 'inferred_origin_proxy:card:origin'],
            ['bug-b', 'Different cause B', 0.8, rule, 'deterministic', 'worker_layer1', 'inferred_origin_proxy:card:origin'],
            ['native', 'Unclassified current link', 0.8, 'human-rule', 'cognitive', 'human', ''],
        ])
    monkeypatch.setattr(kg_service, '_get_graph_store', lambda: SimpleNamespace(get_constraint_detail=read))
    result = kg_service.KGService().explain_constraint('board', 'constraint')
    first, second, native = result['violations']
    assert first['id'] != second['id']
    assert first['assertion_basis'] == second['assertion_basis'] == 'origin_proxy'
    assert first['causal_conclusion'] == second['causal_conclusion'] == 'not_established'
    assert first['origin_card_ref'] == second['origin_card_ref'] == 'card:origin'
    assert native == {'id': 'native', 'title': 'Unclassified current link', 'confidence': 0.8}


def test_proxy_label_requires_matching_closed_provenance():
    source = dict(rule_id='violates/bug_origin_proxy_tr/origin@v2.1', layer='deterministic',
        created_by='worker_layer1', fallback_reason='inferred_origin_proxy:card:origin')
    assert bug_origin_proxy_read_metadata(**source)['assertion_basis'] == 'origin_proxy'
    for change in ({'created_by': 'human'}, {'layer': 'cognitive'},
        {'rule_id': 'violates/other@v2.1'}, {'fallback_reason': 'inferred_origin_proxy:card:other'}):
        assert bug_origin_proxy_read_metadata(**(source | change)) == {}


@pytest.mark.parametrize('row', [
    ['old', 'Short association'],
    ['old', 'Missing provenance', 0.8],
    ['old', 'Truncated', 0.8, 'rule', 'layer', 'writer'],
    ['future', 'Extra field', 0.8, 'rule', 'layer', 'writer', '', 'extra'],
])
def test_constraint_explanation_refuses_incompatible_association_rows(monkeypatch, row):
    def read(*_):
        return ([['constraint', 'Constraint', 'Content', 'Reason', 'spec:one:tr:a', 1]], [], [row])
    monkeypatch.setattr(kg_service, '_get_graph_store', lambda: SimpleNamespace(get_constraint_detail=read))
    with pytest.raises(kg_service.KGToolError) as error:
        kg_service.KGService().explain_constraint('board', 'constraint')
    assert error.value.code == 'graph_contract_incompatible'
