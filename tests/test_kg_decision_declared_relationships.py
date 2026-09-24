"""Decision derivations are declared links, never narrative co-occurrence."""
import pytest

from okto_pulse.core.application.processors.deterministic_kg import DeterministicWorker


def spec():
    return {'id': 'owner', 'board_id': 'board', 'title': 'Decision', 'status': 'draft',
        'description': '', 'context': '', 'decisions': [],
        'functional_requirements': [{'id': 'fr_one', 'text': 'First'}, {'id': 'fr_two', 'text': 'Second'}],
        'technical_requirements': [{'id': 'tr_one', 'text': 'Technical'}],
        'test_scenarios': [], 'acceptance_criteria': []}


def relations(source):
    result = DeterministicWorker().process_spec(source)
    nodes = {node.candidate_id: node for node in result.nodes}
    return result, {(nodes[edge.to_candidate_id].source_artifact_ref, edge.rule_id, edge.confidence)
        for edge in result.edges if edge.edge_type == 'derives_from'
        and nodes[edge.from_candidate_id].node_type == 'Decision'}


@pytest.mark.parametrize('case', ['empty', 'unknown', 'legacy', 'explicit', 'duplicate'])
def test_only_unambiguous_declared_requirements_are_emitted(case):
    source = spec()
    if case == 'legacy':
        source['context'] = '## Decisions\n- Keep evidence local\n'
    else:
        links = ['fr_one', 'tr_one', 'fr_one'] if case == 'explicit' else ['fr_missing'] if case == 'unknown' else []
        if case == 'duplicate':
            source['functional_requirements'].append({'id': 'fr_one', 'text': 'Ambiguous'})
            links = ['fr_one']
        source['decisions'] = [{'id': 'dec_one', 'title': 'Keep evidence local', 'linked_requirements': links}]
    result, actual = relations(source)
    expected = {(f'spec:owner:{section}:{identity}', 'derives_from/explicit_link@v2.1', 1.0)
        for section, identity in [('fr', 'fr_one'), ('tr', 'tr_one')]} if case == 'explicit' else set()
    assert actual == expected
    assert any(node.node_type == 'Decision' for node in result.nodes)
    intent = next(item for item in result.relational_projection_active_set_intents if item.namespace == 'decision_requirements')
    assert len(intent.active_edges) == len(expected)


def test_reordering_preserves_declared_identity_and_legacy_fr_index_is_supported():
    source = spec()
    source['decisions'] = [{'id': 'dec_one', 'title': 'Choice', 'linked_requirements': ['fr_one', 'tr_one']}]
    before = relations(source)[1]
    source['functional_requirements'].reverse()
    assert relations(source)[1] == before
    source['decisions'][0]['linked_requirements'] = [0]
    assert relations(source)[1] == {('spec:owner:fr:fr_two', 'derives_from/explicit_link@v2.1', 1.0)}


@pytest.mark.parametrize('links,expected', [
    (['First'], {'spec:owner:fr:fr_one'}),
    (['Technical'], {'spec:owner:tr:tr_one'}),
    (['First', 'Technical'], {'spec:owner:fr:fr_one', 'spec:owner:tr:tr_one'}),
])
def test_unique_legacy_requirement_text_matches_the_declared_domain_reference(links, expected):
    source = spec()
    source['decisions'] = [{'id': 'dec_one', 'title': 'Choice', 'linked_requirements': links}]
    assert {row[0] for row in relations(source)[1]} == expected


@pytest.mark.parametrize('case', ['id_before_text', 'unknown_id_as_text', 'duplicate_text', 'cross_collection_text'])
def test_legacy_text_never_steals_canonical_identity_or_selects_ambiguous_target(case):
    source = spec()
    token = 'fr_one' if case == 'id_before_text' else 'fr_missing' if case == 'unknown_id_as_text' else 'Repeated'
    source['functional_requirements'][1]['text'] = token
    if case == 'duplicate_text':
        source['functional_requirements'][0]['text'] = token
    if case == 'cross_collection_text':
        source['technical_requirements'][0]['text'] = token
    source['decisions'] = [{'id': 'dec_one', 'title': 'Choice', 'linked_requirements': [token]}]
    expected = {'spec:owner:fr:fr_one'} if case == 'id_before_text' else set()
    assert {row[0] for row in relations(source)[1]} == expected


@pytest.mark.parametrize('missing', ['context', 'decisions', 'functional_requirements', 'technical_requirements'])
def test_partial_source_cannot_authorize_decision_prune(missing):
    source = spec()
    del source[missing]
    result, _ = relations(source)
    assert not any(item.namespace == 'decision_requirements' for item in result.relational_projection_active_set_intents)
