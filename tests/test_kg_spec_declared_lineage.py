"""Relational lineage uses its declared direction, identity and owner."""
from copy import deepcopy

import pytest

from okto_pulse.core.application.processors.deterministic_kg import DeterministicWorker


def source():
    return {'id': 'owner', 'board_id': 'board', 'title': 'Lineage', 'status': 'draft', 'context': '',
        'functional_requirements': [{'id': 'fr_one', 'text': 'Functional condition'}],
        'technical_requirements': [], 'acceptance_criteria': [], 'test_scenarios': [], 'decisions': [],
        'business_rules': [{'id': 'br_one', 'title': 'Business condition', 'rule': 'Require receipt',
            'when': 'delivery', 'then': 'receipt', 'linked_requirements': ['fr_one']}],
        'integration_requirements': [{'id': 'ir_one', 'title': 'Contract', 'linked_requirements': ['fr_one']}],
        'observability_requirements': [{'id': 'or_one', 'title': 'Observe contract', 'linked_integration_requirements': ['ir_one']}],
        'api_contracts': [{'id': 'api_one', 'method': 'POST', 'path': '/receipt', 'linked_rules': ['br_one']}],
    }


CASES = [
    ('business_rules', 'linked_requirements', 'business_rule', 'br_one', 'derives_from', 'fr', 'fr_one', 'business_rule_requirements'),
    ('integration_requirements', 'linked_requirements', 'integration_requirement', 'ir_one', 'derives_from', 'fr', 'fr_one', 'integration_requirements'),
    ('observability_requirements', 'linked_integration_requirements', 'observability_requirement', 'or_one', 'derives_from', 'integration_requirement', 'ir_one', 'observability_integrations'),
    ('api_contracts', 'linked_rules', 'api_contract', 'api_one', 'implements', 'business_rule', 'br_one', 'api_business_rules'),
]


def projected(value):
    result = DeterministicWorker().process_spec(value)
    refs = {node.candidate_id: node.source_artifact_ref for node in result.nodes}
    return result, {(refs[edge.from_candidate_id], edge.edge_type, refs[edge.to_candidate_id])
        for edge in result.edges if edge.from_candidate_id in refs and edge.to_candidate_id in refs}


@pytest.mark.parametrize('collection,field,section,identity,edge_type,target_section,target_id,namespace', CASES)
def test_declared_lineage_and_explicit_empty_active_set(collection, field, section, identity, edge_type, target_section, target_id, namespace):
    value = source()
    expected = (f'spec:owner:{section}:{identity}', edge_type, f'spec:owner:{target_section}:{target_id}')
    result, actual = projected(value)
    assert expected in actual
    assert len(next(intent for intent in result.relational_projection_active_set_intents if intent.namespace == namespace).active_edges) == 1
    value[collection][0][field] = []
    result, actual = projected(value)
    assert expected not in actual
    assert next(intent for intent in result.relational_projection_active_set_intents if intent.namespace == namespace).active_edges == ()
    del value[collection]
    assert not any(intent.namespace == namespace for intent in projected(value)[0].relational_projection_active_set_intents)


@pytest.mark.parametrize('collection,field,section,identity,edge_type,target_section,target_id,namespace', CASES)
def test_revoked_source_stops_current_relationship_without_claiming_history_deleted(collection, field, section, identity, edge_type, target_section, target_id, namespace):
    value = source()
    value[collection][0]['status'] = 'revoked'
    result, actual = projected(value)
    assert (f'spec:owner:{section}:{identity}', edge_type, f'spec:owner:{target_section}:{target_id}') not in actual
    assert next(intent for intent in result.relational_projection_active_set_intents if intent.namespace == namespace).active_edges == ()


@pytest.mark.parametrize('collection', ['business_rules', 'integration_requirements'])
def test_unique_fr_text_is_supported_but_duplicate_text_does_not_choose_a_target(collection):
    value = source()
    value[collection][0]['linked_requirements'] = ['Functional condition']
    result, _ = projected(value)
    namespace = 'business_rule_requirements' if collection == 'business_rules' else collection
    assert len(next(intent for intent in result.relational_projection_active_set_intents if intent.namespace == namespace).active_edges) == 1
    value['functional_requirements'].append({'id': 'fr_other', 'text': 'Functional condition'})
    assert next(intent for intent in projected(value)[0].relational_projection_active_set_intents if intent.namespace == namespace).active_edges == ()


@pytest.mark.parametrize('collection,namespace', [('business_rules', 'api_business_rules'), ('integration_requirements', 'observability_integrations')])
def test_duplicate_exact_target_ids_are_not_arbitrarily_selected(collection, namespace):
    value = source()
    value[collection].append(deepcopy(value[collection][0]))
    result, _ = projected(value)
    assert next(intent for intent in result.relational_projection_active_set_intents if intent.namespace == namespace).active_edges == ()


@pytest.mark.parametrize('collection', ['integration_requirements', 'observability_requirements'])
@pytest.mark.parametrize('missing', ['functional_requirements', 'technical_requirements'])
def test_partial_requirement_source_cannot_prune_either_target_family(collection, missing):
    value = source()
    value[collection][0]['linked_requirements'] = []
    del value[missing]
    assert not any(item.namespace == collection for item in projected(value)[0].relational_projection_active_set_intents)


@pytest.mark.parametrize('collection', ['integration_requirements', 'observability_requirements'])
def test_requirement_text_is_unique_across_fr_tr_and_exact_id_takes_precedence(collection):
    value = source()
    value['technical_requirements'] = [{'id': 'tr_one', 'text': 'Technical condition'}]
    value[collection][0]['linked_requirements'] = ['Technical condition']
    def active_targets():
        result, _ = projected(value)
        refs = {node.candidate_id: node.source_artifact_ref for node in result.nodes}
        intent = next(item for item in result.relational_projection_active_set_intents if item.namespace == collection)
        return {refs[edge.to_candidate_id] for edge in intent.active_edges}
    assert active_targets() == {'spec:owner:tr:tr_one'}
    value['functional_requirements'][0]['text'] = 'Technical condition'
    assert active_targets() == set()
    value[collection][0]['linked_requirements'] = ['tr_one']
    value['functional_requirements'][0]['text'] = 'tr_one'
    assert active_targets() == {'spec:owner:tr:tr_one'}
    value['technical_requirements'].append(deepcopy(value['technical_requirements'][0]))
    assert active_targets() == set()


def test_observability_requirement_and_integration_namespaces_remain_independent():
    value = source()
    value['observability_requirements'][0]['linked_requirements'] = ['fr_one']
    result, _ = projected(value)
    intents = {item.namespace: item for item in result.relational_projection_active_set_intents}
    assert len(intents['observability_requirements'].active_edges) == 1
    assert len(intents['observability_integrations'].active_edges) == 1
    value['observability_requirements'][0]['linked_requirements'] = []
    result, _ = projected(value)
    intents = {item.namespace: item for item in result.relational_projection_active_set_intents}
    assert intents['observability_requirements'].active_edges == ()
    assert len(intents['observability_integrations'].active_edges) == 1


@pytest.mark.parametrize('collection,section,identity,target_section,target_id', [
    ('integration_requirements', 'integration_requirement', 'ir_one', 'tr', 'tr_one'),
    ('observability_requirements', 'observability_requirement', 'or_one', 'fr', 'fr_one'),
    ('observability_requirements', 'observability_requirement', 'or_one', 'tr', 'tr_one'),
])
def test_declared_requirement_lineage_includes_supported_fr_tr_targets(
    collection, section, identity, target_section, target_id,
):
    value = source()
    value['technical_requirements'] = [{'id': 'tr_one', 'text': 'Technical condition'}]
    value[collection][0]['linked_requirements'] = [target_id]
    expected = (f'spec:owner:{section}:{identity}', 'derives_from', f'spec:owner:{target_section}:{target_id}')
    result, actual = projected(value)
    assert expected in actual
    intent = next(item for item in result.relational_projection_active_set_intents if item.namespace == collection)
    assert len(intent.active_edges) == 1
    value[collection][0]['linked_requirements'] = []
    result, actual = projected(value)
    assert expected not in actual
    assert next(item for item in result.relational_projection_active_set_intents if item.namespace == collection).active_edges == ()
