from dataclasses import replace

import pytest

from okto_pulse.core.application.processors.deterministic_kg import (
    RelationalProjectionActiveSetIntent,
    RelationalProjectionActiveRef,
)
from okto_pulse.core.kg.primitives import KGPrimitiveError, _validate_projection_intent_collection


def intent(namespace):
    return RelationalProjectionActiveSetIntent('spec', 'owner', namespace, ())


def test_distinct_namespaces_are_preserved_in_order():
    intents = (intent('dependencies'), intent('scenario_criteria'))
    _validate_projection_intent_collection(intents, session_id='test')
    assert [item.namespace for item in intents] == ['dependencies', 'scenario_criteria']


@pytest.mark.parametrize('intents', [None, [], [intent('dependencies')], (intent('a'),) * 17])
def test_unbounded_or_mutable_collection_is_rejected(intents):
    with pytest.raises(KGPrimitiveError, match='bounded tuple'):
        _validate_projection_intent_collection(intents, session_id='test')


def test_duplicate_namespace_is_rejected_even_when_members_are_empty():
    with pytest.raises(KGPrimitiveError, match='distinct'):
        _validate_projection_intent_collection((intent('dependencies'), intent('dependencies')), session_id='test')


def test_member_cannot_be_owned_by_two_namespaces():
    ref = RelationalProjectionActiveRef('Criterion', 'candidate', 'spec:owner:ac:one')
    intents = tuple(replace(intent(namespace), active_refs=(ref,)) for namespace in ('a', 'b'))
    with pytest.raises(KGPrimitiveError, match='exactly one owner'):
        _validate_projection_intent_collection(intents, session_id='test')


def test_memory_scenario_compensation_preserves_parallel_writers_and_is_repeatable():
    from okto_pulse.core.kg.interfaces.graph_transaction import ProjectionActiveSetIntent
    from okto_pulse.core.kg.providers.testing.memory_graph_store import InMemoryGraphStore, _InMemoryGraphTransactionScope
    store = InMemoryGraphStore()
    for kind, identity, ref in [('Entity', 'root', 'spec:owner'), ('TestScenario', 'ts', 'spec:owner:test_scenario:one'), ('Criterion', 'ac', 'spec:owner:ac:one')]:
        store.create_node('board', kind, identity, {'source_artifact_ref': ref})
    for layer, writer in [('deterministic', 'worker_layer1'), ('cognitive', 'human')]:
        store.create_edge('board', 'tests', 'ts', 'ac', {'rule_id': 'tests/ac_match@v2.1', 'layer': layer, 'created_by': writer}, from_type='TestScenario', to_type='Criterion')
    before = [dict(edge) for edge in store._board_edges('board')]
    scope = _InMemoryGraphTransactionScope('board', store)
    receipt = scope.reconcile_projection_active_set(ProjectionActiveSetIntent('spec', 'owner', 'scenario_criteria', owner_node_id='root'))
    assert len(store._board_edges('board')) == 1
    for _ in range(2):
        scope.compensate_projection_active_set(receipt)
        assert sorted(store._board_edges('board'), key=lambda edge: edge['layer']) == sorted(before, key=lambda edge: edge['layer'])


def test_partial_spec_source_does_not_authorize_empty_scenario_replacement():
    from okto_pulse.core.application.processors.deterministic_kg import DeterministicWorker
    worker = DeterministicWorker()
    result = worker.process_spec({'id': 'owner', 'title': 'Partial', 'acceptance_criteria': []})
    assert not any(item.namespace == 'scenario_criteria' for item in result.relational_projection_active_set_intents)
    complete = worker.process_spec({'id': 'owner', 'title': 'Complete', 'acceptance_criteria': [], 'test_scenarios': []})
    assert any(item.namespace == 'scenario_criteria' and item.active_edges == () for item in complete.relational_projection_active_set_intents)
