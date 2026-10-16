"""Net effects retain exact old values without turning reuses into creations."""

from copy import deepcopy
from dataclasses import asdict

import pytest

from okto_pulse.core.application.projection_effects import capture_projection_property_effects
from okto_pulse.core.kg.interfaces.graph_transaction import GraphNodePropertyBeforeImage
from okto_pulse.core.kg.transaction import TransactionOrchestrator
from okto_pulse.core.ports.projection_effects import (
    ProjectionPropertyEffect, ProjectionPropertyEffects, validated_projection_effect_extension,
)


class Scope:
    def __init__(self):
        self.rows = {('Entity', 'prior'): {'title': 'old', 'attestation_count': 1}}

    def create_node(self, kind, identity, attrs, *, source_session_id):
        self.rows[(kind, identity)] = {**deepcopy(attrs), 'source_session_id': source_session_id}

    def update_node(self, kind, identity, attrs):
        self.rows[(kind, identity)].update(deepcopy(attrs))

    def snapshot_node_properties(self, kind, identity, names):
        row = self.rows.get((kind, identity))
        return None if row is None else GraphNodePropertyBeforeImage(kind, identity,
            {name: deepcopy(row.get(name)) for name in names})


def test_net_effects_capture_first_before_last_after_and_never_reclassify_created_nodes():
    scope = Scope()
    transaction = TransactionOrchestrator(scope, 'session', 'board')
    transaction.update_node('Entity', 'prior', {'title': 'intermediate'})
    transaction.update_node('Entity', 'prior', {'title': 'final', 'attestation_count': 2})
    transaction.create_node('Entity', 'new', {'title': 'fresh'})
    transaction.update_node('Entity', 'new', {'title': 'fresh-final'})
    before = deepcopy(transaction.records)
    counters = asdict(transaction.counters)
    effects = capture_projection_property_effects(scope, transaction.records, board_id='board', session_id='session')
    assert effects.nodes == (ProjectionPropertyEffect.from_values('Entity', 'prior',
        {'title': 'old', 'attestation_count': 1}, {'title': 'final', 'attestation_count': 2}),)
    assert transaction.records == before and asdict(transaction.counters) == counters
    assert [record.entity_id for record in transaction.records if record.kind == 'node'] == ['new']
    assert ProjectionPropertyEffects.from_payload(effects.to_payload()) == effects
    assert validated_projection_effect_extension({'projection_property_effects': effects.to_payload()},
        board_id='board', session_id='session') == {'projection_property_effects': effects.to_payload()}
    with pytest.raises(ValueError, match='scope_mismatch'):
        validated_projection_effect_extension({'projection_property_effects': effects.to_payload()},
            board_id='another', session_id='session')
    damaged = effects.to_payload()
    damaged['nodes'][0]['after']['title'] = 'forged'
    with pytest.raises(ValueError, match='payload_changed'):
        ProjectionPropertyEffects.from_payload(damaged)


def test_protection_without_change_is_not_a_mutation_and_missing_final_state_fails_closed():
    scope = Scope()
    transaction = TransactionOrchestrator(scope, 'session', 'board')
    transaction.protect_node_properties('Entity', 'prior', ('title',))
    assert capture_projection_property_effects(scope, transaction.records, board_id='board', session_id='session') is None
    del scope.rows[('Entity', 'prior')]
    with pytest.raises(ValueError, match='final_snapshot_missing'):
        capture_projection_property_effects(scope, transaction.records, board_id='board', session_id='session')


@pytest.mark.parametrize('before,after', [({'unknown': 1}, {'unknown': 2}),
    ({'title': 'before'}, {'content': 'after'}), ({'id': 'old'}, {'id': 'new'}), ({'title': 'same'}, {'title': 'same'})])
def test_effect_contract_refuses_unknown_properties_identity_writes_and_non_deltas(before, after):
    with pytest.raises(ValueError, match='projection_effect_'):
        ProjectionPropertyEffect.from_values('Entity', 'node', before, after)
