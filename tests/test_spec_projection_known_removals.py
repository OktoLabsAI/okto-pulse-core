"""Source completeness and endpoint identity for pending Spec prerequisites."""
from types import SimpleNamespace

import pytest

from okto_pulse.core.application.processors.deterministic_kg import (
    RelationalProjectionActiveEdgeRef, RelationalProjectionActiveSetIntent,
)
from okto_pulse.core.kg.projection_removals import dependency_removal_intents


def declaration():
    rule = 'precedes/spec_dependency/d@v2.0'
    ref = RelationalProjectionActiveEdgeRef('e', 'precedes', 'kgref:Entity:spec:pending', 'root', rule)
    return dict(intents=(RelationalProjectionActiveSetIntent('spec', 'owner', 'dependencies', (), (ref,)),),
        nodes={'root': SimpleNamespace(node_type='Entity', source_artifact_ref='spec:owner')},
        edges={'e': SimpleNamespace(edge_type=ref.edge_type, from_candidate_id=ref.from_candidate_id,
            to_candidate_id=ref.to_candidate_id, rule_id=rule)})


def resolve(endpoint):
    return ('physical-owner', 'Entity') if endpoint == 'kgref:Entity:spec:owner' else (None, None)


def test_pending_source_identity_is_retained_without_materializing_it():
    intent, = dependency_removal_intents(**declaration(), resolve_endpoint=resolve)
    assert intent.owner_node_id == 'physical-owner'
    assert intent.expected_edges[0].source_ref == 'spec:pending'
    assert intent.expected_edges[0].target_ref == 'spec:owner'
    assert not intent.active_edges and not intent.active_nodes


def test_unmaterialized_owner_has_no_removal_work():
    assert dependency_removal_intents(**declaration(), resolve_endpoint=lambda _: (None, None)) == ()


def test_provider_failure_does_not_authorize_removal():
    def fail(_endpoint):
        raise RuntimeError('unavailable')
    with pytest.raises(RuntimeError, match='unavailable'):
        dependency_removal_intents(**declaration(), resolve_endpoint=fail)


def test_foreign_owner_cannot_supply_removal_authority():
    payload = declaration()
    payload['nodes']['root'].source_artifact_ref = 'spec:foreign'
    with pytest.raises(ValueError, match='endpoint_invalid'):
        dependency_removal_intents(**payload, resolve_endpoint=resolve)


@pytest.mark.asyncio
async def test_cancelled_spec_preserves_noop_after_source_fence(monkeypatch):
    from okto_pulse.core.application.processors import consolidation
    events, lease = [], object()
    entry = SimpleNamespace(id='q', board_id='b', artifact_type='spec', artifact_id='s',
        work_kind='consolidate', source='state_transition', claim_token='token')
    async def fence(*_args):
        events.append('fence')
        return True
    async def prepare(*_args):
        events.append('cancelled source')
        return True
    async def durable(**kwargs):
        assert kwargs['write_lease'] is lease
        events.append('durable')
    def enter(_ref):
        events.append('writer')
        return lease
    monkeypatch.setattr(consolidation, '_queue_claim_is_current_and_unfenced', fence)
    monkeypatch.setattr(consolidation, '_prepare_deterministic_projection', prepare)
    monkeypatch.setattr(consolidation, '_ensure_board_graph_durable', durable)
    assert await consolidation._process_queue_entry(object(), entry,
        enter_graph_write=enter, deferred_session_ids=[]) is True
    assert events == ['writer', 'fence', 'cancelled source', 'durable']
