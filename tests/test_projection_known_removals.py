"""KG §4.5: source authority precedes removal; graph absence is technical debt."""
from types import SimpleNamespace

import pytest

from okto_pulse.core.application.processors.deterministic_kg import (
    RelationalProjectionActiveEdgeRef, RelationalProjectionActiveSetIntent,
)
from okto_pulse.core.kg.projection_removals import card_removal_intents
from okto_pulse.core.ports.card_projection import CARD_SCENARIO_RULES, CARD_CHILD_FAMILIES


def declaration():
    rule = sorted(CARD_SCENARIO_RULES)[0]
    edge = SimpleNamespace(edge_type='supports', from_candidate_id='root',
        to_candidate_id='kgref:TestScenario:spec:s:test_scenario:new', rule_id=rule)
    ref = RelationalProjectionActiveEdgeRef('edge', edge.edge_type, edge.from_candidate_id,
        edge.to_candidate_id, rule)
    intents = (RelationalProjectionActiveSetIntent('card', 'c', 'card_parent', ()),
        RelationalProjectionActiveSetIntent('card', 'c', 'card_scenarios', (), (ref,))) + tuple(
            RelationalProjectionActiveSetIntent('card', 'c', family.namespace, ())
            for family in CARD_CHILD_FAMILIES)
    return dict(intents=intents, nodes={'root': SimpleNamespace(node_type='Entity', source_artifact_ref='card:c')},
        edges={'edge': edge})


def test_lookup_failure_is_not_permission_to_remove():
    def fail(_endpoint):
        raise RuntimeError('provider unavailable')
    with pytest.raises(RuntimeError, match='provider unavailable'):
        card_removal_intents(**declaration(), resolve_endpoint=fail)


def test_pending_target_keeps_its_logical_identity_and_complete_source_set():
    plans = card_removal_intents(**declaration(), resolve_endpoint=lambda _: (None, None))
    assert len(plans) == 2 + len(CARD_CHILD_FAMILIES)
    assert plans[1].expected_edges[0].target_ref == 'spec:s:test_scenario:new'
    assert all(not item.active_edges and not item.active_nodes for item in plans)
    assert card_removal_intents(**declaration(), resolve_endpoint=lambda _: ('physical', 'TestScenario')) == ()


def test_incomplete_source_declaration_cannot_be_pruned():
    payload = declaration()
    payload['intents'] = payload['intents'][1:]
    with pytest.raises(ValueError, match='source_incomplete'):
        card_removal_intents(**payload, resolve_endpoint=lambda _: (None, None))


@pytest.mark.asyncio
@pytest.mark.parametrize('claim_current', [True, False])
async def test_source_is_read_only_after_graph_lease_and_sql_claim_fence(monkeypatch, claim_current):
    from okto_pulse.core.application.processors import consolidation
    events = []
    entry = SimpleNamespace(id='q', board_id='b', artifact_type='card', artifact_id='c',
        work_kind='consolidate', source='state_transition', claim_token='token')
    async def fence(*_args):
        events.append('SQL fence')
        return claim_current
    async def prepare(*_args):
        events.append('source read')
        return False
    def enter(_ref):
        events.append('graph lease')
        return object()
    monkeypatch.setattr(consolidation, '_queue_claim_is_current_and_unfenced', fence)
    monkeypatch.setattr(consolidation, '_prepare_deterministic_projection', prepare)
    if claim_current:
        with pytest.raises(consolidation.KGPrimitiveError, match='source could not be prepared'):
            await consolidation._process_queue_entry(object(), entry,
                enter_graph_write=enter, deferred_session_ids=[])
        assert events == ['graph lease', 'SQL fence', 'source read']
    else:
        with pytest.raises(consolidation._QueueClaimLostOrFenced):
            await consolidation._process_queue_entry(object(), entry,
                enter_graph_write=enter, deferred_session_ids=[])
        assert events == ['graph lease', 'SQL fence']
