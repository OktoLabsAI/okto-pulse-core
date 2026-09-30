"""Derive observed scenario links from a complete, scoped relational source."""
from collections import Counter
import hashlib
import json

from okto_pulse.core.ports.card_projection import CARD_SCENARIO_NAMESPACE, card_scenario_rule
from okto_pulse.core.application.processors.deterministic_kg import (
    EmittedEdge, RelationalProjectionActiveEdgeRef, RelationalProjectionActiveSetIntent,
)


def _ids(value, field):
    if value is None:
        return set()
    if not isinstance(value, (list, tuple)) or any(type(item) is not str or not item.strip() or ':' in item for item in value):
        raise ValueError('card_scenario_source_invalid:' + field)
    return set(value)


async def prepare_card_scenario_projection(context, *, board_id, card, result, persistence):
    if getattr(card, 'board_id', None) != board_id or not hasattr(card, 'test_scenario_ids'):
        raise ValueError('card_scenario_source_scope_invalid')
    card_links = _ids(card.test_scenario_ids, 'test_scenario_ids')
    spec_id = getattr(card, 'spec_id', None)
    scenarios = []
    if spec_id:
        parent = await persistence.load_artifact(context, artifact_type='spec', artifact_id=spec_id)
        if (parent is None or getattr(parent, 'id', None) != spec_id
                or getattr(parent, 'board_id', None) != board_id or not hasattr(parent, 'test_scenarios')):
            raise ValueError('card_scenario_parent_unavailable')
        scenarios = parent.test_scenarios or []
        if not isinstance(scenarios, list) or any(not isinstance(item, dict) for item in scenarios):
            raise ValueError('card_scenario_source_invalid:test_scenarios')
    elif card_links:
        raise ValueError('card_scenario_parent_unavailable')
    counts = Counter(str(item.get('id') or '') for item in scenarios)
    selected = []
    for scenario in scenarios:
        identity = scenario.get('id')
        from_card = identity in card_links
        from_spec = card.id in _ids(scenario.get('linked_task_ids'), 'linked_task_ids')
        if not (from_card or from_spec):
            continue
        if type(identity) is not str or not identity.strip() or ':' in identity or counts[identity] != 1:
            raise ValueError('card_scenario_identity_ambiguous')
        selected.append((identity, card_scenario_rule(card_reference=from_card, spec_reference=from_spec)))
    if card_links - {identity for identity, _rule in selected}:
        raise ValueError('card_scenario_reference_unresolved')
    roots = [node for node in result.nodes if node.source_artifact_ref == f'card:{card.id}']
    if len(roots) != 1 or roots[0].node_type not in {'Entity', 'Bug'}:
        raise ValueError('card_scenario_owner_unresolved')
    edges = []
    for identity, rule in sorted(selected):
        target = f'kgref:TestScenario:spec:{spec_id}:test_scenario:{identity}'
        edge_id = 'card_scenario_' + hashlib.sha256(f'{card.id}\0{target}'.encode()).hexdigest()
        result.edges.append(EmittedEdge(edge_id, 'supports', roots[0].candidate_id, target, 1.0, rule))
        edges.append(RelationalProjectionActiveEdgeRef(edge_id, 'supports', roots[0].candidate_id, target, rule))
    result.relational_projection_active_set_intents += (RelationalProjectionActiveSetIntent(
        'card', card.id, CARD_SCENARIO_NAMESPACE, (), tuple(edges)),)
    # This is the internal projection idempotency hash, not a change to the
    # Card's domain content/version or to a coverage/evaluation digest.
    result.content_hash = hashlib.sha256(json.dumps(
        ['card-scenarios/v1', result.content_hash, spec_id, sorted(selected)],
        separators=(',', ':'), ensure_ascii=True).encode()).hexdigest()
    return result
