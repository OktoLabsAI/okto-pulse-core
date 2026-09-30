"""Derive observed scenario links from a complete, scoped relational source."""
import hashlib
import json

from okto_pulse.core.ports.card_projection import CARD_SCENARIO_NAMESPACE, CARD_PARENT_NAMESPACE, CARD_PARENT_RULE
from okto_pulse.core.domain.card_scenario_references import analyze_card_scenario_references
from okto_pulse.core.application.processors.deterministic_kg import (
    EmittedEdge, RelationalProjectionActiveEdgeRef, RelationalProjectionActiveSetIntent,
)


async def prepare_card_scenario_projection(context, *, board_id, card, result, persistence):
    if getattr(card, 'board_id', None) != board_id or not hasattr(card, 'test_scenario_ids'):
        raise ValueError('card_scenario_source_scope_invalid')
    spec_id = getattr(card, 'spec_id', None)
    scenarios, parent = None, None
    if spec_id:
        parent = await persistence.load_artifact(context, artifact_type='spec', artifact_id=spec_id)
        if parent is not None and (getattr(parent, 'id', None) != spec_id
                or getattr(parent, 'board_id', None) != board_id or not hasattr(parent, 'test_scenarios')):
            raise ValueError('card_scenario_parent_unavailable')
        if parent is not None:
            scenarios = parent.test_scenarios if parent.test_scenarios is not None else []
    analysis = analyze_card_scenario_references(board_id=board_id, card_id=card.id,
        spec_id=spec_id, card_links=card.test_scenario_ids, parent_exists=parent is not None,
        scenarios=scenarios)
    # Duplicate identities remain an invalid source, never permission to choose
    # one target or retract a valid relation by guessing which record it meant.
    if any(item.reason_code == 'target_ambiguous' for item in analysis.snapshot.findings):
        raise ValueError('card_scenario_identity_ambiguous')
    selected = analysis.links
    roots = [node for node in result.nodes if node.source_artifact_ref == f'card:{card.id}']
    if len(roots) != 1 or roots[0].node_type not in {'Entity', 'Bug'}:
        raise ValueError('card_scenario_owner_unresolved')
    # This existing deterministic containment belongs to the current relation,
    # not to the worker session that happened to create it in the past.
    if parent is None:
        result.edges[:] = [edge for edge in result.edges if edge.rule_id != CARD_PARENT_RULE]
    else:
        for edge in result.edges:
            if edge.rule_id == CARD_PARENT_RULE:
                edge.to_candidate_id = f'kgref:Entity:spec:{spec_id}'
    parent_edges = tuple(RelationalProjectionActiveEdgeRef(edge.candidate_id, edge.edge_type,
        edge.from_candidate_id, edge.to_candidate_id, edge.rule_id)
        for edge in result.edges if edge.rule_id == CARD_PARENT_RULE)
    result.relational_projection_active_set_intents += (RelationalProjectionActiveSetIntent(
        'card', card.id, CARD_PARENT_NAMESPACE, (), parent_edges),)
    edges = []
    for identity, rule in sorted(selected):
        target = f'kgref:TestScenario:spec:{spec_id}:test_scenario:{identity}'
        edge_id = 'card_scenario_' + hashlib.sha256(f'{card.id}\0{target}'.encode()).hexdigest()
        result.edges.append(EmittedEdge(edge_id, 'supports', roots[0].candidate_id, target, 1.0, rule))
        edges.append(RelationalProjectionActiveEdgeRef(edge_id, 'supports', roots[0].candidate_id, target, rule))
    result.relational_projection_active_set_intents += (RelationalProjectionActiveSetIntent(
        'card', card.id, CARD_SCENARIO_NAMESPACE, (), tuple(edges)),)
    result.reference_findings = analysis.snapshot
    # This is the internal projection idempotency hash, not a change to the
    # Card's domain content/version or to a coverage/evaluation digest.
    result.content_hash = hashlib.sha256(json.dumps(
        ['card-scenarios/v2', result.content_hash, analysis.snapshot.source_fingerprint, sorted(selected)],
        separators=(',', ':'), ensure_ascii=True).encode()).hexdigest()
    return result
