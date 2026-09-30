"""Known source removals while a Card projection awaits materialization (§4.5)."""
from dataclasses import dataclass
from datetime import datetime

from okto_pulse.core.kg.interfaces.graph_transaction import (
    ProjectionLogicalEdgeRef, ProjectionRemovalOnlyIntent,
)
from okto_pulse.core.ports.card_projection import (
    CARD_PARENT_RULE, CARD_SCENARIO_RULES,
)


@dataclass(frozen=True)
class DeferredProjectionProgress:
    """Internal queue progress, deliberately not a consolidation ACK/audit."""

    session_id: str
    error_code: str = 'relational_projection_endpoint_pending'


@dataclass(frozen=True)
class GraphRemovalProgress:
    records: tuple
    counters: object
    committed_at: datetime


def card_removal_intents(*, intents, nodes, edges, resolve_endpoint):
    """Validate the complete declaration before considering any removal.

    A provider failure propagates. Only a successful lookup proving absence
    yields removal-only work; absent graph state is never absent source state.
    """
    selected = [intent for intent in intents if intent.owner_type == 'card']
    if not selected:
        return ()
    if not nodes and all(not intent.active_refs and not intent.active_edges for intent in selected):
        # Existing cancelled/archived cleanup is already a complete empty set.
        return ()
    if {intent.namespace for intent in selected} != {'card_parent', 'card_scenarios'}:
        raise ValueError('card_removal_source_incomplete')
    plans, missing = [], False
    for intent in selected:
        if intent.active_refs:
            raise ValueError('card_removal_source_invalid')
        rules = {CARD_PARENT_RULE} if intent.namespace == 'card_parent' else CARD_SCENARIO_RULES
        declared = {edge.candidate_id for edge in intent.active_edges}
        emitted = {key for key, edge in edges.items() if edge.rule_id in rules}
        if len(declared) != len(intent.active_edges) or declared != emitted:
            raise ValueError('card_removal_edge_set_mismatch')
        expected = []
        for ref in intent.active_edges:
            edge = edges[ref.candidate_id]
            if (str(getattr(edge.edge_type, 'value', edge.edge_type)), edge.from_candidate_id,
                    edge.to_candidate_id, edge.rule_id) != (
                    ref.edge_type, ref.from_candidate_id, ref.to_candidate_id, ref.rule_id):
                raise ValueError('card_removal_edge_identity_mismatch')
            source = nodes.get(edge.from_candidate_id)
            target = edge.to_candidate_id.split(':', 2)
            if (source is None or source.source_artifact_ref != f'card:{intent.owner_id}'
                    or len(target) != 3 or target[0] != 'kgref'):
                raise ValueError('card_removal_endpoint_invalid')
            node_id, node_type = resolve_endpoint(edge.to_candidate_id)
            if node_id is None:
                missing = True
            elif node_type != target[1]:
                raise ValueError('card_removal_endpoint_type_mismatch')
            expected.append(ProjectionLogicalEdgeRef(ref.edge_type,
                str(getattr(source.node_type, 'value', source.node_type)), target[1],
                source.source_artifact_ref, target[2], ref.rule_id))
        plans.append(ProjectionRemovalOnlyIntent(owner_type='card', owner_id=intent.owner_id,
            namespace=intent.namespace, expected_edges=tuple(expected)))
    return tuple(plans) if missing else ()
