"""Known source removals while a Card projection awaits materialization (§4.5)."""
from dataclasses import dataclass
from datetime import datetime

from okto_pulse.core.kg.interfaces.graph_transaction import (
    ProjectionLogicalEdgeRef, ProjectionRemovalOnlyIntent,
)
from okto_pulse.core.ports.card_projection import (
    CARD_PARENT_RULE, CARD_SCENARIO_RULES, is_spec_source_reference,
    CARD_CHILD_NAMESPACES, card_child_family,
    CARD_DEPENDENCY_NAMESPACE, is_card_dependency_writer, owns_card_dependency_endpoints,
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
    selected = [intent for intent in intents if intent.owner_type == 'card' and intent.namespace != CARD_DEPENDENCY_NAMESPACE]
    if not selected:
        return ()
    if not nodes and all(not intent.active_refs and not intent.active_edges for intent in selected):
        # Existing cancelled/archived cleanup is already a complete empty set.
        return ()
    if {intent.namespace for intent in selected} != ({'card_parent', 'card_scenarios'} | CARD_CHILD_NAMESPACES):
        raise ValueError('card_removal_source_incomplete')
    plans, missing = [], False
    for intent in selected:
        if intent.active_refs:
            raise ValueError('card_removal_source_invalid')
        rules = ({card_child_family(intent.namespace).rule} if intent.namespace in CARD_CHILD_NAMESPACES
                 else {CARD_PARENT_RULE} if intent.namespace == 'card_parent' else CARD_SCENARIO_RULES)
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


def card_dependency_removal_intents(*, intents, nodes, edges, resolve_endpoint):
    selected = [intent for intent in intents if intent.owner_type == 'card' and intent.namespace == CARD_DEPENDENCY_NAMESPACE]
    if not selected:
        return ()
    if len(selected) != 1 or selected[0].active_refs:
        raise ValueError('card_dependency_source_invalid')
    intent = selected[0]
    declared = {edge.candidate_id for edge in intent.active_edges}
    emitted = {key for key, edge in edges.items() if is_card_dependency_writer(
        rule_id=edge.rule_id, layer='deterministic', created_by='worker_layer1')}
    if len(declared) != len(intent.active_edges) or declared != emitted:
        raise ValueError('card_dependency_edge_set_mismatch')
    expected, missing = [], False
    for ref in intent.active_edges:
        edge = edges[ref.candidate_id]
        if (str(getattr(edge.edge_type, 'value', edge.edge_type)), edge.from_candidate_id,
                edge.to_candidate_id, edge.rule_id) != (
                ref.edge_type, ref.from_candidate_id, ref.to_candidate_id, ref.rule_id):
            raise ValueError('card_dependency_edge_identity_mismatch')
        source = edge.from_candidate_id.split(':', 2)
        target = nodes.get(edge.to_candidate_id)
        target_type = str(getattr(target.node_type, 'value', target.node_type)) if target else None
        if (target is None or len(source) != 3 or source[0] != 'kgref' or ref.edge_type != 'precedes'
                or not owns_card_dependency_endpoints(owner_id=intent.owner_id, source_type=source[1],
                    target_type=target_type, source_ref=source[2], target_ref=target.source_artifact_ref)):
            raise ValueError('card_dependency_endpoint_invalid')
        node_id, node_type = resolve_endpoint(edge.from_candidate_id)
        if node_id is None:
            missing = True
        elif node_type != source[1]:
            raise ValueError('card_dependency_endpoint_type_mismatch')
        expected.append(ProjectionLogicalEdgeRef('precedes', source[1], target_type,
            source[2], target.source_artifact_ref, ref.rule_id))
    return (ProjectionRemovalOnlyIntent('card', intent.owner_id, CARD_DEPENDENCY_NAMESPACE,
        expected_edges=tuple(expected)),) if missing else ()


def dependency_removal_intents(*, intents, nodes, edges, resolve_endpoint):
    """Keep expected prerequisite identities while retracting removed ones."""
    selected = [intent for intent in intents if intent.owner_type == 'spec' and intent.namespace == 'dependencies']
    if not selected:
        return ()
    if len(selected) != 1 or selected[0].active_refs:
        raise ValueError('dependency_removal_source_invalid')
    intent = selected[0]
    declared = {edge.candidate_id for edge in intent.active_edges}
    emitted = {key for key, edge in edges.items() if str(edge.rule_id or '').startswith('precedes/spec_dependency/')}
    if len(declared) != len(intent.active_edges) or declared != emitted:
        raise ValueError('dependency_removal_edge_set_mismatch')
    expected, endpoints, missing = [], set(), False
    for ref in intent.active_edges:
        edge = edges[ref.candidate_id]
        if (str(getattr(edge.edge_type, 'value', edge.edge_type)), edge.from_candidate_id,
                edge.to_candidate_id, edge.rule_id) != (
                ref.edge_type, ref.from_candidate_id, ref.to_candidate_id, ref.rule_id):
            raise ValueError('dependency_removal_edge_identity_mismatch')
        source = edge.from_candidate_id.split(':', 2)
        target = nodes.get(edge.to_candidate_id)
        if (ref.edge_type != 'precedes' or len(source) != 3 or source[:2] != ['kgref', 'Entity']
                or not is_spec_source_reference(source[2]) or source[2] in endpoints
                or target is None or str(getattr(target.node_type, 'value', target.node_type)) != 'Entity'
                or target.source_artifact_ref != f'spec:{intent.owner_id}'):
            raise ValueError('dependency_removal_endpoint_invalid')
        endpoints.add(source[2])
        node_id, node_type = resolve_endpoint(edge.from_candidate_id)
        if node_id is None:
            missing = True
        elif node_type != 'Entity':
            raise ValueError('dependency_removal_endpoint_type_mismatch')
        expected.append(ProjectionLogicalEdgeRef('precedes', 'Entity', 'Entity',
            source[2], target.source_artifact_ref, ref.rule_id))
    if not missing:
        return ()
    owner_id, owner_type = resolve_endpoint(f'kgref:Entity:spec:{intent.owner_id}')
    if owner_id is None:
        # Nothing was materialized for this owner. The existing read-only
        # prerequisite barrier handles this wait without a removal commit.
        return ()
    if owner_type != 'Entity':
        raise ValueError('dependency_removal_owner_invalid')
    return (ProjectionRemovalOnlyIntent('spec', intent.owner_id, 'dependencies',
        owner_node_id=owner_id, expected_edges=tuple(expected)),)
