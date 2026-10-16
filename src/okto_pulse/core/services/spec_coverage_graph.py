"""Compare a bounded observation with the existing deterministic Spec projector.

The comparison is diagnostic, not a new coverage resolver or projection writer.
No checkpoint is manufactured from a matching set of nodes or relations.
"""
from okto_pulse.core.application.processors.deterministic_kg import DeterministicWorker
from okto_pulse.core.ports.spec_coverage_query import (
    MAX_SPEC_COVERAGE_ITEMS, MAX_SPEC_COVERAGE_FACTS, SpecCoverageGraphScope,
)


def build_spec_coverage_graph_scope(snapshot):
    projected = DeterministicWorker().process_spec(vars(snapshot.spec))
    root = f'spec:{snapshot.scope.spec_id}'
    nodes = {node.candidate_id: (node.node_type, node.source_artifact_ref)
        for node in projected.nodes
        if node.source_artifact_ref == root or node.source_artifact_ref.startswith(root + ':')}
    identities = tuple(sorted(nodes.values()))
    if len(identities) != len(set(identities)):
        raise ValueError('spec_coverage_ambiguous_source_identity')
    relations = tuple(sorted({(*nodes[edge.from_candidate_id], edge.edge_type,
        *nodes[edge.to_candidate_id], edge.rule_id, edge.layer, edge.created_by)
        for edge in projected.edges if edge.from_candidate_id in nodes and edge.to_candidate_id in nodes}))
    # Cards are authorized by the relational inventory. Their observed supports
    # links remain observations; this scope does not reconstruct Card writers.
    identities += tuple(sorted(('Bug' if str(getattr(card.card_type, 'value', card.card_type)) == 'bug' else 'Entity',
        f'card:{card.id}') for card in snapshot.cards if not card.archived))
    if len(identities) > 2 * MAX_SPEC_COVERAGE_ITEMS + 1 or len(relations) > MAX_SPEC_COVERAGE_FACTS:
        raise ValueError('spec_coverage_graph_scope_bound')
    return SpecCoverageGraphScope(tuple(sorted(identities)), relations)


def project_spec_coverage_graph(query, snapshot):
    graph, scope = snapshot.graph, snapshot.graph_scope
    if graph is None:
        return {'state': 'restricted' if not query.read_graph else 'unavailable',
            'complete_for_scope': False, 'expected_nodes': None, 'observed_nodes': None,
            'missing_nodes': None, 'missing_relations': None}, [], 'unknown', None
    if not query.read_graph:
        raise ValueError('spec_coverage_graph_outside_authority')
    if scope is None or scope != build_spec_coverage_graph_scope(snapshot):
        raise ValueError('spec_coverage_graph_scope_mismatch')
    if graph.state not in {'observed', 'restricted', 'unavailable'} or type(graph.truncated) is not bool:
        raise ValueError('spec_coverage_graph_state_invalid')
    if graph.state != 'observed':
        if graph.nodes or graph.relations:
            raise ValueError('spec_coverage_graph_state_inconsistent')
        return {'state': graph.state, 'complete_for_scope': False, 'expected_nodes': None,
            'observed_nodes': None, 'missing_nodes': None, 'missing_relations': None}, [], 'unavailable', graph.generation
    if len(graph.nodes) > 2 * MAX_SPEC_COVERAGE_ITEMS + 1 or len(graph.relations) > MAX_SPEC_COVERAGE_FACTS:
        raise ValueError('spec_coverage_graph_observation_bound')
    expected, observed = set(scope.nodes), set(graph.nodes)
    if len(observed) != len(graph.nodes) or not observed <= expected:
        raise ValueError('spec_coverage_graph_identity_invalid')
    actual_edges = set(graph.relations)
    if any(len(edge) != 8 or tuple(edge[:2]) not in observed or tuple(edge[3:5]) not in observed
           for edge in actual_edges):
        raise ValueError('spec_coverage_graph_endpoint_outside_scope')
    expected_edges = set(scope.relations)
    missing = expected - observed
    missing_edges = expected_edges - actual_edges
    rows = [{'kind': 'structure_node', 'node_type': kind, 'subject_ref': ref,
        'observation': 'observed' if (kind, ref) in observed else 'not_found_in_projection'}
        for kind, ref in sorted(expected)]
    rows += [{'kind': 'structure_relation', 'source_type': edge[0], 'source_ref': edge[1],
        'relation': edge[2], 'target_type': edge[3], 'target_ref': edge[4],
        'rule_id': edge[5], 'layer': edge[6], 'created_by': edge[7],
        'observation': ('observed_expected' if edge in expected_edges else 'graph_only')
            if edge in actual_edges else 'not_found_in_projection',
        'authority': 'structural_observation_not_delivery_proof'}
        for edge in sorted(expected_edges | actual_edges)]
    state = 'incomplete' if missing or missing_edges or graph.truncated else 'unknown'
    return {'state': 'observed', 'complete_for_scope': False,
        'expected_nodes': len(expected), 'observed_nodes': len(observed),
        'missing_nodes': len(missing), 'missing_relations': len(missing_edges),
        'truncated': graph.truncated, 'comparison_scope': 'spec_projector_and_authorized_card_identities',
        'interpretation': 'matching_observations_are_not_a_full_projection_checkpoint'}, rows, state, graph.generation
