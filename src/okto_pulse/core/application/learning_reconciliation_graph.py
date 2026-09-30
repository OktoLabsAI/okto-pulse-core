"""Recognize governed Learning graph effects without authorizing new semantics."""
from collections import Counter
from dataclasses import asdict, replace

from okto_pulse.core.application.learning_reconciliation import source_basis
from okto_pulse.core.application.learning_supersedence import read_learning_scope_replacements
from okto_pulse.core.kg.logical_transfer import LogicalSchemaIndex, LogicalTimestamp, canonical_bytes, encode_value
from okto_pulse.core.kg.schemas import EdgeCandidate
from okto_pulse.core.ports.cognitive_projection import compare_cognitive_projection, cognitive_projection_source_node


def _inventory(schema, nodes, relations):
    if (type(nodes) is not tuple or type(relations) is not tuple
            or len(nodes) > 100_000 or len(relations) > 500_000):
        raise ValueError('learning_reconciliation_graph_limit')
    index, identities, edges, total = LogicalSchemaIndex.build(schema), {}, {}, 0
    counts = Counter()
    for node in nodes:
        index.validate_node(node)
        key = node.type_name, node.key
        if key in identities:
            raise ValueError('learning_reconciliation_graph_identity_duplicate')
        identities[key] = node
        total += len(canonical_bytes({'identity': key,
            'properties': {name: encode_value(value) for name, value in node.properties.items()}}))
        if total > 64 * 1024 * 1024:
            raise ValueError('learning_reconciliation_graph_limit')
    for edge in relations:
        index.validate_relation(edge)
        if (edge.source_type, edge.source_key) not in identities or (edge.target_type, edge.target_key) not in identities:
            raise ValueError('learning_reconciliation_graph_orphan')
        encoded = canonical_bytes({'identity': (edge.layout_name, edge.source_type, edge.source_key,
            edge.target_type, edge.target_key),
            'properties': {name: encode_value(value) for name, value in edge.properties.items()}})
        counts[encoded] += 1
        edges[encoded] = edge
        total += len(encoded)
        if total > 64 * 1024 * 1024:
            raise ValueError('learning_reconciliation_graph_limit')
    return identities, counts, edges


async def qualify(context, store, *, schema, execution, before_nodes, before_relations, after_nodes, after_relations):
    basis = await source_basis(context, store, execution=execution)
    before, old_counts, old_edges = _inventory(schema, before_nodes, before_relations)
    after, new_counts, new_edges = _inventory(schema, after_nodes, after_relations)
    learning_key = 'Learning', basis.learning_id
    added = set(after) - set(before)
    if set(before) - set(after) or added - {learning_key} or learning_key not in after:
        raise ValueError('learning_reconciliation_graph_nodes_unowned')
    bugs = [node for key, node in before.items() if key[0] == 'Bug'
        and node.properties.get('source_artifact_ref') == 'bug:' + basis.bug_id]
    if len(bugs) != 1:
        raise ValueError('learning_reconciliation_graph_bug_ambiguous')
    bug_key = 'Bug', bugs[0].key
    capture = await store.read_fingerprint_in_context(context, board_id=execution.board_id,
        node_id=basis.learning_id, generation=basis.generation, fingerprint=basis.capture_fingerprint)
    history = await store.read_history_in_context(context, board_id=execution.board_id,
        node_id=basis.learning_id, generation=basis.generation)
    head = asdict(history[-1])
    expected = cognitive_projection_source_node(schema=schema, board_id=execution.board_id, record=head)
    actual = after[learning_key]
    # Literal replay retains original semantic provenance. The existing writer
    # can stamp a newly recreated node with this invocation's graph session.
    # Reuse SQL may omit that property and fall back to its new append session;
    # the existing graph node instead keeps its exact before-image birth session.
    session = actual.properties.get('source_session_id')
    wanted_session = expected.properties.get('source_session_id')
    if session != wanted_session:
        owned_session = (execution.consolidation_session_id if learning_key in added
            else before[learning_key].properties.get('source_session_id'))
        if session != owned_session:
            raise ValueError('learning_reconciliation_graph_session_unowned')
        actual = replace(actual, properties={**actual.properties, 'source_session_id': wanted_session})
    parity = compare_cognitive_projection(schema=schema, board_id=execution.board_id, record=head, node=actual)
    if parity.state != 'matched':
        raise ValueError('learning_reconciliation_graph_literal_changed')
    participants = {learning_key, bug_key}
    if basis.scoped_target_id is not None:
        participants.add(('Learning', basis.scoped_target_id))
    changed = []
    for key, original in before.items():
        current = after[key]
        fields = tuple(sorted(name for name in set(original.properties) | set(current.properties)
            if original.properties.get(name) != current.properties.get(name)))
        if not fields:
            continue
        # Existing governed reuse advances only source_content_hash. It never
        # repairs or rewrites old semantic fields, even to match a source row.
        if key != learning_key and (key not in participants or set(fields) - {'relevance_score', 'last_recomputed_at'}):
            raise ValueError('learning_reconciliation_graph_properties_unowned')
        if key == learning_key:
            if (set(fields) - {'relevance_score', 'last_recomputed_at', 'source_content_hash'}
                    or ('source_content_hash' in fields and (capture.payload['intent']['kind'] != 'reuse'
                        or current.properties.get('source_content_hash') != basis.capture_fingerprint))):
                raise ValueError('learning_reconciliation_graph_properties_unowned')
        changed.append({'node_type': key[0], 'node_id': key[1], 'fields': list(fields)})

    def association(edge, learning_id):
        return (edge.layout_name, edge.source_type, edge.source_key, edge.target_type, edge.target_key) == (
            'validates', 'Learning', learning_id, 'Bug', bug_key[1])

    introduced = [new_edges[key] for key in (new_counts - old_counts).elements()]
    removed = [old_edges[key] for key in (old_counts - new_counts).elements()]
    # The authored writer submits the unmodified EdgeCandidate defaults;
    # TransactionOrchestrator.create_edge supplies this provenance envelope.
    edge_defaults = {'confidence': EdgeCandidate.model_fields['confidence'].get_default(),
        'created_by_session_id': execution.consolidation_session_id,
        'layer': 'cognitive', 'rule_id': '', 'created_by': execution.consolidation_session_id, 'fallback_reason': ''}
    if (len(introduced) > 1 or any(not association(edge, basis.learning_id)
            or set(edge.properties) != set(edge_defaults) | {'created_at'}
            or any(edge.properties.get(name) != value for name, value in edge_defaults.items())
            or type(edge.properties.get('created_at')) is not LogicalTimestamp for edge in introduced)
            or not any(association(edge, basis.learning_id) for edge in after_relations)):
        raise ValueError('learning_reconciliation_graph_edges_unowned')
    if removed:
        if basis.scoped_target_id is None or any(not association(edge, basis.scoped_target_id) for edge in removed):
            raise ValueError('learning_reconciliation_graph_edges_unowned')
        target_history = await store.read_history_in_context(context, board_id=execution.board_id,
            node_id=basis.scoped_target_id, generation=capture.payload['intent']['target_generation'])
        if not target_history:
            raise ValueError('learning_reconciliation_graph_scope_unproved')
        claims = await read_learning_scope_replacements(context, store, head=target_history[-1])
        if not any(claim.capture.record_fingerprint == basis.capture_fingerprint
                and claim.capture.node_id == basis.learning_id for claim in claims):
            raise ValueError('learning_reconciliation_graph_scope_unproved')
    return {'format': 'learning-reconciliation-graph-delta/v1',
        'introduced_nodes': len(added), 'introduced_edges': len(introduced), 'removed_edges': len(removed),
        'changed_nodes': sorted(changed, key=lambda row: (row['node_type'], row['node_id']))}
