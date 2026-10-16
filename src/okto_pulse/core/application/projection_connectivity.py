"""Bounded inventory adaptation to the existing connectivity guard."""

from collections import defaultdict
import json

from okto_pulse.core.kg.connectivity_guard import KGNodeConnectivityGuard, KGNodeRef
from okto_pulse.core.kg.logical_transfer import LOGICAL_NULL, LogicalSchemaIndex, canonical_bytes, encode_value
from okto_pulse.core.kg.orphan_integrity import _safe_writer_path
from okto_pulse.core.kg.schema_contract import NODE_TYPES
from okto_pulse.core.ports.projection_connectivity import ProjectionConnectivityObservation


def observe(*, schema, board_id, nodes, relations, selected):
    if (schema.scope != 'board' or type(board_id) is not str or not board_id
            or any(type(items) is not tuple for items in (nodes, relations, selected))
            or len(nodes) > 100_000 or len(relations) > 500_000 or len(selected) > 100_000
            or any(type(key) is not tuple or len(key) != 2 or any(type(v) is not str for v in key) for key in selected)
            or len(set(selected)) != len(selected)):
        raise ValueError('projection_connectivity_inventory_invalid')
    index, budget, wanted = LogicalSchemaIndex.build(schema), 0, set(selected)
    identities, snapshots, refs, groups = {}, {}, [], defaultdict(list)

    def charge(record, identity):
        nonlocal budget
        budget += len(canonical_bytes({'identity': identity,
            'properties': {name: encode_value(value) for name, value in record.properties.items()}}))
        if budget > 64 * 1024 * 1024:
            raise ValueError('projection_connectivity_inventory_limit')

    def value(node, name):
        item = node.properties.get(name)
        return None if item is LOGICAL_NULL else item

    for node in nodes:
        index.validate_node(node)
        key = node.type_name, node.key
        charge(node, key)
        if key in identities:
            raise ValueError('projection_connectivity_duplicate_node')
        # Types are part of identity; equal keys in different tables never alias.
        identity = json.dumps(key, ensure_ascii=True, separators=(',', ':'))
        identities[key] = identity
        if node.type_name not in NODE_TYPES:
            continue
        source, layer, maturity = (value(node, name) for name in ('source_artifact_ref', 'graph_layer', 'maturity_status'))
        snapshots[key] = {'candidate_id': identity, 'node_type': node.type_name,
            'source_artifact_ref': source, 'graph_layer': layer, 'maturity_status': maturity}
        refs.append(KGNodeRef(identity, node.type_name, source, layer, maturity))
        if node.type_name == 'Bug':
            # The existing type-aware probe also recognizes a Bug's raw UUID.
            # This read-only alias is never a graph identity or a written edge.
            refs.append(KGNodeRef(node.key, node.type_name, source, layer, maturity))
        if key in wanted:
            groups[_safe_writer_path(value(node, 'created_by_agent'), value(node, 'source_session_id'))].append(key)
    if wanted - set(snapshots):
        raise ValueError('projection_connectivity_selected_node_missing')
    edges = []
    for ordinal, relation in enumerate(relations):
        index.validate_relation(relation)
        charge(relation, (relation.layout_name, relation.source_type, relation.source_key,
            relation.target_type, relation.target_key))
        source, target = (relation.source_type, relation.source_key), (relation.target_type, relation.target_key)
        if source not in identities or target not in identities:
            raise ValueError('projection_connectivity_endpoint_missing')
        edges.append({'candidate_id': str(ordinal), 'edge_type': relation.layout_name,
            'from_candidate_id': identities[source], 'to_candidate_id': identities[target]})
    results, guard = [], KGNodeConnectivityGuard()
    for writer, keys in sorted(groups.items()):
        observed = guard.validate(board_id=board_id, writer_path=writer,
            kg_health_state='projected_not_reconciled', nodes=tuple(snapshots[key] for key in keys),
            edges=tuple(edges), existing_node_refs=tuple(refs))
        violations, advisories = defaultdict(set), defaultdict(set)
        for violation in observed.violations:
            violations[violation.candidate_id].add(violation.reason)
        for advisory in observed.advisories:
            advisories[advisory['candidate_id']].add(advisory['reason'])
        allowlisted = set(observed.allowlisted_roots)
        for kind, key in keys:
            identity = identities[(kind, key)]
            reasons = tuple(sorted(violations[identity]))
            outcome = 'rejected' if reasons else 'allowlisted' if identity in allowlisted else 'passed'
            results.append(ProjectionConnectivityObservation(kind, key, writer, outcome, reasons,
                tuple(sorted(advisories[identity]))))
    return tuple(sorted(results, key=lambda row: (row.node_type, row.node_id)))
