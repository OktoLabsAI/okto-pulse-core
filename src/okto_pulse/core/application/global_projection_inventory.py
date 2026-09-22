"""Global projection rules over portable values; no runtime queries or writes."""

from collections import Counter, defaultdict
import hashlib

from okto_pulse.core.kg.interfaces.global_discovery_recovery import GlobalDiscoveryBoardSeed
from okto_pulse.core.kg.logical_transfer import (
    LOGICAL_NULL, LogicalSchemaIndex, LogicalVector, canonical_bytes, encode_value,
)
from okto_pulse.core.kg.schema_contract import VECTOR_INDEX_TYPES
from okto_pulse.core.ports.global_projection import GlobalProjectionComparison, GlobalProjectionSource


def _inventory(schema, nodes, relations, scope):
    if (schema.scope != scope or type(nodes) is not tuple or type(relations) is not tuple
            or len(nodes) > 100_000 or len(relations) > 500_000):
        raise ValueError('global_projection_inventory_invalid')
    index, identities, budget = LogicalSchemaIndex.build(schema), {}, 0
    for node in nodes:
        index.validate_node(node)
        key = node.type_name, node.key
        if key in identities:
            raise ValueError('global_projection_duplicate_node')
        identities[key] = node
    for relation in relations:
        index.validate_relation(relation)
        if (relation.source_type, relation.source_key) not in identities or (relation.target_type, relation.target_key) not in identities:
            raise ValueError('global_projection_endpoint_missing')
    for record in (*nodes, *relations):
        identity = ((record.type_name, record.key) if hasattr(record, 'type_name') else
            (record.layout_name, record.source_type, record.source_key, record.target_type, record.target_key))
        budget += len(canonical_bytes({'identity': identity,
            'properties': {key: encode_value(value) for key, value in record.properties.items()}}))
        if budget > 64 * 1024 * 1024:
            raise ValueError('global_projection_inventory_limit')
    return identities


def _value(node, field):
    value = node.properties.get(field, LOGICAL_NULL)
    return None if value is LOGICAL_NULL else value


def source_facts(*, schema, nodes, relations):
    identities = _inventory(schema, nodes, relations, 'board')
    selected, bugs, related = {}, Counter(), defaultdict(set)
    for (kind, key), node in identities.items():
        if (kind not in VECTOR_INDEX_TYPES or _value(node, 'embedding') is None
                or _value(node, 'revocation_reason') is not None or _value(node, 'superseded_by') is not None):
            continue
        if not key or key in selected:
            raise ValueError('global_projection_source_identity_invalid')
        selected[key] = node
    for relation in relations:
        source = selected.get(relation.source_key)
        if source is None or relation.source_type != 'Learning' or source.type_name != 'Learning':
            continue
        target = identities[(relation.target_type, relation.target_key)]
        layer = _value(target, 'graph_layer') or 'legacy_unknown'
        if relation.layout_name == 'validates' and relation.target_type == 'Bug' and layer == 'canonical':
            bugs[source.key] += 1  # Existing publication policy counts occurrences.
        if relation.layout_name == 'relates_to':
            related[source.key].add((relation.target_type, None if layer == 'legacy_unknown' else layer))
    facts = []
    for key, node in sorted(selected.items()):
        embedding = _value(node, 'embedding')
        if type(embedding) is not LogicalVector:
            raise ValueError('global_projection_vector_invalid')
        facts.append(GlobalProjectionSource(key, node.type_name, _value(node, 'title') or '', embedding.components,
            (_value(node, 'source_artifact_ref') or '') if node.type_name == 'Learning' else '',
            _value(node, 'graph_layer') or 'legacy_unknown', bugs[key],
            tuple(sorted(related[key], key=lambda endpoint: (endpoint[0], endpoint[1] or 'legacy_unknown')))))
    return tuple(facts)


def compare(*, schema, nodes, relations, seeds):
    actual = _inventory(schema, nodes, relations, 'global_discovery')
    required = {'Board': {'board_id', 'name', 'summary', 'summary_embedding', 'topic_count', 'entity_count', 'decision_count', 'last_sync_at'},
        'DecisionDigest': {'id', 'board_id', 'original_node_id', 'title', 'one_line_summary', 'node_type', 'graph_layer',
            'source_revoked', 'embedding', 'created_at'}}
    if any(set(schema.node_type(kind).property_names()) != fields for kind, fields in required.items()):
        raise ValueError('global_projection_schema_unsupported')
    if any(edge.properties for edge in relations if edge.layout_name == 'CONTAINS_DECISION'):
        raise ValueError('global_projection_relation_properties_unsupported')
    if type(seeds) is not tuple or len(seeds) > 256 or any(type(seed) is not GlobalDiscoveryBoardSeed for seed in seeds):
        raise ValueError('global_projection_seeds_invalid')
    expected, expected_relations, owners = {}, Counter(), set()
    budget = 0

    def add(kind, key, properties):
        nonlocal budget
        if (kind, key) in expected or len(expected) >= 100_000:
            raise ValueError('global_projection_expected_identity_invalid')
        encoded = {field: encode_value(value) for field, value in properties.items()}
        budget += len(canonical_bytes({'identity': (kind, key), 'properties': encoded}))
        if budget > 64 * 1024 * 1024:
            raise ValueError('global_projection_inventory_limit')
        expected[(kind, key)] = encoded

    def vector(kind, field, components):
        definition = schema.node_type(kind).property_def(field)
        space = schema.vector_space(definition.vector_space)
        value = LogicalVector(space.name, space.storage_dtype, components)
        if len(components) != space.dimension:
            raise ValueError('global_projection_vector_dimension_invalid')
        return value

    for seed in seeds:
        if seed.board_id in owners:
            raise ValueError('global_projection_expected_identity_invalid')
        owners.add(seed.board_id)
        add('Board', seed.board_id, {'board_id': seed.board_id, 'name': seed.board_name or seed.board_id,
            'summary': seed.summary, 'summary_embedding': vector('Board', 'summary_embedding', seed.summary_embedding),
            'decision_count': len(seed.digests), 'topic_count': 0, 'entity_count': 0})
        for digest in seed.digests:
            key = f'dd_{seed.board_id[:8]}_{digest.original_node_id}'
            add('DecisionDigest', key, {'id': key, 'board_id': seed.board_id, 'original_node_id': digest.original_node_id,
                'title': digest.title, 'one_line_summary': digest.summary, 'node_type': digest.node_type,
                'graph_layer': digest.graph_layer, 'source_revoked': False,
                'embedding': vector('DecisionDigest', 'embedding', digest.embedding)})
            expected_relations[('CONTAINS_DECISION', 'Board', seed.board_id, 'DecisionDigest', key)] += 1
    matched = 0
    for key in set(actual) & set(expected):
        if all(encode_value(actual[key].properties.get(field, LOGICAL_NULL)) == value
                for field, value in expected[key].items()):
            matched += 1
    observed_relations = Counter((edge.layout_name, edge.source_type, edge.source_key, edge.target_type, edge.target_key)
        for edge in relations)
    missing, changed, unexpected = len(set(expected) - set(actual)), len(set(expected) & set(actual)) - matched, len(set(actual) - set(expected))
    missing_edges = sum((expected_relations - observed_relations).values())
    unexpected_edges = sum((observed_relations - expected_relations).values())
    fingerprint = hashlib.sha256(canonical_bytes({'nodes': [(key, expected[key]) for key in sorted(expected)],
        'relations': [(key, count) for key, count in sorted(expected_relations.items())]})).hexdigest()
    return GlobalProjectionComparison('matched' if not any((missing, changed, unexpected, missing_edges, unexpected_edges)) else 'mismatch',
        len(expected), matched, missing, changed, unexpected, missing_edges, unexpected_edges, fingerprint)
