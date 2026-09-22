"""Portable, bounded relation comparison against the actual worker proposals."""

from bisect import bisect_left, insort
from collections import Counter, defaultdict
import hashlib
import json

from okto_pulse.core.application.deterministic_projection import require_terminal_cleanup
from okto_pulse.core.kg.logical_transfer import LOGICAL_NULL, LogicalSchemaIndex, canonical_bytes, encode_value
from okto_pulse.core.kg.primitives import _cross_session_entity_source_prefix, _parse_source_ref_endpoint
from okto_pulse.core.kg.schema_contract import NODE_TYPES
from okto_pulse.core.kg.schemas import EdgeCandidate, NodeCandidate
from okto_pulse.core.ports.projection_history import ProjectionSourceIdentity, ProjectionSourceRoot, select_projection_source_roots
from okto_pulse.core.ports.projection_relations import ProjectionRelationComparison


def compare(*, document, schema, nodes, relations, new_sessions):
    require_terminal_cleanup(document)
    if (schema.scope != 'board' or any(type(value) is not tuple for value in (nodes, relations, new_sessions))
            or len(nodes) > 100_000 or len(relations) > 500_000 or len(new_sessions) > 100_000
            or any(type(item) is not str or not item or len(item) > 256 for item in new_sessions)
            or len(set(new_sessions)) != len(new_sessions)):
        raise ValueError('projection_relations_inventory_invalid')
    index, budget = LogicalSchemaIndex.build(schema), len(document) + len(canonical_bytes(new_sessions))
    if budget > 64 * 1024 * 1024:
        raise ValueError('projection_relations_inventory_limit')
    by_key, by_id, by_source, identities = {}, defaultdict(list), defaultdict(list), []
    sessions = set(new_sessions)

    def value(record, name):
        item = record.properties.get(name)
        return None if item is LOGICAL_NULL else item

    def charge(record, identity):
        nonlocal budget
        budget += len(canonical_bytes({'identity': identity,
            'properties': {name: encode_value(item) for name, item in record.properties.items()}}))
        if budget > 64 * 1024 * 1024:
            raise ValueError('projection_relations_inventory_limit')

    for node in nodes:
        index.validate_node(node)
        key = node.type_name, node.key
        charge(node, key)
        if key in by_key:
            raise ValueError('projection_relations_duplicate_node')
        by_key[key] = node
        if node.type_name not in NODE_TYPES:
            continue
        by_id[node.key].append(key)
        source = value(node, 'source_artifact_ref')
        if type(source) is str and source:
            # Keep unclassified historical references literal. Current-source
            # DTO restrictions apply only when that identity is actually used.
            by_source[(node.type_name, source)].append(key)

    plans, roots, candidate_count, edge_count = [], set(), 0, 0
    raw = json.loads(document)
    for plan in raw['plans']:
        projection = plan['projection']
        if projection is None:
            continue
        candidate_count += len(projection['nodes'])
        edge_count += len(projection['edges'])
        if candidate_count > 100_000 or edge_count > 500_000:
            raise ValueError('projection_relations_inventory_limit')
        local = {}
        for payload in projection['nodes']:
            node = NodeCandidate.model_validate(payload)
            root = ProjectionSourceRoot(node.node_type, node.source_artifact_ref)
            if node.candidate_id in local:
                raise ValueError('projection_relations_duplicate_candidate')
            local[node.candidate_id] = root
            roots.add(root)
        plans.append((local, tuple(EdgeCandidate.model_validate(edge) for edge in projection['edges'])))
    # Select current local candidates using precisely the existing reuse rule.
    for root in roots:
        for key in by_source.get((root.node_type, root.source_artifact_ref), ()):
            node = by_key[key]
            identities.append(ProjectionSourceIdentity(node.type_name, node.key, root.source_artifact_ref,
                value(node, 'generation'), value(node, 'superseded_by')))
    available = tuple(sorted(root for root in roots if any(value(by_key[key], 'superseded_by') is None
        for key in by_source.get((root.node_type, root.source_artifact_ref), ()))))
    selected = {root: (node.node_type, node.node_id) for root, node in zip(available,
        select_projection_source_roots(roots=available, nodes=tuple(identities)), strict=True)}
    entity_sources = sorted((source, key) for (kind, source), keys in by_source.items()
        if kind == 'Entity' for key in keys)
    resolved_external = {}

    def unique(keys):
        return keys[0] if len(keys) == 1 else None

    def resolve(endpoint, local):
        if endpoint.startswith('kg:'):
            return unique(by_id.get(endpoint[3:], ()))
        if endpoint in local:
            return selected.get(local[endpoint])
        if endpoint in resolved_external:
            return resolved_external[endpoint]
        source = _parse_source_ref_endpoint(endpoint)
        if source is not None:
            resolved_external[endpoint] = unique([key for key in by_source.get(source, ())
                if value(by_key[key], 'superseded_by') is None])
            return resolved_external[endpoint]
        # The old cross-session prefix lookup has no deterministic tie-breaker.
        # The observer cannot certify an arbitrary winner of that lookup.
        prefix = _cross_session_entity_source_prefix(endpoint)
        if prefix is not None:
            start = bisect_left(entity_sources, (prefix, ()))
            resolved_external[endpoint] = unique([key for ref, key in entity_sources[start:start + 2]
                if ref.startswith(prefix)])
            return resolved_external[endpoint]
        return None

    def attributes(layer, rule, actor, confidence, fallback):
        return canonical_bytes({'layer': layer, 'rule_id': rule, 'created_by': actor,
            'confidence': encode_value(LOGICAL_NULL if confidence is None else confidence), 'fallback_reason': fallback})

    expected, unresolved, issues = set(), 0, []
    issue_count, clipped = 0, False

    def add_issue(message):
        nonlocal issue_count, clipped
        issue_count += 1
        clipped = clipped or len(message) > 4096
        insort(issues, message[:4096])
        if len(issues) > 100:
            issues.pop()

    for local, edges in plans:
        for edge in edges:
            source, target = resolve(edge.from_candidate_id, local), resolve(edge.to_candidate_id, local)
            if source is None or target is None:
                unresolved += 1
                add_issue('endpoint_unresolved:' + edge.candidate_id)
                continue
            if not edge.layer or not edge.rule_id or not edge.created_by:
                raise ValueError('projection_relations_provenance_missing')
            signature = attributes(edge.layer, edge.rule_id, edge.created_by, edge.confidence, edge.fallback_reason or '')
            expected.add((edge.edge_type, *source, *target, signature.hex()))

    observed, new, existing = Counter(), [], []
    for relation in relations:
        index.validate_relation(relation)
        identity = (relation.layout_name, relation.source_type, relation.source_key,
            relation.target_type, relation.target_key)
        charge(relation, identity)
        if (relation.source_type, relation.source_key) not in by_key or (relation.target_type, relation.target_key) not in by_key:
            raise ValueError('projection_relations_endpoint_missing')
        signature = attributes(*(value(relation, name) for name in
            ('layer', 'rule_id', 'created_by', 'confidence', 'fallback_reason')))
        key = (*identity, signature.hex())
        observed[key] += 1
        if value(relation, 'created_by_session_id') in sessions:
            new.append(key)
        else:
            existing.append(key)
    missing = expected - observed.keys()
    unexpected = [key for key in new if key not in expected]
    unplanned_existing = sum(key not in expected for key in existing)
    # All occurrences of an ambiguous duplicate group stay unclassified: there
    # is no source-owned basis for picking one historical occurrence as current.
    duplicate_expected = sum(observed[key] for key in expected if observed[key] > 1)
    for key in missing:
        add_issue('relation_missing:' + ':'.join(key[:5]))
    for key in unexpected:
        add_issue('new_relation_unplanned:' + ':'.join(key[:5]))
    return ProjectionRelationComparison(len(expected), len(expected) - len(missing), len(missing), unresolved,
        len(unexpected), unplanned_existing, duplicate_expected,
        hashlib.sha256(canonical_bytes(sorted(expected))).hexdigest(), tuple(issues), clipped or issue_count > 100)
