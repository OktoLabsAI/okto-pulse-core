"""Explain declared origin/dependency paths without manufacturing history."""
from collections import deque
from dataclasses import asdict
import hashlib
import json
import re
from okto_pulse.core.ports.analytics_foundation import require_utc_datetime
from okto_pulse.core.ports.lineage_query import (
    LineageQuery, LineageSnapshot, MAX_LINEAGE_BYTES, MAX_LINEAGE_EDGES, MAX_LINEAGE_NODES,
)


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def project_lineage(query: LineageQuery, snapshot: LineageSnapshot) -> dict:
    if (query.board_id, query.subject_ref, query.actor_scope_ref) != (
            snapshot.board_id, snapshot.subject_ref, snapshot.actor_scope_ref):
        raise ValueError('lineage_scope_mismatch')
    nodes = {node.subject_ref: node for node in snapshot.nodes}
    if (len(nodes) != len(snapshot.nodes) or len(nodes) > MAX_LINEAGE_NODES
            or len(snapshot.relations) > MAX_LINEAGE_EDGES or query.subject_ref not in nodes):
        raise ValueError('lineage_source_invalid')
    allowed = {'precedes', 'derived_from', 'belongs_to', 'originates_bug', 'feeds_ideation', 'amendment_of', 'affects', 'regression_test'}
    relations = sorted(set(snapshot.relations), key=lambda edge: (edge.source_ref, edge.target_ref, edge.relation, edge.provenance_ref))
    adjacent = {}
    for edge in relations:
        if (edge.source_ref not in nodes or edge.target_ref not in nodes
                or edge.relation not in allowed or not edge.provenance_ref):
            raise ValueError('lineage_relation_invalid')
        # An amendment is an association from its own source record. A partial
        # revision never becomes a Spec-to-Spec normative supersedes relation.
        if edge.relation == 'amendment_of' and not edge.source_ref.startswith('amendment_hotfix_revision:'):
            raise ValueError('lineage_amendment_source_invalid')
        adjacent.setdefault(edge.source_ref, []).append((edge, edge.target_ref, 'outgoing'))
        adjacent.setdefault(edge.target_ref, []).append((edge, edge.source_ref, 'incoming'))
    queue = deque([(query.subject_ref, ())])
    visited = {query.subject_ref}
    rows, frontier = [], set()
    expansions = 0
    while queue:
        current, path = queue.popleft()
        for edge, target, direction in adjacent.get(current, ()):
            if target in visited:
                continue
            if len(path) >= query.max_depth or expansions >= MAX_LINEAGE_EDGES:
                frontier.add(target)
                continue
            expansions += 1
            visited.add(target)
            step = {**asdict(edge), 'direction': direction}
            next_path = (*path, step)
            node = nodes[target]
            rows.append({'subject_ref': target, 'entity_type': node.entity_type,
                'title': node.title[:240], 'status': node.status,
                'depth': len(next_path), 'path': list(next_path)})
            queue.append((target, next_path))
    # A node first seen beyond one path's horizon may be reached by a shorter
    # path later. The frontier must not falsely keep such a node as unexplored.
    frontier -= visited
    rows.sort(key=lambda row: (row['depth'], row['subject_ref']))
    result = {'view': 'lineage', 'subject_ref': query.subject_ref,
        'authority': 'informational', 'data_source': 'relational',
        'scope': {'max_depth': query.max_depth, 'path_selection': 'one_shortest_path_per_target',
            'interpretation': 'workflow_origins_dependencies_and_amendments_not_execution_or_delivery'},
        'projection_freshness': {'state': 'unknown', 'graph_generation': None,
            'source_checkpoint': snapshot.source_revision, 'projection_checkpoint': None,
            'checked_at': require_utc_datetime(snapshot.checked_at, field='lineage_checked_at').isoformat()},
        'completeness': {'complete_for_scope': snapshot.source_complete and not frontier,
            'truncated': bool(frontier),
            'limitations': list(snapshot.limitations) + (['exploration_horizon_reached'] if frontier else [])},
        'counts': {'source_nodes': len(nodes), 'source_relations': len(relations), 'reached_targets': len(rows)},
        'frontier_refs': sorted(frontier), 'items': [], 'next_cursor': None}
    identity = asdict(query); identity.pop('cursor')
    stable = {**result, 'projection_freshness': {k: v for k, v in result['projection_freshness'].items() if k != 'checked_at'}}
    digest = hashlib.sha256(_json([identity, stable, rows, [asdict(edge) for edge in relations]])).hexdigest()
    offset = 0
    if query.cursor is not None:
        match = re.fullmatch(r'lineage-v1:([0-9a-f]{64}):([1-9][0-9]{0,5})', query.cursor)
        if match is None:
            raise ValueError('lineage_cursor_invalid')
        if match[1] != digest:
            raise ValueError('lineage_cursor_stale')
        offset = int(match[2])
        if offset >= len(rows):
            raise ValueError('lineage_cursor_invalid')
    if len(_json(result)) > MAX_LINEAGE_BYTES:
        raise ValueError('lineage_summary_payload_bound')
    for row in rows[offset:offset + query.limit]:
        result['items'].append(row)
        end = offset + len(result['items'])
        result['next_cursor'] = f'lineage-v1:{digest}:{end}' if end < len(rows) else None
        if len(_json(result)) > MAX_LINEAGE_BYTES:
            result['items'].pop()
            if not result['items']:
                raise ValueError('lineage_row_payload_bound')
            result['next_cursor'] = f'lineage-v1:{digest}:{end - 1}'
            break
    return result
