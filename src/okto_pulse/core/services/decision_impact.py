"""Explain explicit Decision paths without confusing shared Cards with tests.

Source links reuse the actual deterministic projectors. Graph-only observations
remain potential. Neither a source link nor an observed supports edge is proof.
"""
from collections import deque
from dataclasses import asdict
import hashlib
import json
import re

from okto_pulse.core.application.processors.deterministic_kg import DeterministicWorker, _spec_child_ref
from okto_pulse.core.application.processors.card_scenario_projection import prepare_card_scenario_projection_from_source
from okto_pulse.core.application.use_cases.base import EntityNotFoundError
from okto_pulse.core.ports.analytics_foundation import require_utc_datetime
from okto_pulse.core.ports.card_projection import CARD_CHILD_FAMILIES
from okto_pulse.core.ports.spec_coverage_query import MAX_SPEC_COVERAGE_BYTES, MAX_SPEC_COVERAGE_FACTS, SpecCoverageGraphScope
from okto_pulse.core.services.spec_coverage_graph import build_spec_coverage_graph_scope

MAX_PATH_EXPANSIONS = 10000


def _json(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode()


def build_decision_impact_scope(snapshot):
    scope = build_spec_coverage_graph_scope(snapshot)
    allowed = set(scope.nodes)
    edges = set(scope.relations)
    for card in snapshot.cards:
        if card.archived:
            continue
        worker = DeterministicWorker()
        projected = prepare_card_scenario_projection_from_source(board_id=snapshot.scope.board_id,
            card=card, parent=snapshot.spec, result=worker.process_card(vars(card)))
        roots = {node.candidate_id: (node.node_type, node.source_artifact_ref) for node in projected.nodes}
        for edge in projected.edges:
            source = roots.get(edge.from_candidate_id)
            target = roots.get(edge.to_candidate_id)
            if edge.to_candidate_id.startswith('kgref:'):
                _, kind, reference = edge.to_candidate_id.split(':', 2)
                target = kind, reference
            if source in allowed and target in allowed:
                edges.add((*source, edge.edge_type, *target, edge.rule_id, edge.layer, edge.created_by))
        if len(edges) > MAX_SPEC_COVERAGE_FACTS:
            raise ValueError('decision_impact_source_bound')
    return SpecCoverageGraphScope(scope.nodes, tuple(sorted(edges)))


def _metadata(snapshot):
    metadata = {}
    for family in CARD_CHILD_FAMILIES:
        for index, value in enumerate(getattr(snapshot.spec, family.field) or []):
            reference = _spec_child_ref(snapshot.scope.spec_id, family.section, value)
            if isinstance(value, dict):
                title = value.get('title') or value.get('text') or value.get('description') or value.get('rule') or reference
                status = value.get('status', 'active')
            else:
                title, status = str(value), 'unknown'
            metadata[reference] = {'title': str(title)[:240], 'status': str(status)}
    for index, value in enumerate(snapshot.spec.test_scenarios or []):
        reference = _spec_child_ref(snapshot.scope.spec_id, 'test_scenario', value)
        metadata[reference] = ({'title': str(value.get('title') or reference)[:240], 'status': str(value.get('status', 'unknown'))}
            if isinstance(value, dict) else {'title': str(value)[:240], 'status': 'unknown'})
    for card in snapshot.cards:
        metadata[f'card:{card.id}'] = {'title': str(getattr(card, 'title', None) or card.id)[:240],
            'status': str(getattr(card.status, 'value', card.status))}
    return metadata


def _history(snapshot, selected):
    decisions = snapshot.spec.decisions or []
    by_id = {}
    for item in decisions:
        if (not isinstance(item, dict) or type(item.get('id')) is not str or not item['id']
                or item['id'].strip() != item['id'] or ':' in item['id'] or item['id'] in by_id):
            raise ValueError('decision_impact_history_ambiguous')
        predecessor = item.get('supersedes_decision_id')
        if predecessor is not None and (type(predecessor) is not str or not predecessor
                or predecessor.strip() != predecessor or ':' in predecessor):
            raise ValueError('decision_impact_history_ambiguous')
        by_id[item['id']] = item
    if selected not in by_id:
        raise EntityNotFoundError('decision', selected)
    queue, seen, rows = deque([selected]), set(), []
    while queue:
        identity = queue.popleft()
        if identity in seen:
            continue
        seen.add(identity)
        current = by_id.get(identity)
        if current is None:
            rows.append({'subject_ref': f'spec:{snapshot.scope.spec_id}:decision:{identity}',
                'title': '', 'status': 'unavailable', 'supersedes_ref': None})
            continue
        predecessor = current.get('supersedes_decision_id')
        rows.append({'subject_ref': f'spec:{snapshot.scope.spec_id}:decision:{identity}',
            'title': str(current.get('title') or identity)[:240], 'status': current.get('status', 'active'),
            'supersedes_ref': f'spec:{snapshot.scope.spec_id}:decision:{predecessor}' if predecessor else None})
        if predecessor:
            queue.append(predecessor)
        queue.extend(key for key, value in by_id.items() if value.get('supersedes_decision_id') == identity)
    return by_id[selected], sorted(rows, key=lambda row: row['subject_ref'])


def _step(edge, current, root):
    source, target, relation = tuple(edge[:2]), tuple(edge[3:5]), edge[2]
    # Do not fan out through the Spec anchor or an unrelated Decision. The
    # traversal follows only the product paths of Q01, not arbitrary proximity.
    if current == root:
        if source == root and relation == 'derives_from' and target[0] in {'Requirement', 'Constraint'}:
            return target, 'outgoing', False
        if target == root and relation == 'supports' and source[1].startswith('card:'):
            return source, 'incoming', False
    elif current[1].startswith('card:'):
        if source == current and relation == 'supports' and target[0] == 'TestScenario':
            return target, 'outgoing', True
    elif current[0] in {'Requirement', 'Constraint', 'Criterion'} and target == current:
        if relation == 'supports' and source[1].startswith('card:'):
            return source, 'incoming', False
        if relation in {'derives_from', 'implements', 'tests'} and source[0] not in {'Decision', 'Entity', 'Bug'}:
            return source, 'incoming', False
    elif current[0] == 'TestScenario' and source == current and relation == 'tests':
        return target, 'outgoing', False
    return None


def project_decision_impact(query, snapshot, scope, graph):
    if (snapshot.scope.board_id, snapshot.scope.spec_id, snapshot.actor_scope_ref) != (query.board_id, query.spec_id, query.actor_scope_ref):
        raise ValueError('decision_impact_scope_mismatch')
    if snapshot.delivery is not None or snapshot.delivery_state != 'restricted':
        raise ValueError('decision_impact_proof_outside_scope')
    if not snapshot.source_complete or scope != build_decision_impact_scope(snapshot):
        raise ValueError('decision_impact_source_incomplete')
    if graph.state not in {'observed', 'unavailable'} or type(graph.truncated) is not bool:
        raise ValueError('decision_impact_graph_invalid')
    allowed, observed = set(scope.nodes), set(graph.nodes)
    if (len(graph.nodes) != len(observed) or not observed <= allowed or len(graph.relations) > MAX_SPEC_COVERAGE_FACTS
            or graph.state == 'unavailable' and (graph.nodes or graph.relations)):
        raise ValueError('decision_impact_graph_outside_scope')
    if any(len(edge) != 8 or tuple(edge[:2]) not in observed or tuple(edge[3:5]) not in observed for edge in graph.relations):
        raise ValueError('decision_impact_graph_outside_scope')
    decision, history = _history(snapshot, query.decision_id)
    root = 'Decision', f'spec:{query.spec_id}:decision:{query.decision_id}'
    current = decision.get('status', 'active') == 'active'
    metadata = _metadata(snapshot)
    expected, actual = set(scope.relations), set(graph.relations)
    edges = sorted(expected | actual)
    adjacent = {}
    for edge in edges:
        adjacent.setdefault(tuple(edge[:2]), []).append(edge)
        adjacent.setdefault(tuple(edge[3:5]), []).append(edge)
    queue = deque([(root, (), False)]) if current else deque()
    visited = {(root, False)}
    targets, frontier = {}, set()
    expansions = 0
    limited = False
    while queue:
        node, path, potential = queue.popleft()
        for edge in adjacent.get(node, ()):
            step = _step(edge, node, root)
            if step is None:
                continue
            target, direction, via_card_scenario = step
            # A stale or legacy co-occurrence edge never establishes a Decision's
            # initial scope, even as a convenient broad potential match.
            if node == root and edge not in expected:
                continue
            next_potential = potential or via_card_scenario or edge not in expected
            key = target, next_potential
            if key in visited:
                continue
            if len(path) >= query.max_depth:
                frontier.add(target[1]); limited = True
                continue
            expansions += 1
            if expansions > MAX_PATH_EXPANSIONS:
                limited = True; frontier.add(target[1]); queue.clear(); break
            visited.add(key)
            item = {'source_ref': edge[1], 'relation': edge[2], 'target_ref': edge[4],
                'direction': direction, 'rule_id': edge[5], 'layer': edge[6], 'created_by': edge[7],
                'source_confirmed': edge in expected, 'graph_observed': edge in actual}
            next_path = (*path, item)
            certainty = 'potential' if next_potential else 'confirmed_link'
            candidate = {'target_ref': target[1], 'target_type': target[0],
                **metadata.get(target[1], {'title': target[1][:240], 'status': 'unknown'}),
                'reach': 'direct' if len(next_path) == 1 else 'indirect', 'certainty': certainty,
                'interpretation': 'potential_shared_card_reach' if via_card_scenario or potential else
                    'graph_only_observation' if edge not in expected else 'declared_link_not_proven_change',
                'path': list(next_path)}
            previous = targets.get(target)
            if previous is None or previous['certainty'] == 'potential' and certainty == 'confirmed_link':
                targets[target] = candidate
            queue.append((target, next_path, next_potential))
    rows = [value for _, value in sorted(targets.items())]
    counts = {'observed_targets': len(rows), 'confirmed_link_targets': sum(row['certainty'] == 'confirmed_link' for row in rows),
        'potential_targets': sum(row['certainty'] == 'potential' for row in rows)}
    state = 'unavailable' if graph.state == 'unavailable' else 'incomplete' if allowed - observed or expected - actual or graph.truncated else 'unknown'
    result = {'view': 'impact', 'subject_ref': root[1], 'authority': 'informational',
        'decision_status': decision.get('status', 'active'), 'current_decision': current,
        'data_source': 'composed' if graph.state == 'observed' else 'relational',
        'scope': {'spec_ref': f'spec:{query.spec_id}', 'max_depth': query.max_depth,
            'path_selection': 'one_representative_path_per_target_prefer_confirmed_link'},
        'projection_freshness': {'state': state, 'graph_generation': graph.generation,
            'source_checkpoint': snapshot.source_revision, 'projection_checkpoint': None,
            'checked_at': require_utc_datetime(snapshot.checked_at, field='decision_impact_checked_at').isoformat()},
        'completeness': {'complete_for_scope': False, 'truncated': limited or graph.truncated,
            'limitations': ['full_projection_checkpoint_unavailable'] + (['exploration_horizon_reached'] if limited else [])},
        'counts': counts, 'frontier_refs': sorted(frontier), 'history': history, 'items': [], 'next_cursor': None}
    identity = asdict(query); identity.pop('cursor')
    stable = {**result, 'projection_freshness': {k: v for k, v in result['projection_freshness'].items() if k != 'checked_at'}}
    digest = hashlib.sha256(_json([identity, stable, rows, sorted(actual)])).hexdigest()
    offset = 0
    if query.cursor is not None:
        match = re.fullmatch(r'decision-impact-v1:([0-9a-f]{64}):([1-9][0-9]{0,5})', query.cursor)
        if match is None:
            raise ValueError('decision_impact_cursor_invalid')
        if match[1] != digest:
            raise ValueError('decision_impact_cursor_stale')
        offset = int(match[2])
        if offset >= len(rows):
            raise ValueError('decision_impact_cursor_invalid')
    if len(_json(result)) > MAX_SPEC_COVERAGE_BYTES:
        raise ValueError('decision_impact_summary_payload_bound')
    for row in rows[offset:offset + query.limit]:
        result['items'].append(row)
        end = offset + len(result['items'])
        result['next_cursor'] = f'decision-impact-v1:{digest}:{end}' if end < len(rows) else None
        if len(_json(result)) > MAX_SPEC_COVERAGE_BYTES:
            result['items'].pop()
            if not result['items']:
                raise ValueError('decision_impact_row_payload_bound')
            result['next_cursor'] = f'decision-impact-v1:{digest}:{end - 1}'
            break
    return result
