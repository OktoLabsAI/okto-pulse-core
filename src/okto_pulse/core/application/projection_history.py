"""Pure, bounded comparison of graph history inventories."""

from dataclasses import replace

from okto_pulse.core.ports.projection_history import (
    ProjectionEdgeFingerprint, ProjectionHistoryDelta, ProjectionNodeChange, ProjectionNodeFingerprint,
    ProjectionSourceIdentity, ProjectionSourceRoot,
)


def _nodes(rows):
    if type(rows) is not tuple or len(rows) > 100_000:
        raise ValueError('projection_history_node_limit')
    found = {}
    for row in rows:
        if type(row) is not ProjectionNodeFingerprint:
            raise TypeError('projection_history_node_required')
        key = row.node_type, row.node_id
        if key in found:
            raise ValueError('projection_history_duplicate_node')
        found[key] = row
    return found


def _edges(rows):
    if type(rows) is not tuple or len(rows) > 500_000:
        raise ValueError('projection_history_edge_limit')
    found, count = {}, 0
    for row in rows:
        if type(row) is not ProjectionEdgeFingerprint:
            raise TypeError('projection_history_edge_required')
        key = (row.edge_type, row.source_type, row.source_id, row.target_type, row.target_id, row.fingerprint)
        if key in found:
            raise ValueError('projection_history_duplicate_edge_group')
        count += row.count
        if count > 500_000:
            raise ValueError('projection_history_edge_limit')
        found[key] = row
    return found


def compare(before_nodes, after_nodes, before_edges, after_edges):
    old, new = _nodes(before_nodes), _nodes(after_nodes)
    previous, current = _edges(before_edges), _edges(after_edges)
    shared = sorted(old.keys() & new.keys())
    retained, added, removed = [], [], []
    for key in sorted(previous.keys() | current.keys()):
        before, after = previous.get(key), current.get(key)
        before_count, after_count = (before.count if before else 0), (after.count if after else 0)
        common = min(before_count, after_count)
        if common:
            retained.append(replace(before, count=common))
        if after_count > common:
            added.append(replace(after, count=after_count - common))
        if before_count > common:
            removed.append(replace(before, count=before_count - common))
    return ProjectionHistoryDelta(
        unchanged_nodes=tuple(old[key] for key in shared if old[key] == new[key]),
        introduced_nodes=tuple(new[key] for key in sorted(new.keys() - old.keys())),
        removed_nodes=tuple(old[key] for key in sorted(old.keys() - new.keys())),
        changed_nodes=tuple(ProjectionNodeChange(old[key], new[key]) for key in shared if old[key] != new[key]),
        retained_edges=tuple(retained), introduced_edges=tuple(added), removed_edges=tuple(removed),
    )


def select_source_roots(roots, nodes):
    if type(roots) is not tuple or type(nodes) is not tuple or max(len(roots), len(nodes)) > 100_000:
        raise ValueError('projection_history_node_limit')
    if any(type(root) is not ProjectionSourceRoot for root in roots):
        raise TypeError('projection_history_root_required')
    if len(set(roots)) != len(roots):
        raise ValueError('projection_history_duplicate_root')
    selected, identities = {}, set()
    wanted = set(roots)
    for node in nodes:
        if type(node) is not ProjectionSourceIdentity:
            raise TypeError('projection_history_source_identity_required')
        key = node.node_type, node.node_id
        if key in identities:
            raise ValueError('projection_history_duplicate_node')
        identities.add(key)
        root = ProjectionSourceRoot(node.node_type, node.source_artifact_ref)
        if root not in wanted or node.superseded_by is not None:
            continue
        current = selected.get(root)
        # kg.primitives._lookup_existing_node uses this exact order: active
        # records, coalesce(generation, 0) DESC, id DESC. No score or similarity.
        if current is None or (node.generation or 0, node.node_id) > (current.generation or 0, current.node_id):
            selected[root] = node
    if set(selected) != wanted:
        raise ValueError('projection_history_current_root_missing')
    return tuple(selected[root] for root in roots)


def is_technical_root(node_type, source_artifact_ref, created_by_agent, source_session_id):
    from okto_pulse.core.kg.connectivity_guard import KGConnectivityRuleRegistry
    from okto_pulse.core.kg.orphan_integrity import _safe_writer_path

    if (type(node_type) is not str or any(value is not None and type(value) is not str
            for value in (source_artifact_ref, created_by_agent, source_session_id))):
        return False  # Malformed historical provenance never earns an exception.
    return KGConnectivityRuleRegistry().is_technical_root_allowlisted(node_type=node_type,
        writer_path=_safe_writer_path(created_by_agent, source_session_id), source_artifact_ref=source_artifact_ref)
