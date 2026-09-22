"""Pure, bounded comparison of graph history inventories."""

from dataclasses import replace

from okto_pulse.core.ports.projection_history import (
    ProjectionEdgeFingerprint, ProjectionHistoryDelta, ProjectionNodeChange, ProjectionNodeFingerprint,
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
