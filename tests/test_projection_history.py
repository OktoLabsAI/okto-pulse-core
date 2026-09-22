"""Inventory differences cannot manufacture graph identity or erase parallels."""

from dataclasses import replace

import pytest

from okto_pulse.core.ports.projection_history import (
    ProjectionNodeFingerprint as Node, ProjectionEdgeFingerprint as Edge, compare_projection_history,
    ProjectionSourceRoot as Root, ProjectionSourceIdentity as Identity, select_projection_source_roots,
)


def test_node_identity_and_relation_multiplicity_are_compared_independently():
    retained = Node('Decision', 'same', 'a' * 64)
    changed = Node('Entity', 'root', 'b' * 64)
    removed = Node('Entity', 'same', 'c' * 64)
    introduced = Node('Bug', 'new', 'd' * 64)
    edge = Edge('supports', 'Decision', 'same', 'Entity', 'root', 'e' * 64, 2)
    old_edges = (edge,)
    new_edges = (replace(edge, count=1), replace(edge, fingerprint='f' * 64, count=1))
    delta = compare_projection_history(before_nodes=(removed, changed, retained),
        after_nodes=(introduced, retained, replace(changed, fingerprint='0' * 64)),
        before_edges=old_edges, after_edges=new_edges)
    assert delta.unchanged_nodes == (retained,)
    assert delta.removed_nodes == (removed,) and delta.introduced_nodes == (introduced,)
    assert delta.changed_nodes[0].before == changed
    assert delta.changed_nodes[0].after.fingerprint == '0' * 64
    assert delta.retained_edges == (replace(edge, count=1),)
    assert delta.removed_edges == (replace(edge, count=1),)
    assert delta.introduced_edges == (new_edges[1],)
    assert not hasattr(delta, 'approved') and not hasattr(delta, 'reconciled')


@pytest.mark.parametrize('damage', ['node_duplicate', 'edge_duplicate', 'edge_overflow'])
def test_ambiguous_or_unbounded_input_is_refused(damage):
    node = Node('Entity', 'n', 'a' * 64)
    edge = Edge('supports', 'Entity', 'n', 'Entity', 'n', 'b' * 64, 500_000)
    nodes = (node, node) if damage == 'node_duplicate' else (node,)
    edges = ((edge, edge) if damage == 'edge_duplicate' else
        (edge, replace(edge, fingerprint='c' * 64, count=1)) if damage == 'edge_overflow' else ())
    with pytest.raises(ValueError, match='duplicate|limit'):
        compare_projection_history(before_nodes=nodes, after_nodes=(), before_edges=edges, after_edges=())


def test_source_root_selection_matches_type_active_generation_and_existing_tie_break():
    root = Root('Entity', 'spec:s')
    current = Identity('Entity', 'z', 'spec:s', 2, None)
    historical = Identity('Entity', 'old', 'spec:s', 99, 'z')
    wrong_type = Identity('Decision', 'd', 'spec:s', 100, None)
    same_generation = replace(current, node_id='a')
    older = replace(current, node_id='older', generation=1)
    result = select_projection_source_roots(roots=(root,),
        nodes=(historical, wrong_type, older, current, same_generation))
    assert result == (current,)
    assert historical.superseded_by == 'z'  # Selection never rewrites old identities.


def test_legacy_null_generation_is_zero_and_missing_active_root_is_not_zero_work():
    root = Root('Entity', 'spec:s')
    legacy = Identity('Entity', 'z', 'spec:s', None, None)
    assert select_projection_source_roots(roots=(root,),
        nodes=(legacy, replace(legacy, node_id='a', generation=0))) == (legacy,)
    with pytest.raises(ValueError, match='current_root_missing'):
        select_projection_source_roots(roots=(root,), nodes=(replace(legacy, superseded_by='missing'),))
    with pytest.raises(ValueError, match='duplicate_root'):
        select_projection_source_roots(roots=(root, root), nodes=(legacy,))
    with pytest.raises(ValueError, match='duplicate_node'):
        select_projection_source_roots(roots=(root,), nodes=(legacy, legacy))
