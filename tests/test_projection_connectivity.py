"""Historical connectivity uses existing rules and keeps typed identities."""

import pytest

from okto_pulse.core.kg.logical_transfer import (
    LogicalNode, LogicalNodeType, LogicalPropertyDef, LogicalRelation, LogicalRelationLayout, LogicalSchema,
)
from okto_pulse.core.ports.projection_connectivity import observe_projection_connectivity


TYPES = ('Entity', 'Decision', 'Learning', 'Bug')
PROPS = (LogicalPropertyDef('id', 'string', False),) + tuple(LogicalPropertyDef(name, 'string') for name in
    ('source_artifact_ref', 'created_by_agent', 'source_session_id', 'graph_layer', 'maturity_status'))
SCHEMA = LogicalSchema('board', tuple(LogicalNodeType(kind, 'id', PROPS) for kind in TYPES),
    tuple(LogicalRelationLayout(*layout) for layout in (
        ('belongs_to', 'Decision', 'Entity'), ('mentions', 'Decision', 'Entity'),
        ('supersedes', 'Decision', 'Decision'), ('validates', 'Learning', 'Bug'),
        ('relates_to', 'Learning', 'Entity'))))
BUG = '00000000-0000-0000-0000-000000000001'


def node(kind, key, *, source='spec:s', layer='canonical', actor='agent-a'):
    return LogicalNode(kind, key, {'id': key, 'source_artifact_ref': source,
        'created_by_agent': actor, 'source_session_id': 'kgses_old',
        'graph_layer': layer, 'maturity_status': 'canonical_eligible'})


def edge(kind, source, target):
    return LogicalRelation(kind, source.type_name, target.type_name, source.key, target.key)


def observe(nodes, edges, selected):
    return observe_projection_connectivity(schema=SCHEMA, board_id='board', nodes=nodes,
        relations=edges, selected=tuple((item.type_name, item.key) for item in selected))


def test_equal_keys_in_different_types_are_distinct_and_input_order_does_not_change_report():
    decision, root = node('Decision', 'same'), node('Entity', 'same')
    relations = (edge('belongs_to', decision, root), edge('mentions', decision, root))
    report = observe((decision, root), relations, (decision,))
    assert report[0].outcome == 'passed' and report[0].reasons == ()
    assert observe((root, decision), relations[::-1], (decision,)) == report
    assert report[0].node_id == decision.key


def test_a_self_loop_does_not_satisfy_a_historical_decisions_judgement():
    decision, root = node('Decision', 'old'), node('Entity', 'root')
    report = observe((decision, root), (edge('belongs_to', decision, root),
        edge('supersedes', decision, decision)), (decision,))
    assert report[0].outcome == 'rejected'
    assert report[0].reasons == ('self_loop_not_connectivity',)


@pytest.mark.parametrize('layer,expected', [('canonical', 'passed'), ('working', 'rejected')])
def test_learning_requires_the_existing_canonical_bug_layer(layer, expected):
    learning = node('Learning', 'old', source='bug:' + BUG)
    bug = node('Bug', BUG, source='card:' + BUG, layer=layer)
    assert observe((learning, bug), (edge('validates', learning, bug),), (learning,))[0].outcome == expected


def test_type_aware_bug_probe_includes_nonincident_raw_uuid_without_promoting_other_types():
    learning = node('Learning', 'old', source='card:' + BUG)
    root = node('Entity', 'root')
    bug = node('Bug', BUG, source='')
    relations = (edge('relates_to', learning, root),)
    # The globally known canonical Bug requires validates, even without an edge
    # from this Learning. A same-key Entity cannot establish that Bug authority.
    assert observe((learning, root, bug), relations, (learning,))[0].outcome == 'rejected'
    other = node('Entity', BUG, source='')
    assert observe((learning, root, other), relations, (learning,))[0].outcome == 'passed'


def test_mixed_evidence_keeps_existing_advisory_and_parallel_occurrences():
    learning = node('Learning', 'old', source='bug:' + BUG)
    canonical = node('Bug', BUG, source='card:' + BUG)
    working = node('Bug', 'working', source='bug:working', layer='working')
    deferred = edge('validates', learning, working)
    edges = (deferred, edge('validates', learning, canonical), deferred)
    result = observe((learning, canonical, working), edges, (learning,))
    assert result[0].outcome == 'passed'
    assert result[0].advisories == ('canonical_learning_mixed_working_edge_deferred',)
    assert observe((learning, canonical, working), edges[::-1], (learning,)) == result
    assert len(edges) == 3  # Observation does not rewrite or deduplicate the inventory.


def test_technical_root_is_observed_without_creating_an_eligibility_claim():
    root = node('Entity', 'root', source='board:board', actor='system:bootstrap')
    report = observe((root,), (), (root,))
    assert report[0].outcome == 'allowlisted' and not hasattr(report[0], 'admitted')


def test_duplicate_missing_selection_and_dangling_edge_are_refused():
    decision, root = node('Decision', 'old'), node('Entity', 'root')
    with pytest.raises(ValueError, match='duplicate_node'):
        observe((decision, decision), (), (decision,))
    with pytest.raises(ValueError, match='selected_node_missing'):
        observe((root,), (), (decision,))
    with pytest.raises(ValueError, match='endpoint_missing'):
        observe((decision,), (edge('belongs_to', decision, root),), (decision,))
