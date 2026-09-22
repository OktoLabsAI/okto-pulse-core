"""Portable Global observations preserve existing source and publication predicates."""

from dataclasses import replace

import pytest

from okto_pulse.core.kg.logical_transfer import (
    LOGICAL_NULL, LogicalNode, LogicalNodeType, LogicalPropertyDef, LogicalRelation,
    LogicalRelationLayout, LogicalSchema, LogicalVector, LogicalVectorSpace,
)
from okto_pulse.core.ports.global_projection import (
    compare_global_projection, global_projection_sources_from_inventory,
)
from test_global_projection import build


SPACE = LogicalVectorSpace('vectors', 'float64', 2, 'cosine', False)
BOARD_PROPS = (LogicalPropertyDef('id', 'string', False), LogicalPropertyDef('embedding', 'vector', vector_space='vectors')) + tuple(
    LogicalPropertyDef(name, 'string') for name in ('title', 'revocation_reason', 'superseded_by', 'graph_layer', 'source_artifact_ref'))
BOARD = LogicalSchema('board', tuple(LogicalNodeType(kind, 'id', BOARD_PROPS) for kind in ('Learning', 'Bug', 'Entity', 'Alternative')),
    (LogicalRelationLayout('validates', 'Learning', 'Bug', ()), LogicalRelationLayout('relates_to', 'Learning', 'Entity', ())),
    vector_spaces=(SPACE,))


def node(kind, key, **changes):
    return LogicalNode(kind, key, {'id': key, 'title': key, 'embedding': LogicalVector('vectors', 'float64', (0.0, -0.0)),
        'revocation_reason': LOGICAL_NULL, 'superseded_by': LOGICAL_NULL, 'graph_layer': 'canonical',
        'source_artifact_ref': 'bug:ticket', **changes})


def facts(nodes, relations=()):
    return global_projection_sources_from_inventory(schema=BOARD, nodes=nodes, relations=relations)


@pytest.mark.parametrize('field,value', [('embedding', LOGICAL_NULL), ('revocation_reason', ''), ('superseded_by', '')])
def test_only_active_embedded_digestable_sources_are_selected(field, value):
    assert facts((node('Learning', 'a', **{field: value}), node('Alternative', 'b'))) == ()


def test_source_layer_is_unknown_without_metadata_and_only_learning_carries_ref():
    result = facts((node('Entity', 'e'), node('Learning', 'l', graph_layer=LOGICAL_NULL)))
    assert result[0].source_artifact_ref == ''
    assert result[1].source_artifact_ref == 'bug:ticket' and result[1].graph_layer == 'legacy_unknown'


def test_typed_identity_ambiguity_is_rejected_only_among_publishable_sources():
    with pytest.raises(ValueError, match='identity_invalid'):
        facts((node('Entity', 'same'), node('Bug', 'same')))
    assert len(facts((node('Entity', 'same'), node('Bug', 'same', superseded_by='old')))) == 1


def test_learning_inputs_keep_existing_occurrence_and_group_semantics():
    nodes = (node('Learning', 'l'), node('Bug', 'b', superseded_by='new', embedding=LOGICAL_NULL),
        node('Entity', 'e', graph_layer=LOGICAL_NULL))
    validates = LogicalRelation('validates', 'Learning', 'Bug', 'l', 'b', {})
    related = LogicalRelation('relates_to', 'Learning', 'Entity', 'l', 'e', {})
    learning = next(row for row in facts(nodes, (validates, validates, related, related)) if row.node_id == 'l')
    # Pin the current source-reader policy; no new target-status filter is invented.
    assert learning.canonical_bug_count == 2 and learning.relates_to_endpoints == (('Entity', None),)
    assert tuple(component.hex() for component in learning.embedding) == ('0x0.0p+0', '-0x0.0p+0')


def test_duplicate_nodes_and_dangling_edges_are_never_partial_inventory():
    with pytest.raises(ValueError, match='duplicate_node'):
        facts((node('Learning', 'a'), node('Learning', 'a')))
    with pytest.raises(ValueError, match='endpoint_missing'):
        facts((node('Learning', 'a'),), (LogicalRelation('validates', 'Learning', 'Bug', 'a', 'absent', {}),))


def props(strings, integers=(), vectors=(), stamps=(), booleans=()):
    return tuple(LogicalPropertyDef(name, kind, nullable=name not in {'id', 'board_id'},
        **({'vector_space': 'vectors'} if kind == 'vector' else {}))
        for names, kind in ((strings, 'string'), (integers, 'int64'), (vectors, 'vector'), (stamps, 'timestamp_us'), (booleans, 'bool'))
        for name in names)


GLOBAL = LogicalSchema('global_discovery', (
    LogicalNodeType('Board', 'board_id', props(('board_id', 'name', 'summary'), ('topic_count', 'entity_count', 'decision_count'),
        ('summary_embedding',), ('last_sync_at',))),
    LogicalNodeType('DecisionDigest', 'id', props(('id', 'board_id', 'original_node_id', 'title', 'one_line_summary', 'node_type', 'graph_layer'),
        vectors=('embedding',), stamps=('created_at',), booleans=('source_revoked',))),
    LogicalNodeType('Topic', 'id', props(('id',))),
), (LogicalRelationLayout('CONTAINS_DECISION', 'Board', 'DecisionDigest', ()),), vector_spaces=(SPACE,))
ROOT = LogicalNode('Board', 'board', {'board_id': 'board', 'name': 'Board', 'summary': 'Summary',
    'summary_embedding': LogicalVector('vectors', 'float64', (0.25, 0.75)),
    'topic_count': 0, 'entity_count': 0, 'decision_count': 1, 'last_sync_at': LOGICAL_NULL})
DIGEST = LogicalNode('DecisionDigest', 'dd_board_a', {'id': 'dd_board_a', 'board_id': 'board', 'original_node_id': 'a',
    'title': 'Sealed learning', 'one_line_summary': 'Sealed learning', 'node_type': 'Learning', 'graph_layer': 'canonical',
    'source_revoked': False, 'embedding': LogicalVector('vectors', 'float64', (0.0, -0.0)), 'created_at': LOGICAL_NULL})
LINK = LogicalRelation('CONTAINS_DECISION', 'Board', 'DecisionDigest', 'board', 'dd_board_a', {})


def compare(nodes=(ROOT, DIGEST), relations=(LINK,)):
    return compare_global_projection(schema=GLOBAL, nodes=nodes, relations=relations, seeds=(build(),))


def test_complete_projection_matches_without_writing_or_timestamp_invention():
    result = compare()
    assert result.state == 'matched' and result.expected_nodes == result.matched_nodes == 2
    assert compare((DIGEST, ROOT)) == result


@pytest.mark.parametrize('field,value', [('title', 'changed'), ('board_id', 'other'), ('source_revoked', True),
    ('source_revoked', LOGICAL_NULL), ('graph_layer', 'working'), ('embedding', LogicalVector('vectors', 'float64', (0.0, 0.0)))])
def test_equal_counts_do_not_hide_wrong_content_visibility_layer_or_vector(field, value):
    result = compare((ROOT, replace(DIGEST, properties={**DIGEST.properties, field: value})))
    assert result.state == 'mismatch' and result.changed_nodes == 1


def test_auxiliary_history_is_not_certified_by_omission():
    result = compare((ROOT, DIGEST, LogicalNode('Topic', 'old', {'id': 'old'})))
    assert result.state == 'mismatch' and result.unexpected_nodes == 1


def test_missing_or_duplicate_owner_links_remain_mismatched():
    assert compare(relations=()).missing_relations == 1
    assert compare(relations=(LINK, LINK)).unexpected_relations == 1
    result = compare(nodes=(), relations=())
    assert result.missing_nodes == 2 and result.missing_relations == 1
