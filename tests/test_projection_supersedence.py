"""The existing NC8 trail is observed exactly; it is not a generic edge waiver."""
from dataclasses import replace

import pytest

from okto_pulse.core.kg.logical_transfer import (
    LOGICAL_NULL, LogicalNode, LogicalNodeType, LogicalPropertyDef, LogicalRelation,
    LogicalRelationLayout, LogicalSchema, LogicalTimestamp,
)
from okto_pulse.core.kg.node_identity import derive_natural_key, mint_node_id
from okto_pulse.core.ports.projection_effects import (
    ProjectionPropertyEffect, ProjectionPropertyEffects, observe_bug_projection_supersedence,
)


SCHEMA = LogicalSchema('board', (LogicalNodeType('Bug', 'id', tuple(
    LogicalPropertyDef(name, kind, name != 'id') for name, kind in (
        ('id', 'string'), ('title', 'string'), ('content', 'string'),
        ('source_artifact_ref', 'string'), ('source_session_id', 'string'),
        ('human_curated', 'bool'), ('generation', 'int64'), ('created_at', 'timestamp_us'),
        ('superseded_by', 'string'), ('superseded_at', 'timestamp_us'), ('revocation_reason', 'string'),
    ))),), (LogicalRelationLayout('supersedes', 'Bug', 'Bug', tuple(
        LogicalPropertyDef(name, kind) for name, kind in (
            ('confidence', 'float64'), ('created_at', 'timestamp_us'), ('created_by', 'string'),
            ('created_by_session_id', 'string'), ('fallback_reason', 'string'),
            ('layer', 'string'), ('rule_id', 'string'),
        ))),))


def fixture():
    successor_id = mint_node_id('board', 'Bug', derive_natural_key('card:id', 'Bug', 'New title'), 1)
    before = LogicalNode('Bug', 'historical', {'id': 'historical', 'title': 'Old title',
        'content': 'Retained historical account', 'source_artifact_ref': 'bug:id',
        'source_session_id': 'original-birth', 'human_curated': False, 'generation': LOGICAL_NULL,
        'created_at': LOGICAL_NULL, 'superseded_by': LOGICAL_NULL,
        'superseded_at': LOGICAL_NULL, 'revocation_reason': LOGICAL_NULL})
    after = replace(before, properties={**before.properties, 'superseded_by': successor_id,
        'superseded_at': LogicalTimestamp(2), 'revocation_reason': 'semantic change on NC-8 reuse (MKG-D trail)'})
    successor = LogicalNode('Bug', successor_id, {**before.properties, 'id': successor_id,
        'title': 'New title', 'content': 'Current source account', 'source_artifact_ref': 'card:id',
        'source_session_id': 'new-session', 'generation': 1, 'created_at': LogicalTimestamp(1)})
    edge = LogicalRelation(layout_name='supersedes', source_type='Bug', target_type='Bug',
        source_key=successor_id, target_key='historical', properties={
        'confidence': 1.0, 'created_at': LogicalTimestamp(3), 'created_by': 'new-session',
        'created_by_session_id': 'new-session', 'fallback_reason': '', 'layer': 'cognitive', 'rule_id': ''})
    effect = ProjectionPropertyEffects('board', 'new-session', (ProjectionPropertyEffect.from_values(
        'Bug', 'historical', {'superseded_by': None, 'superseded_at': None, 'revocation_reason': None},
        {'superseded_by': successor_id, 'superseded_at': '1970-01-01T00:00:00.000002Z',
         'revocation_reason': 'semantic change on NC-8 reuse (MKG-D trail)'}),))
    return dict(schema=SCHEMA, board_id='board', session_id='new-session', before=before,
        after=after, successor=successor, relation=edge, effects=(effect,))


def test_exact_trail_preserves_old_identity_and_semantics_without_approving_history():
    data = fixture()
    result = observe_bug_projection_supersedence(**data)
    assert result.predecessor.before.node_id == result.predecessor.after.node_id == 'historical'
    assert result.edge.source_id == result.successor.node_id == data['successor'].key
    assert result.edge.target_id == 'historical' and result.edge.count == 1
    assert not hasattr(result, 'approved')
    assert data['after'].properties['source_session_id'] == 'original-birth'


@pytest.mark.parametrize('target,patch', [
    ('before', {'human_curated': True}), ('before', {'superseded_by': 'already-replaced'}),
    ('after', {'title': 'Lost original title'}), ('after', {'content': 'Lost history'}),
    ('after', {'source_session_id': 'rewritten-birth'}),
    ('after', {'revocation_reason': 'other reason'}),
    ('successor', {'title': ' Old title '}), ('successor', {'generation': 2}),
    ('successor', {'source_artifact_ref': 'card:other'}),
    ('successor', {'source_artifact_ref': 'bug:id:learning:child'}),
    ('successor', {'source_session_id': 'foreign-session'}),
    ('relation', {'layer': 'deterministic'}), ('relation', {'rule_id': 'invented'}),
    ('relation', {'confidence': 0.7}), ('relation', {'created_by_session_id': 'foreign-session'}),
    ('relation', {'created_at': LogicalTimestamp(0)}),
])
def test_unowned_changes_and_approximations_are_rejected(target, patch):
    data = fixture()
    data[target] = replace(data[target], properties={**data[target].properties, **patch})
    with pytest.raises(ValueError):
        observe_bug_projection_supersedence(**data)


def test_ack_scope_and_exact_property_trace_are_required():
    data = fixture()
    for effects in ((), (replace(data['effects'][0], board_id='other'),),
                    (replace(data['effects'][0], session_id='other'),), data['effects'] * 2):
        with pytest.raises(ValueError):
            observe_bug_projection_supersedence(**{**data, 'effects': effects})
