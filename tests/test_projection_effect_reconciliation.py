"""Property composition preserves untouched portable values exactly."""

from dataclasses import replace

import pytest

from okto_pulse.core.kg.logical_transfer import (
    LOGICAL_NULL, LogicalNode, LogicalNodeType, LogicalPropertyDef, LogicalSchema,
    LogicalTimestamp, LogicalVector, LogicalVectorSpace,
)
from okto_pulse.core.ports.projection_effects import (
    ProjectionPropertyEffect, ProjectionPropertyEffects, reconcile_projection_node_effects,
)


SCHEMA = LogicalSchema('board', (LogicalNodeType('Entity', 'id', (
    LogicalPropertyDef('id', 'string', False), LogicalPropertyDef('title', 'string'),
    LogicalPropertyDef('relevance_score', 'float64'), LogicalPropertyDef('attestation_count', 'int64'),
    LogicalPropertyDef('resolved_at', 'timestamp_us'), LogicalPropertyDef('source_updated_at', 'timestamp_us'),
    LogicalPropertyDef('embedding', 'vector', vector_space='entity'),
)),), vector_spaces=(LogicalVectorSpace('entity', 'float64', 2, 'cosine', False),))


def node(**changes):
    return LogicalNode('Entity', 'prior', {'id': 'prior', 'title': 'old', 'relevance_score': -0.0,
        'attestation_count': 1, 'resolved_at': LOGICAL_NULL, 'source_updated_at': LogicalTimestamp(0),
        'embedding': LogicalVector('entity', 'float64', (0.0, -0.0)), **changes})


def effect(session, before, after):
    return ProjectionPropertyEffects('board', session,
        (ProjectionPropertyEffect.from_values('Entity', 'prior', before, after),))


def prove(before, after, effects):
    return reconcile_projection_node_effects(schema=SCHEMA, board_id='board', before=before, after=after, effects=effects)


def test_exact_ordered_composition_retains_untouched_signed_zero():
    prior = node()
    final = node(title='final', attestation_count=2, source_updated_at=LogicalTimestamp(1))
    effects = (effect('one', {'title': 'old'}, {'title': 'middle'}),
        effect('two', {'title': 'middle', 'attestation_count': 1,
            'source_updated_at': '1970-01-01T00:00:00.000000Z'},
            {'title': 'final', 'attestation_count': 2, 'source_updated_at': '1970-01-01T00:00:00.000001Z'}))
    result = prove(prior, final, effects)
    assert result.before.node_id == result.after.node_id == 'prior'
    assert result.before.fingerprint != result.after.fingerprint
    with pytest.raises(ValueError, match='before_mismatch'):
        prove(prior, final, effects[::-1])
    with pytest.raises(ValueError, match='scope_or_replay'):
        prove(prior, final, effects + effects[:1])
    with pytest.raises(ValueError, match='scope_or_replay'):
        prove(prior, final, (replace(effects[0], board_id='other'),))


@pytest.mark.parametrize('changed', [dict(relevance_score=0.0), dict(embedding=LogicalVector('entity', 'float64', (0.0, 0.0))),
    dict(attestation_count=2), dict(resolved_at=LogicalTimestamp(0))])
def test_undeclared_changes_fail_even_when_python_equality_would_hide_them(changed):
    with pytest.raises(ValueError, match='final_node_mismatch'):
        prove(node(), node(title='new', **changed), (effect('one', {'title': 'old'}, {'title': 'new'}),))


def test_absent_is_not_null_and_untraced_property_removal_is_not_preservation():
    prior = node()
    absent = LogicalNode('Entity', 'prior', {key: value for key, value in prior.properties.items() if key != 'resolved_at'})
    with pytest.raises(ValueError, match='prior_property_absent'):
        prove(absent, node(resolved_at=LogicalTimestamp(0)),
            (effect('one', {'resolved_at': None}, {'resolved_at': '1970-01-01T00:00:00.000000Z'}),))
    with pytest.raises(ValueError, match='final_node_mismatch'):
        prove(prior, absent, (effect('one', {'title': 'old'}, {'title': 'new'}),))


@pytest.mark.parametrize('after', [True, 1.0, '2'])
def test_property_types_are_not_coerced(after):
    with pytest.raises(ValueError, match='value_type_invalid'):
        prove(node(), node(attestation_count=2), (effect('one', {'attestation_count': 1}, {'attestation_count': after}),))


def test_lossy_nan_changes_and_unmatched_traces_fail_closed():
    with pytest.raises(ValueError, match='lossy_observation'):
        prove(node(relevance_score=float('nan')), node(relevance_score=1.0),
            (effect('one', {'relevance_score': {'type': 'float', 'value': 'NaN'}}, {'relevance_score': 1.0}),))
    other = ProjectionPropertyEffects('board', 'one',
        (ProjectionPropertyEffect.from_values('Entity', 'different', {'title': 'old'}, {'title': 'new'}),))
    with pytest.raises(ValueError, match='node_trace_missing'):
        prove(node(), node(), (other,))


def test_timestamp_range_and_vector_space_survive_explicit_effects():
    final = node(source_updated_at=LogicalTimestamp(2**63 - 1), embedding=LogicalVector('entity', 'float64', (1.0, 2.0)))
    prove(node(), final, (effect('one', {'source_updated_at': '1970-01-01T00:00:00.000000Z', 'embedding': [0.0, -0.0]},
        {'source_updated_at': {'type': 'timestamp', 'micros': str(2**63 - 1)}, 'embedding': [1.0, 2.0]}),))
