"""Durable source identity alone never proves the projected content matches."""

from copy import deepcopy
import json

import pytest

from okto_pulse.core.kg.logical_transfer import (
    LOGICAL_NULL, LogicalNode, LogicalNodeType, LogicalPropertyDef, LogicalSchema,
    LogicalTimestamp, LogicalVector, LogicalVectorSpace,
)
from okto_pulse.core.kg.logical_transfer.errors import LogicalTransferError
from okto_pulse.core.ports.cognitive_projection import compare_cognitive_projection, validate_cognitive_projection_sources
from okto_pulse.core.ports.kg_cognitive_source import canonical_cognitive_source_fingerprint


SCHEMA = LogicalSchema('board', (LogicalNodeType('Learning', 'id', (
    LogicalPropertyDef('id', 'string', False), LogicalPropertyDef('title', 'string'),
    LogicalPropertyDef('generation', 'int64'), LogicalPropertyDef('created_at', 'timestamp_us'),
    LogicalPropertyDef('source_session_id', 'string'), LogicalPropertyDef('human_curated', 'bool'),
    LogicalPropertyDef('query_hits', 'int64'), LogicalPropertyDef('source_created_at', 'timestamp_us'),
    LogicalPropertyDef('embedding', 'vector', vector_space='learning'),
)),), vector_spaces=(LogicalVectorSpace('learning', 'float64', 2, 'cosine', False),))


def record():
    return {'board_id': 'board', 'node_type': 'Learning', 'node_id': 'old', 'generation': 0,
        'source_revision': 2, 'source_session_id': 'session', 'evidence_refs': ['spec:s'],
        'payload': {'title': 'sealed', 'generation': 0, 'created_at': '1970-01-01T00:00:00.000001',
            'human_curated': True, 'query_hits': 0, 'embedding': [0.0, -0.0]}}


def node(**changes):
    return LogicalNode('Learning', 'old', {'id': 'old', 'title': 'sealed', 'generation': 0,
        'created_at': LogicalTimestamp(1), 'source_session_id': 'session', 'human_curated': True,
        'query_hits': 0, 'source_created_at': LOGICAL_NULL,
        'embedding': LogicalVector('learning', 'float64', (0.0, -0.0)), **changes})


def compare(source=None, observed=None):
    return compare_cognitive_projection(schema=SCHEMA, board_id='board',
        record=source or record(), node=observed or node())


def test_sealed_payload_matches_without_writes_and_usage_is_separately_observed():
    source, observed = record(), node(query_hits=12)
    original = deepcopy(source)
    result = compare(source, observed)
    assert result.state == 'matched' and result.usage_differences == ('query_hits',)
    assert result.differing_fields == () and source == original
    assert result.source_revision == 2
    assert result.source_fingerprint == canonical_cognitive_source_fingerprint(
        board_id='board', node_type='Learning', node_id='old', generation=0,
        payload=source['payload'], evidence_refs=source['evidence_refs'])


def test_authenticated_sql_json_cells_match_without_rewriting_the_snapshot():
    source = record()
    encoded = {**source, 'payload': json.dumps(source['payload']), 'evidence_refs': json.dumps(source['evidence_refs'])}
    original = deepcopy(encoded)
    assert compare(encoded) == compare(source)
    assert encoded == original


@pytest.mark.parametrize('changes,field', [({'title': 'other'}, 'title'),
    ({'created_at': LogicalTimestamp(2)}, 'created_at'), ({'generation': 1}, 'generation'),
    ({'human_curated': False}, 'human_curated'), ({'source_session_id': 'other'}, 'source_session_id'),
    ({'source_created_at': LogicalTimestamp(0)}, 'source_created_at'),
    ({'embedding': LogicalVector('learning', 'float64', (0.0, 0.0))}, 'embedding')])
def test_identity_does_not_hide_content_birth_generation_provenance_or_vector_changes(changes, field):
    result = compare(observed=node(**changes))
    assert result.state == 'different' and result.differing_fields == (field,)


def test_missing_projection_is_not_a_successful_empty_inventory():
    result = compare_cognitive_projection(schema=SCHEMA, board_id='board', record=record(), node=None)
    assert result.state == 'missing_node'


def test_corrupt_fingerprint_and_cross_board_source_are_refused():
    source = record()
    source['record_fingerprint'] = '0' * 64
    with pytest.raises(Exception, match='fingerprint_mismatch'):
        compare(source)
    source = record()
    source['board_id'] = 'other'
    with pytest.raises(ValueError, match='source_invalid'):
        compare(source)


@pytest.mark.parametrize('stamp', ['1970-01-01T00:00:00.0000001', '1970-01-01'], ids=['submicrosecond', 'date_only'])
def test_unrepresentable_or_date_only_timestamps_are_not_silently_coerced(stamp):
    source = record()
    source['payload']['created_at'] = stamp
    with pytest.raises(ValueError, match='timestamp_representation_unsupported'):
        compare(source)


def test_explicit_offset_preserves_the_same_instant():
    source = record()
    source['payload']['created_at'] = '1969-12-31T21:00:00.000001-03:00'
    assert compare(source).state == 'matched'


def test_full_source_inventory_preserves_original_sql_cells_and_selects_latest():
    old, current = record(), record()
    old['source_revision'] = 1
    current['payload'] = json.dumps(current['payload'])
    current['evidence_refs'] = json.dumps(current['evidence_refs'])
    records = (current, old)
    before = deepcopy(records)
    result = validate_cognitive_projection_sources(schema=SCHEMA, board_id='board', records=records)
    assert result == (current,) and result[0] is current and records == before


@pytest.mark.parametrize('mutation', ['bool_generation', 'bool_revision', 'bad_timestamp', 'wrong_payload_id',
    'wrong_payload_generation', 'wrong_vector_dimension', 'unknown_property', 'bad_evidence', 'cross_board'])
def test_unselected_old_revision_cannot_hide_malformed_source(mutation):
    old, current = record(), record()
    old['source_revision'] = 1
    if mutation == 'bool_generation':
        old['generation'] = False
    elif mutation == 'bool_revision':
        old['source_revision'] = True
    elif mutation == 'bad_timestamp':
        old['payload']['created_at'] = 'yesterday'
    elif mutation == 'wrong_payload_id':
        old['payload']['id'] = 'another'
    elif mutation == 'wrong_payload_generation':
        old['payload']['generation'] = 3
    elif mutation == 'wrong_vector_dimension':
        old['payload']['embedding'] = [0.0]
    elif mutation == 'unknown_property':
        old['payload']['unknown'] = 'hidden'
    elif mutation == 'bad_evidence':
        old['evidence_refs'] = [True]
    else:
        old['board_id'] = 'other'
    with pytest.raises((ValueError, LogicalTransferError)):
        validate_cognitive_projection_sources(schema=SCHEMA, board_id='board', records=(old, current))


def test_inventory_budget_is_aggregate_utf8_including_unselected_revisions(monkeypatch):
    from okto_pulse.core.application import cognitive_projection as policy
    old, current = record(), record()
    old['source_revision'] = 1
    old['payload']['title'] = '\u00e7' * 100
    size = len(json.dumps(old, ensure_ascii=False, allow_nan=False).encode('utf-8'))
    monkeypatch.setattr(policy, '_MAX_SOURCE_BYTES', size + 1)
    assert validate_cognitive_projection_sources(schema=SCHEMA, board_id='board', records=(old,)) == (old,)
    with pytest.raises(ValueError, match='source_limit'):
        validate_cognitive_projection_sources(schema=SCHEMA, board_id='board', records=(old, current))


def test_conflicting_unselected_revisions_still_fail_fingerprint_verification():
    old, duplicate, current = record(), record(), record()
    old['source_revision'] = duplicate['source_revision'] = 1
    duplicate['payload']['title'] = 'conflict'
    with pytest.raises(Exception, match='revision_conflict'):
        validate_cognitive_projection_sources(schema=SCHEMA, board_id='board', records=(old, current, duplicate))
