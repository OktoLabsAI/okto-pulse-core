"""Captures survive inventory without becoming literal canonical graph nodes."""

from copy import deepcopy
import json

import pytest

from okto_pulse.core.ports.learning_capture import validate_learning_capture_payload
from okto_pulse.core.ports.cognitive_projection import (
    compare_cognitive_projection, cognitive_projection_source_node,
    validate_cognitive_projection_sources, observe_cognitive_restoration,
)
from okto_pulse.core.ports.kg_cognitive_source import (
    CognitiveSourceRecord, canonical_cognitive_source_fingerprint,
    register_cognitive_source_store, reset_cognitive_source_store_for_tests,
)
from test_cognitive_projection import SCHEMA, node


def capture_record():
    return {'board_id': 'board', 'node_type': 'Learning', 'node_id': 'old', 'generation': 0,
        'source_revision': 0, 'evidence_refs': ['test_task:test-a'],
        'payload': {'capture_format': 'learning-capture/v1', 'capture_id': 'capture-a',
            'author_id': 'author', 'captured_at': '2026-09-24T12:00:00+00:00',
            'content': 'Retry only idempotent operations.', 'context': 'Worker retries',
            'applicability': 'Operations with an idempotency key',
            'source': {'board_id': 'board', 'bug_id': 'bug-a', 'policy_version': 2,
                'digest': 'a' * 64, 'evidence_refs': ['test_task:test-a']},
            'intent': {'kind': 'create', 'target_node_id': None, 'target_generation': None,
                'expected_fingerprint': None, 'reason': None}}}


def test_capture_is_preserved_without_projection_or_implicit_admission():
    source = capture_record()
    source['record_fingerprint'] = canonical_cognitive_source_fingerprint(
        **{key: source[key] for key in ('board_id', 'node_type', 'node_id', 'generation', 'payload', 'evidence_refs')})
    encoded = {**source, 'payload': json.dumps(source['payload']), 'evidence_refs': json.dumps(source['evidence_refs'])}
    original = deepcopy(encoded)
    assert validate_cognitive_projection_sources(schema=SCHEMA, board_id='board', records=(encoded,)) == (encoded,)
    for present in (None, node()):
        parity = compare_cognitive_projection(schema=SCHEMA, board_id='board', record=encoded, node=present)
        assert parity.state == 'capture_pending_materialization'
        assert parity.source_fingerprint == source['record_fingerprint']
    with pytest.raises(ValueError, match='learning_capture_materialization_required'):
        cognitive_projection_source_node(schema=SCHEMA, board_id='board', record=encoded)
    assert encoded == original


def test_restoration_reports_pending_capture_without_a_literal_candidate():
    from test_cognitive_restoration import SCHEMA as recovery_schema
    report, = observe_cognitive_restoration(schema=recovery_schema, board_id='board',
        records=(capture_record(),), nodes=(), relations=())
    assert report.state == 'capture_pending_materialization'
    assert report.literal_fingerprint is None
    assert report.reasons == ('learning_capture_materialization_required',)


def test_existing_canonical_technical_node_does_not_qualify_a_capture_as_replayed():
    from test_cognitive_replay_qualification import durable, SCHEMA as recovery_schema
    from okto_pulse.core.ports.cognitive_projection import qualify_cognitive_replay
    legacy = durable()
    legacy.update(node_type='Learning', node_id='old')
    present = cognitive_projection_source_node(schema=recovery_schema, board_id='board', record=legacy)
    result, = qualify_cognitive_replay(schema=recovery_schema, board_id='board',
        records=(capture_record(),), nodes=(present,), relations=(), restored=(('Learning', 'old'),))
    assert result.state == 'pending'
    assert result.reasons == ('learning_capture_materialization_required',)
    assert result.source_fingerprint


def test_capture_fingerprint_detects_changed_applicability():
    source = capture_record()
    source['record_fingerprint'] = canonical_cognitive_source_fingerprint(
        **{key: source[key] for key in ('board_id', 'node_type', 'node_id', 'generation', 'payload', 'evidence_refs')})
    source['payload']['applicability'] = 'Unsafe broadened applicability'
    with pytest.raises(Exception, match='fingerprint_mismatch'):
        validate_cognitive_projection_sources(schema=SCHEMA, board_id='board', records=(source,))


@pytest.mark.parametrize('change', ['unknown_format', 'extra', 'untrusted_admitted', 'wrong_board',
    'bool_version', 'digest', 'naive_time', 'empty_content', 'evidence_mismatch', 'duplicate_evidence',
    'create_target', 'unknown_intent', 'reuse_without_cas', 'supersede_self'])
def test_malformed_capture_is_not_hidden_by_a_later_valid_revision(change):
    old, current = capture_record(), capture_record()
    current['source_revision'] = 1
    payload = old['payload']
    if change == 'unknown_format': payload['capture_format'] = 'learning-capture/v2'
    elif change == 'extra': payload['extra'] = 'surprise'
    elif change == 'untrusted_admitted': payload['admitted'] = True
    elif change == 'wrong_board': payload['source']['board_id'] = 'other'
    elif change == 'bool_version': payload['source']['policy_version'] = True
    elif change == 'digest': payload['source']['digest'] = 'A' * 64
    elif change == 'naive_time': payload['captured_at'] = '2026-09-24T12:00:00'
    elif change == 'empty_content': payload['content'] = ' '
    elif change == 'evidence_mismatch': payload['source']['evidence_refs'] = ['test_task:other']
    elif change == 'duplicate_evidence': payload['source']['evidence_refs'] *= 2
    elif change == 'create_target': payload['intent']['target_node_id'] = 'other'
    elif change == 'unknown_intent': payload['intent']['kind'] = 'automatic_merge'
    elif change == 'reuse_without_cas': payload['intent']['kind'] = 'reuse'
    else:
        payload['intent'] = {'kind': 'supersede', 'target_node_id': 'old',
            'target_generation': 0, 'expected_fingerprint': 'b' * 64, 'reason': 'narrow applicability'}
    with pytest.raises(ValueError, match='learning_capture_payload_invalid'):
        validate_cognitive_projection_sources(schema=SCHEMA, board_id='board', records=(old, current))


@pytest.mark.parametrize('kind,target', [('reuse', 'old'), ('supersede', 'previous')])
def test_explicit_intent_preserves_target_cas_and_applicability_without_auto_linking(kind, target):
    source = capture_record()
    source['payload']['intent'] = {'kind': kind, 'target_node_id': target,
        'target_generation': 0, 'expected_fingerprint': 'b' * 64, 'reason': 'explicit applicability decision'}
    assert validate_learning_capture_payload(source['payload'],
        **{key: source[key] for key in ('board_id', 'node_type', 'node_id', 'generation', 'evidence_refs')})


def test_utf8_aggregate_limit_is_not_character_count():
    source = capture_record()
    for key in ('content', 'context', 'applicability'):
        source['payload'][key] = '\u00e7' * 65536
    with pytest.raises(ValueError, match='learning_capture_payload_limit'):
        validate_cognitive_projection_sources(schema=SCHEMA, board_id='board', records=(source,))


def test_legacy_replay_reports_capture_and_never_touches_graph(monkeypatch):
    from okto_pulse.core.kg import canonical_cognitive_preservation as replay
    from test_kg_cognitive_replay import _MemoryStore
    source = capture_record()
    store = _MemoryStore()
    store.records.append(CognitiveSourceRecord(**source))
    def forbidden(*args, **kwargs):
        pytest.fail('capture must not be probed or written as a literal graph node')
    monkeypatch.setattr(replay, '_node_present', forbidden)
    monkeypatch.setattr(replay, '_create_node', forbidden)
    register_cognitive_source_store(store)
    try:
        report = replay.replay_durable_cognitive('board')
    finally:
        reset_cognitive_source_store_for_tests()
    assert report['replayed_cognitive_count'] == 0
    assert report['replay_failed'] == [{'node_id': 'old', 'error': 'learning_capture_materialization_required'}]
