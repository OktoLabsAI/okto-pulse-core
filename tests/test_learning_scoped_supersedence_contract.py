"""An explicit replacement scope must not be retrofitted onto v1 history."""
from copy import deepcopy
from dataclasses import replace

import pytest

from okto_pulse.core.ports.learning_capture import (
    LearningCaptureIntent, LearningCaptureSourceRef, learning_capture_intent_payload,
    parse_learning_capture_source_ref, validate_learning_capture_payload,
)
from okto_pulse.core.ports.cognitive_projection import compare_cognitive_projection, cognitive_projection_source_node
from test_learning_capture_projection import capture_record
from test_cognitive_projection import SCHEMA


def scoped_record():
    record = capture_record()
    record['payload']['capture_format'] = 'learning-capture/v2'
    record['payload']['intent'] = learning_capture_intent_payload(LearningCaptureIntent(
        'supersede', 'target', 0, 'a' * 64, 'Restricted to the corrected deployment', 'source_bug'))
    return record


@pytest.mark.parametrize('kind', ['create', 'reuse', 'supersede'])
def test_unscoped_request_keeps_exact_v1_intent_fields(kind):
    intent = LearningCaptureIntent() if kind == 'create' else LearningCaptureIntent(kind, 'target', 0, 'a' * 64, 'Reason')
    assert set(learning_capture_intent_payload(intent)) == {
        'kind', 'target_node_id', 'target_generation', 'expected_fingerprint', 'reason'}


def test_v2_explicit_source_bug_scope_is_durable_but_not_a_literal_projection():
    record = scoped_record()
    before = deepcopy(record)
    assert validate_learning_capture_payload(record['payload'], **{key: record[key] for key in (
        'board_id', 'node_type', 'node_id', 'generation', 'evidence_refs')})
    assert record['payload']['source']['bug_id'] == 'bug-a'
    assert compare_cognitive_projection(schema=SCHEMA, board_id='board', record=record,
        node=None).state == 'capture_pending_materialization'
    with pytest.raises(ValueError, match='learning_capture_materialization_required'):
        cognitive_projection_source_node(schema=SCHEMA, board_id='board', record=record)
    assert record == before


@pytest.mark.parametrize('damage', ['v1_scope', 'missing_scope', 'global', 'unknown_bug', 'reuse', 'extra'])
def test_scope_cannot_be_inferred_broadened_or_added_to_old_format(damage):
    record = scoped_record()
    payload, intent = record['payload'], record['payload']['intent']
    if damage == 'v1_scope': payload['capture_format'] = 'learning-capture/v1'
    elif damage == 'missing_scope': del intent['scope']
    elif damage in {'global', 'unknown_bug'}: intent['scope'] = damage
    elif damage == 'reuse': intent['kind'] = 'reuse'
    else: intent['scope_bug_id'] = 'other-bug'
    with pytest.raises(ValueError, match='learning_capture_payload_invalid'):
        validate_learning_capture_payload(payload, **{key: record[key] for key in (
            'board_id', 'node_type', 'node_id', 'generation', 'evidence_refs')})


@pytest.mark.parametrize('kind', ['create', 'reuse'])
def test_other_intents_do_not_acquire_replacement_scope(kind):
    original = LearningCaptureIntent('supersede', 'target', 0, 'a' * 64, 'Reason', 'source_bug')
    with pytest.raises(ValueError, match='intent_invalid'):
        replace(original, kind=kind)


@pytest.mark.parametrize('node_id,generation', [('node', 0), ('node:opaque/% ç', 7)])
def test_capture_source_reference_round_trip_is_exact(node_id, generation):
    value = LearningCaptureSourceRef(node_id, generation, 'a' * 64)
    assert parse_learning_capture_source_ref(value.encode()) == value
    assert parse_learning_capture_source_ref('bug:unrelated') is None


@pytest.mark.parametrize('tail', [
    'node:01:' + 'a' * 64, 'node:-1:' + 'a' * 64, 'node:true:' + 'a' * 64,
    'node:0:' + 'A' * 64, '%ZZ:0:' + 'a' * 64, ':0:' + 'a' * 64,
    'node:0:short', 'node:0:' + 'a' * 64 + ':extra',
])
def test_malformed_reserved_source_reference_fails_closed(tail):
    with pytest.raises(ValueError, match='source_ref_invalid'):
        parse_learning_capture_source_ref('learning-capture-source/v1:' + tail)


def test_unknown_reference_version_is_not_reinterpreted_as_legacy_evidence():
    with pytest.raises(ValueError, match='source_ref_invalid'):
        parse_learning_capture_source_ref('learning-capture-source/v2:node:0:' + 'a' * 64)


def test_scoped_syntax_without_qualified_target_cannot_enter_commit():
    from okto_pulse.core.domain.learning_materialization import CapturedLearningProjection
    from okto_pulse.core.ports.kg_cognitive_source import CognitiveSourceRecord
    capture = CognitiveSourceRecord(**scoped_record())
    plan = CapturedLearningProjection(capture, capture, 'bug-a')
    with pytest.raises(ValueError, match='learning_scope_target_required'):
        plan.require_scope_target()


def test_v1_supersede_is_not_silently_given_source_bug_scope():
    from okto_pulse.core.domain.learning_materialization import CapturedLearningProjection
    from okto_pulse.core.ports.kg_cognitive_source import CognitiveSourceRecord
    raw = scoped_record()
    raw['payload']['capture_format'] = 'learning-capture/v1'
    del raw['payload']['intent']['scope']
    capture = CognitiveSourceRecord(**raw)
    with pytest.raises(ValueError, match='learning_materialization_intent_unsupported'):
        CapturedLearningProjection(capture, capture, 'bug-a').require_literal_head()
