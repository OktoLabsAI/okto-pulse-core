"""Exact closing delta, immutable authored basis and reopen applicability."""
from dataclasses import replace
from datetime import datetime, timezone

import pytest

from okto_pulse.core.domain.learning_closeout import (
    bind_learning_capture_to_closed_source, closeout_binding_is_current,
    verify_learning_closeout_binding,
    append_learning_closeout_binding,
)
from okto_pulse.core.ports.bug_cognitive_context import BugCognitiveContext, qualify_bug_semantic_context
from okto_pulse.core.ports.kg_cognitive_source import CognitiveSourceRecord


def source(**changes):
    return qualify_bug_semantic_context(BugCognitiveContext(**(dict(
        board_id='board', bug_id='bug', card_exists=True, card_type='bug',
        status='validation', contract_version='bug-semantic-context/v1',
        source_policy_version=3, conclusions=({'text': 'Authored execution report.'},),
    ) | changes)))


def captured(before):
    refs = ('spec:spec:test_scenario:scenario',)
    return CognitiveSourceRecord(board_id=before.board_id, node_type='Learning', node_id='learning',
        generation=0, source_revision=2, evidence_refs=refs,
        payload=dict(capture_format='learning-capture/v1', capture_id='capture', author_id='author',
            captured_at='2026-09-29T12:00:00+00:00', content='An authored lesson.', context='Context',
            applicability='Scope', source=dict(board_id=before.board_id, bug_id=before.bug_id,
                policy_version=before.source_policy_version, digest=before.source_digest, evidence_refs=list(refs)),
            intent=dict(kind='create', target_node_id=None, target_generation=None, expected_fingerprint=None, reason=None)))


def bind(before, closed, **changes):
    return bind_learning_capture_to_closed_source(**(dict(capture=captured(before), before=before,
        closed=closed, transition_id='transition', actor_id='reviewer',
        bound_at=datetime(2026, 9, 29, 12, tzinfo=timezone.utc), operation='move_card') | changes))


def test_binding_seals_exact_transition_without_rewriting_capture():
    before = source()
    capture = captured(before)
    closed = source(status='done', source_policy_version=4)
    binding = bind(before, closed, capture=capture)
    assert binding.before_digest == before.source_digest
    assert binding.closed_digest == closed.source_digest
    assert binding.capture.fingerprint == capture.record_fingerprint
    assert binding.capture_revision == 2
    assert capture.payload['source']['digest'] == before.source_digest
    assert closeout_binding_is_current(binding, closed)
    assert verify_learning_closeout_binding(binding.model_dump()) == binding


@pytest.mark.parametrize('operation', ['move_card', 'submit_task_validation'])
def test_explicit_reuse_binding_preserves_target_intent_and_requires_current_closed_basis(operation):
    from okto_pulse.core.domain.learning_closeout import qualify_learning_materialization_basis
    before = source()
    record = captured(before)
    record = replace(record, record_fingerprint='', payload={**record.payload,
        'intent': {'kind': 'reuse', 'target_node_id': record.node_id,
            'target_generation': record.generation, 'expected_fingerprint': 'a' * 64,
            'reason': 'Explicit applicability to this correction'}})
    reviews = ({'id': 'review', 'outcome': 'success'},) if operation == 'submit_task_validation' else ()
    closed = source(status='done', source_policy_version=4, validations=reviews)
    binding = bind(before, closed, capture=record, operation=operation, appended_validations=reviews)
    assert binding.capture.fingerprint == record.record_fingerprint
    assert qualify_learning_materialization_basis(record, closed, [binding.model_dump()]) == binding
    with pytest.raises(ValueError, match='current_binding_required'):
        qualify_learning_materialization_basis(record, closed, [])


def test_validation_append_must_be_the_exact_server_delta():
    before = source()
    review = {'id': 'review', 'outcome': 'success', 'reviewer_id': 'reviewer'}
    closed = source(status='done', source_policy_version=4, validations=(review,))
    binding = bind(before, closed, operation='submit_task_validation', appended_validations=(review,))
    assert closeout_binding_is_current(binding, closed)
    with pytest.raises(ValueError, match='unexpected_source_change'):
        bind(before, closed, operation='submit_task_validation', appended_validations=({'id': 'other'},))
    with pytest.raises(ValueError, match='unexpected_source_change'):
        bind(before, closed)


@pytest.mark.parametrize('change', [
    {'conclusions': ({'text': 'New conclusion not in the authored basis.'},)},
    {'conclusions': ({'text': 'Authored execution report.'}, {'text': 'Even an appended new conclusion.'})},
    {'comments': ({'id': 'comment', 'content': 'Changed technical evidence.'},)},
    {'test_scenarios': ({'id': 'scenario', 'status': 'failed'},)},
    {'board_id': 'foreign-board'}, {'bug_id': 'other-bug'},
    {'action_plan': 'Changed plan'}, {'validations': ({'outcome': 'failed'},)},
])
def test_binding_refuses_unadmitted_source_delta(change):
    with pytest.raises(ValueError, match='unexpected_source_change'):
        bind(source(), source(status='done', source_policy_version=4, **change))


@pytest.mark.parametrize('change', [
    {'status': 'in_progress'}, {'source_policy_version': 5},
    {'comments': ({'content': 'New evidence'},)}, {'board_id': 'other-board'},
])
def test_reopening_or_changed_source_does_not_reuse_historical_binding(change):
    before = source()
    closed = source(status='done', source_policy_version=4)
    binding = bind(before, closed)
    changed = qualify_bug_semantic_context(replace(closed, source_digest=None, **change))
    assert not closeout_binding_is_current(binding, changed)
    assert closeout_binding_is_current(binding, closed)


def test_tampered_binding_is_unavailable_not_admitted_or_absent():
    binding = bind(source(), source(status='done', source_policy_version=4)).model_dump()
    with pytest.raises(ValueError, match='integrity_invalid'):
        verify_learning_closeout_binding({**binding, 'actor_id': 'forged'})


def test_stale_capture_cannot_be_requalified_by_a_matching_closed_source():
    before = source()
    stale = captured(source(source_policy_version=2))
    with pytest.raises(ValueError, match='capture_basis_mismatch'):
        bind(before, source(status='done', source_policy_version=4), capture=stale)


def test_append_preserves_history_and_transition_idempotency_without_overwrite():
    before, closed = source(), source(status='done', source_policy_version=4)
    first = bind(before, closed)
    history = append_learning_closeout_binding(None, first)
    assert append_learning_closeout_binding(history, first) == history
    second = bind(before, closed, transition_id='later-transition')
    assert append_learning_closeout_binding(history, second) == history + [second.model_dump()]
    with pytest.raises(ValueError, match='transition_conflict'):
        append_learning_closeout_binding(history, bind(before, closed, actor_id='other-reviewer'))
    with pytest.raises(ValueError, match='history_invalid'):
        append_learning_closeout_binding(history + history, second)
    with pytest.raises(ValueError, match='integrity_invalid'):
        append_learning_closeout_binding([{**history[0], 'closed_digest': 'f' * 64}], second)
