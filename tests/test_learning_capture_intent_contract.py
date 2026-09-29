from dataclasses import replace

import pytest

from okto_pulse.core.ports.learning_capture import CreateLearningCapture, LearningCaptureIntent


@pytest.mark.parametrize('kind', ['create', 'reuse', 'supersede'])
def test_typed_capture_intent_preserves_explicit_target_and_cas(kind):
    intent = (LearningCaptureIntent() if kind == 'create' else
        LearningCaptureIntent(kind, 'target', 2, 'a' * 64, 'Explicit scoped reason'))
    request = CreateLearningCapture('board', 'bug', 'capture', 'b' * 64, 1,
        'Lesson', 'Context', 'Scope', ('scenario',), intent)
    assert request.intent.kind == kind


@pytest.mark.parametrize('changes', [dict(kind='automatic'), dict(target_generation=True),
    dict(target_generation=-1), dict(expected_fingerprint='A' * 64), dict(reason=' '),
    dict(target_node_id=''), dict(kind='create')])
def test_intent_refuses_implicit_or_malformed_target(changes):
    original = LearningCaptureIntent('reuse', 'target', 0, 'a' * 64, 'Explicit scope')
    with pytest.raises(ValueError, match='intent_invalid'):
        replace(original, **changes)
