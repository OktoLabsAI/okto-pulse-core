from dataclasses import replace

import pytest
from pydantic import ValidationError

from okto_pulse.core.domain.learning_submission import (
    LearningSubmission, LearningSubmissionReceipt, learning_submission_replay,
    learning_submission_request_digest, qualify_learning_submission_basis,
)
from okto_pulse.core.models.schemas import CardMove
from okto_pulse.core.ports.bug_cognitive_context import qualify_bug_semantic_context
from test_learning_closeout_binding import source


def submission(**changes):
    return LearningSubmission(**(dict(capture_id='capture', expected_source_digest='a' * 64,
        expected_source_version=1, content='Lesson', context='Correction', applicability='Component',
        scenario_ids=['scenario']) | changes))


@pytest.mark.parametrize('status', ['in_progress', 'validation'])
def test_only_the_authored_report_and_requested_status_become_the_new_basis(status):
    initial = source(status='in_progress')
    report = {'text': 'New report', 'author_id': 'author'}
    captured = qualify_bug_semantic_context(replace(initial, status=status, source_policy_version=4,
        conclusions=initial.conclusions + (report,), source_digest=None))
    assert qualify_learning_submission_basis(initial=initial, captured=captured,
        conclusion=report, capture_status=status) == captured


@pytest.mark.parametrize('change', [dict(title='Changed'), dict(spec_id='foreign'),
    dict(comments=({'text': 'Concurrent comment'},)), dict(validations=({'result': 'pass'},)),
    dict(conclusions=({'text': 'Unrelated report'},)), dict(status='done')])
def test_unexpected_changes_cannot_be_silently_rebased(change):
    initial = source(status='in_progress')
    report = {'text': 'New report'}
    changed = replace(initial, source_policy_version=4, conclusions=initial.conclusions + (report,), source_digest=None)
    changed = qualify_bug_semantic_context(replace(changed, **change))
    with pytest.raises(ValueError, match='unexpected_source_change'):
        qualify_learning_submission_basis(initial=initial, captured=changed, conclusion=report, capture_status='in_progress')


@pytest.mark.parametrize('change', [dict(expected_source_version=True), dict(capture_id=' '),
    dict(content=' '), dict(scenario_ids=[]), dict(scenario_ids=['duplicate', 'duplicate']),
    dict(scenario_ids=['']), dict(author_id='impersonation')])
def test_submission_is_closed_and_rejects_invalid_content(change):
    with pytest.raises(ValidationError):
        submission(**change)


def test_replay_seals_actor_report_content_and_placement_without_claiming_currentness():
    move = CardMove(status='done', conclusion='Report', learning_submission=submission())
    digest = learning_submission_request_digest(board_id='b', bug_id='bug', actor_id='author', move=move)
    receipt = LearningSubmissionReceipt(capture_id='capture', request_digest=digest,
        from_status='in_progress', to_status='done')
    history = [{'text': 'Report', 'author_id': 'author', 'learning_submission': receipt.model_dump()}]
    assert learning_submission_replay(history, actor_id='author', capture_id='capture', request_digest=digest) == receipt
    assert learning_submission_replay(history, actor_id='other', capture_id='capture', request_digest=digest) is None
    for changed in [move.model_copy(update={'conclusion': 'Other'}), move.model_copy(update={'placement': 'start'}),
        move.model_copy(update={'learning_submission': submission(content='Other')})]:
        other = learning_submission_request_digest(board_id='b', bug_id='bug', actor_id='author', move=changed)
        with pytest.raises(ValueError, match='idempotency_conflict'):
            learning_submission_replay(history, actor_id='author', capture_id='capture', request_digest=other)
    with pytest.raises(ValueError, match='history_invalid'):
        learning_submission_replay(history * 2, actor_id='author', capture_id='capture', request_digest=digest)
