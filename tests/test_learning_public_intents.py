from copy import deepcopy

import pytest
from pydantic import ValidationError

from okto_pulse.core.domain.learning_submission import LearningSubmission, learning_submission_request_digest
from okto_pulse.core.models.learning_capture import LearningCaptureCreateRequest
from okto_pulse.core.models.schemas import CardMove


def body():
    return dict(board_id='board', capture_id='capture', expected_source_digest='a' * 64,
        expected_source_version=1, content='Lesson', context='Context', applicability='Scope', scenario_ids=['scenario'])


def intent(kind='supersede'):
    if kind == 'create': return dict(kind=kind)
    return dict(kind=kind, target_node_id='target', target_generation=0,
        expected_fingerprint='b' * 64, reason='Explicit applicability',
        **({'scope': 'source_bug'} if kind == 'supersede' else {}))


@pytest.mark.parametrize('kind', ['create', 'reuse', 'supersede'])
def test_public_intents_preserve_the_exact_target_and_scope_in_both_writers(kind):
    request = LearningCaptureCreateRequest(**body(), intent=intent(kind))
    command = request.command('bug')
    assert command.intent.kind == kind
    draft = LearningSubmission(**{key: value for key, value in body().items() if key != 'board_id'}, intent=intent(kind))
    assert draft.intent.command() == command.intent
    if kind != 'create':
        assert command.intent.expected_fingerprint == 'b' * 64
        assert command.intent.scope == ('source_bug' if kind == 'supersede' else None)


@pytest.mark.parametrize('damage', ['missing_scope', 'global', 'reuse_scope', 'create_target', 'missing_reason',
    'blank_reason', 'generation_bool', 'fingerprint', 'implicit_kind', 'approved'])
def test_public_schema_rejects_implicit_scope_or_client_authority(damage):
    value = intent()
    if damage == 'missing_scope': del value['scope']
    elif damage == 'global': value['scope'] = 'global'
    elif damage == 'reuse_scope': value['kind'] = 'reuse'
    elif damage == 'create_target': value['kind'] = 'create'
    elif damage == 'missing_reason': del value['reason']
    elif damage == 'blank_reason': value['reason'] = ' '
    elif damage == 'generation_bool': value['target_generation'] = True
    elif damage == 'fingerprint': value['expected_fingerprint'] = 'bad'
    elif damage == 'implicit_kind': del value['kind']
    else: value['approved'] = True
    with pytest.raises(ValidationError): LearningCaptureCreateRequest(**body(), intent=value)
    with pytest.raises(ValidationError):
        LearningSubmission(**{key: item for key, item in body().items() if key != 'board_id'}, intent=value)


def test_absent_intent_preserves_create_and_the_existing_compound_digest():
    from okto_pulse.core.domain.quality_canonicalization import canonical_sha256
    request = LearningCaptureCreateRequest(**body())
    assert request.command('bug').intent.kind == 'create'
    move = CardMove(status='validation', learning_submission={key: item for key, item in body().items() if key != 'board_id'})
    legacy = deepcopy(move.model_dump(mode='json', exclude_none=True))
    assert 'intent' not in legacy['learning_submission']
    assert learning_submission_request_digest(board_id='board', bug_id='bug', actor_id='actor', move=move) == canonical_sha256(
        dict(board_id='board', bug_id='bug', actor_id='actor', move=legacy))
