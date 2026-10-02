"""Public validation projection cannot bypass DTO validation on old data."""
from copy import deepcopy

import pytest

from okto_pulse.core.models.schemas import project_task_validation_public


@pytest.mark.parametrize('damage', ['identity', 'score', 'gate_failures'])
def test_invalid_history_is_refused_without_mutation(damage):
    entry = dict(id='validation', card_id='card', board_id='board',
                 confidence=90, request_digest='private', idempotency_key='private')
    if damage == 'identity':
        del entry['id']
    elif damage == 'score':
        entry['confidence'] = 'not-a-score'
    else:
        entry['completion_gate_failures'] = 'invalid'
    original = deepcopy(entry)
    with pytest.raises(ValueError):
        project_task_validation_public(entry)
    assert entry == original


@pytest.mark.parametrize('value', [None, [], 'old validation'])
def test_non_mapping_history_is_refused(value):
    with pytest.raises(ValueError, match='task_validation_response_invalid'):
        project_task_validation_public(value)


def test_sealed_native_response_remains_replay_stable_and_private():
    entry = dict(id='validation', card_id='card', board_id='board', confidence=90,
                 request_digest='private', idempotency_key='private',
                 response=dict(id='validation', card_id='card', board_id='board',
                               confidence=91, card_status='rejected',
                               validation_outcome='success', completion_outcome='rejected',
                               completion_gate_failures=[{'code': 'dependencies_incomplete'}]))
    original = deepcopy(entry)
    result = project_task_validation_public(entry, replayed=True)
    assert result['confidence'] == 91
    assert result['validation_outcome'] == 'success'
    assert result['completion_outcome'] == 'rejected'
    assert result['replayed'] is True
    assert {'response', 'request_digest', 'idempotency_key'}.isdisjoint(result)
    assert entry == original
