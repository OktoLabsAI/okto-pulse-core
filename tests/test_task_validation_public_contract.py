"""Public validation projection cannot bypass DTO validation on old data."""
from copy import deepcopy

import pytest

from okto_pulse.core.models.schemas import TaskValidationSubmit, project_task_validation_public


from task_validation_native_fixtures import native_entry


@pytest.mark.parametrize('damage', ['identity', 'score', 'gate_failures'])
def test_invalid_history_is_refused_without_mutation(damage):
    entry = native_entry()
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
    entry = native_entry()
    entry['response'] = dict(native_entry(), confidence=91, card_status='rejected',
                            validation_outcome='success', completion_outcome='rejected',
                            completion_gate_failures=[{'code': 'dependencies_incomplete'}])
    original = deepcopy(entry)
    result = project_task_validation_public(entry, replayed=True)
    assert result['confidence'] == 91
    assert result['validation_outcome'] == 'success'
    assert result['completion_outcome'] == 'rejected'
    assert result['replayed'] is True
    assert {'response', 'request_digest', 'idempotency_key'}.isdisjoint(result)
    assert entry == original


@pytest.mark.parametrize('alias', ['evaluator_id', 'evaluator_name', 'completeness', 'drift', 'summary', 'verdict'])
def test_removed_alias_is_refused_without_inference(alias):
    entry = dict(native_entry(), **{alias: 'old'})
    original = deepcopy(entry)
    with pytest.raises(ValueError, match='Extra inputs'):
        project_task_validation_public(entry)
    assert entry == original


@pytest.mark.parametrize('field', [
    'reviewer_id', 'reviewer_name', 'confidence', 'estimated_completeness',
    'estimated_drift', 'general_justification', 'recommendation', 'outcome',
    'validation_outcome', 'completion_outcome', 'resolved_thresholds',
    'reviewer_separation', 'expected_subject_version', 'subject_version',
    'threshold_violations', 'completion_gate_failures', 'created_at', 'card_status',
])
@pytest.mark.parametrize('damage', ['missing', 'null'])
def test_sparse_predecessor_is_refused_without_mutation(field, damage):
    entry = native_entry()
    if damage == 'missing':
        del entry[field]
    else:
        entry[field] = None
    original = deepcopy(entry)
    with pytest.raises(ValueError):
        project_task_validation_public(entry)
    assert entry == original


def test_submit_accepts_only_current_version_field():
    payload = dict(
        expected_subject_version=1, idempotency_key='attempt', confidence=90,
        confidence_justification='Checked evidence', estimated_completeness=95,
        completeness_justification='Checked scope', estimated_drift=0,
        drift_justification='No scope drift', recommendation='approve',
        general_justification='Current implementation satisfies requirements',
    )
    assert TaskValidationSubmit.model_validate(payload).expected_subject_version == 1
    old = dict(payload, expected_card_version=1)
    with pytest.raises(ValueError, match='Extra inputs'):
        TaskValidationSubmit.model_validate(old)
    del old['expected_subject_version']
    with pytest.raises(ValueError):
        TaskValidationSubmit.model_validate(old)
