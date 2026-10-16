"""Current Done admission preserves captures and rejects historical substitutes."""
from dataclasses import replace

import pytest

from test_learning_closeout_binding import source, captured, bind
from okto_pulse.core.domain.learning_closeout import qualify_learning_materialization_basis


def test_capture_authored_after_done_needs_no_invented_transition():
    done = source(status='done')
    record = captured(done)
    assert qualify_learning_materialization_basis(record, done, None) is None
    assert record.payload['source']['digest'] == done.source_digest


def test_pre_done_capture_uses_exact_current_closing_binding():
    before, done = source(), source(status='done', source_policy_version=4)
    record, binding = captured(before), bind(before, done)
    assert qualify_learning_materialization_basis(record, done, [binding.model_dump()]) == binding
    assert record.payload['source']['digest'] == before.source_digest


@pytest.mark.parametrize('change', [
    {'status': 'in_progress'}, {'source_policy_version': 5},
    {'conclusions': ({'text': 'A new correction.'},)},
    {'test_scenarios': ({'id': 'scenario', 'status': 'failed'},)},
    {'load_errors': ('provider_unavailable',)},
])
def test_reopen_or_changed_basis_never_reuses_historical_admission(change):
    before, done = source(), source(status='done', source_policy_version=4)
    changed = source(**({'status': 'done', 'source_policy_version': 4} | change))
    with pytest.raises(ValueError, match='learning_materialization_(source_not_eligible|current_binding_required)'):
        qualify_learning_materialization_basis(captured(before), changed, [bind(before, done).model_dump()])


@pytest.mark.parametrize('damage', ['absent', 'tamper', 'duplicate', 'revision', 'different_capture'])
def test_binding_presence_or_convenient_other_capture_is_insufficient(damage):
    before, done = source(), source(status='done', source_policy_version=4)
    record, binding = captured(before), bind(before, done).model_dump()
    history = [binding]
    if damage == 'absent':
        history = []
    elif damage == 'tamper':
        binding['actor_id'] = 'other'
    elif damage == 'duplicate':
        history.append(binding)
    elif damage == 'revision':
        record = replace(record, source_revision=3)
    else:
        record = replace(record, node_id='other', record_fingerprint='')
    with pytest.raises(ValueError):
        qualify_learning_materialization_basis(record, done, history)
