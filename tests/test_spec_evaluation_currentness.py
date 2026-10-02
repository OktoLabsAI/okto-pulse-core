from copy import deepcopy

import pytest

from okto_pulse.core.domain.spec_evaluation import (
    spec_evaluation_is_current, previous_spec_evaluations, project_spec_evaluation,
)


@pytest.mark.parametrize('recorded,expected', [(2, True), (1, False)])
def test_currentness_uses_server_edition(recorded, expected):
    row = {'id': 'review', 'recommendation': 'reject', 'stale': False}
    if recorded is not None:
        row['spec_edition'] = recorded
    assert spec_evaluation_is_current(row, 2) is expected
    assert not spec_evaluation_is_current({**row, 'stale': True}, 2)


def test_reopen_changes_only_lifecycle_metadata_and_preserves_recorded_edition():
    rows = [{'id': 'review', 'spec_edition': 2, 'recommendation': 'reject', 'overall_score': 90, 'created_at': 'original',
             'evaluator_id': 'reviewer', 'overall_justification': 'Original finding', 'stale': False},
            {'id': 'already-old', 'stale': True, 'stale_reason': 'card_cancelled', 'spec_edition': 1}]
    original = deepcopy(rows)
    previous = previous_spec_evaluations(rows, reopened_in_edition=3)
    assert rows == original and previous[1] == original[1]
    assert previous[0] == {**original[0], 'stale': True, 'stale_reason': 'spec_reopened', 'stale_in_edition': 3}
    assert previous[0]['spec_edition'] == 2
    assert previous_spec_evaluations(previous, reopened_in_edition=4) == previous
    view = project_spec_evaluation(previous[0], 3)
    assert view['lifecycle_state'] == 'previous'
    assert 'edition_origin' not in view
    assert not view['is_current']


@pytest.mark.parametrize('edition', [None, 0, -1, True, '2'])
@pytest.mark.parametrize('stale', [False, True])
def test_invalid_edition_is_refused_on_read_gate_and_reopen_without_mutation(edition, stale):
    row = {'id': 'review', 'recommendation': 'approve', 'stale': stale}
    if edition is not None:
        row['spec_edition'] = edition
    original = deepcopy(row)
    for operation in (
        lambda: spec_evaluation_is_current(row, 2),
        lambda: project_spec_evaluation(row, 2),
        lambda: previous_spec_evaluations([row], reopened_in_edition=3),
    ):
        with pytest.raises(ValueError, match='spec_evaluation_edition_required'):
            operation()
        assert row == original
