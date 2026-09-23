from copy import deepcopy

import pytest

from okto_pulse.core.domain.spec_evaluation import (
    spec_evaluation_is_current, previous_spec_evaluations, project_spec_evaluation,
)


@pytest.mark.parametrize('recorded,expected', [(None, True), (2, True), (1, False), (True, False), ('2', False)])
def test_currentness_uses_server_edition_and_preserves_unscoped_legacy(recorded, expected):
    row = {'id': 'review', 'recommendation': 'reject', 'stale': False}
    if recorded is not None:
        row['spec_edition'] = recorded
    assert spec_evaluation_is_current(row, 2) is expected
    assert not spec_evaluation_is_current({**row, 'stale': True}, 2)


def test_reopen_changes_only_lifecycle_metadata_and_never_invents_legacy_edition():
    rows = [{'id': 'legacy', 'recommendation': 'reject', 'overall_score': 90, 'created_at': 'original',
             'evaluator_id': 'reviewer', 'overall_justification': 'Original finding', 'stale': False},
            {'id': 'already-old', 'stale': True, 'stale_reason': 'card_cancelled', 'spec_edition': 1}]
    original = deepcopy(rows)
    previous = previous_spec_evaluations(rows, reopened_in_edition=3)
    assert rows == original and previous[1] == original[1]
    assert previous[0] == {**original[0], 'stale': True, 'stale_reason': 'spec_reopened', 'stale_in_edition': 3}
    assert 'spec_edition' not in previous[0]
    assert previous_spec_evaluations(previous, reopened_in_edition=4) == previous
    view = project_spec_evaluation(previous[0], 3)
    assert view['lifecycle_state'] == 'previous' and view['edition_origin'] == 'legacy_unknown'
    assert not view['is_current']
