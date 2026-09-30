"""Durable diagnostic identity must not conflate different broken references."""
from dataclasses import replace

import pytest

from okto_pulse.core.ports.projection_findings import ProjectionFindingSnapshot, ProjectionReferenceFinding


def finding(**changes):
    values = dict(board_id='board', owner_type='card', owner_id='card',
        namespace='card_scenarios', source_selector='card:card:test_scenario_ids',
        target_ref='spec:spec:test_scenario:ts_missing', reason_code='target_absent')
    return ProjectionReferenceFinding(**(values | changes))


def test_reason_changes_keep_identity_but_targets_and_boards_do_not():
    original = finding()
    assert replace(original, reason_code='target_outside_scope').finding_id == original.finding_id
    assert replace(original, target_ref='spec:spec:test_scenario:ts_other').finding_id != original.finding_id
    assert replace(original, board_id='foreign').finding_id != original.finding_id
    assert replace(original, source_selector='card:card:another_field').finding_id != original.finding_id


def test_absent_obligation_slot_does_not_invent_a_target():
    value = finding(source_selector='card:card:parent_spec', target_ref=None, reason_code='parent_required')
    assert value.target_ref is None
    assert len(value.finding_id) == 64


def snapshot(findings):
    return ProjectionFindingSnapshot('board', 'card', 'card', 'card_scenarios', 'a' * 64, findings)


def test_explicit_empty_snapshot_is_distinct_from_unavailable():
    assert snapshot(()).findings == ()
    with pytest.raises(ValueError, match='collection_invalid'):
        snapshot(None)


@pytest.mark.parametrize('damage', ['foreign', 'duplicate', 'same_target_new_reason'])
def test_snapshot_refuses_scope_drift_and_duplicate_identity(damage):
    first = finding()
    second = replace(first, board_id='foreign') if damage == 'foreign' else first
    if damage == 'same_target_new_reason':
        second = replace(first, reason_code='target_outside_scope')
    with pytest.raises(ValueError, match='scope_mismatch|identity_duplicate'):
        snapshot((first, second))
