"""No aggregate success can hide an incomplete offline projection."""

from dataclasses import replace

import pytest

from okto_pulse.core.ports.global_projection import GlobalProjectionComparison
from okto_pulse.core.ports.projection_completion import BoardProjectionCompletion, require_projection_completion
from okto_pulse.core.ports.projection_qualification import ProjectionHistoryQualification
from okto_pulse.core.ports.projection_relations import ProjectionRelationComparison


GLOBAL = GlobalProjectionComparison('matched', 2, 2, 0, 0, 0, 0, 0, 'a' * 64)
BOARD = BoardProjectionCompletion('board',
    ProjectionHistoryQualification('current_source_reconciled', 1, 0, 1, 0, ()),
    ProjectionRelationComparison(1, 1, 0, 0, 0, 0, 0, 'b' * 64, (), False), 0, 0, 0, 0)


def check(board=BOARD, global_comparison=GLOBAL, expected=('board',)):
    return require_projection_completion(expected_boards=expected, boards=(board,), global_comparison=global_comparison)


def test_complete_sources_and_history_are_required_without_granting_runtime_authority():
    assert check() is None


@pytest.mark.parametrize('field', ['historical_orphans', 'rejected_connectivity',
    'unmatched_cognitive_sources', 'unqualified_restored_nodes'])
def test_pending_graph_facts_cannot_be_overridden(field):
    with pytest.raises(ValueError, match='pending'):
        check(replace(BOARD, **{field: 1}))


@pytest.mark.parametrize('field,value', [('state', 'pending'), ('unclassified_node_count', 1),
    ('unclassified_relation_count', 1), ('reasons', ('unclassified',)), ('state', 'not_applicable')])
def test_pending_history_cannot_be_disguised_by_state(field, value):
    with pytest.raises(ValueError, match='history_pending'):
        check(replace(BOARD, history=replace(BOARD.history, **{field: value})))


@pytest.mark.parametrize('field', ['unresolved_count', 'unexpected_new_count',
    'unplanned_existing_count', 'duplicate_expected_count'])
def test_historical_and_current_relations_must_all_be_reconciled(field):
    with pytest.raises(ValueError, match='relations_pending'):
        check(replace(BOARD, relations=replace(BOARD.relations, **{field: 1})))


@pytest.mark.parametrize('field', ['missing_nodes', 'changed_nodes', 'unexpected_nodes',
    'missing_relations', 'unexpected_relations'])
def test_global_matched_label_cannot_hide_unmatched_inventory(field):
    with pytest.raises(ValueError, match='global_pending'):
        check(global_comparison=replace(GLOBAL, **{field: 1}))


def test_missing_unknown_duplicate_scope_and_unavailable_global_fail_closed():
    for expected in ((), ('other',), ('board', 'board')):
        with pytest.raises(ValueError, match='scope_invalid'):
            check(expected=expected)
    with pytest.raises(ValueError, match='global_evidence_required'):
        check(global_comparison=None)
    with pytest.raises(ValueError, match='scope_invalid'):
        require_projection_completion(expected_boards=('board', 'other'), boards=(BOARD, BOARD), global_comparison=GLOBAL)


def test_boolean_counts_are_not_completion_evidence():
    with pytest.raises(ValueError, match='board_evidence_invalid'):
        replace(BOARD, historical_orphans=False)
    with pytest.raises(ValueError, match='global_evidence_invalid'):
        check(global_comparison=replace(GLOBAL, missing_nodes=False))
