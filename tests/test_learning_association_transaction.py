"""Optional capability fails closed before any scoped removal."""
from types import SimpleNamespace

import pytest

from okto_pulse.core.kg.interfaces.graph_errors import GraphCapabilityUnavailable
from okto_pulse.core.kg.interfaces.graph_transaction import (
    LearningBugAssociationReceipt, ProjectionEdgeBeforeImage,
)
from okto_pulse.core.kg.transaction import TransactionOrchestrator


def test_missing_capability_does_not_fall_back_to_global_supersedence():
    scope = SimpleNamespace(create_node=lambda *args: None)
    orch = TransactionOrchestrator(scope, board_id='board')
    with pytest.raises(GraphCapabilityUnavailable, match='learning_association_replacement_unavailable'):
        orch.replace_learning_bug_association('old', 'new', 'bug')
    assert orch.records == []


@pytest.mark.parametrize('damage', ['foreign_board', 'foreign_edge', 'different_relation', 'self'])
def test_receipt_never_authorizes_another_association(damage):
    edge = ProjectionEdgeBeforeImage('validates', 'Learning', 'Bug', 'old', 'bug', {})
    if damage == 'foreign_edge':
        edge = ProjectionEdgeBeforeImage('validates', 'Learning', 'Bug', 'other', 'bug', {})
    elif damage == 'different_relation':
        edge = ProjectionEdgeBeforeImage('supersedes', 'Learning', 'Learning', 'old', 'bug', {})
    if damage != 'foreign_board':
        with pytest.raises(ValueError, match='receipt_invalid'):
            LearningBugAssociationReceipt('board', 'old', 'old' if damage == 'self' else 'new', 'bug', (edge,))
        return
    calls = []
    scope = SimpleNamespace(create_node=lambda *args: None,
        snapshot_learning_bug_association=lambda *args: LearningBugAssociationReceipt('foreign', 'old', 'new', 'bug', (edge,)),
        remove_learning_bug_association=lambda *args: calls.append('remove'),
        restore_learning_bug_association=lambda *args: calls.append('restore'))
    orch = TransactionOrchestrator(scope, board_id='board')
    with pytest.raises(ValueError, match='receipt_invalid'):
        orch.replace_learning_bug_association('old', 'new', 'bug')
    assert calls == [] and orch.records == []
