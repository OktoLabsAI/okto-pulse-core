"""Completion precondition for an authenticated offline projection.

The edition authenticates complete source, ACK, history and graph inventories
under its writer fences. These facts are not client assertions. Passing this
precondition does not activate a runtime or replace installation/rollback proof.
"""

from dataclasses import dataclass
import re

from .global_projection import GlobalProjectionComparison
from .projection_qualification import ProjectionHistoryQualification
from .projection_relations import ProjectionRelationComparison


@dataclass(frozen=True, slots=True)
class BoardProjectionCompletion:
    board_id: str
    history: ProjectionHistoryQualification
    relations: ProjectionRelationComparison
    historical_orphans: int
    rejected_connectivity: int
    unmatched_cognitive_sources: int
    unqualified_restored_nodes: int

    def __post_init__(self):
        if (type(self.board_id) is not str or not 1 <= len(self.board_id) <= 4096
                or type(self.history) is not ProjectionHistoryQualification
                or type(self.relations) is not ProjectionRelationComparison
                or any(type(value) is not int or not 0 <= value <= 100_000 for value in (
                    self.historical_orphans, self.rejected_connectivity,
                    self.unmatched_cognitive_sources, self.unqualified_restored_nodes))):
            raise ValueError('projection_completion_board_evidence_invalid')


def require_projection_completion(*, expected_boards, boards, global_comparison):
    """Require complete source scope and every existing reconciliation predicate.

    No summary state can override a missing observation, unmatched count or
    pending historical classification. The caller must rederive all evidence;
    literal preservation/restoration alone never qualifies cognitive authority.
    """
    if (type(expected_boards) is not tuple or type(boards) is not tuple
            or len(expected_boards) > 100_000 or len(boards) != len(expected_boards)
            or any(type(key) is not str or not 1 <= len(key) <= 4096 for key in expected_boards)
            or len(set(expected_boards)) != len(expected_boards)
            or any(type(board) is not BoardProjectionCompletion for board in boards)
            or len({board.board_id for board in boards}) != len(boards)
            or {board.board_id for board in boards} != set(expected_boards)):
        raise ValueError('projection_completion_scope_invalid')
    if type(global_comparison) is not GlobalProjectionComparison:
        raise ValueError('projection_completion_global_evidence_required')
    counts = (global_comparison.expected_nodes, global_comparison.matched_nodes,
        global_comparison.missing_nodes, global_comparison.changed_nodes,
        global_comparison.unexpected_nodes, global_comparison.missing_relations,
        global_comparison.unexpected_relations)
    if (any(type(value) is not int or not 0 <= value <= 500_000 for value in counts)
            or type(global_comparison.expected_sha256) is not str
            or re.fullmatch('[0-9a-f]{64}', global_comparison.expected_sha256) is None):
        raise ValueError('projection_completion_global_evidence_invalid')
    if (global_comparison.state != 'matched' or any(counts[2:])
            or counts[0] != counts[1]):
        raise ValueError('projection_completion_global_pending')
    for board in boards:
        history, relations = board.history, board.relations
        history_counts = (history.current_source_node_count, history.unclassified_node_count,
            history.current_relation_count, history.unclassified_relation_count)
        if (any(type(value) is not int or not 0 <= value <= 500_000 for value in history_counts)
                or type(history.reasons) is not tuple):
            raise ValueError('projection_completion_history_evidence_invalid')
        if (history.state not in {'not_applicable', 'current_source_reconciled'}
                or history.reasons or history.unclassified_node_count or history.unclassified_relation_count
                or (history.state == 'not_applicable' and any(history_counts))):
            raise ValueError('projection_completion_history_pending')
        if (relations.expected_count != relations.matched_count or any((relations.missing_count,
                relations.unresolved_count, relations.unexpected_new_count, relations.unplanned_existing_count,
                relations.duplicate_expected_count)) or relations.issues or relations.issues_truncated):
            raise ValueError('projection_completion_relations_pending')
        if board.historical_orphans or board.rejected_connectivity:
            raise ValueError('projection_completion_connectivity_pending')
        if board.unmatched_cognitive_sources or board.unqualified_restored_nodes:
            raise ValueError('projection_completion_cognitive_pending')
