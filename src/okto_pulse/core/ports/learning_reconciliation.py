"""Internal upgrade work selection, without evidence or mutation authority."""
from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True, slots=True)
class LearningReconciliationSelection:
    node_id: str
    generations: tuple[int, ...]
    state: Literal['awaiting_revalidation', 'source_unavailable', 'ambiguous_generation']
    reasons: tuple[str, ...]
    work_refs: tuple[str, ...] = ()


def select_learning_reconciliation(*, schema, board_id, records, nodes):
    """Select bounded historical work from an authenticated offline snapshot.

    Validates all source revisions before filtering and selects the most recent
    explicit capture per origin, never a narrative hash or inferred association.
    The edition owns the offline fences and complete graph/source census. Each
    returned work reference still requires the ordinary source/evidence/binding
    and immutable lineage checks inside the governed materializer. This does
    not close a hold, grant admission, alter history, or certify applicability.
    """
    from okto_pulse.core.application.learning_reconciliation import select
    return select(schema=schema, board_id=board_id, records=records, nodes=nodes)
