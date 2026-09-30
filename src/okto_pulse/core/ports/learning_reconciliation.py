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


@dataclass(frozen=True, slots=True)
class LearningReconciliationExecution:
    """One internal execution, not an admission or history-ownership receipt."""

    board_id: str
    work_ref: str
    consolidation_session_id: str | None
    materialized: bool


async def execute_learning_reconciliation(*, board_id, work_ref, relational_scope_factory):
    """Execute exact selected authorship through the existing governed writer.

    Trusted composition owns the private candidate and its offline fences. The
    returned session identifies this invocation's committed SQL effects, never
    a session inferred from graph presence or an older cognitive source row.
    All source/evidence/binding/lineage and ordinary write guards still apply.
    A failed invocation may have committed effects in the private candidate;
    callers must discard or independently verify it, not treat failure as no-op.
    This is not a public maintenance surface or a completion certificate.
    """
    from okto_pulse.core.application.learning_reconciliation import execute
    return await execute(board_id=board_id, work_ref=work_ref,
        relational_scope_factory=relational_scope_factory)


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
