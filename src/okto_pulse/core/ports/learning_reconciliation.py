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
class LearningReconciliationExecutionPlan:
    selections: tuple[LearningReconciliationSelection, ...]
    work_refs: tuple[str, ...]


def plan_learning_reconciliation_execution(*, schema, board_id, records, nodes):
    """Order pending reuse before older associations without changing selection history."""
    from okto_pulse.core.application.learning_reconciliation import execution_plan
    return execution_plan(schema=schema, board_id=board_id, records=records, nodes=nodes)


@dataclass(frozen=True, slots=True)
class LearningReconciliationExecution:
    """One internal execution, not an admission or history-ownership receipt."""

    board_id: str
    work_ref: str
    consolidation_session_id: str | None
    materialized: bool


@dataclass(frozen=True, slots=True)
class LearningReconciliationSourceBasis:
    bug_id: str
    learning_id: str
    generation: int
    capture_fingerprint: str
    audit_content_hash: str
    scoped_target_id: str | None


async def learning_reconciliation_source_basis(context, store, *, execution):
    """Derive audit identity from verified authorship, without write authority."""
    from okto_pulse.core.application.learning_reconciliation import source_basis
    return await source_basis(context, store, execution=execution)


async def qualify_learning_reconciliation_graph_delta(context, store, *, schema, execution,
        before_nodes, before_relations, after_nodes, after_relations):
    """Classify one execution's complete portable graph delta against source history.

    The edition authenticates both inventories and SQL ownership separately.
    This comparison grants no permission, current applicability or completion.
    """
    from okto_pulse.core.application.learning_reconciliation_graph import qualify
    return await qualify(context, store, schema=schema, execution=execution,
        before_nodes=before_nodes, before_relations=before_relations,
        after_nodes=after_nodes, after_relations=after_relations)


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


def qualify_learning_reconciliation_debt_change(*, before, after, execution):
    """Recognize only the writer's exact technical-debt transition.

    The edition separately proves that execution/audit/graph/evidence belong to
    the private candidate. This pure comparison cannot grant or close a hold.
    """
    from okto_pulse.core.application.learning_reconciliation import qualify_debt_change
    return qualify_debt_change(before=before, after=after, execution=execution)


async def require_learning_reconciliation_source_append(context, store, *, appended, execution):
    """Check authored lineage for one committed candidate source append batch.

    The edition supplies the complete, verified history as of this execution,
    including both sides of a scoped replacement atomically. It separately
    proves that these are the only new rows and binds the session to its audit.
    This read-only check grants neither current applicability nor admission.
    """
    from okto_pulse.core.application.learning_reconciliation import require_source_append
    await require_source_append(context, store, appended=appended, execution=execution)


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
