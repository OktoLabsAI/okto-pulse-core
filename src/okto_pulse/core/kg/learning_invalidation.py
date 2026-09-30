"""Fenced invalidation of a proved obsolete authored Learning association."""
from functools import partial
from dataclasses import dataclass

from okto_pulse.core.application.learning_invalidation import qualify_obsolete_learning_association
from okto_pulse.core.kg.blocking_io import run_blocking_graph_io
from okto_pulse.core.kg.guarded_write import guarded_board_write
from okto_pulse.core.kg.interfaces.graph_errors import GraphCapabilityUnavailable
from okto_pulse.core.kg.interfaces.graph_transaction import LearningAssociationInvalidationTransaction
from okto_pulse.core.ports.bug_cognitive_context import (
    CanonicalBugNodeResolver, resolve_canonical_bug_node_read_port,
)
from okto_pulse.core.services.application_kg import get_current_provider_registry


@dataclass(frozen=True)
class LearningInvalidationResult:
    changed: bool
    supported: bool = False


async def invalidate_obsolete_learning_association(scope_factory, *, board_id, work,
    fingerprint, actor_id, acknowledge):
    from okto_pulse.core.kg.primitives import run_cancellation_atomic

    return await run_cancellation_atomic(_invalidate(scope_factory, board_id=board_id,
        work=work, fingerprint=fingerprint, actor_id=actor_id, acknowledge=acknowledge),
        task_name='core.kg.learning_invalidation_commit')


async def _invalidate(scope_factory, *, board_id, work, fingerprint, actor_id, acknowledge):
    """Acknowledge only after durable mutation; compensate on a lost ledger CAS.

    The selected work is an input, not authority. Re-read semantic source and
    full authored provenance inside the writer/source fences. An older receipt
    cannot remove a pair supported by the most recent capture for that origin.
    """
    from okto_pulse.core.kg.cognitive_closeout_production import COGNITIVE_CLOSEOUT_COMMIT_OPERATION

    provider = get_current_provider_registry().graph_transaction
    resolver = resolve_canonical_bug_node_read_port()
    if provider is None or not isinstance(resolver, CanonicalBugNodeResolver):
        raise GraphCapabilityUnavailable('learning_association_invalidation_unavailable')
    run = lambda fn: run_blocking_graph_io(fn, task_name='core.kg.learning_invalidation')
    with guarded_board_write(board_id, operation=COGNITIVE_CLOSEOUT_COMMIT_OPERATION,
            owner_id=actor_id, mutation_ref=f'learning-invalidation:{work.learning_id}:{fingerprint}') as lease:
        async with scope_factory() as context:
            obsolete = await qualify_obsolete_learning_association(context,
                board_id=board_id, work=work, fingerprint=fingerprint)
            target = await run(partial(resolver.resolve_current, board_id=board_id, bug_id=work.bug_id))
            if target is None:
                raise ValueError('learning_capture_awaiting_canonical_bug')
            scope = await provider.begin(board_id)
            receipt, commit_attempted = None, False
            try:
                if not isinstance(scope, LearningAssociationInvalidationTransaction):
                    raise GraphCapabilityUnavailable('learning_association_invalidation_unavailable')
                projection = obsolete.projection
                properties = tuple(sorted(projection.head.payload))
                before = await run(partial(scope.snapshot_node_properties,
                    'Learning', work.learning_id, properties))
                if before is None:
                    raise ValueError('learning_materialization_projection_pending')
                projection.require_literal_graph_fields(before.attrs, dict(projection.head.payload))
                receipt = await run(partial(scope.snapshot_learning_invalidation, work.learning_id, target))
                if obsolete.reason is None:
                    if not receipt.removed_edges:
                        raise ValueError('learning_materialization_projection_pending')
                    await scope.rollback()
                    await run(lease.ensure_durable)
                    lease.ensure_owned(failure_phase='before_learning_support_ack')
                    return LearningInvalidationResult(acknowledge(supported=True), supported=True)
                await run(partial(scope.invalidate_learning_association, receipt))
                commit_attempted = True
                await scope.commit()
                await run(lease.ensure_durable)
                lease.ensure_owned(failure_phase='before_learning_invalidation_ack')
                if acknowledge():
                    return LearningInvalidationResult(True)
            except BaseException:
                await scope.rollback()
                if not commit_attempted:
                    raise
                # A commit may apply and then raise. Restoration is idempotent
                # whether the engine committed or rolled back that attempt.
                await _restore(provider, board_id, receipt, lease, run)
                raise
            # A substantive writer changed the work record while the graph was
            # being reconciled. Restore the exact pair and preserve that writer.
            await _restore(provider, board_id, receipt, lease, run)
            return LearningInvalidationResult(False)


async def _restore(provider, board_id, receipt, lease, run):
    scope = await provider.begin(board_id)
    try:
        await run(partial(scope.restore_learning_invalidation, receipt))
        await scope.commit()
    except BaseException:
        await scope.rollback()
        raise
    await run(partial(lease.ensure_durable, mutation_ref='learning-invalidation-compensation'))
