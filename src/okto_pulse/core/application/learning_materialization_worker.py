"""Bounded worker attempt for one durable authored capture; no extraction."""
from dataclasses import dataclass
from functools import partial
import re

from okto_pulse.core.domain.learning_closeout import LearningCaptureSelection
from okto_pulse.core.ports.bug_cognitive_context import (
    CanonicalBugNodeResolver, qualify_bug_semantic_context,
    resolve_bug_cognitive_context_assembler, resolve_canonical_bug_node_read_port,
)
from okto_pulse.core.ports.kg_cognitive_source import (
    CognitiveSourceUnavailable, FingerprintCognitiveSourceReader, require_cognitive_source_store,
)
from okto_pulse.core.ports.learning_capture import validate_learning_capture_payload


@dataclass(frozen=True)
class CaptureMaterializationAttempt:
    outcome: str
    reason: str


async def inspect_capture_work_basis(scope_factory, *, board_id, work, fingerprint):
    """Revalidate an existing projection receipt without materializing it.

    Canonical graph presence is not a source/evidence proof. The existing
    revalidator checks the selected historical capture, current source and
    signed evidence under its relational fence; no source history is rewritten.
    """
    from okto_pulse.core.application.learning_capture import revalidate_learning_capture_for_materialization

    try:
        if work.fingerprint is not None and work.fingerprint != fingerprint:
            return CaptureMaterializationAttempt('materialization_failed', 'learning_capture_work_source_mismatch')
        async with scope_factory() as context:
            await revalidate_learning_capture_for_materialization(context, board_id=board_id,
                bug_id=work.bug_id, learning_id=work.learning_id, generation=work.generation,
                expected_fingerprint=fingerprint)
        return None
    except Exception as exc:
        if isinstance(exc, ValueError) and str(exc) == 'learning_materialization_source_not_eligible':
            return CaptureMaterializationAttempt('materialization_pending', 'learning_capture_awaiting_done')
        return _materialization_failure(exc)


def _materialization_failure(exc):
    from okto_pulse.core.kg.guarded_write import GuardedWriteError
    from okto_pulse.core.kg.primitives import KGPrimitiveError

    pending = lambda reason: CaptureMaterializationAttempt('materialization_pending', reason)
    failed = lambda reason: CaptureMaterializationAttempt('materialization_failed', reason)
    if isinstance(exc, CognitiveSourceUnavailable):
        return pending(exc.failure_reason)
    if isinstance(exc, (GuardedWriteError, KGPrimitiveError)):
        return pending(exc.code) if exc.retryable else failed(exc.code)
    if isinstance(exc, (OSError, TimeoutError, ConnectionError)):
        return pending(type(exc).__name__)
    # Integrity, authority and unknown failures never become technical
    # eligibility or a semantic waiver. Preserve the work as failed.
    reason = getattr(exc, 'failure_reason', None)
    if reason is None and isinstance(exc, ValueError) and re.fullmatch(r'[a-z][a-z0-9_]{0,127}', str(exc)):
        reason = str(exc)
    if reason == 'learning_materialization_scope_replaced':
        return CaptureMaterializationAttempt('scope_replaced', reason)
    if reason == 'learning_materialization_projection_pending':
        return pending(reason)
    return failed(reason or type(exc).__name__)


async def materialize_capture_work(scope_factory, *, board_id, work, fingerprint, persister):
    from okto_pulse.core.kg.blocking_io import run_blocking_graph_io

    pending = lambda reason: CaptureMaterializationAttempt('materialization_pending', reason)
    failed = lambda reason: CaptureMaterializationAttempt('materialization_failed', reason)
    try:
        if work.fingerprint is not None and work.fingerprint != fingerprint:
            return failed('learning_capture_work_source_mismatch')
        selection = LearningCaptureSelection(learning_id=work.learning_id,
            generation=work.generation, fingerprint=fingerprint)
        store = require_cognitive_source_store()
        reader = resolve_bug_cognitive_context_assembler()
        if not isinstance(store, FingerprintCognitiveSourceReader) or reader is None:
            return pending('learning_capture_reader_unavailable')
        async with scope_factory() as context:
            capture = await store.read_fingerprint_in_context(context, board_id=board_id,
                node_id=work.learning_id, generation=work.generation, fingerprint=selection.fingerprint)
            if (capture is None or capture.record_fingerprint != selection.fingerprint
                    or capture.payload.get('source', {}).get('bug_id') != work.bug_id
                    or not validate_learning_capture_payload(dict(capture.payload), board_id=board_id,
                        node_id=capture.node_id, node_type=capture.node_type,
                        generation=capture.generation, evidence_refs=capture.evidence_refs)):
                return failed('learning_capture_work_source_mismatch')
            source = qualify_bug_semantic_context(await reader.assemble_semantic(context,
                board_id=board_id, bug_id=work.bug_id))
            if not source.verified:
                return pending('learning_capture_source_unavailable')
            if source.board_id != board_id or source.bug_id != work.bug_id or source.card_type != 'bug':
                return failed('learning_capture_work_scope_mismatch')
            if not source.eligible_for_closeout:
                return pending('learning_capture_awaiting_done')
        resolver = resolve_canonical_bug_node_read_port()
        if not isinstance(resolver, CanonicalBugNodeResolver):
            return pending('learning_capture_canonical_resolver_unavailable')
        target = await run_blocking_graph_io(
            partial(resolver.resolve_current, board_id=board_id, bug_id=work.bug_id),
            task_name='core.kg.learning_capture.worker_target')
        if target is None:
            return pending('learning_capture_awaiting_canonical_bug')
        # The persister revalidates current source, signed evidence, exact
        # binding and graph target under its own fences before mutation.
        confirmed = await persister.persist_authored_learning(board_id, work.bug_id, selection,
            raise_failures=True)
        return (CaptureMaterializationAttempt('persisted', 'authored_capture_materialized')
            if confirmed else pending('learning_capture_projection_not_confirmed'))
    except Exception as exc:
        return _materialization_failure(exc)
