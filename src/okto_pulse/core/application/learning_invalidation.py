"""Qualify known obsolete applicability without changing authored history."""
from dataclasses import dataclass

from okto_pulse.core.application.learning_capture import (
    resolve_learning_capture_projection, _require_current_capture_evidence,
)
from okto_pulse.core.domain.learning_closeout import qualify_learning_materialization_basis
from okto_pulse.core.domain.learning_materialization import CapturedLearningProjection
from okto_pulse.core.ports.application_persistence import get_application_persistence_port
from okto_pulse.core.ports.bug_cognitive_context import (
    BugSemanticWriteSnapshotReader, qualify_bug_semantic_context,
    resolve_bug_cognitive_context_assembler,
)
from okto_pulse.core.ports.kg_cognitive_source import (
    FingerprintCognitiveSourceReader, TransactionalCognitiveSourceReader,
    require_cognitive_source_store,
)


@dataclass(frozen=True)
class LearningAssociationBasis:
    projection: CapturedLearningProjection
    reason: str | None


async def qualify_obsolete_learning_association(context, *, board_id, work, fingerprint):
    """Retain the caller's relational fence until its graph mutation is final.

    A later capture for the same Bug may still support the pair. Walk the
    verified authored chain and assess its most recent scope before pruning.
    Missing, corrupt or unavailable sources are never an empty active set.
    """
    reader = resolve_bug_cognitive_context_assembler()
    store = require_cognitive_source_store()
    if (not isinstance(reader, BugSemanticWriteSnapshotReader)
            or not isinstance(store, TransactionalCognitiveSourceReader)
            or not isinstance(store, FingerprintCognitiveSourceReader)):
        raise ValueError('learning_capture_transaction_capability_unavailable')
    if work.fingerprint != fingerprint:
        raise ValueError('learning_capture_work_source_mismatch')
    source = qualify_bug_semantic_context(await reader.assemble_semantic_for_write(
        context, board_id=board_id, bug_id=work.bug_id))
    if not source.verified or source.board_id != board_id or source.bug_id != work.bug_id:
        raise ValueError('learning_capture_source_changed_or_unavailable')
    head = await store.read_latest_in_context(context, board_id=board_id,
        node_id=work.learning_id, generation=work.generation)
    capture = await store.read_fingerprint_in_context(context, board_id=board_id,
        node_id=work.learning_id, generation=work.generation, fingerprint=fingerprint)
    if (head is None or capture is None or capture.record_fingerprint != fingerprint
            or capture.payload.get('source', {}).get('bug_id') != work.bug_id):
        raise ValueError('learning_capture_selected_record_unavailable')
    projection = await resolve_learning_capture_projection(context, store,
        capture=capture, head=head, bug_id=work.bug_id)
    if projection.is_initial or projection.association_capture is None:
        raise ValueError('learning_materialization_projection_pending')
    persistence = get_application_persistence_port()
    bug = await persistence.get(context, entity='card', record_id=work.bug_id)
    if bug is None:
        raise ValueError('learning_capture_source_changed_or_unavailable')
    bug = await persistence.refresh(context, bug)
    if bug.board_id != board_id or str(getattr(bug.status, 'value', bug.status)) != source.status:
        raise ValueError('learning_capture_source_changed_or_unavailable')
    try:
        qualify_learning_materialization_basis(projection.association_capture, source,
            getattr(bug, 'learning_closeout_bindings', None))
    except ValueError as exc:
        if str(exc) in {'learning_materialization_source_not_eligible',
                'learning_materialization_current_binding_required'}:
            return LearningAssociationBasis(projection, str(exc))
        raise
    _require_current_capture_evidence(source, projection.association_capture)
    return LearningAssociationBasis(projection, None)
