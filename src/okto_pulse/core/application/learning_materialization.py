"""Bind the existing governed consolidation commit to a durable authored capture."""

from okto_pulse.core.application.learning_capture import revalidate_learning_capture_for_materialization
from okto_pulse.core.domain.learning_materialization import (
    LEARNING_CAPTURE_CANDIDATE_ID, learning_capture_projection_fields,
)
from okto_pulse.core.kg.schemas import NodeCandidate
from okto_pulse.core.ports.kg_cognitive_source import (
    ConditionalCognitiveSourceWriter, require_cognitive_source_store,
)


def authored_learning_candidate(capture, projection=None) -> NodeCandidate:
    return NodeCandidate(candidate_id=LEARNING_CAPTURE_CANDIDATE_ID, node_type='Learning',
        **(projection.fields if projection is not None else learning_capture_projection_fields(capture)))


def require_capture_candidates(projection, candidates, edges):
    expected = authored_learning_candidate(projection.capture, projection)
    if (set(candidates) != {LEARNING_CAPTURE_CANDIDATE_ID}
            or candidates[LEARNING_CAPTURE_CANDIDATE_ID].model_dump() != expected.model_dump()
            or getattr(candidates[LEARNING_CAPTURE_CANDIDATE_ID], '_source_projection_metadata', None) is not None
            or len(edges) != 1):
        raise ValueError('learning_materialization_candidate_mismatch')
    edge, = edges.values()
    if (str(getattr(edge.edge_type, 'value', edge.edge_type)) != 'validates'
            or edge.from_candidate_id != LEARNING_CAPTURE_CANDIDATE_ID
            or not edge.to_candidate_id.startswith('kg:') or not edge.to_candidate_id[3:]):
        raise ValueError('learning_materialization_candidate_mismatch')


async def prepare_captured_learning_commit(context, session, selection):
    if context is None or session.artifact_type != 'bug':
        raise ValueError('learning_materialization_transaction_required')
    if not isinstance(require_cognitive_source_store(), ConditionalCognitiveSourceWriter):
        raise ValueError('learning_materialization_conditional_append_required')
    basis = await revalidate_learning_capture_for_materialization(context,
        board_id=session.board_id, bug_id=session.artifact_id,
        learning_id=selection.learning_id, generation=selection.generation,
        expected_fingerprint=selection.fingerprint)
    projection = basis.projection
    if basis.capture.generation != 0 and basis.capture.payload['intent']['kind'] != 'reuse':
        raise ValueError('learning_materialization_generation_unsupported')
    require_capture_candidates(projection, session.node_candidates, session.edge_candidates)
    return projection
