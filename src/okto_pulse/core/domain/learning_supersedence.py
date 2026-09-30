"""Qualify explicit, source-scoped replacement evidence in immutable history.

No relationship is authorized by an opaque ref alone. The capture, target
predecessor, target claim revision and successor birth must agree exactly.
These checks do not attest current Bug eligibility or perform a graph write.
"""
from dataclasses import dataclass, replace

from okto_pulse.core.domain.learning_materialization import CapturedLearningProjection
from okto_pulse.core.ports.kg_cognitive_source import CognitiveSourceRecord, latest_cognitive_source_records
from okto_pulse.core.ports.learning_capture import (
    LEARNING_SCOPED_CAPTURE_FORMAT, LearningCaptureSourceRef,
    parse_learning_capture_source_ref, validate_learning_capture_payload,
)


@dataclass(frozen=True)
class LearningScopeReplacement:
    previous: CognitiveSourceRecord
    claimed: CognitiveSourceRecord
    capture: CognitiveSourceRecord
    successor: CognitiveSourceRecord

    @property
    def bug_id(self) -> str:
        return self.capture.payload['source']['bug_id']

    @property
    def source_basis(self) -> tuple[str, int, str]:
        source = self.capture.payload['source']
        return self.bug_id, source['policy_version'], source['digest']


def learning_scope_replacement_is_current(replacement, source, history) -> bool:
    """Historical linkage is not applicability to a reopened correction.

    This predicate grants no materialization authority. The caller retains the
    source write fence and supplies server-owned closeout bindings. In particular,
    merely recognizing a binding does not enable supersede in a closing writer.
    """
    from okto_pulse.core.domain.learning_closeout import (
        closeout_binding_is_current, verify_learning_closeout_binding,
    )
    from okto_pulse.core.ports.bug_cognitive_context import qualify_bug_semantic_context

    source = qualify_bug_semantic_context(source)
    if not source.verified:
        raise ValueError('learning_scope_source_unavailable')
    if replacement.capture.board_id != source.board_id:
        raise ValueError('learning_scope_source_mismatch')
    if replacement.bug_id != source.bug_id:
        return False
    if history is not None and type(history) is not list:
        raise ValueError('learning_closeout_history_invalid')
    bindings, identities = [], set()
    for raw in history or []:
        binding = verify_learning_closeout_binding(raw)
        if (binding.board_id != source.board_id or binding.bug_id != source.bug_id
                or binding.transition_id in identities):
            raise ValueError('learning_closeout_history_invalid')
        identities.add(binding.transition_id)
        bindings.append(binding)
    if not source.eligible_for_closeout:
        return False
    if replacement.source_basis == (source.bug_id, source.source_policy_version, source.source_digest):
        return True
    capture = replacement.capture
    matching = [binding for binding in bindings if (
        binding.capture.learning_id == capture.node_id
        and binding.capture.generation == capture.generation
        and binding.capture.fingerprint == capture.record_fingerprint
        and binding.capture_revision == capture.source_revision
        and binding.before_digest == capture.payload['source']['digest']
        and binding.before_version == capture.payload['source']['policy_version']
        and closeout_binding_is_current(binding, source))]
    if len(matching) > 1:
        raise ValueError('learning_scope_claim_ambiguous')
    return len(matching) == 1


def _identity(record):
    return record.board_id, record.node_type, record.node_id, record.generation


def scope_reference_additions(history: tuple[CognitiveSourceRecord, ...]):
    """Find claim introductions and reject their loss in later literal heads.

    A pending capture contains its own evidence, not projected target refs;
    the next literal must still preserve every previously admitted claim.
    This distinction prevents pending reuse from looking like claim deletion.
    """
    if type(history) is not tuple:
        raise ValueError('learning_scope_history_invalid')
    latest_cognitive_source_records(history)
    if not history:
        return ()
    seen, additions, previous = set(), [], None
    identity = _identity(history[0])
    if identity[1] != 'Learning':
        raise ValueError('learning_scope_history_invalid')
    for record in history:
        if (_identity(record) != identity or (previous is not None
                and record.source_revision <= previous.source_revision)):
            raise ValueError('learning_scope_history_invalid')
        refs = [(value, parse_learning_capture_source_ref(value)) for value in record.evidence_refs]
        refs = [(value, reference) for value, reference in refs if reference is not None]
        if validate_learning_capture_payload(dict(record.payload), board_id=record.board_id,
                node_type=record.node_type, node_id=record.node_id, generation=record.generation,
                evidence_refs=record.evidence_refs):
            if refs:
                raise ValueError('learning_scope_history_invalid')
            previous = record
            continue
        current = {value for value, _ in refs}
        if len(current) != len(refs) or not seen.issubset(current):
            raise ValueError('learning_scope_history_claim_lost')
        introduced = [(value, reference) for value, reference in refs if value not in seen]
        if introduced:
            if len(introduced) != 1 or previous is None:
                raise ValueError('learning_scope_history_invalid')
            additions.append((previous, record, introduced[0][1]))
        seen = current
        previous = record
    return tuple(additions)


def qualify_learning_scope_replacement(*, previous, claimed, capture, successor) -> LearningScopeReplacement:
    latest_cognitive_source_records((previous, claimed, capture, successor))
    payload = dict(capture.payload)
    valid = validate_learning_capture_payload(payload, board_id=capture.board_id,
        node_type=capture.node_type, node_id=capture.node_id, generation=capture.generation,
        evidence_refs=capture.evidence_refs)
    if (not valid or payload['capture_format'] != LEARNING_SCOPED_CAPTURE_FORMAT
            or payload['intent']['kind'] != 'supersede' or payload['intent']['scope'] != 'source_bug'):
        raise ValueError('learning_scope_claim_invalid')
    intent = payload['intent']
    reference = LearningCaptureSourceRef(capture.node_id, capture.generation, capture.record_fingerprint).encode()
    if (_identity(previous) != (capture.board_id, 'Learning', intent['target_node_id'], intent['target_generation'])
            or _identity(claimed) != _identity(previous)
            or _identity(successor) != _identity(capture)
            or previous.record_fingerprint != intent['expected_fingerprint']
            or claimed.source_revision != previous.source_revision + 1
            or successor.source_revision != capture.source_revision + 1
            or 'capture_format' in previous.payload or 'capture_format' in successor.payload
            or previous.payload.get('graph_layer') != 'canonical'
            or previous.payload.get('maturity_status') != 'canonical_eligible'
            or previous.payload.get('superseded_by') or previous.payload.get('revocation_reason')
            or any(type(previous.payload.get(key)) is not str or not previous.payload[key].strip()
                   for key in ('content', 'context', 'created_by_agent', 'created_at'))
            or dict(claimed.payload) != dict(previous.payload)
            or reference in previous.evidence_refs
            or claimed.evidence_refs != (*previous.evidence_refs, reference)):
        raise ValueError('learning_scope_claim_invalid')
    plan = CapturedLearningProjection(capture, successor, payload['source']['bug_id'])
    plan.require_authored_graph_fields(successor.payload)
    if successor.evidence_refs != plan.evidence_refs:
        raise ValueError('learning_scope_claim_invalid')
    return LearningScopeReplacement(previous, claimed, capture, successor)


def prepare_learning_scope_replacement(*, previous, capture, successor) -> LearningScopeReplacement:
    """Derive the exact target revision from admitted authored provenance.

    The original target payload and all prior evidence remain byte-for-byte
    semantic inputs. No node field is repurposed to carry scope or retirement.
    The caller must qualify the current source and target history separately.
    """
    reference = LearningCaptureSourceRef(capture.node_id, capture.generation, capture.record_fingerprint).encode()
    claimed = replace(previous, source_revision=previous.source_revision + 1, record_fingerprint='',
        evidence_refs=(*previous.evidence_refs, reference),
        source_session_id=successor.source_session_id, committed_at=successor.committed_at)
    return qualify_learning_scope_replacement(previous=previous, claimed=claimed,
        capture=capture, successor=successor)
