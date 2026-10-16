"""Resolve scoped replacement evidence through public transactional ports."""
from dataclasses import replace
from okto_pulse.core.domain.learning_supersedence import (
    scope_reference_additions, qualify_learning_scope_replacement,
    prepare_learning_scope_replacement,
)
from okto_pulse.core.ports.kg_cognitive_source import (
    TransactionalCognitiveHistoryReader, latest_cognitive_source_records,
    ConditionalCognitiveSourceWriter, BoundedCognitiveHistoryReader, CognitiveSourceUnavailable,
)
from okto_pulse.core.ports.learning_capture import is_scoped_learning_supersede, LearningCaptureTargetConflict


class LearningScopeHistoryPageReader:
    """Read historical linkage for one capture page, without currentness claims.

    Histories are cached only inside this request/UOW. The 200-record budget is
    shared by all lineage lookups on the page; an overflow never looks complete.
    No graph projection, source applicability, permission or write is inferred.
    """
    def __init__(self, context, store):
        self.context, self.store = context, store
        self.remaining = 200
        self.histories = {}

    async def _history(self, board_id, node_id, generation):
        identity = (board_id, node_id, generation)
        if identity in self.histories:
            return self.histories[identity]
        if self.remaining == 0:
            raise CognitiveSourceUnavailable('cognitive_source_history_limit', board_id=board_id, node_id=node_id)
        try:
            records = await self.store.read_bounded_history_in_context(self.context,
                board_id=board_id, node_id=node_id, generation=generation, max_records=self.remaining)
        except CognitiveSourceUnavailable as exc:
            if exc.failure_reason == 'cognitive_source_history_limit':
                self.remaining = 0
            raise
        if (type(records) is not tuple or len(records) > self.remaining
                or any((row.board_id, row.node_type, row.node_id, row.generation)
                    != (board_id, 'Learning', node_id, generation) for row in records)
                or any(left.source_revision >= right.source_revision for left, right in zip(records, records[1:]))):
            raise ValueError('learning_scope_history_invalid')
        latest_cognitive_source_records(records)
        self.remaining -= len(records)
        self.histories[identity] = records
        return records

    async def lineage(self, capture):
        if not is_scoped_learning_supersede(capture.payload):
            return None
        intent = capture.payload['intent']
        result = dict(contract_version='learning-scope-history/v1', scope='source_bug',
            bug_id=capture.payload['source']['bug_id'], capture_fingerprint=capture.record_fingerprint,
            target=dict(learning_id=intent['target_node_id'], generation=intent['target_generation'],
                fingerprint=intent['expected_fingerprint']), state='unverified', limitation='not_recorded',
            current_applicability='not_assessed', graph_projection='not_assessed')
        if not isinstance(self.store, BoundedCognitiveHistoryReader):
            return {**result, 'limitation': 'history_capability_unavailable'}
        try:
            target = await self._history(capture.board_id, intent['target_node_id'], intent['target_generation'])
            successor_history = await self._history(capture.board_id, capture.node_id, capture.generation)
        except CognitiveSourceUnavailable as exc:
            if exc.failure_reason != 'cognitive_source_history_limit':
                raise
            return {**result, 'limitation': 'history_limit'}
        matches = [(previous, claimed) for previous, claimed, reference in scope_reference_additions(target)
            if (reference.node_id, reference.generation, reference.fingerprint)
                == (capture.node_id, capture.generation, capture.record_fingerprint)]
        exact_capture = [row for row in successor_history if row.record_fingerprint == capture.record_fingerprint]
        successors = [row for row in successor_history if row.source_revision == capture.source_revision + 1]
        if (len(exact_capture) != 1 or exact_capture[0].source_revision != capture.source_revision
                or exact_capture[0].payload != capture.payload or exact_capture[0].evidence_refs != capture.evidence_refs):
            raise ValueError('learning_scope_history_invalid')
        if not target:
            return {**result, 'limitation': 'target_history_unavailable'}
        if not matches and not successors:
            return result
        if len(matches) != 1 or len(successors) != 1:
            raise ValueError('learning_scope_claim_invalid')
        previous, claimed = matches[0]
        replacement = qualify_learning_scope_replacement(previous=previous, claimed=claimed,
            capture=capture, successor=successors[0])
        return {**result, 'state': 'recorded', 'limitation': None,
            'target_claim': {'revision': claimed.source_revision, 'fingerprint': claimed.record_fingerprint},
            'successor_birth': {'revision': replacement.successor.source_revision,
                'fingerprint': replacement.successor.record_fingerprint}}


async def bind_learning_scope_materialization(context, store, *, projection, source):
    """Fence the selected scope's exact target, or prove its committed claim.

    Replay may observe later target history, but never rewrites it. The final
    joint CAS still compares this observed target head with the successor head.
    """
    capture = projection.capture
    if not is_scoped_learning_supersede(capture.payload):
        return projection
    intent = capture.payload['intent']
    target = await store.read_latest_in_context(context, board_id=capture.board_id,
        node_id=intent['target_node_id'], generation=intent['target_generation'])
    if target is None:
        raise LearningCaptureTargetConflict(None)
    latest_cognitive_source_records((target,))
    if (target.board_id, target.node_type, target.node_id, target.generation) != (
            capture.board_id, 'Learning', intent['target_node_id'], intent['target_generation']):
        raise ValueError('learning_scope_claim_invalid')
    if 'capture_format' in target.payload:
        raise ValueError('learning_materialization_projection_pending')
    if (target.payload.get('graph_layer') != 'canonical'
            or target.payload.get('maturity_status') != 'canonical_eligible'
            or target.payload.get('superseded_by') or target.payload.get('revocation_reason')
            or any(type(target.payload.get(key)) is not str or not target.payload[key].strip()
                for key in ('content', 'context', 'created_by_agent', 'created_at'))):
        raise ValueError('learning_capture_target_not_eligible')
    claims = await read_learning_scope_replacements(context, store, head=target)
    own = [claim for claim in claims if (claim.capture.node_id, claim.capture.generation,
        claim.capture.record_fingerprint) == (capture.node_id, capture.generation, capture.record_fingerprint)]
    current = await current_learning_scope_replacement(context, claims, source=source)
    if own:
        if len(own) != 1 or current != own[0]:
            raise ValueError('learning_scope_claim_invalid')
        previous = own[0].previous
    else:
        if target.record_fingerprint != intent['expected_fingerprint']:
            raise LearningCaptureTargetConflict(target)
        if current is not None:
            raise ValueError('learning_capture_target_replaced_in_scope')
        previous = target
    return replace(projection, scope_target=previous, scope_target_head=target,
        scope_claim_committed=bool(own))


async def stage_learning_scope_replacement(context, store, *, previous, capture, successor):
    """Stage both source revisions in the caller's one conditional UOW.

    This internal write is not admission: the governed caller must already
    hold the source fence and qualify current applicability, target history and
    the graph projection. It must compensate the graph on a late CAS/UOW failure.
    No independent commit, event, permission or capture re-authoring occurs here.
    """
    if context is None or not isinstance(store, ConditionalCognitiveSourceWriter):
        raise ValueError('learning_materialization_conditional_append_required')
    replacement = prepare_learning_scope_replacement(previous=previous,
        capture=capture, successor=successor)
    await store.append_many_if_current_in_context(context, (replacement.successor, replacement.claimed),
        expected_fingerprints=(capture.record_fingerprint, previous.record_fingerprint))
    return replacement


async def read_learning_scope_replacements(context, store, *, head):
    if not isinstance(store, TransactionalCognitiveHistoryReader):
        raise ValueError('learning_scope_history_unavailable')
    history = await store.read_history_in_context(context, board_id=head.board_id,
        node_id=head.node_id, generation=head.generation)
    if (not history or history[-1].record_fingerprint != head.record_fingerprint
            or history[-1].source_revision != head.source_revision
            or any((record.board_id, record.node_type, record.node_id, record.generation)
                != (head.board_id, 'Learning', head.node_id, head.generation) for record in history)):
        raise ValueError('learning_scope_history_changed')
    claims, loaded = [], {}
    for previous, claimed, reference in scope_reference_additions(history):
        identity = (reference.node_id, reference.generation)
        if identity not in loaded:
            loaded[identity] = await store.read_history_in_context(context, board_id=head.board_id,
                node_id=reference.node_id, generation=reference.generation)
        records = loaded[identity]
        latest_cognitive_source_records(records)
        if any((record.board_id, record.node_type, record.node_id, record.generation)
                != (head.board_id, 'Learning', *identity) for record in records):
            raise ValueError('learning_scope_claim_invalid')
        captures = [record for record in records if record.record_fingerprint == reference.fingerprint]
        if len(captures) != 1:
            raise ValueError('learning_scope_claim_invalid')
        capture, = captures
        successors = [record for record in records if record.source_revision == capture.source_revision + 1]
        if len(successors) != 1:
            raise ValueError('learning_scope_claim_invalid')
        claims.append(qualify_learning_scope_replacement(previous=previous, claimed=claimed,
            capture=capture, successor=successors[0]))
    scopes = [claim.source_basis for claim in claims]
    if len(scopes) != len(set(scopes)):
        raise ValueError('learning_scope_claim_ambiguous')
    return tuple(claims)


async def current_learning_scope_replacement(context, replacements, *, source):
    """Resolve applicability while the caller holds the semantic source fence."""
    from okto_pulse.core.domain.learning_supersedence import learning_scope_replacement_is_current
    from okto_pulse.core.ports.application_persistence import get_application_persistence_port

    relevant = tuple(claim for claim in replacements if claim.bug_id == source.bug_id)
    if not relevant:
        return None
    persistence = get_application_persistence_port()
    bug = await persistence.get(context, entity='card', record_id=source.bug_id)
    if bug is None:
        raise ValueError('learning_capture_source_changed_or_unavailable')
    bug = await persistence.refresh(context, bug)
    if (bug.board_id != source.board_id
            or str(getattr(bug.status, 'value', bug.status)) != source.status):
        raise ValueError('learning_capture_source_changed_or_unavailable')
    current = [claim for claim in relevant if learning_scope_replacement_is_current(
        claim, source, getattr(bug, 'learning_closeout_bindings', None))]
    if len(current) > 1:
        raise ValueError('learning_scope_claim_ambiguous')
    if current:
        # A matching digest/binding is not a substitute for checking the
        # original signed evidence against its present verifier/ledger state.
        from okto_pulse.core.application.learning_capture import _require_current_capture_evidence
        _require_current_capture_evidence(source, current[0].capture)
    return current[0] if current else None
