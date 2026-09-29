"""Stage authored Learning content, never infer it or certify implementation."""

from datetime import datetime, timezone
from dataclasses import asdict, dataclass, replace
import json

from okto_pulse.core.kg.node_identity import mint_node_id
from okto_pulse.core.ports.bug_cognitive_context import (
    BugSemanticWriteSnapshotReader, qualify_bug_semantic_context,
    BugCognitiveContext,
    resolve_bug_cognitive_context_assembler,
)
from okto_pulse.core.ports.kg_cognitive_source import (
    CognitiveSourceRecord, CognitiveSourceConflict, ConditionalCognitiveSourceWriter, TransactionalCognitiveSourceReader,
    FingerprintCognitiveSourceReader,
    require_cognitive_source_store, latest_cognitive_source_records,
)
from okto_pulse.core.ports.learning_capture import (
    CreateLearningCapture, LearningCaptureHistoryReader, LearningCaptureTargetConflict,
    LearningCaptureIdentityReservation, validate_learning_capture_payload,
)
from okto_pulse.core.services.test_scenario_lifecycle import scenario_has_authenticated_required_evidence
from okto_pulse.core.domain.learning_materialization import CapturedLearningProjection


def _authenticated_scenario(source, scenario):
    evidence = (scenario.get('evidence') or scenario.get('latest_evidence')) if scenario else None
    return bool(source.spec_id and isinstance(evidence, dict) and evidence.get('execution_receipt')
        and scenario_has_authenticated_required_evidence(board_id=source.board_id,
            spec_id=source.spec_id, scenario=scenario, acceptance_criteria=list(source.acceptance_criteria)))


async def get_learning_capture_source(context, *, board_id: str, bug_id: str):
    reader = resolve_bug_cognitive_context_assembler()
    if reader is None:
        raise ValueError('learning_capture_transaction_capability_unavailable')
    source = qualify_bug_semantic_context(await reader.assemble_semantic(context, board_id=board_id, bug_id=bug_id))
    if not source.verified or source.board_id != board_id or source.bug_id != bug_id:
        raise ValueError('learning_capture_source_changed_or_unavailable')
    scenarios = []
    identities = set()
    for row in source.test_scenarios:
        identity = row.get('id')
        if type(identity) is not str or not identity or identity in identities:
            raise ValueError('learning_capture_evidence_ambiguous')
        identities.add(identity)
        scenario = dict(row)
        scenarios.append({'id': identity, 'title': str(scenario.get('title') or identity),
            'status': str(scenario.get('status') or ''), 'verification_method': scenario.get('verification_method'),
            'authenticated': _authenticated_scenario(source, scenario)})
    return {'contract_version': 'learning-capture-context/v1', 'board_id': board_id, 'bug_id': bug_id,
        'source_digest': source.source_digest, 'source_policy_version': source.source_policy_version,
        'scenarios': scenarios}


async def list_learning_captures(context, *, board_id: str, bug_id: str, cursor=None, limit=20):
    store = require_cognitive_source_store()
    if not isinstance(store, LearningCaptureHistoryReader):
        raise ValueError('learning_capture_history_unavailable')
    if (type(limit) is not int or not 1 <= limit <= 50
            or (cursor is not None and (type(cursor) is not str or not 1 <= len(cursor) <= 4096))):
        raise ValueError('learning_capture_page_invalid')
    page = await store.read_capture_history_in_context(context, board_id=board_id, bug_id=bug_id,
        cursor=cursor, limit=limit)
    if len(page.records) > 200:
        raise ValueError('learning_capture_history_limit')
    latest_cognitive_source_records(page.records)
    items = []
    for record in page.records:
        payload = dict(record.payload)
        if (record.board_id != board_id or not validate_learning_capture_payload(payload,
                board_id=board_id, node_type=record.node_type, node_id=record.node_id,
                generation=record.generation, evidence_refs=record.evidence_refs)
                or payload['source']['bug_id'] != bug_id):
            raise ValueError('learning_capture_history_unavailable')
        items.append({'learning_id': record.node_id, 'generation': record.generation,
            'source_revision': record.source_revision, 'fingerprint': record.record_fingerprint,
            'capture': payload})
    if len(json.dumps(items, ensure_ascii=False).encode('utf-8')) > 8 * 1024 * 1024:
        raise ValueError('learning_capture_history_limit')
    # A historical capture alone does not prove current applicability or
    # projection state. Consumers compare a fresh preview explicitly.
    return {'contract_version': 'learning-capture-history/v1', 'board_id': board_id,
        'bug_id': bug_id, 'items': items, 'next_cursor': page.next_cursor}


async def stage_new_learning_capture(context, request: CreateLearningCapture, *, author_id: str, captured_at: datetime):
    """Called only after the application boundary's complete authorization.

    A returned record is staged, not committed. The same caller UOW must own
    rollback/commit; no graph operation or Done transition occurs here.
    """
    if type(request) is not CreateLearningCapture or not author_id:
        raise ValueError('learning_capture_request_invalid')
    reader = resolve_bug_cognitive_context_assembler()
    store = require_cognitive_source_store()
    if (not isinstance(reader, BugSemanticWriteSnapshotReader)
            or not isinstance(store, ConditionalCognitiveSourceWriter)
            or not isinstance(store, TransactionalCognitiveSourceReader)
            or not isinstance(store, LearningCaptureIdentityReservation)):
        raise ValueError('learning_capture_transaction_capability_unavailable')
    source = qualify_bug_semantic_context(await reader.assemble_semantic_for_write(
        context, board_id=request.board_id, bug_id=request.bug_id))
    if (not source.verified or source.board_id != request.board_id or source.bug_id != request.bug_id
            or source.source_digest != request.expected_source_digest
            or source.source_policy_version != request.expected_source_version):
        raise ValueError('learning_capture_source_changed_or_unavailable')
    scenarios = {}
    for scenario in source.test_scenarios:
        identity = scenario.get('id')
        if identity in scenarios:
            raise ValueError('learning_capture_evidence_ambiguous')
        scenarios[identity] = dict(scenario)
    refs = []
    for identity in request.scenario_ids:
        scenario = scenarios.get(identity)
        # The shared consumer preserves structural legacy evidence for old
        # workflows. New capture references require an authenticated receipt.
        if not _authenticated_scenario(source, scenario):
            raise ValueError('learning_capture_evidence_not_authenticated')
        refs.append(f'spec:{source.spec_id}:test_scenario:{identity}')
    node_id = mint_node_id(request.board_id, 'Learning',
        'capture:' + json.dumps([author_id, request.capture_id], separators=(',', ':')), 0)
    generation, target = 0, None
    if request.intent.kind == 'reuse':
        node_id, generation = request.intent.target_node_id, request.intent.target_generation
    prior = await store.reserve_capture_identity_in_context(context, board_id=request.board_id,
        author_id=author_id, capture_id=request.capture_id)
    payload = {'capture_format': 'learning-capture/v1', 'capture_id': request.capture_id,
        'author_id': author_id, 'captured_at': captured_at.isoformat(),
        'content': request.content, 'context': request.context, 'applicability': request.applicability,
        'source': {'board_id': source.board_id, 'bug_id': source.bug_id,
            'policy_version': source.source_policy_version, 'digest': source.source_digest, 'evidence_refs': refs},
        'intent': asdict(request.intent)}
    validate_learning_capture_payload(payload, board_id=source.board_id, node_type='Learning',
        node_id=node_id, generation=generation, evidence_refs=refs)
    if prior is not None:
        if not validate_learning_capture_payload(dict(prior.payload), board_id=request.board_id,
                node_type=prior.node_type, node_id=prior.node_id, generation=prior.generation, evidence_refs=prior.evidence_refs):
            raise ValueError('learning_capture_idempotency_conflict')
        previous = dict(prior.payload)
        previous.pop('captured_at', None)
        comparison = {key: value for key, value in payload.items() if key != 'captured_at'}
        if ((prior.board_id, prior.node_type, prior.node_id, prior.generation)
                != (request.board_id, 'Learning', node_id, generation) or previous != comparison):
            raise ValueError('learning_capture_idempotency_conflict')
        return prior
    if request.intent.kind != 'create':
        target = await store.read_latest_in_context(context, board_id=request.board_id,
            node_id=request.intent.target_node_id, generation=request.intent.target_generation)
        if target is None or target.record_fingerprint != request.intent.expected_fingerprint:
            raise LearningCaptureTargetConflict(target)
        _require_learning_intent_target(request, target)
    record = CognitiveSourceRecord(board_id=source.board_id, node_type='Learning', node_id=node_id,
        generation=generation, payload=payload, evidence_refs=tuple(refs),
        source_revision=target.source_revision + 1 if request.intent.kind == 'reuse' else 0,
        source_session_id='capture:' + request.capture_id, committed_at=captured_at.isoformat())
    try:
        if request.intent.kind == 'supersede':
            # Compare both identities in the edition's one atomic batch. Replaying
            # the unchanged target is a semantic no-op, not a target mutation.
            await store.append_many_if_current_in_context(context, (record, target),
                expected_fingerprints=(None, target.record_fingerprint))
        else:
            await store.append_many_if_current_in_context(context, (record,),
                expected_fingerprints=(target.record_fingerprint if target is not None else None,))
    except CognitiveSourceConflict as exc:
        if (target is None or exc.failure_reason != 'cognitive_source_head_changed'
                or exc.board_id != request.board_id or exc.node_id != target.node_id):
            raise
        current = await store.read_latest_in_context(context, board_id=request.board_id,
            node_id=target.node_id, generation=target.generation)
        raise LearningCaptureTargetConflict(current) from exc
    from okto_pulse.core.events.bus import publish
    from okto_pulse.core.events.types import LearningCaptureAdmitted
    from okto_pulse.core.domain.learning_closeout import LearningCaptureSelection
    # Admission and its work notification share this caller-owned transaction.
    # The engine emits the event; authorship remains explicit in the capture.
    # Exact retries returned above and never mint a second work notification.
    await publish(LearningCaptureAdmitted(board_id=source.board_id, bug_id=source.bug_id,
        actor_type='system', capture_author_id=author_id, occurred_at=captured_at,
        capture=LearningCaptureSelection(learning_id=record.node_id,
            generation=record.generation, fingerprint=record.record_fingerprint)), session=context)
    return record


def _require_learning_intent_target(request, target):
    """Admission of a target reference, not its graph/applicability proof."""
    latest_cognitive_source_records((target,))
    intent, payload = request.intent, target.payload
    if ((target.board_id, target.node_type, target.node_id, target.generation)
            != (request.board_id, 'Learning', intent.target_node_id, intent.target_generation)
            or target.record_fingerprint != intent.expected_fingerprint
            or 'capture_format' in payload or payload.get('graph_layer') != 'canonical'
            or payload.get('maturity_status') != 'canonical_eligible'
            or payload.get('superseded_by') or payload.get('revocation_reason')
            or any(type(payload.get(key)) is not str or not payload[key].strip()
                   for key in ('content', 'context', 'created_by_agent', 'created_at'))):
        raise ValueError('learning_capture_target_not_eligible')
    if intent.kind == 'reuse' and request.content != payload['content']:
        raise ValueError('learning_capture_reuse_content_changed')


@dataclass(frozen=True)
class LearningMaterializationBasis:
    """Current relational admission only, not graph admission or persistence."""

    capture: CognitiveSourceRecord
    source: BugCognitiveContext
    closeout_transition_id: str | None
    head: CognitiveSourceRecord
    projection: CapturedLearningProjection


async def resolve_learning_capture_projection(context, store, *, capture, head, bug_id):
    """Prove an exact authored revision chain, without inferring associations.

    Every reuse points at an immutable literal predecessor. Walking strictly
    backwards proves that a later head retains the selected capture, and keeps
    old recovery receipts from overwriting later authored associations. This
    is projection provenance only, not current Bug applicability or authority.
    """
    latest_cognitive_source_records((capture, head))
    identity = (capture.board_id, 'Learning', capture.node_id, capture.generation)

    async def read_exact(fingerprint, before):
        if not isinstance(store, FingerprintCognitiveSourceReader):
            raise ValueError('learning_capture_history_unavailable')
        record = await store.read_fingerprint_in_context(context, board_id=capture.board_id,
            node_id=capture.node_id, generation=capture.generation, fingerprint=fingerprint)
        if (record is None or record.record_fingerprint != fingerprint
                or (record.board_id, record.node_type, record.node_id, record.generation) != identity
                or record.source_revision >= before):
            raise ValueError('learning_materialization_projection_conflict')
        latest_cognitive_source_records((record,))
        return record

    current, latest_plan = head, None
    while True:
        if (current.board_id, current.node_type, current.node_id, current.generation) != identity:
            raise ValueError('learning_materialization_projection_conflict')
        if 'capture_format' in current.payload:
            if current.record_fingerprint != capture.record_fingerprint:
                raise ValueError('learning_materialization_projection_pending')
            owner = current
        else:
            fingerprint = current.payload.get('source_content_hash')
            owner = (capture if fingerprint == capture.record_fingerprint else
                await read_exact(fingerprint, current.source_revision))
        if not validate_learning_capture_payload(dict(owner.payload), board_id=capture.board_id,
                node_type=owner.node_type, node_id=owner.node_id, generation=owner.generation,
                evidence_refs=owner.evidence_refs):
            raise ValueError('learning_materialization_projection_conflict')
        intent = owner.payload['intent']
        predecessor = (await read_exact(intent['expected_fingerprint'], owner.source_revision)
            if intent['kind'] == 'reuse' else None)
        plan = CapturedLearningProjection(owner, current, owner.payload['source']['bug_id'], predecessor)
        plan.require_literal_head()
        latest_plan = latest_plan or plan
        if owner.record_fingerprint == capture.record_fingerprint:
            return replace(latest_plan, capture=capture, bug_id=bug_id,
                projection_capture=latest_plan.capture)
        if predecessor is None or owner.source_revision <= capture.source_revision:
            raise ValueError('learning_materialization_projection_conflict')
        current = predecessor


async def revalidate_learning_capture_for_materialization(
    context, *, board_id: str, bug_id: str, learning_id: str,
    generation: int, expected_fingerprint: str,
) -> LearningMaterializationBasis:
    """Internal worker precondition, retaining the caller's relational fence.

    Reads only one selected head and the Bug's existing transition history.
    No canonical node, Learning revision, retry work or history is invented.
    Graph eligibility, governed admission and compensation remain separate
    obligations for the materializer that consumes this value in the same UOW.
    """
    from okto_pulse.core.domain.learning_closeout import (
        LearningCaptureSelection, qualify_learning_materialization_basis,
    )
    from okto_pulse.core.ports.application_persistence import get_application_persistence_port

    selection = LearningCaptureSelection(learning_id=learning_id, generation=generation,
        fingerprint=expected_fingerprint)
    reader, store = resolve_bug_cognitive_context_assembler(), require_cognitive_source_store()
    if (not isinstance(reader, BugSemanticWriteSnapshotReader)
            or not isinstance(store, TransactionalCognitiveSourceReader)):
        raise ValueError('learning_capture_transaction_capability_unavailable')
    source = qualify_bug_semantic_context(await reader.assemble_semantic_for_write(
        context, board_id=board_id, bug_id=bug_id))
    if not source.verified or source.board_id != board_id or source.bug_id != bug_id:
        raise ValueError('learning_capture_source_changed_or_unavailable')
    record = await store.read_latest_in_context(context, board_id=board_id,
        node_id=selection.learning_id, generation=selection.generation)
    if record is None:
        raise ValueError('learning_capture_selected_record_unavailable')
    head = record
    if record.record_fingerprint != selection.fingerprint:
        if not isinstance(store, FingerprintCognitiveSourceReader):
            raise ValueError('learning_capture_history_unavailable')
        record = await store.read_fingerprint_in_context(context, board_id=board_id,
            node_id=selection.learning_id, generation=selection.generation, fingerprint=selection.fingerprint)
        if record is None:
            raise ValueError('learning_capture_history_unavailable')
    if (record.node_id != selection.learning_id or record.generation != selection.generation
            or record.record_fingerprint != selection.fingerprint):
        raise ValueError('learning_capture_selection_changed')
    persistence = get_application_persistence_port()
    bug = await persistence.get(context, entity='card', record_id=bug_id)
    if bug is None:
        raise ValueError('learning_capture_source_changed_or_unavailable')
    bug = await persistence.refresh(context, bug)
    if bug.board_id != board_id or str(getattr(bug.status, 'value', bug.status)) != source.status:
        raise ValueError('learning_capture_source_changed_or_unavailable')
    binding = qualify_learning_materialization_basis(record, source,
        getattr(bug, 'learning_closeout_bindings', None))
    _require_current_capture_evidence(source, record)
    projection = await resolve_learning_capture_projection(context, store,
        capture=record, head=head, bug_id=bug_id)
    return LearningMaterializationBasis(record, source, binding.transition_id if binding else None, head, projection)


async def stage_report_learning_capture(context, *, initial, captured, conclusion,
    submission, author_id, capture_status):
    """Use only the server's admitted report delta, never rebase old content."""
    from okto_pulse.core.domain.learning_submission import qualify_learning_submission_basis

    source = qualify_learning_submission_basis(initial=initial, captured=captured,
        conclusion=conclusion, capture_status=capture_status)
    request = CreateLearningCapture(board_id=source.board_id, bug_id=source.bug_id,
        capture_id=submission.capture_id, expected_source_digest=source.source_digest,
        expected_source_version=source.source_policy_version, content=submission.content,
        context=submission.context, applicability=submission.applicability,
        scenario_ids=tuple(submission.scenario_ids))
    return await stage_new_learning_capture(context, request, author_id=author_id,
        captured_at=datetime.now(timezone.utc))


async def revalidate_learning_capture_for_closeout(
    context, *, board_id: str, bug_id: str, learning_id: str,
    generation: int, expected_fingerprint: str,
) -> CognitiveSourceRecord:
    """Re-read a selected capture under the semantic source write fence.

    Internal application operation, called after lifecycle authorization. This
    is neither authorization to close nor a durable closeout receipt. The caller
    must retain this UOW through its final source checks, mutation and outbox;
    changing conclusion/status afterwards does not requalify the old capture.
    No graph operation, commit, policy conversion or legacy hold waiver occurs.
    """
    if (any(type(value) is not str or not value.strip() or len(value) > 4096
            for value in (board_id, bug_id, learning_id))
            or type(generation) is not int or generation < 0
            or type(expected_fingerprint) is not str or len(expected_fingerprint) != 64
            or any(char not in '0123456789abcdef' for char in expected_fingerprint)):
        raise ValueError('learning_capture_selection_invalid')
    reader = resolve_bug_cognitive_context_assembler()
    store = require_cognitive_source_store()
    if (not isinstance(reader, BugSemanticWriteSnapshotReader)
            or not isinstance(store, TransactionalCognitiveSourceReader)):
        raise ValueError('learning_capture_transaction_capability_unavailable')
    source = qualify_bug_semantic_context(await reader.assemble_semantic_for_write(
        context, board_id=board_id, bug_id=bug_id))
    if not source.verified or source.board_id != board_id or source.bug_id != bug_id:
        raise ValueError('learning_capture_source_changed_or_unavailable')
    record = await store.read_latest_in_context(context, board_id=board_id,
        node_id=learning_id, generation=generation)
    if record is None:
        raise ValueError('learning_capture_selected_record_unavailable')
    # The port verifies stored history; independently verify the returned head.
    latest_cognitive_source_records((record,))
    if (record.board_id != board_id or record.node_id != learning_id
            or record.generation != generation or record.record_fingerprint != expected_fingerprint):
        raise ValueError('learning_capture_selection_changed')
    payload = dict(record.payload)
    if not validate_learning_capture_payload(payload, board_id=board_id,
            node_type=record.node_type, node_id=learning_id, generation=generation,
            evidence_refs=record.evidence_refs):
        raise ValueError('learning_capture_selected_record_unavailable')
    if (payload['source']['bug_id'] != bug_id
            or payload['source']['digest'] != source.source_digest
            or payload['source']['policy_version'] != source.source_policy_version):
        raise ValueError('learning_capture_source_changed_or_unavailable')
    # Reuse/supersede payload syntax alone does not prove target applicability.
    # Those operations need their own admitted target/CAS contract.
    if payload['intent']['kind'] != 'create':
        raise ValueError('learning_capture_intent_not_admitted')
    _require_current_capture_evidence(source, record)
    return record


def _require_current_capture_evidence(source, record):
    scenarios = {}
    for row in source.test_scenarios:
        identity = row.get('id')
        if type(identity) is not str or not identity:
            raise ValueError('learning_capture_evidence_ambiguous')
        ref = f'spec:{source.spec_id}:test_scenario:{identity}'
        if ref in scenarios:
            raise ValueError('learning_capture_evidence_ambiguous')
        scenarios[ref] = dict(row)
    for ref in record.evidence_refs:
        if not _authenticated_scenario(source, scenarios.get(ref)):
            raise ValueError('learning_capture_evidence_not_authenticated')
