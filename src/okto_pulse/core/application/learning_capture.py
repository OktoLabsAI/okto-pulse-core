"""Stage authored Learning content, never infer it or certify implementation."""

from datetime import datetime, timezone
from dataclasses import dataclass
import json

from okto_pulse.core.kg.node_identity import mint_node_id
from okto_pulse.core.ports.bug_cognitive_context import (
    BugSemanticWriteSnapshotReader, qualify_bug_semantic_context,
    BugCognitiveContext,
    resolve_bug_cognitive_context_assembler,
)
from okto_pulse.core.ports.kg_cognitive_source import (
    CognitiveSourceRecord, ConditionalCognitiveSourceWriter, TransactionalCognitiveSourceReader,
    HistoricalCognitiveSourceReader, FingerprintCognitiveSourceReader,
    require_cognitive_source_store, latest_cognitive_source_records,
)
from okto_pulse.core.ports.learning_capture import (
    CreateLearningCapture, LearningCaptureHistoryReader, validate_learning_capture_payload,
)
from okto_pulse.core.services.test_scenario_lifecycle import scenario_has_authenticated_required_evidence


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
            or not isinstance(store, TransactionalCognitiveSourceReader)):
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
    payload = {'capture_format': 'learning-capture/v1', 'capture_id': request.capture_id,
        'author_id': author_id, 'captured_at': captured_at.isoformat(),
        'content': request.content, 'context': request.context, 'applicability': request.applicability,
        'source': {'board_id': source.board_id, 'bug_id': source.bug_id,
            'policy_version': source.source_policy_version, 'digest': source.source_digest, 'evidence_refs': refs},
        'intent': {'kind': 'create', 'target_node_id': None, 'target_generation': None,
            'expected_fingerprint': None, 'reason': None}}
    validate_learning_capture_payload(payload, board_id=source.board_id, node_type='Learning',
        node_id=node_id, generation=0, evidence_refs=refs)
    prior = await store.read_latest_in_context(context, board_id=request.board_id, node_id=node_id, generation=0)
    if prior is not None:
        if 'capture_format' not in prior.payload:
            # Materialization appends a literal projection at the same identity.
            # Retry compares the immutable authored birth, never that projection.
            if not isinstance(store, HistoricalCognitiveSourceReader):
                raise ValueError('learning_capture_history_unavailable')
            prior = await store.read_revision_in_context(context, board_id=request.board_id,
                node_id=node_id, generation=0, source_revision=0)
            if prior is None:
                raise ValueError('learning_capture_history_unavailable')
        if not validate_learning_capture_payload(dict(prior.payload), board_id=request.board_id,
                node_type=prior.node_type, node_id=node_id, generation=0, evidence_refs=prior.evidence_refs):
            raise ValueError('learning_capture_idempotency_conflict')
        previous = dict(prior.payload)
        previous.pop('captured_at', None)
        comparison = {key: value for key, value in payload.items() if key != 'captured_at'}
        if prior.node_type != 'Learning' or previous != comparison:
            raise ValueError('learning_capture_idempotency_conflict')
        return prior
    record = CognitiveSourceRecord(board_id=source.board_id, node_type='Learning', node_id=node_id,
        generation=0, payload=payload, evidence_refs=tuple(refs),
        source_session_id='capture:' + request.capture_id, committed_at=captured_at.isoformat())
    await store.append_many_if_current_in_context(context, (record,), expected_fingerprints=(None,))
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


@dataclass(frozen=True)
class LearningMaterializationBasis:
    """Current relational admission only, not graph admission or persistence."""

    capture: CognitiveSourceRecord
    source: BugCognitiveContext
    closeout_transition_id: str | None
    head: CognitiveSourceRecord


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
    from okto_pulse.core.domain.learning_materialization import CapturedLearningProjection
    projection = CapturedLearningProjection(record, head, bug_id)
    projection.require_literal_head()
    return LearningMaterializationBasis(record, source, binding.transition_id if binding else None, head)


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
