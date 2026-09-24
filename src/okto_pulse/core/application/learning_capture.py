"""Stage authored Learning content, never infer it or certify implementation."""

from datetime import datetime
import json

from okto_pulse.core.kg.node_identity import mint_node_id
from okto_pulse.core.ports.bug_cognitive_context import (
    BugSemanticWriteSnapshotReader, qualify_bug_semantic_context,
    resolve_bug_cognitive_context_assembler,
)
from okto_pulse.core.ports.kg_cognitive_source import (
    CognitiveSourceRecord, ConditionalCognitiveSourceWriter, TransactionalCognitiveSourceReader,
    require_cognitive_source_store,
)
from okto_pulse.core.ports.learning_capture import CreateLearningCapture, validate_learning_capture_payload
from okto_pulse.core.services.test_scenario_lifecycle import scenario_has_authenticated_required_evidence


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
        evidence = (scenario.get('evidence') or scenario.get('latest_evidence')) if scenario else None
        # The shared consumer preserves structural legacy evidence for old
        # workflows. New capture references require an authenticated receipt.
        if (not source.spec_id or not isinstance(evidence, dict) or not evidence.get('execution_receipt')
                or not scenario_has_authenticated_required_evidence(board_id=source.board_id,
                    spec_id=source.spec_id, scenario=scenario, acceptance_criteria=list(source.acceptance_criteria))):
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
    return record
