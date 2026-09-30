"""Find replayable authorship without retrospectively inventing semantics."""
from collections import defaultdict
import json

from okto_pulse.core.domain.learning_materialization_work import LearningCaptureWorkRef
from okto_pulse.core.kg.logical_transfer import LogicalSchemaIndex
from okto_pulse.core.ports.cognitive_projection import validate_cognitive_projection_sources
from okto_pulse.core.ports.kg_cognitive_source import canonical_cognitive_source_fingerprint
from okto_pulse.core.ports.learning_reconciliation import LearningReconciliationSelection


def qualify_debt_change(*, before, after, execution):
    from datetime import datetime
    from okto_pulse.core.domain.learning_materialization_work import parse_learning_capture_work_ref
    from okto_pulse.core.kg.canonical_learning_partition import _canonical_evidence_for, _technical_partition_wait
    from okto_pulse.core.ports.canonical_debt import CanonicalDebtRecord
    from okto_pulse.core.ports.learning_reconciliation import LearningReconciliationExecution
    from okto_pulse.core.services.canonical_debt_service import canonical_debt_evidence_replacement

    if (type(before) is not CanonicalDebtRecord or type(after) is not CanonicalDebtRecord
            or type(execution) is not LearningReconciliationExecution
            or not execution.consolidation_session_id or type(after.updated_at) is not datetime):
        return False
    work = parse_learning_capture_work_ref(execution.work_ref)
    if work is None or work.fingerprint is None:
        return False
    evidence = _canonical_evidence_for(work.learning_id, 'bug:' + work.bug_id)
    if (before.board_id != execution.board_id or before.source_ref != evidence['source_ref']
            or before.content_hash != evidence['content_hash']):
        return False
    replacement = canonical_debt_evidence_replacement(before, evidence=evidence,
        actor_id='cognitive_closeout_worker', now=after.updated_at, eligible_debt=_technical_partition_wait)
    return replacement is not None and after == replacement


async def source_basis(context, store, *, execution):
    from okto_pulse.core.application.learning_capture import resolve_learning_capture_projection
    from okto_pulse.core.application.learning_materialization import authored_learning_candidate
    from okto_pulse.core.domain.learning_materialization_work import parse_learning_capture_work_ref
    from okto_pulse.core.kg.session_manager import compute_content_hash
    from okto_pulse.core.ports.learning_capture import is_scoped_learning_supersede
    from okto_pulse.core.ports.learning_reconciliation import (
        LearningReconciliationExecution, LearningReconciliationSourceBasis,
    )
    if type(execution) is not LearningReconciliationExecution or not execution.consolidation_session_id:
        raise ValueError('learning_reconciliation_execution_invalid')
    work = parse_learning_capture_work_ref(execution.work_ref)
    if work is None or work.fingerprint is None:
        raise ValueError('learning_reconciliation_fingerprinted_work_required')
    capture = await store.read_fingerprint_in_context(context, board_id=execution.board_id,
        node_id=work.learning_id, generation=work.generation, fingerprint=work.fingerprint)
    history = await store.read_history_in_context(context, board_id=execution.board_id,
        node_id=work.learning_id, generation=work.generation)
    if (capture is None or not history or capture.payload.get('source', {}).get('bug_id') != work.bug_id):
        raise ValueError('learning_reconciliation_source_unavailable')
    plan = await resolve_learning_capture_projection(context, store,
        capture=capture, head=history[-1], bug_id=work.bug_id)
    node = authored_learning_candidate(capture, plan)
    return LearningReconciliationSourceBasis(work.bug_id, work.learning_id, work.generation, work.fingerprint,
        compute_content_hash(node.content or node.title or '', work.bug_id, execution.board_id),
        capture.payload['intent']['target_node_id'] if is_scoped_learning_supersede(capture.payload) else None)


async def require_source_append(context, store, *, appended, execution):
    from okto_pulse.core.application.learning_capture import resolve_learning_capture_projection
    from okto_pulse.core.application.learning_supersedence import read_learning_scope_replacements
    from okto_pulse.core.domain.learning_materialization_work import parse_learning_capture_work_ref
    from okto_pulse.core.ports.kg_cognitive_source import (
        CognitiveSourceRecord, FingerprintCognitiveSourceReader, latest_cognitive_source_records,
    )
    from okto_pulse.core.ports.learning_capture import is_scoped_learning_supersede
    from okto_pulse.core.ports.learning_reconciliation import LearningReconciliationExecution

    if (type(execution) is not LearningReconciliationExecution
            or not execution.consolidation_session_id or type(appended) is not tuple
            or not 1 <= len(appended) <= 2 or any(type(row) is not CognitiveSourceRecord for row in appended)
            or not isinstance(store, FingerprintCognitiveSourceReader)):
        raise ValueError('learning_reconciliation_source_append_invalid')
    work = parse_learning_capture_work_ref(execution.work_ref)
    if work is None or work.fingerprint is None:
        raise ValueError('learning_reconciliation_fingerprinted_work_required')
    latest_cognitive_source_records(appended)
    if any(row.board_id != execution.board_id or row.node_type != 'Learning'
            or row.source_session_id != execution.consolidation_session_id
            or 'capture_format' in row.payload for row in appended):
        raise ValueError('learning_reconciliation_source_append_unowned')
    primary = [row for row in appended if (row.node_id, row.generation) == (work.learning_id, work.generation)]
    if len(primary) != 1:
        raise ValueError('learning_reconciliation_source_append_unowned')
    head, = primary
    capture = await store.read_fingerprint_in_context(context, board_id=execution.board_id,
        node_id=work.learning_id, generation=work.generation, fingerprint=work.fingerprint)
    if (capture is None or capture.record_fingerprint != work.fingerprint
            or (capture.board_id, capture.node_type, capture.node_id, capture.generation)
                != (execution.board_id, 'Learning', work.learning_id, work.generation)
            or head.source_revision != capture.source_revision + 1):
        raise ValueError('learning_reconciliation_source_append_unowned')
    plan = await resolve_learning_capture_projection(context, store,
        capture=capture, head=head, bug_id=work.bug_id)
    plan.require_literal_head()
    if capture.payload['source']['bug_id'] != work.bug_id:
        raise ValueError('learning_reconciliation_source_append_unowned')
    if is_scoped_learning_supersede(capture.payload):
        targets = [row for row in appended if row is not head]
        if len(targets) != 1:
            raise ValueError('learning_reconciliation_source_append_unowned')
        target, = targets
        claims = await read_learning_scope_replacements(context, store, head=target)
        own = [claim for claim in claims if claim.capture == capture and claim.successor == head
            and claim.claimed == target]
        if len(own) != 1 or target.committed_at != head.committed_at:
            raise ValueError('learning_reconciliation_source_append_unowned')
    elif len(appended) != 1:
        raise ValueError('learning_reconciliation_source_append_unowned')


async def execute(*, board_id, work_ref, relational_scope_factory):
    from okto_pulse.core.domain.learning_closeout import LearningCaptureSelection
    from okto_pulse.core.domain.learning_materialization_work import parse_learning_capture_work_ref
    from okto_pulse.core.kg.cognitive_closeout_production import ConsolidationPipelinePersister
    from okto_pulse.core.ports.learning_reconciliation import LearningReconciliationExecution

    if (type(board_id) is not str or not board_id.strip() or len(board_id) > 256
            or type(work_ref) is not str or len(work_ref) > 25000
            or not callable(relational_scope_factory)):
        raise ValueError('learning_reconciliation_execution_invalid')
    work = parse_learning_capture_work_ref(work_ref)
    if work is None or work.fingerprint is None:
        raise ValueError('learning_reconciliation_fingerprinted_work_required')
    # Keep the existing cognitive actor; system:* would select the unrelated
    # deterministic ownership bypass in the consolidation primitives.
    persister = ConsolidationPipelinePersister(relational_scope_factory)
    result = await persister._authored_learning_result(board_id, work.bug_id,
        LearningCaptureSelection(learning_id=work.learning_id, generation=work.generation,
            fingerprint=work.fingerprint), raise_failures=True)
    return LearningReconciliationExecution(board_id, work_ref,
        result.committed_session_id, result.materialized)


def execution_plan(*, schema, board_id, records, nodes):
    from okto_pulse.core.domain.learning_materialization_work import parse_learning_capture_work_ref
    from okto_pulse.core.ports.learning_reconciliation import LearningReconciliationExecutionPlan
    selections = select(schema=schema, board_id=board_id, records=records, nodes=nodes)
    revisions = {}
    for row in records:
        if row['node_type'] != 'Learning':
            continue
        payload = json.loads(row['payload']) if type(row['payload']) is str else row['payload']
        if 'capture_format' not in payload:
            continue
        refs = json.loads(row['evidence_refs']) if type(row['evidence_refs']) is str else row['evidence_refs']
        fingerprint = canonical_cognitive_source_fingerprint(board_id=board_id, node_type='Learning',
            node_id=row['node_id'], generation=row['generation'], payload=payload, evidence_refs=refs)
        key = row['node_id'], row['generation'], fingerprint
        if key in revisions:
            raise ValueError('learning_reconciliation_capture_ambiguous')
        revisions[key] = row.get('source_revision', 0)
    ordered = []
    for selection in selections:
        for reference in selection.work_refs:
            work = parse_learning_capture_work_ref(reference)
            ordered.append((work.learning_id, -revisions[work.learning_id, work.generation, work.fingerprint],
                work.bug_id, reference))
    # A pending later reuse must acquire its literal before earlier origin
    # recovery can prove that chain. This grants no new eligibility or authority.
    return LearningReconciliationExecutionPlan(selections, tuple(row[-1] for row in sorted(ordered)))


def select(*, schema, board_id, records, nodes):
    # Validate the complete history even if there is no Learning to return.
    latest = validate_cognitive_projection_sources(schema=schema, board_id=board_id, records=records)
    if type(nodes) is not tuple or len(nodes) > 100_000:
        raise ValueError('learning_reconciliation_graph_limit')
    index, seen, identities = LogicalSchemaIndex.build(schema), set(), set()
    for node in nodes:
        index.validate_node(node)
        identity = node.type_name, node.key
        if identity in seen:
            raise ValueError('learning_reconciliation_graph_identity_duplicate')
        seen.add(identity)
        if node.type_name == 'Learning':
            identities.add(node.key)
    generations, captures = defaultdict(set), defaultdict(dict)
    for row in latest:
        if row['node_type'] == 'Learning':
            identities.add(row['node_id'])
            generations[row['node_id']].add(row['generation'])
    for row in records:
        if row['node_type'] != 'Learning':
            continue
        payload = json.loads(row['payload']) if type(row['payload']) is str else row['payload']
        if 'capture_format' not in payload:
            continue
        bug_id = payload['source']['bug_id']
        key = row['node_id'], row['generation']
        previous = captures[key].get(bug_id)
        if previous is None or row.get('source_revision', 0) > previous[0].get('source_revision', 0):
            captures[key][bug_id] = (row, payload)
    result = []
    for identity in sorted(identities):
        versions = tuple(sorted(generations[identity]))
        if len(versions) > 1:
            result.append(LearningReconciliationSelection(identity, versions, 'ambiguous_generation',
                ('learning_historical_generation_ambiguous',)))
            continue
        selected = captures.get((identity, versions[0]), {}) if versions else {}
        if not selected:
            result.append(LearningReconciliationSelection(identity, versions, 'source_unavailable',
                ('learning_original_semantic_source_unavailable',)))
            continue
        refs = []
        for bug_id, (row, payload) in sorted(selected.items()):
            evidence = json.loads(row['evidence_refs']) if type(row['evidence_refs']) is str else row['evidence_refs']
            fingerprint = canonical_cognitive_source_fingerprint(board_id=board_id,
                node_type='Learning', node_id=identity, generation=versions[0],
                payload=payload, evidence_refs=evidence)
            refs.append(LearningCaptureWorkRef(bug_id, identity, versions[0], fingerprint).encode())
        result.append(LearningReconciliationSelection(identity, versions, 'awaiting_revalidation',
            ('learning_source_evidence_binding_and_lineage_revalidation_required',), tuple(refs)))
    return tuple(result)
