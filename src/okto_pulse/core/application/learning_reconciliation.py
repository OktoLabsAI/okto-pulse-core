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
