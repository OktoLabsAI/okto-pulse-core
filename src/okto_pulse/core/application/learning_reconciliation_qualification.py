"""Exact capture composition after authenticated phase and currentness proofs."""

from dataclasses import asdict
import json
import re

from okto_pulse.core.domain.learning_materialization_work import parse_learning_capture_work_ref
from okto_pulse.core.ports.cognitive_projection import CognitiveProjectionParity
from okto_pulse.core.ports.learning_reconciliation import (
    LearningReconciliationApplicability, LearningReconciliationExecution,
    LearningReconciliationQualification, LearningReconciliationSelection,
)


def _text(value, limit=4096):
    return type(value) is str and bool(value.strip()) and len(value) <= limit


def _digest(value):
    return type(value) is str and re.fullmatch('[0-9a-f]{64}', value) is not None


def require_consistent(*, qualification, parity):
    if (type(qualification) is not LearningReconciliationQualification
            or type(parity) is not tuple or len(parity) > 100_000
            or any(type(row) is not CognitiveProjectionParity for row in parity)
            or type(qualification.qualified_sources) is not tuple
            or len(qualification.qualified_sources) > 100_000
            or type(qualification.reasons) is not tuple
            or type(qualification.unmatched_cognitive_sources) is not int
            or not 0 <= qualification.unmatched_cognitive_sources <= 100_000):
        raise ValueError('learning_reconciliation_qualification_evidence_invalid')
    if qualification.state != 'current_captures_reconciled' or qualification.reasons:
        raise ValueError('learning_reconciliation_qualification_pending')
    candidates = {}
    for row in parity:
        if row.node_type == 'Learning' and row.state == 'capture_pending_materialization':
            candidates.setdefault((row.node_id, row.generation, row.source_fingerprint), []).append(row)
    qualified = set()
    for key in qualification.qualified_sources:
        if (type(key) is not tuple or len(key) != 3 or not _text(key[0])
                or type(key[1]) is not int or key[1] < 0 or not _digest(key[2]) or key in qualified):
            raise ValueError('learning_reconciliation_qualification_evidence_invalid')
        matching = candidates.get(key, ())
        if len(matching) != 1 or matching[0].differing_fields or matching[0].usage_differences:
            raise ValueError('learning_reconciliation_qualification_parity_invalid')
        qualified.add(key)
    unmatched = sum(row.state != 'matched' and not (row.node_type == 'Learning'
        and row.state == 'capture_pending_materialization'
        and (row.node_id, row.generation, row.source_fingerprint) in qualified) for row in parity)
    if unmatched != qualification.unmatched_cognitive_sources:
        raise ValueError('learning_reconciliation_qualification_count_invalid')
    return unmatched


def qualify(*, board_id, selections, executions, applicability, parity):
    """No summary, empty selection or unrelated proof can clear pending work."""
    if not _text(board_id, 256):
        raise ValueError('learning_reconciliation_qualification_scope_invalid')
    for rows, kind in ((selections, LearningReconciliationSelection),
            (executions, LearningReconciliationExecution),
            (applicability, LearningReconciliationApplicability), (parity, CognitiveProjectionParity)):
        if type(rows) is not tuple or len(rows) > 100_000 or any(type(row) is not kind for row in rows):
            raise ValueError('learning_reconciliation_qualification_evidence_invalid')
    if sum(len(json.dumps(asdict(row)).encode('utf-8')) for rows in
            (selections, executions, applicability, parity) for row in rows) > 64 * 1024 * 1024:
        raise ValueError('learning_reconciliation_qualification_limit')
    parity_keys = [(row.node_type, row.node_id, row.generation, row.source_revision) for row in parity]
    if (len(set(parity_keys)) != len(parity_keys)
            or any(not _text(row.node_id) or type(row.generation) is not int or row.generation < 0
                or type(row.source_revision) is not int or row.source_revision < 0
                or not _digest(row.source_fingerprint) or type(row.differing_fields) is not tuple
                or type(row.usage_differences) is not tuple for row in parity)):
        raise ValueError('learning_reconciliation_qualification_parity_invalid')
    selected, nodes, reasons = {}, set(), set()
    for row in selections:
        if (not _text(row.node_id) or row.node_id in nodes or type(row.generations) is not tuple
                or type(row.work_refs) is not tuple or len(row.work_refs) > 100_000
                or any(type(version) is not int or version < 0 for version in row.generations)):
            raise ValueError('learning_reconciliation_qualification_selection_invalid')
        nodes.add(row.node_id)
        if row.state != 'awaiting_revalidation' or len(row.generations) != 1 or not row.work_refs:
            reasons.add('learning_selection_unresolved')
            if row.work_refs:
                raise ValueError('learning_reconciliation_qualification_selection_invalid')
            continue
        for reference in row.work_refs:
            work = parse_learning_capture_work_ref(reference) if type(reference) is str else None
            if (work is None or work.fingerprint is None or reference in selected
                    or work.learning_id != row.node_id or work.generation != row.generations[0]):
                raise ValueError('learning_reconciliation_qualification_selection_invalid')
            selected[reference] = work
    if (len({row.work_ref for row in executions}) != len(executions)
            or {row.work_ref for row in executions} != set(selected)
            or any(row.board_id != board_id or type(row.materialized) is not bool for row in executions)):
        raise ValueError('learning_reconciliation_qualification_execution_scope_invalid')
    observed = {}
    for row in applicability:
        key = row.bug_id, row.learning_id, row.generation, row.capture_fingerprint
        if (row.board_id != board_id or not _text(row.bug_id) or not _text(row.learning_id)
                or type(row.generation) is not int or row.generation < 0
                or not all(_digest(value) for value in (row.capture_fingerprint, row.source_digest, row.head_fingerprint))
                or type(row.source_policy_version) is not int or row.source_policy_version < 1
                or (row.closeout_transition_id is not None and not _text(row.closeout_transition_id))
                or key in observed):
            raise ValueError('learning_reconciliation_qualification_applicability_invalid')
        if row.closeout_transition_id is None:
            reasons.add('learning_closeout_binding_unavailable')
        observed[key] = row
    expected, sessions = set(), set()
    for row in executions:
        work = selected[row.work_ref]
        if not row.materialized:
            reasons.add('learning_execution_not_materialized')
            continue
        if not _text(row.consolidation_session_id) or row.consolidation_session_id in sessions:
            raise ValueError('learning_reconciliation_qualification_execution_invalid')
        sessions.add(row.consolidation_session_id)
        expected.add((work.bug_id, work.learning_id, work.generation, work.fingerprint))
    if set(observed) != expected:
        raise ValueError('learning_reconciliation_qualification_applicability_scope_invalid')
    qualified = set()
    if not reasons:
        available = {(row.learning_id, row.generation, row.capture_fingerprint) for row in observed.values()}
        for row in parity:
            key = row.node_id, row.generation, row.source_fingerprint
            if (row.node_type == 'Learning' and row.state == 'capture_pending_materialization'
                    and not row.differing_fields and not row.usage_differences and key in available):
                qualified.add(key)
    unmatched = sum(row.state != 'matched' and not (row.node_type == 'Learning'
        and row.state == 'capture_pending_materialization'
        and (row.node_id, row.generation, row.source_fingerprint) in qualified) for row in parity)
    if unmatched:
        reasons.add('cognitive_source_unreconciled')
    return LearningReconciliationQualification('pending' if reasons else 'current_captures_reconciled',
        tuple(sorted(reasons)), tuple(sorted(qualified)), unmatched)
