"""A candidate session cannot legitimize invented authorship or scope history."""
from dataclasses import replace

import pytest

from okto_pulse.core.domain.learning_materialization_work import LearningCaptureWorkRef
from okto_pulse.core.ports.learning_reconciliation import (
    LearningReconciliationExecution, require_learning_reconciliation_source_append,
)
from test_learning_materialization_projection import projection, literal
from test_learning_reuse_projection import History, reuse
from test_learning_scope_history import records


def execution(capture):
    return LearningReconciliationExecution(capture.board_id,
        LearningCaptureWorkRef(capture.payload['source']['bug_id'], capture.node_id,
            capture.generation, capture.record_fingerprint).encode(), 'kgses_candidate', True)


def committed(row):
    return replace(row, source_session_id='kgses_candidate', committed_at='2026-09-30T12:00:00+00:00')


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['create', 'reuse', 'scoped'])
async def test_exact_authored_append_uses_existing_lineage_policy(kind):
    original = projection()
    capture, head = original.capture, committed(literal(original))
    history, appended = [capture, head], (head,)
    if kind == 'reuse':
        plan, head = reuse(head, capture)
        head = committed(head)
        capture = plan.capture
        history.extend((capture, head))
        appended = (head,)
    elif kind == 'scoped':
        _, claim, capture, head = records(board_id=capture.board_id, predecessor=head)
        claim, head = committed(claim), committed(head)
        history.extend((capture, head, claim))
        appended = (head, claim)
    await require_learning_reconciliation_source_append(None, History(*history),
        appended=appended, execution=execution(capture))
    # A final graph-probe failure does not erase an acknowledged SQL append.
    await require_learning_reconciliation_source_append(None, History(*history),
        appended=appended, execution=replace(execution(capture), materialized=False))


@pytest.mark.asyncio
@pytest.mark.parametrize('damage', ['content', 'author', 'evidence', 'revision', 'session',
    'board', 'generation', 'extra', 'uncommitted', 'capture_instead', 'wrong_bug'])
async def test_session_alone_cannot_own_a_changed_or_foreign_revision(damage):
    original = projection()
    capture, head = original.capture, committed(literal(original))
    result = execution(capture)
    if damage in {'content', 'author'}:
        field = 'content' if damage == 'content' else 'created_by_agent'
        head = replace(head, record_fingerprint='', payload={**head.payload, field: 'invented'})
    elif damage == 'evidence': head = replace(head, record_fingerprint='', evidence_refs=())
    elif damage == 'revision': head = replace(head, source_revision=head.source_revision + 1)
    elif damage == 'session': head = replace(head, source_session_id='kgses_other')
    elif damage == 'board': head = replace(head, record_fingerprint='', board_id='other')
    elif damage == 'generation': head = replace(head, record_fingerprint='', generation=4)
    elif damage == 'uncommitted': result = replace(result, consolidation_session_id=None)
    elif damage == 'capture_instead': head = committed(capture)
    elif damage == 'wrong_bug':
        result = replace(result, work_ref=LearningCaptureWorkRef('other-bug', capture.node_id,
            capture.generation, capture.record_fingerprint).encode())
    appended = (head,)
    if damage == 'extra':
        appended += (replace(head, record_fingerprint='', node_id='unrelated'),)
    with pytest.raises(ValueError):
        await require_learning_reconciliation_source_append(None, History(capture, *appended),
            appended=appended, execution=result)


@pytest.mark.asyncio
@pytest.mark.parametrize('damage', ['missing_claim', 'missing_predecessor', 'content', 'evidence',
    'session', 'timestamp', 'missing_successor', 'extra_claim'])
async def test_scoped_replacement_requires_exact_atomic_historical_pair(damage):
    previous, claim, capture, head = records()
    claim, head = committed(claim), committed(head)
    if damage == 'content': claim = replace(claim, record_fingerprint='', payload={**claim.payload, 'content': 'rewritten'})
    elif damage == 'evidence': claim = replace(claim, record_fingerprint='', evidence_refs=claim.evidence_refs[-1:])
    elif damage == 'session': claim = replace(claim, source_session_id='kgses_other')
    elif damage == 'timestamp': claim = replace(claim, committed_at='2026-09-30T12:01:00+00:00')
    appended = (head,) if damage == 'missing_claim' else (head, claim)
    if damage == 'missing_successor': appended = (claim,)
    if damage == 'extra_claim': appended += (claim,)
    history = (capture, head, claim) if damage == 'missing_predecessor' else (previous, capture, head, claim)
    with pytest.raises(ValueError):
        await require_learning_reconciliation_source_append(None, History(*history),
            appended=appended, execution=execution(capture))
