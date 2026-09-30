"""Recovery recognizes the writer's narrow transition, never a new authority."""
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from okto_pulse.core.domain.learning_materialization_work import LearningCaptureWorkRef
from okto_pulse.core.kg.canonical_learning_partition import (
    HISTORICAL_DEBT_REASON, PARTITION_TARGET_STATUS, _stable_content_hash,
)
from okto_pulse.core.ports.canonical_debt import CanonicalDebtRecord
from okto_pulse.core.ports.learning_reconciliation import (
    LearningReconciliationExecution, qualify_learning_reconciliation_debt_change,
)


def inputs():
    stamp = datetime(2026, 9, 30, tzinfo=timezone.utc)
    before = CanonicalDebtRecord(board_id='board', artifact_type='bug', artifact_id='bug-a',
        source_ref='bug:bug-a', content_hash=_stable_content_hash('bug:bug-a', 'learning-a'),
        target_status=PARTITION_TARGET_STATUS, canonical_state='pending', failure_reason=HISTORICAL_DEBT_REASON,
        owner_agent_id='historical-worker', correlation_id='historical-correlation',
        created_at=stamp, updated_at=stamp)
    after = replace(before, canonical_state='committed', evidence_ref='learning-a',
        owner_agent_id='cognitive_closeout_worker', updated_at=stamp + timedelta(seconds=1))
    execution = LearningReconciliationExecution('board',
        LearningCaptureWorkRef('bug-a', 'learning-a', 0, 'a' * 64).encode(), 'kgses_current', True)
    return before, after, execution


@pytest.mark.parametrize('state', ['pending', 'deferred', 'retryable', 'retry_scheduled', 'failed'])
def test_only_exact_existing_technical_transition_is_recognized(state):
    before, after, execution = inputs()
    assert qualify_learning_reconciliation_debt_change(before=replace(before, canonical_state=state),
        after=after, execution=execution)
    # An unavailable final graph probe does not undo SQL already acknowledged.
    # Recognition still is not permission to declare the candidate complete.
    assert qualify_learning_reconciliation_debt_change(before=replace(before, canonical_state=state),
        after=after, execution=replace(execution, materialized=False))


@pytest.mark.parametrize('restriction', [
    {'failure_reason': 'authority_denied'}, {'failure_reason': 'source_absent'},
    {'canonical_state': 'blocked'}, {'canonical_state': 'discarded'}, {'canonical_state': 'committed'},
    {'dlq_ref': 'dlq:integrity'}, {'last_error': 'unresolved integrity'},
    {'source_version': 'unproved-version'}, {'target_status': 'different-gate'},
    {'content_hash': _stable_content_hash('bug:bug-a', 'other-learning')},
    {'source_ref': 'bug:other-bug'}, {'board_id': 'other-board'}, {'artifact_type': 'spec'},
])
def test_substantive_unknown_and_foreign_restrictions_are_not_closed(restriction):
    before, after, execution = inputs()
    assert not qualify_learning_reconciliation_debt_change(before=replace(before, **restriction),
        after=replace(after, **{k: v for k, v in restriction.items() if k != 'canonical_state'}), execution=execution)


@pytest.mark.parametrize('field,value', [
    ('id', 'other-id'), ('source_ref', 'bug:other'), ('evidence_ref', 'other-learning'),
    ('owner_agent_id', 'arbitrary-actor'), ('canonical_state', 'waived'),
    ('correlation_id', 'rewritten-history'), ('failure_reason', None), ('retry_count', 9),
])
def test_unrelated_history_changes_are_refused(field, value):
    before, after, execution = inputs()
    assert not qualify_learning_reconciliation_debt_change(before=before,
        after=replace(after, **{field: value}), execution=execution)


def test_missing_commit_identity_cannot_own_a_debt_transition():
    before, after, execution = inputs()
    assert not qualify_learning_reconciliation_debt_change(before=before, after=after,
        execution=replace(execution, consolidation_session_id=None))
