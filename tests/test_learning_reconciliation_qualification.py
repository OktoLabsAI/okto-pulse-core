"""A whole authenticated phase can satisfy only its exact current captures."""

from dataclasses import replace

import pytest

from okto_pulse.core.domain.learning_materialization_work import LearningCaptureWorkRef
from okto_pulse.core.ports.cognitive_projection import CognitiveProjectionParity
from okto_pulse.core.ports.learning_reconciliation import (
    LearningReconciliationApplicability, LearningReconciliationExecution,
    LearningReconciliationSelection, qualify_learning_reconciliation_sources,
)

WORK = LearningCaptureWorkRef('bug', 'learning', 0, 'a' * 64).encode()
SELECTION = LearningReconciliationSelection('learning', (0,), 'awaiting_revalidation', ('revalidate',), (WORK,))
EXECUTION = LearningReconciliationExecution('board', WORK, 'session', True)
CURRENT = LearningReconciliationApplicability('board', 'bug', 'learning', 0,
    'a' * 64, 'b' * 64, 1, 'c' * 64, 'closeout')
PARITY = CognitiveProjectionParity('Learning', 'learning', 0, 0, 'a' * 64,
    'capture_pending_materialization', (), ())


def qualify(**changes):
    values = dict(board_id='board', selections=(SELECTION,), executions=(EXECUTION,),
        applicability=(CURRENT,), parity=(PARITY,))
    return qualify_learning_reconciliation_sources(**(values | changes))


def test_exact_current_capture_is_satisfied_without_rewriting_prior_parity():
    result = qualify()
    assert result.state == 'current_captures_reconciled'
    assert result.qualified_sources == (('learning', 0, 'a' * 64),)
    assert result.unmatched_cognitive_sources == 0 and result.reasons == ()
    assert PARITY.state == 'capture_pending_materialization'


@pytest.mark.parametrize('change', [dict(node_id='other'), dict(generation=1),
    dict(source_fingerprint='d' * 64), dict(node_type='Decision'),
    dict(state='projection_mismatch'), dict(differing_fields=('content',)),
    dict(usage_differences=('relevance_score',))])
def test_unrelated_or_mismatching_source_cannot_receive_capture_credit(change):
    result = qualify(parity=(replace(PARITY, **change),))
    assert result.state == 'pending' and result.unmatched_cognitive_sources == 1
    assert result.qualified_sources == ()


def test_other_cognitive_pending_source_remains_blocking():
    other = replace(PARITY, node_type='Decision', node_id='other', state='missing')
    result = qualify(parity=(PARITY, other))
    assert result.qualified_sources == (('learning', 0, 'a' * 64),)
    assert result.state == 'pending' and result.unmatched_cognitive_sources == 1


def test_empty_phase_cannot_satisfy_pending_capture():
    result = qualify(selections=(), executions=(), applicability=())
    assert result.state == 'pending' and result.unmatched_cognitive_sources == 1


@pytest.mark.parametrize('state', ['source_unavailable', 'ambiguous_generation'])
def test_unresolved_selection_stays_pending_even_without_parity(state):
    result = qualify(selections=(replace(SELECTION, state=state, work_refs=()),),
        executions=(), applicability=(), parity=())
    assert result.state == 'pending' and result.reasons == ('learning_selection_unresolved',)


def test_nonmaterialized_work_or_missing_binding_is_not_admitted():
    result = qualify(executions=(replace(EXECUTION, materialized=False),), applicability=())
    assert result.state == 'pending' and result.unmatched_cognitive_sources == 1
    result = qualify(applicability=(replace(CURRENT, closeout_transition_id=None),))
    assert result.state == 'pending' and result.unmatched_cognitive_sources == 1


@pytest.mark.parametrize('change', [dict(board_id='other'), dict(bug_id='other'),
    dict(learning_id='other'), dict(generation=1), dict(capture_fingerprint='d' * 64),
    dict(source_digest='invalid'), dict(head_fingerprint='invalid'), dict(source_policy_version=True)])
def test_changed_scope_or_malformed_currentness_is_rejected(change):
    with pytest.raises(ValueError, match='learning_reconciliation_qualification_applicability'):
        qualify(applicability=(replace(CURRENT, **change),))


@pytest.mark.parametrize('change', [dict(executions=()), dict(executions=(EXECUTION, EXECUTION)),
    dict(applicability=()), dict(applicability=(CURRENT, CURRENT)),
    dict(selections=(SELECTION, SELECTION)), dict(parity=(PARITY, PARITY)),
    dict(executions=(replace(EXECUTION, materialized=1),)),
    dict(executions=(replace(EXECUTION, consolidation_session_id=None),))])
def test_missing_duplicate_or_invalid_facts_cannot_supply_credit(change):
    with pytest.raises(ValueError, match='learning_reconciliation_qualification_'):
        qualify(**change)


def test_multiple_origins_require_currentness_for_every_selected_association():
    second = LearningCaptureWorkRef('other-bug', 'learning', 0, 'd' * 64).encode()
    selection = replace(SELECTION, work_refs=(WORK, second))
    execution = replace(EXECUTION, work_ref=second, consolidation_session_id='other-session')
    current = replace(CURRENT, bug_id='other-bug', capture_fingerprint='d' * 64)
    with pytest.raises(ValueError, match='applicability_scope_invalid'):
        qualify(selections=(selection,), executions=(EXECUTION, execution))
    result = qualify(selections=(selection,), executions=(EXECUTION, execution), applicability=(CURRENT, current))
    assert result.state == 'current_captures_reconciled'

