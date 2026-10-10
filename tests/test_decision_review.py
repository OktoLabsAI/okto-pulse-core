from dataclasses import replace
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from okto_pulse.core.domain.decision_review import DecisionReviewFact, resolve_decision_inspection
from okto_pulse.core.models.decision_review import DecisionReviewEntry
from okto_pulse.core.services.reviewer_separation import evaluate_decision_reviewer_separation


def test_conflicts_require_explicit_reconciliation_not_latest_passing():
    passed = DecisionReviewFact('a', 'd', 'x' * 64, 'passed')
    failed = replace(passed, record_id='b', result='failed')
    assert resolve_decision_inspection('d', passed.scope_sha256, (passed, failed)).status == 'conflict'
    again = replace(passed, record_id='c')
    assert resolve_decision_inspection('d', passed.scope_sha256, (passed, failed, again)).status == 'conflict'
    reconciled = replace(again, reconciles=('a', 'b'))
    assert resolve_decision_inspection('d', passed.scope_sha256, (passed, failed, reconciled)).status == 'verified'
    # Revoking the reconciler restores the conflict, not an old green receipt.
    assert resolve_decision_inspection('d', passed.scope_sha256, (passed, failed, replace(reconciled, revoked=True))).status == 'conflict'


@pytest.mark.parametrize('result', ['failed', 'inconclusive', 'aborted', 'unavailable'])
def test_nonpassing_never_satisfies(result):
    fact = DecisionReviewFact('a', 'd', 'a' * 64, result)
    assert resolve_decision_inspection('d', fact.scope_sha256, (fact,)).status == result
    assert resolve_decision_inspection('d', 'b' * 64, (fact,)).status == 'inspection_pending'


@pytest.mark.parametrize('mode,allowed,warning', [('off', True, False), ('warn', True, True), ('enforce', False, False)])
@pytest.mark.parametrize('known', [False, True])
def test_independence_unknown_or_same_author(mode, allowed, warning, known):
    result = evaluate_decision_reviewer_separation(board=SimpleNamespace(settings={'reviewer_separation_mode': mode}),
        reviewer_id='actor', author_ids=('actor',) if known else (), authors_known=known)
    assert result.allowed is allowed and result.warning is warning


def test_client_cannot_add_authority_or_empty_observation():
    for extra in ({'actor_id': 'someone'}, {'approved': True}, {'observed': ' '}):
        with pytest.raises(ValidationError):
            DecisionReviewEntry.model_validate({'decision_id': 'd', 'expected_scope_sha256': 'a' * 64,
                'observed': 'Inspected scope', 'result': 'passed',
                'sources': [{'reference': 'spec:s', 'revision': 'edition:1', 'sha256': 'b' * 64}], **extra})


def test_scope_executor_cannot_review_with_enforced_separation():
    result = evaluate_decision_reviewer_separation(board=SimpleNamespace(settings={}), reviewer_id='executor',
        author_ids=('author',), authors_known=True, executor_ids=('executor',))
    assert not result.allowed and 'decision_scope_executor' in result.conflicts
