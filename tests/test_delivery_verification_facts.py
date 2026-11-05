from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from okto_pulse.core.domain.delivery_assessment import ReadinessReason as Reason
from okto_pulse.core.domain.delivery_verification import (
    NativeVerificationPopulation, NativeVerificationRun, VerificationObservationFact,
    VerificationSourceFact, missing_scenario_reason, verification_sources_issue,
    verification_run_for_receipt, verification_runs_for_sources,
)
from test_delivery_evidence_domain import BINDING, IMPLEMENTATION, SCOPE


NOW = datetime(2026, 10, 13, tzinfo=timezone.utc)
PROOF = replace(IMPLEMENTATION, executions=tuple(replace(p, observed_at=NOW) for p in IMPLEMENTATION.executions))
SOURCE = VerificationSourceFact(PROOF.executions[0].source_ref, PROOF.executions[0].result_revision,
    'a' * 64, 'verification_report.sources')
OBSERVATION = VerificationObservationFact('observation', 'criterion', 'inspection:ports',
    'Public ports', 'Only public ports', 'passed')
RUN = NativeVerificationRun(SCOPE, 'scenario', 'sha256:' + 'a' * 64, 'inspection',
    'receipt', 'report', 'passed', True, (SOURCE,), (OBSERVATION,), NOW + timedelta(seconds=1))


def classify(*runs, complete=True, implementation=PROOF):
    return missing_scenario_reason(scope=SCOPE, binding=BINDING, implementation=implementation,
        scenario_id='scenario', criterion_id='criterion',
        population=NativeVerificationPopulation(tuple(runs), complete))


@pytest.mark.parametrize('outcome,expected', [
    ('passed', Reason.VERIFICATION_ASSOCIATION_MISSING), ('failed', Reason.VERIFICATION_FAILED),
    ('inconclusive', Reason.VERIFICATION_INCONCLUSIVE), ('aborted', Reason.VERIFICATION_INCONCLUSIVE),
    ('unavailable', Reason.VERIFICATION_INCONCLUSIVE),
])
def test_native_unbound_results_keep_distinct_factual_reasons(outcome, expected):
    run = replace(RUN, outcome=outcome, observations=(replace(OBSERVATION, outcome=outcome),))
    reason = classify(run)
    assert reason.code == expected
    assert reason.source_ids == ('receipt',)
    assert reason.criterion_ids == ('criterion',) and reason.scenario_ids == ('scenario',)


def test_mixed_report_uses_applicable_criterion_not_aggregate_failure():
    run = replace(RUN, outcome='failed', observations=(OBSERVATION,
        replace(OBSERVATION, observation_id='other', criterion_id='other', outcome='failed')))
    assert classify(run).code == Reason.VERIFICATION_ASSOCIATION_MISSING
    run = replace(run, observations=(*run.observations, replace(OBSERVATION, observation_id='second', outcome='failed')))
    assert classify(run).code == Reason.VERIFICATION_FAILED


@pytest.mark.parametrize('change', [{'source_ref': 'other'}, {'revision': 'b' * 40}])
def test_old_base_failure_is_mismatch_not_current_product_failure(change):
    run = replace(RUN, sources=(replace(SOURCE, **change),), outcome='failed',
        observations=(replace(OBSERVATION, outcome='failed'),))
    assert classify(run).code == Reason.VERIFICATION_BASE_MISMATCH


def test_unknown_and_unexecuted_do_not_collapse_into_invalid_or_failed():
    assert classify().code == Reason.VERIFICATION_NOT_RUN
    assert classify(complete=False).code == Reason.ASSESSMENT_UNAVAILABLE
    assert classify(replace(RUN, outcome=None, receipt_id=None, authenticated=False)).code == Reason.VERIFICATION_NOT_RUN
    assert classify(replace(RUN, sources=())).code == Reason.VERIFICATION_BASE_UNRESOLVED


def test_invalid_receipt_preserves_native_reasons_without_trusting_failed_claim():
    run = replace(RUN, outcome='failed', authenticated=False,
        rejection_reason=Reason.VERIFICATION_RECEIPT_INVALID, native_reason_codes=('native_signature_invalid',))
    reason = classify(run)
    assert reason.code == Reason.VERIFICATION_RECEIPT_INVALID
    assert reason.native_reason_codes == ('native_signature_invalid',)


def test_distinct_native_heads_are_conflict_not_latest_or_passing_selection():
    assert classify(RUN, replace(RUN, receipt_id='other', observed_at=NOW)).code == Reason.VERIFICATION_CONFLICT
    assert classify(RUN, RUN).code == Reason.VERIFICATION_ASSOCIATION_MISSING


def test_observation_time_and_scope_remain_required_after_exact_base_match():
    assert classify(replace(RUN, observed_at=NOW - timedelta(seconds=1))).native_reason_codes == (
        'verification_observation_predates_implementation',)
    assert classify(replace(RUN, observed_at=None)).code == Reason.VERIFICATION_BASE_UNRESOLVED
    assert classify(replace(RUN, scope=replace(SCOPE, edition=SCOPE.edition + 1))).code == Reason.VERIFICATION_RECEIPT_INVALID
    assert not classify(replace(RUN, scope=replace(SCOPE, board_id='private'), receipt_id='private-receipt')).source_ids
    assert not classify(RUN, replace(RUN, scope=replace(SCOPE, board_id='private'), receipt_id='private-receipt')).source_ids


def test_missing_expected_source_and_conflicting_source_revisions_fail_closed():
    assert verification_sources_issue((('repo:a', 'a'),), ()) == 'verification_base_unresolved'
    assert verification_sources_issue((('repo:a', 'a'), ('repo:a', 'b')), (('repo:a', 'a'),)) == 'verification_base_unresolved'
    assert verification_sources_issue((('repo:a', 'a'),), (('repo:a', 'a'), ('repo:b', 'b'))) == 'verification_base_mismatch'
    assert verification_sources_issue((('repo:a', 'a'), ('repo:b', 'b')), (('repo:a', 'a'), ('repo:b', 'b'))) is None


def test_independent_current_versions_are_selected_before_conflict_classification():
    failed_other = replace(RUN, accepted_result_id='accepted-b', receipt_id='receipt-b',
        sources=(replace(SOURCE, revision='other-commit'),), outcome='failed',
        observations=(replace(OBSERVATION, outcome='failed'),))
    reason = classify(RUN, failed_other)
    assert reason.code == Reason.VERIFICATION_ASSOCIATION_MISSING
    assert reason.source_ids == ('receipt',)
    other_impl = replace(PROOF, executions=tuple(replace(p, result_revision='other-commit') for p in PROOF.executions))
    other = classify(RUN, failed_other, implementation=other_impl)
    assert other.code == Reason.VERIFICATION_FAILED and other.source_ids == ('receipt-b',)


def test_new_same_basis_failure_cannot_borrow_stale_passing_result():
    old = replace(RUN, accepted_result_id='old', current=False)
    failed = replace(RUN, accepted_result_id='new', receipt_id='failure', outcome='failed',
        observations=(replace(OBSERVATION, outcome='failed'),))
    assert classify(old, failed).code == Reason.VERIFICATION_FAILED
    assert classify(old).code == Reason.VERIFICATION_NOT_RUN
    assert verification_run_for_receipt((old, failed), receipt_id=old.receipt_id) is None
    assert verification_run_for_receipt((old, failed), receipt_id=failed.receipt_id, accepted_result_id='new') == failed
    assert verification_run_for_receipt((old, failed), receipt_id=failed.receipt_id, accepted_result_id='old') is None


def test_companion_versions_preserve_ambiguity_until_exact_result_is_selected():
    a = replace(RUN, accepted_result_id='a', sources=(SOURCE, replace(SOURCE, source_ref='companion', revision='a')))
    b = replace(a, accepted_result_id='b', receipt_id='b-receipt',
        sources=(SOURCE, replace(SOURCE, source_ref='companion', revision='b')))
    assert classify(a, b).code == Reason.VERIFICATION_CONFLICT
    assert verification_run_for_receipt((a, b), receipt_id=a.receipt_id, accepted_result_id='a') == a
    assert verification_runs_for_sources((a, b), ((SOURCE.source_ref, SOURCE.revision), ('companion', 'b'))) == (b,)


def test_incomplete_population_cannot_be_rescued_by_an_apparently_valid_result():
    assert classify(RUN, complete=False).code == Reason.ASSESSMENT_UNAVAILABLE
