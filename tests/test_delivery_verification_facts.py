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


def _effective_two_bases(*, companions=False):
    from test_delivery_obligation_assessment import snapshot, with_facts
    from test_effective_delivery_coverage import planned_scenarios

    value = snapshot()
    context = value.effective_context
    implementations = context.implementations
    if not companions:
        second = implementations[1]
        second = replace(second, fact=replace(second.fact, executions=tuple(
            replace(proof, source_ref='independent-source', result_revision='b' * 40)
            for proof in second.fact.executions)))
        implementations = (implementations[0], second)
    tests = tuple(replace(test, fact=replace(test.fact, scenario_id='shared-scenario',
        receipt_id='receipt-' + str(index))) for index, test in enumerate(context.tests))
    runs = tuple(replace(run, scenario_id='shared-scenario', receipt_id=tests[index].fact.receipt_id,
        sources=((SOURCE, replace(SOURCE, source_ref='companion', revision=str(index)))
                 if companions else tuple(replace(SOURCE, source_ref=proof.source_ref,
                     revision=proof.result_revision) for proof in implementations[index].fact.executions)))
        for index, run in enumerate(context.native_verification.runs))
    value = with_facts(value, implementations=implementations, tests=tests)
    return replace(value, effective_context=replace(value.effective_context,
        required_scenarios=planned_scenarios(tests),
        native_verification=replace(context.native_verification, runs=runs)))


@pytest.mark.parametrize('companions', [False, True])
def test_effective_coverage_admits_exact_independent_bases_and_ignores_previous_heads(companions):
    from okto_pulse.core.domain.effective_delivery_coverage import evaluate_effective_facts

    value = _effective_two_bases(companions=companions)
    native = value.effective_context.native_verification
    # Duplicate reads do not create a new head, and historical failures do not
    # invalidate either current, explicitly associated full basis.
    native = replace(native, runs=(*native.runs, native.runs[0],
        replace(native.runs[0], receipt_id='previous', outcome='failed', current=False)))
    value = replace(value, effective_context=replace(value.effective_context, native_verification=native))
    result = evaluate_effective_facts(value)
    assert result.allowed
    assert all(row.row.test_satisfied and not row.row.conflicting_scenario_receipts
               for row in result.contribution_rows)
    assert all(check.satisfied for check in result.scenario_checks)


@pytest.mark.parametrize('companions', [False, True])
def test_effective_conflict_is_limited_to_its_full_basis_and_card(companions):
    from okto_pulse.core.domain.effective_delivery_coverage import evaluate_effective_facts

    value = _effective_two_bases(companions=companions)
    native = value.effective_context.native_verification
    # The conflicting native head deliberately has no Delivery association.
    native = replace(native, runs=(*native.runs,
        replace(native.runs[0], receipt_id='conflicting-head', outcome='failed')))
    value = replace(value, effective_context=replace(value.effective_context, native_verification=native))
    result = evaluate_effective_facts(value)
    rows = {item.card_id: item.row for item in result.contribution_rows}
    assert not rows['ui'].test_satisfied
    assert rows['authorization'].test_satisfied
    assert not rows['authorization'].conflicting_scenario_receipts
    assert rows['ui'].conflicting_scenario_receipts == (
        ('shared-scenario', ('conflicting-head', 'receipt-0')),)
    assert 'delivery_verification_run_conflict' in result.blockers


@pytest.mark.parametrize('damage', ['generation', 'semantics', 'method', 'sources', 'scope', 'incomplete'])
def test_effective_incompatible_authority_explains_why_existing_bindings_cannot_count(damage):
    from okto_pulse.core.domain.effective_delivery_coverage import evaluate_effective_facts

    value = _effective_two_bases()
    native = value.effective_context.native_verification
    first, second = native.runs
    changes = {
        'generation': {'generation': 2},
        'semantics': {'scenario_sha256': 'sha256:' + 'b' * 64},
        'method': {'method': 'inspection'},
        'sources': {'sources': ()},
        'scope': {'scope': replace(second.scope, board_id='foreign-board')},
        'incomplete': {},
    }
    native = replace(native, runs=(first, replace(second, **changes[damage])), complete=damage != 'incomplete')
    value = replace(value, effective_context=replace(value.effective_context, native_verification=native))
    result = evaluate_effective_facts(value)
    expected = {'scope': Reason.VERIFICATION_RECEIPT_INVALID,
                'incomplete': Reason.ASSESSMENT_UNAVAILABLE}.get(damage, Reason.VERIFICATION_CONFLICT)
    assert not any(item.row.test_satisfied for item in result.contribution_rows)
    assert result.scenario_checks
    assert {reason.code for check in result.scenario_checks for reason in check.reasons} == {expected}
    if damage == 'scope':
        assert not any(reason.source_ids for check in result.scenario_checks for reason in check.reasons)


def test_unresolved_association_cannot_contaminate_another_binding_of_the_same_implementation():
    from okto_pulse.core.domain.delivery_evidence import DeliveryObligation
    from okto_pulse.core.domain.effective_delivery_coverage import evaluate_effective_facts
    from test_delivery_obligation_assessment import snapshot, with_facts
    from test_effective_delivery_coverage import planned_scenarios

    value = snapshot()
    context = value.effective_context
    first = context.inventory.rows[0]
    second_binding = replace(first.binding, obligation_ref='fr:second')
    owner = first.contributions[0]
    rows = (replace(first, contributions=(owner,)),
            replace(first, binding=second_binding, contributions=(owner,)))
    implementation = context.implementations[0]
    implementation = replace(implementation, fact=replace(implementation.fact,
        bindings=(first.binding, second_binding),
        contributions=(*implementation.fact.contributions,
            replace(implementation.fact.contributions[0], binding=second_binding))),
        scopes=(*implementation.scopes, replace(implementation.scopes[0], binding=second_binding)))
    original = context.tests[0]
    uncorroborated = replace(original, fact=replace(original.fact, id='other-binding',
        bindings=(second_binding,), receipt_id='unresolved-receipt'))
    value = with_facts(value, implementations=(implementation,), tests=(original, uncorroborated))
    value = replace(value, obligations=(value.obligations[0],
        DeliveryObligation(second_binding, second_binding.obligation_ref)),
        effective_context=replace(value.effective_context,
            inventory=replace(context.inventory, rows=rows),
            required_scenarios=planned_scenarios((original, uncorroborated)),
            native_verification=replace(context.native_verification, runs=(context.native_verification.runs[0],))))
    result = evaluate_effective_facts(value)
    assessed = {row.binding: row for row in result.rows}
    assert assessed[first.binding].test_satisfied
    assert not assessed[first.binding].scenario_findings
    assert not assessed[second_binding].test_satisfied
    assert not assessed[first.binding].conflicting_scenario_receipts
    assert 'unresolved-receipt' not in repr(assessed[first.binding])
