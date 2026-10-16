"""Test Card units consume the shared evaluated joins, not obligation averages."""

from dataclasses import replace

import pytest

from okto_pulse.core.domain.delivery_assessment import assess_test_delivery, aggregate_delivery_metrics
from okto_pulse.core.domain.delivery_evidence import DeliveryPhase, DeliveryWaiverFact
from okto_pulse.core.domain.effective_delivery_coverage import evaluate_effective_facts
from test_delivery_obligation_assessment import snapshot, with_facts


def assess(value, ids=('scenario-ui', 'scenario-authorization')):
    return assess_test_delivery(value, card_id='test-card-1', scenario_ids=ids)


def outcome(value, scenario, result):
    context = value.effective_context
    tests = tuple(replace(t, fact=replace(t.fact, result=result)) if t.fact.scenario_id == scenario else t
        for t in context.tests)
    value = with_facts(value, tests=tests)
    native = replace(context.native_verification, runs=tuple(replace(r, outcome=result)
        if r.scenario_id == scenario else r for r in context.native_verification.runs))
    return replace(value, effective_context=replace(value.effective_context, native_verification=native))


def test_M20_test_card_counts_scenarios_even_when_one_obligation_has_two_conditions():
    value = outcome(snapshot(), 'scenario-authorization', 'failed')
    result = assess(value)
    metrics = aggregate_delivery_metrics(result)
    assert not result.obligations
    assert metrics.implementation.status == 'not_applicable' and metrics.implementation.percent is None
    assert metrics.verification.unit == 'scenario' and metrics.verification.total == 2
    assert metrics.verification.percent == 50
    assert {row.scenario_id: row.verification.state for row in result.scenarios} == {
        'scenario-ui': 'satisfied', 'scenario-authorization': 'failed'}
    assert not evaluate_effective_facts(value, require_lifecycle=False).allowed


def test_known_current_positive_without_binding_requires_association():
    value = snapshot()
    result = assess(with_facts(value, tests=value.effective_context.tests[:1]))
    assert aggregate_delivery_metrics(result).verification.percent == 50
    missing = next(row for row in result.scenarios if row.scenario_id == 'scenario-authorization')
    assert missing.verification.state == 'missing'
    assert {r.code for r in missing.verification.reasons} == {'verification_association_missing'}


def test_native_retry_can_pass_without_rewriting_old_failure_or_unrelated_scenarios():
    value = snapshot()
    old = replace(value.effective_context.tests[0], fact=replace(value.tests[0],
        id='old', receipt_id='old-receipt', result='failed', current_verified_run=False))
    value = with_facts(value, tests=(*value.effective_context.tests, old))
    before = repr(value)
    assert aggregate_delivery_metrics(assess(value)).verification.percent == 100
    assert repr(value) == before


def test_scenario_result_from_other_card_updates_the_same_assigned_native_scenario():
    value = snapshot()
    tests = tuple(replace(t, fact=replace(t.fact, card_id='runner-card')) for t in value.effective_context.tests)
    plans = tuple(replace(p, test_card_ids=(*p.test_card_ids, 'runner-card')) for p in value.effective_context.required_scenarios)
    value = with_facts(value, tests=tests)
    value = replace(value, effective_context=replace(value.effective_context, required_scenarios=plans))
    result = assess(value)
    assert aggregate_delivery_metrics(result).verification.percent == 100
    assert all(row.verification.evidence_ids for row in result.scenarios)
    assert {t.card_id for t in value.tests} == {'runner-card'}


def test_proof_100_before_test_card_done_keeps_lifecycle_pending():
    value = snapshot()
    value = with_facts(value, tests=tuple(replace(t, fact=replace(t.fact, card_status='in_progress'))
        for t in value.effective_context.tests))
    result = assess(value)
    assert aggregate_delivery_metrics(result).verification.percent == 100
    assert all(row.verification.proof_satisfied and not row.verification.lifecycle_satisfied for row in result.scenarios)


def test_missing_one_implementation_cannot_be_hidden_by_another_contribution():
    value = snapshot()
    value = with_facts(value, implementations=value.effective_context.implementations[:1])
    result = assess(value)
    assert aggregate_delivery_metrics(result).verification.percent == 50
    missing = next(row for row in result.scenarios if row.scenario_id == 'scenario-authorization')
    assert missing.verification.reasons[0].code == 'verification_dependency_pending'


@pytest.mark.parametrize('damage', ['base', 'receipt', 'native_conflict'])
def test_invalid_native_current_proof_keeps_exact_scenario_cause(damage):
    value = snapshot()
    context = value.effective_context
    first, second = context.native_verification.runs
    if damage == 'base':
        first = replace(first, sources=tuple(replace(s, revision='b' * 40) for s in first.sources))
        value = with_facts(value, tests=(context.tests[1],))
        runs = (first, second)
        expected = 'verification_base_mismatch'
    elif damage == 'receipt':
        first = replace(first, authenticated=False)
        runs = (first, second)
        expected = 'verification_receipt_invalid'
    else:
        runs = (first, replace(first, receipt_id='conflict'), second)
        expected = 'verification_conflict'
    value = replace(value, effective_context=replace(value.effective_context,
        native_verification=replace(context.native_verification, runs=runs)))
    result = assess(value)
    failed = next(row for row in result.scenarios if row.scenario_id == 'scenario-ui')
    assert expected in {r.code for r in failed.verification.reasons}
    assert aggregate_delivery_metrics(result).verification.percent == 50


@pytest.mark.parametrize('scenario_ids', [None, (), ('missing',), ('scenario-ui', 'missing')])
def test_missing_native_subject_is_unknown_without_fabricated_digest(scenario_ids):
    result = assess(snapshot(), scenario_ids)
    metrics = aggregate_delivery_metrics(result)
    assert metrics.verification.status == 'unknown'
    assert metrics.verification.percent is metrics.verification.total is None
    assert all(row.scenario_id != 'missing' for row in result.scenarios)


def test_duplicate_assignments_count_once_and_only_requested_scenarios_are_counted():
    value = outcome(snapshot(), 'scenario-authorization', 'failed')
    result = assess(value, ('scenario-ui', 'scenario-ui'))
    metrics = aggregate_delivery_metrics(result)
    assert metrics.verification.percent == 100 and metrics.verification.total == 1


@pytest.mark.parametrize('disposition,state,percent', [('risk_accepted', 'waived', 0), ('not_applicable', 'not_applicable', None)])
def test_spec_phase_exception_applies_to_checks_without_creating_a_card_waiver(disposition, state, percent):
    value = with_facts(snapshot(), tests=())
    waiver = DeliveryWaiverFact('human-exception', value.scope, value.obligations[0].binding,
        DeliveryPhase.TEST, 'Human decision for the Spec obligation', 'human', 'authority', True, disposition)
    result = assess(replace(value, waivers=(waiver,)))
    assert all(row.verification.state == state for row in result.scenarios)
    assert aggregate_delivery_metrics(result).verification.percent == percent
    assert value.waivers == ()


def test_obsolete_risk_is_unused_when_current_scenario_has_proof():
    value = snapshot()
    waiver = DeliveryWaiverFact('human-exception', value.scope, value.obligations[0].binding,
        DeliveryPhase.TEST, 'Accepted risk', 'human', 'authority', True, 'risk_accepted')
    result = assess(replace(value, waivers=(waiver,)))
    assert aggregate_delivery_metrics(result).verification.percent == 100
    assert all(row.verification.unused_exception_ids == ('human-exception',) for row in result.scenarios)


def test_foreign_native_run_never_discloses_its_receipt_or_provides_credit():
    value = snapshot()
    native = value.effective_context.native_verification
    native = replace(native, runs=(*native.runs, replace(native.runs[0],
        scope=replace(value.scope, board_id='private-board'), receipt_id='private-receipt')))
    result = assess(replace(value, effective_context=replace(value.effective_context, native_verification=native)))
    assert aggregate_delivery_metrics(result).verification.status == 'unknown'
    assert 'private' not in repr(result)


def test_mixed_report_cannot_turn_partially_verified_scenario_into_success():
    from okto_pulse.core.domain.delivery_verification import VerificationObservationFact
    value = snapshot()
    context = value.effective_context
    mixed = replace(context.tests[0], criterion_ids=('functional', 'technical'),
        verification_method='inspection', passing_criterion_ids=('functional',),
        fact=replace(context.tests[0].fact, result='failed', verified_implementation_ids=('ui', 'authorization')))
    plan = replace(context.required_scenarios[0], criterion_ids=mixed.criterion_ids, method='inspection')
    run = replace(context.native_verification.runs[0], method='inspection', outcome='failed',
        observations=tuple(VerificationObservationFact('o-' + criterion, criterion, 'inspection:scope',
            'Expected', 'Observed', result) for criterion, result in [('functional', 'passed'), ('technical', 'failed')]))
    value = with_facts(value, tests=(mixed,))
    value = replace(value, effective_context=replace(value.effective_context, admitted_methods=frozenset({'inspection'}),
        required_scenarios=(plan,), native_verification=replace(context.native_verification, runs=(run,))))
    result = assess(value, ('scenario-ui',))
    assert aggregate_delivery_metrics(result).verification.percent == 0
    assert result.scenarios[0].verification.state == 'failed'
    checks = evaluate_effective_facts(value, require_lifecycle=False).scenario_checks
    assert {c.criterion_id: c.satisfied for c in checks} == {'functional': True, 'technical': False}


def test_duplicate_association_of_exact_proof_does_not_create_a_missing_test():
    value = snapshot()
    first = value.effective_context.implementations[0]
    duplicate = replace(first, fact=replace(first.fact, id='equivalent', actor_id='different-author'))
    value = with_facts(value, implementations=(*value.effective_context.implementations, duplicate))
    result = assess(value)
    assert aggregate_delivery_metrics(result).verification.percent == 100
    assert {check.implementation_id for check in evaluate_effective_facts(value).scenario_checks} == {
        'ui', 'authorization', 'equivalent'}
