"""P03: full native scenario topology cannot be replaced by selected bindings."""

from dataclasses import replace

import pytest

from okto_pulse.core.domain.delivery_evidence import DeliveryPhase, DeliveryWaiverFact
from okto_pulse.core.domain.effective_delivery_coverage import evaluate_effective_delivery_coverage
from test_delivery_evidence_domain import SNAPSHOT
from test_effective_delivery_coverage import case, planned_scenarios


def evaluate(implementations, tests, required, *, waivers=()):
    inventory, _, _ = case()
    return evaluate_effective_delivery_coverage(
        inventory=inventory,
        snapshot=replace(SNAPSHOT, implementations=tuple(i.fact for i in implementations),
                         tests=tuple(t.fact for t in tests), waivers=waivers),
        implementations=implementations, tests=tests,
        admitted_methods=frozenset({'automated_test'}), required_scenarios=required,
    )


@pytest.mark.parametrize('complement', ['failed', 'ready', 'unsigned', 'unbound'])
def test_V01_V02_V03_V05_all_required_scenarios_count(complement):
    _, implementations, tests = case()
    additional = replace(tests[0], fact=replace(tests[0].fact,
        id='complement', scenario_id='update-path'))
    required = planned_scenarios((*tests, additional))
    if complement in {'failed', 'ready'}:
        additional = replace(additional, fact=replace(additional.fact, result=complement))
    elif complement == 'unsigned':
        additional = replace(additional, fact=replace(additional.fact, current_verified_run=False))
    selected = tests if complement == 'unbound' else (*tests, additional)
    result = evaluate(implementations, selected, required)
    assert not result.allowed
    assert result.rows[0].implementation_satisfied
    assert not result.rows[0].test_satisfied
    assert result.rows[0].missing_scenarios == (('ui', 'functional', 'update-path'),)
    assert result.rows[0].missing_criteria == (('ui', 'functional'),)


def test_all_required_scenarios_pass_without_inflating_criterion_count():
    _, implementations, tests = case()
    additional = replace(tests[0], fact=replace(tests[0].fact,
        id='complement', scenario_id='update-path'))
    tests = (*tests, additional)
    result = evaluate(implementations, tests, planned_scenarios(tests))
    assert result.allowed
    assert not result.rows[0].missing_scenarios


@pytest.mark.parametrize('required', [None, (), (False,)])
def test_missing_or_malformed_topology_never_uses_bound_subset(required):
    _, implementations, tests = case()
    result = evaluate(implementations, tests, required)
    assert not result.allowed
    assert result.rows[0].implementation_satisfied
    assert not result.rows[0].test_satisfied


def test_duplicate_scenario_identity_is_not_arbitrarily_resolved():
    _, implementations, tests = case()
    required = planned_scenarios(tests)
    result = evaluate(implementations, tests, (*required, required[0]))
    assert not result.allowed
    assert 'delivery_verification_scenario_population_unavailable' in result.blockers


@pytest.mark.parametrize('damage', ['method', 'assignment', 'incomplete'])
def test_bound_result_cannot_override_native_plan(damage):
    _, implementations, tests = case()
    required = planned_scenarios(tests)
    changed = replace(required[0], **{
        'method': {'method': 'inspection'},
        'assignment': {'test_card_ids': ('different-test',)},
        'incomplete': {'complete': False},
    }[damage])
    result = evaluate(implementations, tests, (changed, required[1]))
    assert not result.allowed
    assert result.rows[0].missing_scenarios == (('ui', 'functional', 'scenario-ui'),)


def test_V04_stale_previous_attempt_does_not_block_native_current_retry():
    _, implementations, tests = case()
    previous = replace(tests[0], fact=replace(tests[0].fact,
        id='old-failure', result='failed', current_verified_run=False, receipt_id='old-receipt'))
    result = evaluate(implementations, (*tests, previous), planned_scenarios(tests))
    assert result.allowed
    assert previous.fact.id in result.rejected_record_ids


@pytest.mark.parametrize('outcome', ['passed', 'failed'])
def test_V06_distinct_current_receipts_without_native_supersession_are_conflict(outcome):
    _, implementations, tests = case()
    conflicting = replace(tests[0], fact=replace(tests[0].fact,
        id='conflicting-run', result=outcome, receipt_id='another-receipt'))
    result = evaluate(implementations, (*tests, conflicting), planned_scenarios(tests))
    assert not result.allowed
    assert result.rows[0].implementation_satisfied
    assert 'delivery_verification_run_conflict' in result.blockers
    assert result.rows[0].conflicting_scenario_receipts == (
        ('scenario-ui', tuple(sorted((tests[0].fact.receipt_id, 'another-receipt')))),)


def test_two_bindings_to_identical_current_receipt_are_not_conflicting_attempts():
    _, implementations, tests = case()
    additional = replace(tests[0], fact=replace(tests[0].fact, id='another-binding'))
    result = evaluate(implementations, (*tests, additional), planned_scenarios(tests))
    assert result.allowed
    assert not result.rows[0].conflicting_scenario_receipts


def test_authorized_exception_preserves_conflict_detail_without_claiming_proof():
    inventory, implementations, tests = case()
    conflicting = replace(tests[0], fact=replace(tests[0].fact,
        id='conflicting-run', result='failed', receipt_id='another-receipt'))
    waiver = DeliveryWaiverFact('waiver', SNAPSHOT.scope, inventory.rows[0].binding,
        DeliveryPhase.TEST, 'Explicitly accepted risk', 'human', 'authorization', True,
        disposition='risk_accepted')
    result = evaluate(implementations, (*tests, conflicting), planned_scenarios(tests), waivers=(waiver,))
    assert result.allowed
    row = result.rows[0]
    assert row.test_waiver_ids == ('waiver',)
    assert row.conflicting_scenario_receipts
    assert tests[0].fact.id not in row.test_ids
