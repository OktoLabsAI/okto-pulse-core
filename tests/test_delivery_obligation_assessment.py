"""Metrics produced from canonical proof evaluation, not hand-built scores."""

from dataclasses import replace

import pytest

from okto_pulse.core.domain.delivery_assessment import assess_obligation_delivery, aggregate_delivery_metrics
from okto_pulse.core.domain.delivery_evidence import DeliveryPhase, DeliveryWaiverFact, DeliveryRecordRejection
from okto_pulse.core.domain.effective_delivery_coverage import EffectiveDeliveryContext
from okto_pulse.core.domain.enums import CardType
from test_delivery_evidence_domain import SNAPSHOT, BINDING
from test_effective_delivery_coverage import case, planned_scenarios
from test_delivery_verification_facts import NOW, RUN, SOURCE
from okto_pulse.core.domain.delivery_verification import NativeVerificationPopulation


def snapshot():
    inventory, implementations, tests = case()
    implementations = tuple(replace(i, fact=replace(i.fact,
        executions=tuple(replace(p, observed_at=NOW) for p in i.fact.executions))) for i in implementations)
    native = NativeVerificationPopulation(tuple(replace(RUN,
        scenario_id=t.fact.scenario_id, receipt_id=t.fact.receipt_id,
        observations=(), method='automated_test', sources=(SOURCE,)) for t in tests), True)
    return replace(SNAPSHOT, implementations=tuple(i.fact for i in implementations),
        tests=tuple(t.fact for t in tests), effective_context=EffectiveDeliveryContext(
            inventory, implementations, tests, frozenset({'automated_test'}),
            required_scenarios=planned_scenarios(tests), native_verification=native))


def with_facts(value, *, implementations=None, tests=None):
    context = value.effective_context
    implementations = context.implementations if implementations is None else implementations
    tests = context.tests if tests is None else tests
    return replace(value, implementations=tuple(i.fact for i in implementations), tests=tuple(t.fact for t in tests),
        effective_context=replace(context, implementations=implementations, tests=tests))


def test_proof_and_lifecycle_are_separate_without_forged_done():
    value = snapshot()
    value = with_facts(value,
        implementations=tuple(replace(i, fact=replace(i.fact, card_status='in_progress')) for i in value.effective_context.implementations),
        tests=tuple(replace(t, fact=replace(t.fact, card_status='in_progress')) for t in value.effective_context.tests))
    before = repr(value)
    result = assess_obligation_delivery(value)
    metrics = aggregate_delivery_metrics(result)
    assert metrics.implementation.percent == metrics.verification.percent == 100
    assert not result.obligations[0].implementation.lifecycle_satisfied
    assert not result.obligations[0].verification.lifecycle_satisfied
    assert repr(value) == before


@pytest.mark.parametrize('card_type', [CardType.NORMAL, CardType.BUG])
def test_local_contribution_complete_does_not_complete_global_obligation(card_type):
    value = snapshot()
    own = replace(value.effective_context.implementations[0], fact=replace(
        value.implementations[0], card_type=card_type))
    value = with_facts(value, implementations=(own,))
    local = aggregate_delivery_metrics(assess_obligation_delivery(value, card_id='ui', card_type=card_type))
    global_result = assess_obligation_delivery(value)
    global_metrics = aggregate_delivery_metrics(global_result)
    assert local.implementation.percent == local.verification.percent == 100
    assert global_metrics.implementation.percent == global_metrics.verification.percent == 0
    assert global_metrics.implementation.total == 1
    assert global_result.obligations[0].implementation.state == 'partial'


@pytest.mark.parametrize('absence', ['topology', 'verifier', 'native_population'])
def test_verification_unknown_keeps_independent_implementation(absence):
    value = snapshot()
    changes = {'topology': dict(required_scenarios=None), 'verifier': dict(admitted_methods=None),
        'native_population': dict(native_verification=replace(value.effective_context.native_verification, complete=False))}[absence]
    value = replace(value, complete=False, facts_complete=True,
        effective_context=replace(value.effective_context, **changes))
    result = assess_obligation_delivery(value)
    metrics = aggregate_delivery_metrics(result)
    assert result.completeness.evaluation_complete is False
    assert metrics.implementation.percent == 100
    assert metrics.verification.percent is None and metrics.verification.status == 'unknown'
    assert metrics.verification.total == 1


@pytest.mark.parametrize('outcome,expected', [('failed', 'failed'), ('inconclusive', 'missing'), ('passed', 'missing')])
def test_unbound_current_outcomes_explain_verification_without_free_credit(outcome, expected):
    value = snapshot()
    native = value.effective_context.native_verification
    native = replace(native, runs=(replace(native.runs[0], outcome=outcome), native.runs[1]))
    value = with_facts(value, tests=value.effective_context.tests[1:])
    value = replace(value, effective_context=replace(value.effective_context, native_verification=native))
    result = assess_obligation_delivery(value)
    metrics = aggregate_delivery_metrics(result)
    assert metrics.implementation.percent == 100 and metrics.verification.percent == 0
    assert result.obligations[0].verification.state == expected
    assert result.obligations[0].verification.reasons[0].scenario_ids == ('scenario-ui',)
    if outcome == 'passed':
        assert result.obligations[0].verification.reasons[0].code == 'verification_association_missing'


def test_conflict_preserves_implementation_and_cannot_be_replaced_by_passing_receipt():
    value = snapshot()
    tests = value.effective_context.tests
    duplicate = replace(tests[0], fact=replace(tests[0].fact, id='another', receipt_id='another-receipt'))
    value = with_facts(value, tests=(*tests, duplicate))
    result = assess_obligation_delivery(value)
    assert result.obligations[0].verification.state == 'conflict'
    assert aggregate_delivery_metrics(result).implementation.percent == 100


@pytest.mark.parametrize('disposition,state,percent', [('risk_accepted', 'waived', 0), ('not_applicable', 'not_applicable', None)])
def test_exception_changes_classification_not_factual_proof(disposition, state, percent):
    value = with_facts(snapshot(), tests=())
    waiver = DeliveryWaiverFact('waiver', value.scope, BINDING, DeliveryPhase.TEST,
        'Human decision', 'human', 'authority', True, disposition)
    result = assess_obligation_delivery(replace(value, waivers=(waiver,)))
    phase = result.obligations[0].verification
    assert phase.state == state and phase.intrinsic_state == 'missing'
    assert not phase.proof_satisfied
    assert aggregate_delivery_metrics(result).verification.percent == percent
    sufficient = assess_obligation_delivery(replace(snapshot(), waivers=(replace(waiver, disposition='risk_accepted'),)))
    assert sufficient.obligations[0].verification.unused_exception_ids == ('waiver',)
    assert aggregate_delivery_metrics(sufficient).verification.percent == 100


def test_risk_exception_does_not_make_unavailable_verification_known():
    value = snapshot()
    waiver = DeliveryWaiverFact('waiver', value.scope, BINDING, DeliveryPhase.TEST,
        'Human accepted risk', 'human', 'authority', True, 'risk_accepted')
    value = replace(value, waivers=(waiver,), effective_context=replace(value.effective_context,
        admitted_methods=None))
    result = assess_obligation_delivery(value)
    assert result.obligations[0].verification.state == 'waived'
    assert result.obligations[0].verification.intrinsic_state == 'unknown'
    assert not result.completeness.verification_complete
    metrics = aggregate_delivery_metrics(result)
    assert metrics.verification.status == 'unknown' and metrics.verification.percent is None
    assert metrics.implementation.percent == 100


def test_known_missing_allocation_keeps_unit_in_denominator():
    value = snapshot()
    inventory = value.effective_context.inventory
    missing = replace(inventory.rows[0], contributions=(), blockers=('delivery_inventory_work_unassigned',))
    value = replace(value, complete=False, facts_complete=True,
        effective_context=replace(value.effective_context, inventory=replace(inventory, rows=(missing,))))
    result = assess_obligation_delivery(value)
    metric = aggregate_delivery_metrics(result).implementation
    assert not result.completeness.scope_resolved
    assert metric.total == 1 and metric.percent == 0
    assert result.obligations[0].implementation.reasons[0].code == 'scope_allocation_unresolved'


def test_partial_declarations_never_accumulate_to_complete():
    value = snapshot()
    implementations = tuple(replace(i, fact=replace(i.fact,
        contributions=tuple(replace(c, contribution='partial') for c in i.fact.contributions)))
        for i in value.effective_context.implementations)
    result = assess_obligation_delivery(with_facts(value, implementations=implementations))
    assert result.obligations[0].implementation.state == 'partial'
    assert aggregate_delivery_metrics(result).implementation.percent == 0


def test_irrelevant_rejected_history_cannot_lower_sufficient_current_proof():
    value = snapshot()
    rejected = DeliveryRecordRejection('old', value.scope, 'ui', 'implementation', (BINDING,),
        ('delivery_investigation_receipt_revoked',))
    assert aggregate_delivery_metrics(assess_obligation_delivery(replace(value, rejected_records=(rejected,)))) == (
        aggregate_delivery_metrics(assess_obligation_delivery(value)))


def test_actual_receipt_revocation_removes_credit_and_retains_native_cause():
    value = snapshot()
    damaged = replace(value.effective_context.implementations[0], fact=replace(value.implementations[0],
        executions=tuple(replace(p, current_accepted_execution=False,
            rejection_reasons=('delivery_investigation_receipt_revoked',)) for p in value.implementations[0].executions)))
    value = with_facts(value, implementations=(damaged, value.effective_context.implementations[1]))
    result = assess_obligation_delivery(value)
    assert result.obligations[0].implementation.state == 'stale'
    assert any(reason.code == 'implementation_proof_revoked' for reason in result.obligations[0].implementation.reasons)
    assert aggregate_delivery_metrics(result).implementation.percent == 0


def test_foreign_rejection_neither_leaks_identity_nor_yields_success():
    value = snapshot()
    rejected = DeliveryRecordRejection('private-record', replace(value.scope, board_id='private'),
        'private-card', 'implementation', (BINDING,), ('delivery_record_revoked',))
    result = assess_obligation_delivery(replace(value, rejected_records=(rejected,)))
    assert aggregate_delivery_metrics(result).implementation.status == 'unknown'
    assert 'private' not in repr(result)


def test_native_fallback_card_obligation_is_calculable_without_artificial_test():
    value = snapshot()
    context = value.effective_context
    binding = replace(BINDING, obligation_ref='card:ui')
    contribution = replace(context.inventory.rows[0].contributions[0], scope='whole_card', criterion_ids=())
    obligation = replace(context.inventory.rows[0], binding=binding, family='card', contributions=(contribution,))
    own = context.implementations[0]
    own = replace(own, fact=replace(own.fact, bindings=(binding,),
        contributions=(replace(own.fact.contributions[0], binding=binding),)),
        scopes=(replace(own.scopes[0], binding=binding),))
    value = replace(value, obligations=(replace(value.obligations[0], binding=binding),),
        effective_context=replace(context, inventory=replace(context.inventory, rows=(obligation,))))
    value = with_facts(value, implementations=(own,), tests=())
    result = assess_obligation_delivery(value, card_id='ui', card_type=CardType.NORMAL)
    metrics = aggregate_delivery_metrics(result)
    assert metrics.implementation.percent == 100
    assert metrics.verification.percent == 0
    assert result.obligations[0].binding == binding


@pytest.mark.parametrize('status,state,percent', [('verified', 'satisfied', 100), ('failed', 'failed', 0),
    ('conflict', 'conflict', 0), ('unavailable', 'unknown', None), ('revoked', 'stale', 0)])
def test_decision_reuses_native_inspection_without_invented_implementation(status, state, percent):
    from okto_pulse.core.domain.decision_verification import DecisionVerificationPlan, DecisionVerification
    from okto_pulse.core.domain.decision_review import DecisionInspectionState
    value = snapshot()
    context = value.effective_context
    binding = replace(BINDING, obligation_ref='decision:choice')
    plan = DecisionVerificationPlan('choice', DecisionVerification(inspection={
        'condition': 'Scope decision is coherent', 'scope_refs': [{'kind': 'spec', 'id': value.scope.spec_id}]}), ())
    obligation = replace(context.inventory.rows[0], binding=binding, family='decision', contributions=(), decision_plan=plan)
    value = replace(value, obligations=(replace(value.obligations[0], binding=binding),),
        effective_context=replace(context, inventory=replace(context.inventory, rows=(obligation,)),
            decision_inspections=(DecisionInspectionState('choice', 'd' * 64, status, ('review',)),)))
    value = with_facts(value, implementations=(), tests=())
    result = assess_obligation_delivery(value)
    metrics = aggregate_delivery_metrics(result)
    assert result.obligations[0].implementation.state == 'not_applicable'
    assert metrics.implementation.percent is None
    assert result.obligations[0].verification.state == state
    assert metrics.verification.percent == percent
    assert result.obligations[0].verification.evidence_ids == ('review',)
