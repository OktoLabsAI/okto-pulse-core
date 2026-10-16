"""P02 obligation aggregation counts proof, never maturity or lifecycle."""

from dataclasses import replace

import pytest

from okto_pulse.core.domain.delivery_assessment import (
    AssessmentCompleteness, DeliveryAssessment, ObligationAssessment,
    UnitPhaseAssessment, UnitState, aggregate_delivery_metrics,
    ScenarioAssessment,
    apply_delivery_exceptions,
)
from okto_pulse.core.domain.delivery_evidence import DeliveryBinding, DeliveryScope, DeliveryPhase, DeliveryWaiverFact
from okto_pulse.core.domain.enums import CardType


def phase(state, *, done=False):
    return UnitPhaseAssessment(state=UnitState(state),
        intrinsic_state=UnitState.MISSING if state == 'waived' else UnitState(state),
        proof_satisfied=True if state == 'satisfied' else None if state == 'unknown' else False,
        lifecycle_satisfied=done, exception_ids=('waiver',) if state == 'waived' else ())


def assessment(verification, *, implementation=None, done=False):
    implementation = implementation or ['satisfied'] * len(verification)
    rows = tuple(ObligationAssessment(DeliveryBinding(f'fr:r{i}', 'a' * 64), 1, ('task',),
        phase(implemented, done=done), phase(verified, done=done))
        for i, (implemented, verified) in enumerate(zip(implementation, verification, strict=True)))
    return DeliveryAssessment(DeliveryScope('board', 'spec', 1),
        AssessmentCompleteness(True, True, True, True), rows)


@pytest.mark.parametrize('verified,expected', [(0, 0), (6, 75), (8, 100)], ids=['M01', 'M02', 'M03'])
def test_independent_metrics_before_done(verified, expected):
    result = aggregate_delivery_metrics(assessment(['satisfied'] * verified + ['missing'] * (8 - verified)))
    assert result.implementation.percent == 100
    assert result.verification.percent == expected


def test_M05_round_down_instead_of_manufacturing_completion():
    result = aggregate_delivery_metrics(assessment(['satisfied'] * 999 + ['missing']))
    assert result.verification.percent == 99
    assert result.verification.unresolved == 1


@pytest.mark.parametrize('state,expected,applicable', [('not_applicable', 100, 6), ('waived', 75, 8)], ids=['M08', 'M09'])
def test_applicability_is_distinct_from_accepting_risk(state, expected, applicable):
    result = aggregate_delivery_metrics(assessment(['satisfied'] * 6 + [state] * 2))
    assert result.verification.percent == expected
    assert result.verification.applicable == applicable
    assert result.verification.total == 8


def test_M06_unknown_population_preserves_partial_counts_and_null_denominator():
    value = assessment(['satisfied', 'missing'])
    value = replace(value, completeness=replace(value.completeness, population_known=False))
    result = aggregate_delivery_metrics(value)
    for metric in (result.implementation, result.verification):
        assert metric.status == 'unknown'
        assert metric.percent is metric.total is metric.applicable is metric.unresolved is None
        assert metric.known_counts is not None
    assert result.verification.known_counts.satisfied == 1


def test_M07_structural_na_is_not_artificial_completion():
    result = aggregate_delivery_metrics(assessment(['not_applicable'], implementation=['not_applicable']))
    for metric in (result.implementation, result.verification):
        assert metric.status == 'not_applicable' and metric.percent is None
        assert metric.applicable == 0 and metric.not_applicable == 1


def test_phase_unknown_does_not_erase_known_implementation():
    result = aggregate_delivery_metrics(assessment(['unknown']))
    assert result.implementation.percent == 100
    assert result.verification.status == 'unknown' and result.verification.percent is None
    assert result.verification.total == result.verification.unknown == 1


def test_M23_duplicate_identical_units_do_not_inflate_score():
    value = assessment(['satisfied', 'missing'])
    result = aggregate_delivery_metrics(replace(value, obligations=value.obligations + (value.obligations[0],)))
    assert result == aggregate_delivery_metrics(value)
    assert result.verification.percent == 50 and result.verification.total == 2


def test_conflicting_duplicate_or_foreign_edition_never_produces_success():
    value = assessment(['satisfied'])
    conflict = replace(value.obligations[0], verification=phase('failed'))
    for rows in ((value.obligations[0], conflict), (replace(value.obligations[0], spec_edition=2),)):
        result = aggregate_delivery_metrics(replace(value, obligations=rows))
        assert result.verification.status == 'unknown' and result.verification.percent is None


def test_D06_conflicting_duplicates_are_deterministic_without_choosing_a_passing_record():
    value = assessment(['satisfied'])
    conflict = replace(value.obligations[0], verification=phase('failed'))
    first = aggregate_delivery_metrics(replace(value, obligations=(value.obligations[0], conflict)))
    second = aggregate_delivery_metrics(replace(value, obligations=(conflict, value.obligations[0])))
    assert first == second
    assert first.verification.known_counts.satisfied == 0
    assert first.verification.known_counts.unknown == 1


def test_M14_rollup_counts_unique_obligations_instead_of_averaging_cards():
    value = assessment(['satisfied', 'missing', 'missing'])
    rows = (replace(value.obligations[0], contribution_card_ids=('a', 'b', 'c', 'd')), *value.obligations[1:])
    result = aggregate_delivery_metrics(replace(value, obligations=rows))
    assert result.verification.total == 3 and result.verification.percent == 33


def test_D06_metric_order_and_pagination_do_not_change_the_population():
    value = assessment(['satisfied', 'failed', 'missing'])
    alternate = replace(value, obligations=tuple(reversed(value.obligations)),
        completeness=replace(value.completeness, page_complete=False))
    assert aggregate_delivery_metrics(value) == aggregate_delivery_metrics(alternate)


def test_M20_test_card_counts_assigned_scenarios_with_structural_na_implementation():
    value = assessment([])
    value = replace(value, card_id='test', card_type=CardType.TEST,
        scenarios=tuple(ScenarioAssessment(f's{i}', 'a' * 64, 1, 'test', phase(state))
            for i, state in enumerate(['satisfied', 'satisfied', 'satisfied', 'failed'])))
    result = aggregate_delivery_metrics(value)
    assert result.implementation.status == 'not_applicable'
    assert result.implementation.percent is None
    assert result.verification.unit == 'scenario'
    assert result.verification.percent == 75 and result.verification.total == 4


def test_empty_unresolved_spec_is_unknown_not_vacuous_na():
    result = aggregate_delivery_metrics(assessment([]))
    assert result.implementation.status == result.verification.status == 'unknown'
    assert result.verification.total is result.verification.percent is None


def test_pure_metric_result_satisfies_closed_transport_equations():
    from dataclasses import asdict
    from okto_pulse.core.models.delivery_readiness import DeliveryMetric
    result = aggregate_delivery_metrics(assessment(['satisfied', 'waived', 'not_applicable', 'missing']))
    for metric in (result.implementation, result.verification):
        parsed = DeliveryMetric.model_validate(asdict(metric))
        assert parsed.percent == metric.percent


def waiver(disposition=None):
    return DeliveryWaiverFact('exception', DeliveryScope('board', 'spec', 1),
        DeliveryBinding('fr:r0', 'a' * 64), DeliveryPhase.TEST,
        'Explicit human decision', 'human', 'authorization', True, disposition)


def apply_waiver(result, value):
    return apply_delivery_exceptions(result, scope=DeliveryScope('board', 'spec', 1),
        binding=DeliveryBinding('fr:r0', 'a' * 64), phase=DeliveryPhase.TEST, waivers=(value,))


def test_M10_historical_waiver_is_risk_not_na_and_is_not_rewritten():
    value = waiver()
    before = repr(value)
    result = apply_waiver(phase('missing'), value)
    assert value.legacy_disposition_unspecified
    assert result.state == 'waived' and result.intrinsic_state == 'missing'
    assert result.proof_satisfied is False
    assert repr(value) == before


def test_M11_sufficient_proof_makes_old_risk_exception_unused():
    result = apply_waiver(phase('satisfied'), waiver())
    assert result.state == 'satisfied' and result.proof_satisfied
    assert result.exception_ids == ()
    assert result.unused_exception_ids == ('exception',)


def test_authorized_na_preserves_intrinsic_failure_for_details():
    result = apply_waiver(phase('failed'), waiver('not_applicable'))
    assert result.state == 'not_applicable' and result.intrinsic_state == 'failed'
    assert not result.proof_satisfied
    assert result.exception_ids == ('exception',)


@pytest.mark.parametrize('change', ['authority', 'edition', 'obligation', 'digest', 'phase', 'invalid_disposition'])
def test_inapplicable_or_unauthorized_exception_does_not_remove_work(change):
    value = waiver('not_applicable')
    if change == 'authority':
        value = replace(value, current_authorized=False)
    elif change == 'edition':
        value = replace(value, scope=replace(value.scope, edition=2))
    elif change == 'obligation':
        value = replace(value, binding=replace(value.binding, obligation_ref='fr:other'))
    elif change == 'digest':
        value = replace(value, binding=replace(value.binding, semantic_sha256='b' * 64))
    elif change == 'phase':
        value = replace(value, phase=DeliveryPhase.IMPLEMENTATION)
    else:
        value = replace(value, disposition='looks_inapplicable')
    original = phase('missing')
    assert apply_waiver(original, value) == original
