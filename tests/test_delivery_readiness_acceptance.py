"""P11 explicit plan cases over the shared factual assessment and diagnosis."""
from okto_pulse.core.domain.delivery_assessment import (
    aggregate_delivery_metrics, assess_obligation_delivery,
)
from okto_pulse.core.services.delivery_readiness_diagnostics import compose_delivery_diagnostics
from test_delivery_obligation_assessment import snapshot, with_facts


def test_M16_planned_contributions_without_proof_never_receive_delivery_credit():
    value = with_facts(snapshot(), implementations=(), tests=())
    inventory = value.effective_context.inventory
    assert inventory.population_complete
    assert inventory.rows[0].contributions
    assert all(contribution.card_id for contribution in inventory.rows[0].contributions)

    assessment = assess_obligation_delivery(value)
    metrics = aggregate_delivery_metrics(assessment)
    assert metrics.implementation.percent == metrics.verification.percent == 0
    assert metrics.implementation.total == metrics.verification.total == 1
    assert not assessment.obligations[0].implementation.proof_satisfied
    assert not assessment.obligations[0].verification.proof_satisfied
    assert value.effective_context.inventory is inventory


def test_D03_two_scenario_causes_count_the_shared_obligation_only_once():
    # Both authenticated native runs exist; each still needs its own association.
    value = with_facts(snapshot(), tests=())
    assessment = assess_obligation_delivery(value)
    metrics = aggregate_delivery_metrics(assessment)
    diagnosis = compose_delivery_diagnostics(assessment, value)
    causes = diagnosis.graph.causes

    assert len(causes) == 2
    assert {cause.code for cause in causes} == {'verification_association_missing'}
    assert len({cause.id for cause in causes}) == 2
    assert {unit.ref for cause in causes for unit in cause.units} == {
        assessment.obligations[0].binding.obligation_ref,
    }
    assert metrics.implementation.percent == 100
    assert metrics.verification.percent == 0
    assert metrics.verification.total == metrics.verification.unresolved == 1
