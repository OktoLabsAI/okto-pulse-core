"""P05: one authorized proof universe, independent local projections."""
from dataclasses import replace

import pytest

from okto_pulse.core.domain import effective_delivery_coverage as coverage
from okto_pulse.core.domain.delivery_assessment import aggregate_delivery_metrics, ReadinessIntegrityError
from okto_pulse.core.domain.delivery_evidence import DeliveryPhase, DeliveryWaiverFact
from okto_pulse.core.ports.delivery_readiness import ReadinessAccessDenied, ReadinessReadError
from okto_pulse.core.services.delivery_readiness import EvaluatedDeliveryReadiness
from test_delivery_obligation_assessment import snapshot, with_facts
from test_delivery_readiness_access import batch
from test_delivery_scenario_assessment import outcome


def prepared(value):
    result = batch(value)
    return replace(result, cards=tuple(replace(c, scenario_ids=('scenario-ui', 'scenario-authorization'))
        if c.card_type == 'test' else c for c in result.cards))


def changed(kind):
    value = snapshot()
    context = value.effective_context
    if kind == 'missing_contribution':
        return with_facts(value, implementations=context.implementations[:1])
    if kind == 'missing_association':
        return with_facts(value, tests=context.tests[:1])
    if kind == 'failed':
        return outcome(value, 'scenario-authorization', 'failed')
    if kind == 'open':
        return with_facts(value,
            implementations=tuple(replace(i, fact=replace(i.fact, card_status='in_progress')) for i in context.implementations),
            tests=tuple(replace(t, fact=replace(t.fact, card_status='in_progress')) for t in context.tests))
    if kind == 'alias':
        original = context.implementations[0]
        return with_facts(value, implementations=(*context.implementations,
            replace(original, fact=replace(original.fact, id='same-proof'))))
    if kind == 'waived':
        value = with_facts(value, tests=())
        return replace(value, waivers=(DeliveryWaiverFact('waiver', value.scope, value.obligations[0].binding,
            DeliveryPhase.TEST, 'Human risk decision', 'human', 'authority', True, 'risk_accepted'),))
    if kind == 'incomplete':
        return replace(value, facts_complete=False)
    if kind == 'missing_topology':
        return replace(value, effective_context=replace(context, required_scenarios=None))
    if kind == 'partial':
        return with_facts(value, implementations=tuple(replace(i, fact=replace(i.fact,
            contributions=tuple(replace(c, contribution='partial') for c in i.fact.contributions)))
            for i in context.implementations))
    if kind == 'stale':
        return with_facts(value, implementations=tuple(replace(i, scopes=tuple(replace(s, scope_sha256='d' * 64)
            for s in i.scopes)) for i in context.implementations))
    return value


@pytest.mark.parametrize('kind', ['current', 'missing_contribution', 'missing_association', 'failed', 'open',
    'alias', 'waived', 'incomplete', 'missing_topology', 'partial', 'stale'])
def test_shared_projections_preserve_native_results_without_new_evaluations(monkeypatch, kind):
    value = changed(kind)
    expected = {None: coverage.assess_effective_obligations(value)}
    expected.update({identity: coverage.assess_effective_obligations(value, card_id=identity, card_type='normal')
        for identity in ('ui', 'authorization')})
    expected['test-card-1'] = coverage.assess_effective_scenarios(value, card_id='test-card-1',
        scenario_ids=('scenario-ui', 'scenario-authorization'))
    calls = []
    original = coverage.evaluate_effective_facts
    def counted(*args, **kwargs):
        calls.append(kwargs['require_lifecycle'])
        return original(*args, **kwargs)
    monkeypatch.setattr(coverage, 'evaluate_effective_facts', counted)
    reader = EvaluatedDeliveryReadiness(prepared(value), actor_id='reader')
    assert calls == [False, True]
    for identity, result in expected.items():
        assert reader.assessment(identity) == result
        assert reader.assessment(identity) is reader.assessment(identity)
        diagnostic = reader.diagnostics(identity)
        assert diagnostic is reader.diagnostics(identity)
        assert diagnostic.scope == value.scope and diagnostic.card_id == identity
    assert calls == [False, True]


def test_global_gap_preserves_local_credit_and_cross_card_verification():
    value = changed('missing_contribution')
    reader = EvaluatedDeliveryReadiness(prepared(value), actor_id='reader')
    assert aggregate_delivery_metrics(reader.assessment()).implementation.percent == 0
    own = aggregate_delivery_metrics(reader.assessment('ui'))
    assert own.implementation.percent == own.verification.percent == 100
    other = aggregate_delivery_metrics(reader.assessment('authorization'))
    assert other.implementation.percent == other.verification.percent == 0
    tests = aggregate_delivery_metrics(reader.assessment('test-card-1'))
    assert tests.implementation.status == 'not_applicable' and tests.verification.percent == 50
    assert all(t.card_id != 'ui' for t in value.tests)


def test_lifecycle_and_aliases_are_preserved_without_forged_completion():
    reader = EvaluatedDeliveryReadiness(prepared(changed('open')), actor_id='reader')
    own = reader.assessment('ui').obligations[0]
    assert own.implementation.proof_satisfied and own.verification.proof_satisfied
    assert not own.implementation.lifecycle_satisfied and not own.verification.lifecycle_satisfied
    alias = EvaluatedDeliveryReadiness(prepared(changed('alias')), actor_id='reader').assessment('ui')
    assert 'same-proof' in alias.obligations[0].implementation.evidence_ids
    assert aggregate_delivery_metrics(alias).verification.percent == 100


@pytest.mark.parametrize('denial', ['absent', 'wrong_actor', 'wrong_board', 'evidence', 'spec'])
def test_access_is_checked_before_evaluating_any_proof(monkeypatch, denial):
    value = prepared(snapshot())
    if denial == 'absent':
        value = replace(value, access=None)
    elif denial.startswith('wrong_'):
        field = 'actor_id' if denial == 'wrong_actor' else 'board_id'
        value = replace(value, access=replace(value.access, **{field: 'foreign'}))
    else:
        permission = 'spec.entity.read' if denial == 'spec' else 'code_traceability.evidence.read'
        value = replace(value, access=replace(value.access, allowed_operations=value.access.allowed_operations - {permission}))
    monkeypatch.setattr(coverage, 'evaluate_effective_facts', lambda *a, **kw: pytest.fail('unauthorized proof read'))
    with pytest.raises((ReadinessAccessDenied, ReadinessReadError)):
        EvaluatedDeliveryReadiness(value, actor_id='reader')


def test_card_permission_scope_and_new_snapshot_are_not_cached_across_reads():
    value = prepared(snapshot())
    original = EvaluatedDeliveryReadiness(value, actor_id='reader')
    with pytest.raises(ReadinessReadError, match='readiness_scope_mismatch'):
        original.assessment('foreign')
    restricted = replace(value, access=replace(value.access,
        allowed_operations=value.access.allowed_operations - {'card.entity.read'}))
    new = EvaluatedDeliveryReadiness(restricted, actor_id='reader')
    assert new.assessment() == original.assessment()
    with pytest.raises(ReadinessAccessDenied):
        new.assessment('ui')
    altered = EvaluatedDeliveryReadiness(prepared(changed('missing_contribution')), actor_id='reader')
    assert aggregate_delivery_metrics(altered.assessment()).implementation.percent == 0
    assert aggregate_delivery_metrics(original.assessment()).implementation.percent == 100


def test_evaluated_results_cannot_be_reused_for_a_different_input():
    value = snapshot()
    shared = coverage.EvaluatedDeliverySnapshot(value)
    with pytest.raises(ReadinessIntegrityError, match='readiness_assessment_snapshot_mismatch'):
        coverage.assess_effective_obligations(replace(value), _evaluation=shared)
    with pytest.raises(ReadinessIntegrityError, match='readiness_assessment_snapshot_mismatch'):
        coverage.assess_effective_scenarios(replace(value), card_id='test-card-1',
            scenario_ids=('scenario-ui',), _evaluation=shared)


def test_near_native_card_limit_evaluates_universe_only_twice(monkeypatch):
    value = snapshot()
    template = value.effective_context.inventory.rows[0]
    rows = tuple(replace(template, binding=replace(template.binding, obligation_ref=f'card:card-{i}'),
        contributions=(replace(template.contributions[0], card_id=f'card-{i}'),),
        declared_card_ids=(f'card-{i}',)) for i in range(4500))
    value = with_facts(value, implementations=(), tests=())
    value = replace(value, obligations=tuple(replace(value.obligations[0], binding=row.binding) for row in rows),
        effective_context=replace(value.effective_context,
            inventory=replace(value.effective_context.inventory, rows=rows)))
    request = prepared(value)
    request = replace(request, cards=tuple(replace(request.cards[0], id=f'card-{i}') for i in range(4500)))
    calls = []
    original = coverage.evaluate_effective_facts
    def counted(*args, **kwargs):
        calls.append(kwargs['require_lifecycle'])
        return original(*args, **kwargs)
    monkeypatch.setattr(coverage, 'evaluate_effective_facts', counted)
    reader = EvaluatedDeliveryReadiness(request, actor_id='reader')
    assert len(reader.assessment().obligations) == 4500
    for card in request.cards:
        assessment = reader.assessment(card.id)
        assert len(assessment.obligations) == 1
        assert aggregate_delivery_metrics(assessment).implementation.percent == 0
    assert calls == [False, True]


def test_capacity_has_no_truncated_score_or_proof_evaluation(monkeypatch):
    request = replace(prepared(snapshot()), unavailable_reason_codes=('readiness_limit_exceeded', 'readiness_population_limit'))
    monkeypatch.setattr(coverage, 'evaluate_effective_facts', lambda *a, **kw: pytest.fail('partial universe evaluated'))
    reader = EvaluatedDeliveryReadiness(request, actor_id='reader')
    for identity in (None, 'ui', 'test-card-1'):
        assessment = reader.assessment(identity)
        assert not assessment.completeness.population_known and not assessment.completeness.evaluation_complete
        metrics = aggregate_delivery_metrics(assessment)
        assert metrics.verification.status == 'unknown' and metrics.verification.percent is None
        assert metrics.implementation.percent is None
        assert metrics.implementation.status == ('not_applicable' if identity == 'test-card-1' else 'unknown')
        diagnostic = reader.diagnostics(identity)
        assert len(diagnostic.graph.causes) == 1
        assert diagnostic.graph.causes[0].native_reason_codes == request.unavailable_reason_codes
