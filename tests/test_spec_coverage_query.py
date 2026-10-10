"""KG §6.4: reuse both authorities without promoting links into proof."""
from dataclasses import replace
from datetime import datetime, timezone, timedelta
from types import SimpleNamespace

import pytest

from okto_pulse.core.domain.delivery_evidence import DeliveryBinding, DeliveryObligation, DeliveryPhase, DeliveryScope, DeliveryWaiverFact
from okto_pulse.core.domain.effective_delivery_coverage import EffectiveDeliveryContext
from okto_pulse.core.domain.enums import CardStatus, TestScenarioStatus as ScenarioStatus
from okto_pulse.core.ports.spec_coverage_query import SpecCoverageQuery, SpecCoverageSnapshot
from okto_pulse.core.services.analytics_service import spec_coverage_summary
from okto_pulse.core.services.spec_coverage_query import project_spec_coverage
from test_delivery_evidence_domain import SNAPSHOT as FACT_SNAPSHOT, SCOPE, BINDING, IMPLEMENTATION, TEST as VERIFIED_TEST
from okto_pulse.core.domain.effective_delivery_coverage import DeliveryScopeAttestation, ScopedImplementationFact, ScopedTestFact
from okto_pulse.core.domain.effective_delivery_inventory import EffectiveDeliveryInventory, EffectiveDeliveryObligation
from okto_pulse.core.domain.implementation_responsibility import ImplementationResponsibilityPlan, RequirementContribution
from test_effective_delivery_coverage import case as adopted_case

def native_delivery_snapshot(**changes):
    """Declare the native inventory and its exact admitted fact population."""
    snapshot = replace(FACT_SNAPSHOT, **changes)
    if "effective_context" in changes:
        return snapshot
    scope_hash = "b" * 64
    contribution = RequirementContribution(
        "task-1", "direct", "selected_criteria", ("ac",), None, (), scope_hash,
    )
    inventory = EffectiveDeliveryInventory(
        tuple(EffectiveDeliveryObligation(row.binding, "fr", (contribution,), ())
              for row in snapshot.obligations),
        ImplementationResponsibilityPlan((), True, ()), True, (),
    )
    context = EffectiveDeliveryContext(
        inventory,
        tuple(ScopedImplementationFact(
            fact, tuple(DeliveryScopeAttestation(binding, scope_hash)
                        for binding in fact.bindings),
        ) for fact in snapshot.implementations),
        tuple(ScopedTestFact(fact, ("ac",), "automated_test") for fact in snapshot.tests),
        frozenset({"automated_test"}),
    )
    return replace(snapshot, effective_context=context)


SNAPSHOT = native_delivery_snapshot()


NOW = datetime(2026, 10, 1, tzinfo=timezone.utc)
QUERY = SpecCoverageQuery('board', 'spec', 'actor:one')


def source(delivery=SNAPSHOT, **changes):
    spec = SimpleNamespace(id='spec', board_id='board', edition=2, version=3, title='Source Spec',
        acceptance_criteria=[{'id': 'ac', 'text': 'Condition'}],
        functional_requirements=[{'id': 'fr', 'text': 'Requirement'}],
        test_scenarios=[{'id': 'scenario', 'linked_criteria': ['ac'], 'linked_task_ids': ['test-card']}],
        business_rules=[], api_contracts=[], technical_requirements=[], decisions=[],
        integration_requirements=[], observability_requirements=[])
    card = SimpleNamespace(id='test-card', spec_id='spec', board_id='board', status='done',
        card_type='test', archived=False, policy_version=1)
    return replace(SpecCoverageSnapshot(SCOPE, 'actor:one', 'source-revision', NOW,
        spec, (card,), True, delivery), **changes)


def test_kg58_linked_done_test_card_is_structural_coverage_without_delivery_proof():
    facts = source(native_delivery_snapshot(implementations=(), tests=()))
    result = project_spec_coverage(QUERY, facts)
    assert result['structure']['summary'] == spec_coverage_summary(facts.spec, cards=list(facts.cards))
    assert result['structure']['summary']['scenario_task_linkage_pct'] == 100
    assert result['structure']['summary']['ac_coverage_pct'] == 100
    assert result['structure']['interpretation'] == 'planning_links_not_delivery_proof'
    assert result['items'][0]['implementation'] == result['items'][0]['verification'] == 'missing'
    assert result['delivery']['counts']['verification_proven'] == 0
    assert result['authority'] == 'informational'
    assert result['projection_freshness']['state'] == 'unknown'


def test_current_admitted_implementation_and_test_remain_distinct():
    result = project_spec_coverage(QUERY, source())
    row = result['items'][0]
    assert row['implementation'] == row['verification'] == 'proven'
    assert row['implementation_record_refs'] == ['impl']
    assert row['verification_record_refs'] == ['test']
    assert result['delivery']['counts'] == {'obligations': 1, 'implementation_proven': 1,
        'verification_proven': 1, 'observed_obligations': 1, 'decisions': 0, 'decisions_verified': 0}
    assert 'approved' not in result and 'allowed' not in result


@pytest.mark.parametrize('changes', [dict(current_verified_run=False), dict(result=ScenarioStatus.FAILED),
    dict(card_status=CardStatus.IN_PROGRESS), dict(verified_implementation_ids=()),
    dict(bindings=(DeliveryBinding(BINDING.obligation_ref, 'f' * 64),))])
def test_wrong_revision_reopened_failed_or_unauthenticated_tests_do_not_gain_credit(changes):
    result = project_spec_coverage(QUERY, source(native_delivery_snapshot(tests=(replace(VERIFIED_TEST, **changes),))))
    assert result['items'][0]['verification'] == 'missing'
    assert result['items'][0]['verification_record_refs'] == []
    assert result['delivery']['rejected_record_refs'] == ['test']


def test_waiver_satisfies_an_obligation_but_is_never_presented_as_proof():
    waiver = DeliveryWaiverFact('waiver', SCOPE, BINDING, DeliveryPhase.TEST,
        'Authorized exception', 'human', 'authorization-receipt', True)
    result = project_spec_coverage(QUERY, source(native_delivery_snapshot(tests=(), waivers=(waiver,))))
    assert result['items'][0]['verification'] == 'satisfied_with_waiver'
    assert result['items'][0]['verification_waiver_refs'] == ['waiver']
    assert result['delivery']['counts']['verification_proven'] == 0


def test_partial_contribution_is_not_whole_requirement_credit():
    inventory, implementations, tests = adopted_case()
    snapshot = native_delivery_snapshot(implementations=(implementations[0].fact,), tests=(tests[0].fact,),
        effective_context=EffectiveDeliveryContext(inventory, implementations[:1], tests[:1], frozenset({'automated_test'})))
    result = project_spec_coverage(QUERY, source(snapshot))
    row = result['items'][0]
    assert row['implementation'] == row['verification'] == 'partial'
    assert row['required_card_refs'] == ['card:authorization', 'card:ui']
    assert row['missing_card_refs'] == ['card:authorization']
    assert result['delivery']['counts']['implementation_proven'] == 0


@pytest.mark.parametrize('damage', ['context', 'inventory', 'methods', 'population'])
def test_adopted_unavailable_authority_cannot_appear_as_complete_zero(damage):
    inventory, implementations, tests = adopted_case()
    context = EffectiveDeliveryContext(inventory, implementations, tests, frozenset({'automated_test'}))
    if damage == 'context':
        context = object()
    elif damage == 'inventory':
        context = replace(context, inventory=replace(inventory, population_complete=False))
    elif damage == 'methods':
        context = replace(context, admitted_methods=None)
    else:
        context = replace(context, implementations=())
    snapshot = native_delivery_snapshot(implementations=tuple(row.fact for row in implementations),
        tests=tuple(row.fact for row in tests), effective_context=context)
    result = project_spec_coverage(QUERY, source(snapshot))
    assert result['delivery']['complete_for_scope'] is False
    assert result['delivery']['counts']['verification_proven'] is None
    assert result['delivery']['counts']['obligations'] is None
    assert all(row['verification'] == 'unknown' for row in result['items'])
    assert result['delivery']['blockers']


@pytest.mark.parametrize('state', ['restricted', 'unavailable'])
def test_unreadable_proof_exposes_no_records_or_counts(state):
    result = project_spec_coverage(QUERY, source(None, delivery_state=state))
    assert result['items'] == []
    assert all(value is None for value in result['delivery']['counts'].values())
    assert result['delivery']['state'] == state
    assert result['completeness']['complete_for_scope'] is False


def test_incomplete_source_does_not_use_zero_denominator_as_complete_coverage():
    result = project_spec_coverage(QUERY, source(source_complete=False))
    assert result['structure']['summary'] is None
    assert result['items'][0]['implementation'] == result['items'][0]['verification'] == 'unknown'
    assert result['delivery']['counts']['implementation_proven'] is None


@pytest.mark.parametrize('delivery', [native_delivery_snapshot(complete=False),
    native_delivery_snapshot(implementations=(IMPLEMENTATION, IMPLEMENTATION))])
def test_incomplete_or_ambiguous_proof_never_becomes_green(delivery):
    result = project_spec_coverage(QUERY, source(delivery))
    if delivery.complete:
        assert result['items'] == []
        assert 'delivery_scoped_population_mismatch' in result['delivery']['blockers']
    else:
        assert result['items'][0]['verification'] == 'unknown'
    assert result['delivery']['counts']['verification_proven'] is None
    assert result['delivery']['complete_for_scope'] is False


@pytest.mark.parametrize('field,value', [('board_id', 'foreign'), ('spec_id', 'foreign'), ('actor_scope_ref', 'other')])
def test_scope_mismatch_is_refused(field, value):
    with pytest.raises(ValueError, match='scope_mismatch'):
        project_spec_coverage(replace(QUERY, **{field: value}), source())


def test_foreign_delivery_scope_and_foreign_card_are_refused():
    with pytest.raises(ValueError, match='delivery_scope_mismatch'):
        project_spec_coverage(QUERY, source(native_delivery_snapshot(scope=DeliveryScope('other', 'spec', 2))))
    facts = source()
    facts.cards[0].board_id = 'other'
    with pytest.raises(ValueError, match='card_outside_scope'):
        project_spec_coverage(QUERY, facts)


def paged():
    obligations = tuple(DeliveryObligation(DeliveryBinding(f'fr:{i}', 'a' * 64), f'Requirement {i}') for i in range(3))
    return source(native_delivery_snapshot(obligations=obligations, implementations=(), tests=()))


def test_pagination_uses_whole_scope_counts_and_stable_observation_clock():
    facts = paged()
    request = replace(QUERY, limit=1)
    first = project_spec_coverage(request, facts)
    second = project_spec_coverage(replace(request, cursor=first['next_cursor']), replace(facts, checked_at=NOW + timedelta(seconds=5)))
    assert first['delivery']['counts']['obligations'] == second['delivery']['counts']['obligations'] == 3
    assert first['items'][0]['obligation_ref'] != second['items'][0]['obligation_ref']


def test_mutation_without_spec_version_change_invalidates_cursor():
    facts = paged()
    request = replace(QUERY, limit=1)
    cursor = project_spec_coverage(request, facts)['next_cursor']
    with pytest.raises(ValueError, match='cursor_stale'):
        project_spec_coverage(replace(request, cursor=cursor), replace(facts, source_revision='source-link-change'))
    facts.spec.test_scenarios[0]['linked_task_ids'] = []
    with pytest.raises(ValueError, match='cursor_stale'):
        project_spec_coverage(replace(request, cursor=cursor), facts)


@pytest.mark.parametrize('cursor', ['bad', 'spec-coverage-v1:' + 'a' * 64 + ':0', 'x' * 257])
def test_malformed_cursor_is_rejected(cursor):
    with pytest.raises(ValueError, match='cursor_invalid'):
        project_spec_coverage(replace(QUERY, cursor=cursor), source())


def test_source_count_and_serialized_nested_payload_have_independent_bounds(monkeypatch):
    import okto_pulse.core.services.spec_coverage_query as module
    facts = source()
    with pytest.raises(ValueError, match='source_bound'):
        project_spec_coverage(QUERY, replace(facts, cards=facts.cards * 1001))
    monkeypatch.setattr(module, 'MAX_SPEC_COVERAGE_BYTES', 100)
    with pytest.raises(ValueError, match='summary_payload_bound'):
        project_spec_coverage(QUERY, facts)
