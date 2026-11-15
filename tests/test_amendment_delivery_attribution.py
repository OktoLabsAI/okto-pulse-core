"""Explicit amendment proof is never a same-scope ledger shortcut."""
from dataclasses import replace
import pytest

from okto_pulse.core.domain.delivery_evidence import (
    AmendmentDeliveryAttribution, DeliveryBinding, DeliveryObligation,
    DeliveryScope, DeliveryEvidenceSnapshot,
)
from okto_pulse.core.domain.effective_delivery_coverage import (
    EffectiveDeliveryContext, amendment_source_proof, evaluate_effective_facts,
    assess_effective_obligations,
)
from okto_pulse.core.domain.effective_delivery_inventory import EffectiveDeliveryInventory, EffectiveDeliveryObligation
from okto_pulse.core.domain.implementation_responsibility import ImplementationResponsibilityPlan, RequirementContribution
from okto_pulse.core.domain.enums import CardStatus, CardType
from okto_pulse.core.domain.delivery_assessment import assess_delivery_gate
from test_effective_delivery_coverage import case, planned_scenarios
from test_delivery_evidence_domain import SNAPSHOT


def attributed():
    inventory, implementations, tests = case()
    source = replace(SNAPSHOT, implementations=tuple(i.fact for i in implementations),
        tests=tuple(t.fact for t in tests), waivers=(), facts_complete=True,
        effective_context=EffectiveDeliveryContext(inventory, implementations, tests,
            frozenset({'automated_test'}), required_scenarios=planned_scenarios(tests)))
    proof = amendment_source_proof(source)
    assert proof and proof[3] and proof[4]
    binding = DeliveryBinding('card:bug', 'e' * 64)
    scope = DeliveryScope(source.scope.board_id, 'original', 1)
    contribution = RequirementContribution('bug', 'card_scope', 'whole_card', (), None, (), 'f' * 64)
    row = EffectiveDeliveryObligation(binding, 'card', (contribution,), ())
    context = EffectiveDeliveryContext(EffectiveDeliveryInventory((row,),
        ImplementationResponsibilityPlan((), True, ()), True, ()), (), (),
        frozenset({'automated_test'}), required_scenarios=())
    attribution = AmendmentDeliveryAttribution('amendment', scope, 'bug', binding,
        CardStatus.DONE, source.scope, proof[0], proof[1], proof[2], source, True, True)
    return DeliveryEvidenceSnapshot(scope, (DeliveryObligation(binding, 'Bug'),),
        complete=True, effective_context=context, facts_complete=True,
        amendment_attributions=(attribution,))


def test_explicit_attribution_matches_gate_card_and_spec_without_scope_rewrite():
    snapshot = attributed()
    original = snapshot.amendment_attributions[0].source_snapshot
    assert all(i.scope == original.scope for i in original.implementations)
    assert snapshot.implementations == snapshot.tests == ()
    result = evaluate_effective_facts(snapshot)
    assert result.allowed and result.rows[0].implementation_satisfied and result.rows[0].test_satisfied
    assert result.contribution_rows[0].row == result.rows[0]
    card = assess_effective_obligations(snapshot, card_id='bug', card_type=CardType.BUG)
    assert assess_delivery_gate(card).allowed
    assert assess_delivery_gate(assess_effective_obligations(snapshot)).allowed


@pytest.mark.parametrize('damage', [
    lambda a: replace(a, origin_current=False),
    lambda a: replace(a, origin_scope=DeliveryScope('foreign', 'original', 1)),
    lambda a: replace(a, revision_scope=DeliveryScope(a.revision_scope.board_id, 'other', 1)),
    lambda a: replace(a, binding=DeliveryBinding('card:other', 'e' * 64)),
    lambda a: replace(a, inventory_sha256='0' * 64),
    lambda a: replace(a, implementation_ids=('unrelated',)),
    lambda a: replace(a, source_snapshot=replace(a.source_snapshot, complete=False)),
])
def test_wrong_or_incomplete_attribution_never_grants_credit(damage):
    snapshot = attributed()
    result = evaluate_effective_facts(replace(snapshot,
        amendment_attributions=(damage(snapshot.amendment_attributions[0]),)))
    assert not result.allowed and not result.rows[0].implementation_satisfied


def test_legacy_and_competing_confirmations_do_not_imply_delivery():
    snapshot = attributed()
    for attrs in ((), snapshot.amendment_attributions * 2):
        assert not evaluate_effective_facts(replace(snapshot, amendment_attributions=attrs)).allowed


def test_reset_coverage_removes_verification_but_preserves_implementation():
    snapshot = attributed()
    a = replace(snapshot.amendment_attributions[0], coverage_current=False)
    row = evaluate_effective_facts(replace(snapshot, amendment_attributions=(a,))).rows[0]
    assert row.implementation_satisfied and not row.test_satisfied


def test_partial_source_declaration_cannot_be_promoted():
    snapshot = attributed()
    a = snapshot.amendment_attributions[0]
    source = a.source_snapshot
    old = source.effective_context.implementations[0]
    partial = replace(old, fact=replace(old.fact,
        contributions=tuple(replace(c, contribution='partial') for c in old.fact.contributions)))
    scoped = (partial, *source.effective_context.implementations[1:])
    source = replace(source, implementations=tuple(i.fact for i in scoped),
        effective_context=replace(source.effective_context, implementations=scoped))
    row = evaluate_effective_facts(replace(snapshot,
        amendment_attributions=(replace(a, source_snapshot=source),))).rows[0]
    assert not row.implementation_satisfied and not row.test_satisfied


def test_bug_lifecycle_is_independent_from_current_delivery_proof():
    snapshot = attributed()
    a = replace(snapshot.amendment_attributions[0], bug_status=CardStatus.IN_PROGRESS)
    snapshot = replace(snapshot, amendment_attributions=(a,))
    assert evaluate_effective_facts(snapshot, require_lifecycle=False).allowed
    assert not evaluate_effective_facts(snapshot, require_lifecycle=True).allowed
