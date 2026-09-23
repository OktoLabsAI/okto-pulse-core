from dataclasses import replace

import pytest

from okto_pulse.core.domain.delivery_evidence import (
    DeliveryContribution,
    DeliveryPhase,
    DeliveryWaiverFact,
    evaluate_delivery_coverage,
)
from okto_pulse.core.domain.effective_delivery_coverage import (
    DeliveryScopeAttestation,
    ScopedImplementationFact,
    ScopedTestFact,
    read_scoped_implementation,
)
from okto_pulse.core.domain.effective_delivery_inventory import (
    EffectiveDeliveryInventory,
    EffectiveDeliveryObligation,
)
from okto_pulse.core.domain.implementation_responsibility import (
    ImplementationResponsibilityPlan,
    RequirementContribution,
)
from okto_pulse.core.ports.delivery_inventory import default_delivery_inventory_policy
from test_delivery_evidence_domain import BINDING, IMPLEMENTATION, SNAPSHOT, TEST


def case():
    planned = tuple(
        RequirementContribution(
            card, "direct", "selected_criteria", (criterion,), None, (), digest * 64
        )
        for card, criterion, digest in [
            ("ui", "functional", "a"),
            ("authorization", "technical", "b"),
        ]
    )
    inventory = EffectiveDeliveryInventory(
        (EffectiveDeliveryObligation(BINDING, "fr", planned, ()),),
        ImplementationResponsibilityPlan((), True, ()),
        True,
        (),
    )
    implementations = tuple(
        ScopedImplementationFact(
            replace(
                IMPLEMENTATION,
                id=p.card_id,
                card_id=p.card_id,
                contributions=(DeliveryContribution(BINDING, "complete"),),
            ),
            (DeliveryScopeAttestation(BINDING, p.scope_sha256),),
        )
        for p in planned
    )
    tests = tuple(
        ScopedTestFact(
            replace(
                TEST, id="test-" + p.card_id, verified_implementation_ids=(p.card_id,)
            ),
            p.criterion_ids,
            "automated_test",
        )
        for p in planned
    )
    return inventory, implementations, tests


def evaluate(
    inventory,
    implementations,
    tests,
    *,
    snapshot=None,
    methods=frozenset({"automated_test"}),
):
    snapshot = snapshot or replace(
        SNAPSHOT,
        implementations=tuple(row.fact for row in implementations),
        tests=tuple(row.fact for row in tests),
    )
    return default_delivery_inventory_policy().effective_coverage(
        inventory=inventory,
        snapshot=snapshot,
        implementations=implementations,
        tests=tests,
        admitted_methods=methods,
    )


def test_reproduction_one_card_credits_legacy_but_not_adopted_multi_card_requirement():
    inventory, implementations, tests = case()
    snapshot = replace(
        SNAPSHOT, implementations=(implementations[0].fact,), tests=(tests[0].fact,)
    )
    assert evaluate_delivery_coverage(snapshot).allowed
    result = evaluate(inventory, implementations[:1], tests[:1], snapshot=snapshot)
    assert not result.allowed
    assert result.rows[0].missing_card_ids == ("authorization",)
    assert result.rows[0].implementation_ids == (
        "ui",
    )  # factual partial work remains visible
    assert not result.rows[0].implementation_satisfied


def test_all_contributions_and_exact_criteria_complete_without_combining_authorities():
    result = evaluate(*case())
    assert result.allowed
    assert result.rows[0].required_card_ids == ("authorization", "ui")
    assert result.rows[0].implementation_ids == ("authorization", "ui")


def test_functional_passing_does_not_conceal_missing_technical_condition():
    inventory, implementations, tests = case()
    # One authenticated run names both implementations but observes functional only.
    run = replace(
        tests[0],
        fact=replace(
            tests[0].fact, verified_implementation_ids=("ui", "authorization")
        ),
    )
    result = evaluate(inventory, implementations, (run,))
    assert result.rows[0].implementation_satisfied and not result.rows[0].test_satisfied
    assert result.rows[0].missing_criteria == (("authorization", "technical"),)


def test_multiple_runs_can_cover_all_selected_conditions_without_repeating_proof():
    inventory, implementations, tests = case()
    both = replace(
        tests[0],
        fact=replace(
            tests[0].fact, verified_implementation_ids=("ui", "authorization")
        ),
        criterion_ids=("functional", "technical"),
    )
    assert evaluate(inventory, implementations, (both,)).allowed


def mixed_report_case():
    inventory, implementations, tests = case()
    mixed = replace(tests[0], verification_method='inspection', criterion_ids=('functional', 'technical'),
        passing_criterion_ids=('functional',), fact=replace(tests[0].fact, result='failed',
            verified_implementation_ids=('ui', 'authorization')))
    return inventory, implementations, mixed


def test_one_report_credits_only_observed_passing_criteria_and_keeps_factual_failure():
    inventory, implementations, mixed = mixed_report_case()
    result = evaluate(inventory, implementations, (mixed,), methods=frozenset({'inspection'}))
    assert result.rows[0].missing_criteria == (('authorization', 'technical'),)
    assert result.rows[0].test_ids == (mixed.fact.id,)
    assert mixed.fact.id not in result.rejected_record_ids
    assert not result.allowed and mixed.fact.result == 'failed'
    legacy = replace(SNAPSHOT, implementations=tuple(i.fact for i in implementations), tests=(mixed.fact,))
    assert not evaluate_delivery_coverage(legacy).allowed


def test_whole_scope_criterion_allocation_still_requires_its_own_observation():
    inventory, implementations, tests = case()
    ac_binding = replace(BINDING, obligation_ref='ac:technical')
    contribution = replace(inventory.rows[0].contributions[0], criterion_ids=(), scope='whole_requirement')
    inventory = replace(inventory, rows=(replace(inventory.rows[0], binding=ac_binding, family='ac', contributions=(contribution,)),))
    implementation = replace(implementations[0], fact=replace(implementations[0].fact, bindings=(ac_binding,),
        contributions=(DeliveryContribution(ac_binding, 'complete'),)),
        scopes=(DeliveryScopeAttestation(ac_binding, contribution.scope_sha256),))
    run = replace(tests[0], fact=replace(tests[0].fact, bindings=(ac_binding,)))
    snapshot = replace(SNAPSHOT, obligations=(replace(SNAPSHOT.obligations[0], binding=ac_binding),),
        implementations=(implementation.fact,), tests=(run.fact,))
    result = evaluate(inventory, (implementation,), (run,), snapshot=snapshot)
    assert not result.allowed and result.rows[0].missing_criteria == (('ui', 'technical'),)
    run = replace(run, criterion_ids=('technical',))
    assert evaluate(inventory, (implementation,), (run,), snapshot=snapshot).allowed


@pytest.mark.parametrize('damage', ['unsigned', 'not_done', 'stale_implementation', 'foreign_scope',
    'unsupported_method', 'automated_method', 'incomplete_run', 'empty', 'foreign_criterion', 'duplicate', 'invalid'])
def test_criterion_projection_cannot_bypass_existing_proof_checks(damage):
    inventory, implementations, mixed = mixed_report_case()
    if damage == 'unsigned':
        mixed = replace(mixed, fact=replace(mixed.fact, current_verified_run=False))
    elif damage == 'not_done':
        mixed = replace(mixed, fact=replace(mixed.fact, card_status='in_progress'))
    elif damage == 'stale_implementation':
        mixed = replace(mixed, fact=replace(mixed.fact, verified_implementation_ids=('old',)))
    elif damage == 'foreign_scope':
        mixed = replace(mixed, fact=replace(mixed.fact, scope=replace(mixed.fact.scope, board_id='foreign')))
    elif damage == 'unsupported_method':
        mixed = replace(mixed, verification_method='unknown')
    elif damage == 'automated_method':
        mixed = replace(mixed, verification_method='automated_test')
    elif damage == 'incomplete_run':
        mixed = replace(mixed, fact=replace(mixed.fact, result='ready'))
    else:
        mixed = replace(mixed, passing_criterion_ids={
            'empty': (), 'foreign_criterion': ('foreign',), 'duplicate': ('functional', 'functional'), 'invalid': True,
        }[damage])
    result = evaluate(inventory, implementations, (mixed,), methods=frozenset({'inspection', 'automated_test'}))
    assert not result.allowed
    assert all(not row.test_ids for row in result.rows)


def test_partial_declarations_never_add_up_to_the_approved_card_scope():
    inventory, implementations, tests = case()
    partial = replace(
        implementations[1],
        fact=replace(
            implementations[1].fact,
            contributions=(DeliveryContribution(BINDING, "partial"),),
        ),
    )
    other = replace(partial, fact=replace(partial.fact, id="second-partial"))
    result = evaluate(inventory, (implementations[0], partial, other), tests)
    assert not result.allowed and result.rows[0].missing_card_ids == ("authorization",)


def test_reallocation_invalidates_only_the_affected_contribution_scope():
    inventory, implementations, tests = case()
    obligation = inventory.rows[0]
    changed = replace(obligation.contributions[0], scope_sha256="c" * 64)
    result = evaluate(
        replace(
            inventory,
            rows=(
                replace(
                    obligation, contributions=(changed, obligation.contributions[1])
                ),
            ),
        ),
        implementations,
        tests,
    )
    assert not result.allowed
    assert result.rows[0].missing_card_ids == ("ui",)
    assert result.rows[0].implementation_ids == ("authorization",)


@pytest.mark.parametrize(
    "mutation",
    [
        "legacy",
        "scope_absent",
        "scope_wrong",
        "foreign_card",
        "not_done",
        "proof_stale",
    ],
)
def test_no_implicit_upgrade_or_borrowed_card_credit(mutation):
    inventory, implementations, tests = case()
    first = implementations[0]
    if mutation == "legacy":
        first = replace(first, fact=replace(first.fact, contributions=None))
    if mutation == "scope_absent":
        first = replace(first, scopes=())
    if mutation == "scope_wrong":
        first = replace(first, scopes=(DeliveryScopeAttestation(BINDING, "e" * 64),))
    if mutation == "foreign_card":
        first = replace(first, fact=replace(first.fact, card_id="unallocated"))
    if mutation == "not_done":
        first = replace(first, fact=replace(first.fact, card_status="in_progress"))
    if mutation == "proof_stale":
        first = replace(
            first, fact=replace(first.fact, current_accepted_execution=False)
        )
    result = evaluate(inventory, (first, implementations[1]), tests)
    assert not result.allowed and result.rows[0].missing_card_ids == ("ui",)


@pytest.mark.parametrize(
    "mutation",
    ["unsupported_method", "failed", "unsigned", "stale_ids", "other_criterion"],
)
def test_selected_method_receipt_and_implementation_join_remain_required(mutation):
    inventory, implementations, tests = case()
    run = tests[1]
    if mutation == "unsupported_method":
        run = replace(run, verification_method="inspection")
    if mutation == "failed":
        run = replace(run, fact=replace(run.fact, result="failed"))
    if mutation == "unsigned":
        run = replace(run, fact=replace(run.fact, current_verified_run=False))
    if mutation == "stale_ids":
        run = replace(
            run,
            fact=replace(run.fact, verified_implementation_ids=("old-authorization",)),
        )
    if mutation == "other_criterion":
        run = replace(run, criterion_ids=("functional",))
    result = evaluate(inventory, implementations, (tests[0], run))
    assert not result.allowed and result.rows[0].implementation_satisfied
    assert "test-authorization" in result.rejected_record_ids


def test_missing_method_registry_is_unknown_not_an_empty_requirement():
    result = evaluate(*case(), methods=None)
    assert (
        not result.allowed
        and "delivery_verification_methods_unavailable" in result.blockers
    )


def test_waivers_keep_authorized_phase_specific_meaning_and_missing_work_visible():
    inventory, _, _ = case()
    waiver = DeliveryWaiverFact(
        "waiver",
        SNAPSHOT.scope,
        BINDING,
        DeliveryPhase.IMPLEMENTATION,
        "Approved exemption",
        "human",
        "permission-receipt",
        True,
    )
    snapshot = replace(SNAPSHOT, implementations=(), tests=(), waivers=(waiver,))
    result = evaluate(inventory, (), (), snapshot=snapshot)
    assert result.rows[0].implementation_satisfied and not result.rows[0].test_satisfied
    assert result.rows[0].missing_card_ids == ("authorization", "ui")
    assert not result.rows[0].implementation_ids
    both = replace(
        snapshot,
        waivers=(waiver, replace(waiver, id="test-waiver", phase=DeliveryPhase.TEST)),
    )
    assert evaluate(inventory, (), (), snapshot=both).allowed


def test_population_mismatch_or_ambiguous_identity_fails_closed():
    inventory, implementations, tests = case()
    result = evaluate(inventory, implementations, tests, snapshot=SNAPSHOT)
    assert result.blockers == ("delivery_scoped_population_mismatch",)
    duplicated = evaluate(inventory, (*implementations, implementations[0]), tests)
    assert not duplicated.allowed


def test_resolution_budget_never_approves_a_truncated_empty_result():
    inventory, implementations, tests = case()
    result = evaluate(
        replace(inventory, rows=inventory.rows * 5001), implementations, tests
    )
    assert not result.allowed and result.blockers == (
        "delivery_effective_resolution_limit",
    )


def test_several_runs_cover_distinct_criteria_of_one_implementation():
    inventory, implementations, tests = case()
    plan = replace(
        inventory.rows[0].contributions[0], criterion_ids=("functional", "technical")
    )
    inventory = replace(
        inventory, rows=(replace(inventory.rows[0], contributions=(plan,)),)
    )
    assert not evaluate(inventory, implementations[:1], tests[:1]).allowed
    second = replace(
        tests[1], fact=replace(tests[1].fact, verified_implementation_ids=("ui",))
    )
    assert evaluate(inventory, implementations[:1], (tests[0], second)).allowed


def test_persisted_scope_reader_preserves_history_and_exact_binding_identity():
    _, implementations, _ = case()
    fact = implementations[0].fact
    assert read_scoped_implementation(fact, {}).scopes == ()
    payload = {
        "scope_contract_version": "card-contribution-scope/v1",
        "contribution_scopes": [
            {"obligation_ref": BINDING.obligation_ref, "scope_sha256": "a" * 64},
        ],
    }
    assert read_scoped_implementation(fact, payload) == implementations[0]


@pytest.mark.parametrize(
    "payload",
    [
        {"scope_contract_version": "card-contribution-scope/v1"},
        {"contribution_scopes": []},
        {"scope_contract_version": "unknown", "contribution_scopes": []},
        {
            "scope_contract_version": "card-contribution-scope/v1",
            "contribution_scopes": [
                {"obligation_ref": "foreign", "scope_sha256": "a" * 64}
            ],
        },
        {
            "scope_contract_version": "card-contribution-scope/v1",
            "contribution_scopes": [
                {"obligation_ref": BINDING.obligation_ref, "scope_sha256": "wrong"}
            ],
        },
        {
            "scope_contract_version": "card-contribution-scope/v1",
            "contribution_scopes": [
                {
                    "obligation_ref": BINDING.obligation_ref,
                    "scope_sha256": "a" * 64,
                    "approved": True,
                }
            ],
        },
    ],
)
def test_malformed_scope_payload_never_falls_back_to_legacy(payload):
    with pytest.raises(ValueError, match="delivery_contribution_scope"):
        read_scoped_implementation(IMPLEMENTATION, payload)


def test_actual_inherited_rule_is_allocated_only_to_authorization_card():
    from test_effective_delivery_inventory import inventory as resolve_inventory
    from test_implementation_responsibility import split_population, card

    inventory, _ = resolve_inventory(
        split_population(), [card("ui"), card("authorization")]
    )
    rule = next(row for row in inventory.rows if row.family == "br")
    assert [c.card_id for c in rule.contributions] == ["authorization"]
    # Isolate this obligation's delivery verdict; the complete inventory still
    # keeps supplemental planning gaps (covered by the inventory tests).
    inventory = replace(inventory, rows=(rule,))
    planned = rule.contributions[0]
    implementation = ScopedImplementationFact(
        replace(
            IMPLEMENTATION,
            id="br-impl",
            card_id="authorization",
            bindings=(rule.binding,),
            contributions=(DeliveryContribution(rule.binding, "complete"),),
        ),
        (DeliveryScopeAttestation(rule.binding, planned.scope_sha256),),
    )
    test = ScopedTestFact(
        replace(
            TEST, bindings=(rule.binding,), verified_implementation_ids=("br-impl",)
        ),
        planned.criterion_ids,
        "automated_test",
    )
    snapshot = replace(
        SNAPSHOT,
        obligations=(replace(SNAPSHOT.obligations[0], binding=rule.binding),),
        implementations=(implementation.fact,),
        tests=(test.fact,),
    )
    assert evaluate(inventory, (implementation,), (test,), snapshot=snapshot).allowed
    forged = replace(implementation, fact=replace(implementation.fact, card_id="ui"))
    assert not evaluate(
        inventory,
        (forged,),
        (test,),
        snapshot=replace(snapshot, implementations=(forged.fact,)),
    ).allowed
