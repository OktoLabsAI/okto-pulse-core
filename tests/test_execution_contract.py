from dataclasses import replace
from types import SimpleNamespace

import pytest

from okto_pulse.core.domain.execution_contract import (
    SpecExecutionContractAdoption,
    adopt_execution_contract,
    execution_contract,
    new_execution_contract,
)
from okto_pulse.core.domain.delivery_evidence import (
    DeliveryContribution,
    evaluate_delivery_coverage,
    require_test_result_admission,
)
from okto_pulse.core.domain.effective_delivery_coverage import EffectiveDeliveryContext
from test_delivery_evidence_domain import BINDING, SNAPSHOT
from test_effective_delivery_coverage import case


def spec(**changes):
    return SimpleNamespace(id="spec", board_id="board", version=4, edition=2, **changes)


def test_explicit_adoption_preserves_identity_and_actor_without_inventing_content():
    legacy = spec(execution_contract=None)
    request = SpecExecutionContractAdoption(
        expected_spec_version=4, expected_spec_edition=2
    )
    result = adopt_execution_contract(legacy, request, actor_id="author")
    assert result == new_execution_contract(
        board_id="board",
        spec_id="spec",
        edition=2,
        actor_id="author",
        origin="explicit_revision",
    )
    assert legacy.execution_contract is None
    legacy.execution_contract = result
    assert (
        adopt_execution_contract(legacy, request, actor_id="different-author") is None
    )


@pytest.mark.parametrize("version,edition", [(3, 2), (4, 1), (5, 2)])
def test_adoption_requires_exact_revision_even_when_already_adopted(version, edition):
    current = spec(
        execution_contract=new_execution_contract(
            board_id="board",
            spec_id="spec",
            edition=2,
            actor_id="author",
            origin="new_spec",
        )
    )
    with pytest.raises(ValueError, match="version_conflict"):
        adopt_execution_contract(
            current,
            SpecExecutionContractAdoption(
                expected_spec_version=version, expected_spec_edition=edition
            ),
            actor_id="author",
        )


@pytest.mark.parametrize(
    "change",
    [
        {"board_id": "foreign"},
        {"spec_id": "foreign"},
        {"adopted_in_edition": 3},
        {"contract_version": "future"},
    ],
)
def test_marker_mismatch_is_not_legacy_fallback(change):
    marker = new_execution_contract(
        board_id="board",
        spec_id="spec",
        edition=2,
        actor_id="author",
        origin="new_spec",
    )
    with pytest.raises(ValueError):
        execution_contract(spec(execution_contract={**marker, **change}))


def adopted_snapshot(implementations=None, tests=None):
    inventory, defaults, runs = case()
    implementations = defaults if implementations is None else implementations
    tests = runs if tests is None else tests
    return replace(
        SNAPSHOT,
        implementations=tuple(item.fact for item in implementations),
        tests=tuple(item.fact for item in tests),
        effective_context=EffectiveDeliveryContext(
            inventory, implementations, tests, frozenset({"automated_test"})
        ),
    )


def test_common_evaluator_requires_all_contributions_only_when_contract_is_selected():
    _, implementations, tests = case()
    snapshot = adopted_snapshot(implementations[:1], tests[:1])
    result = evaluate_delivery_coverage(snapshot)
    assert not result.allowed and result.rows[0].missing_card_ids == ("authorization",)
    assert not result.rows[0].implementation_satisfied
    assert evaluate_delivery_coverage(replace(snapshot, effective_context=None)).allowed
    assert evaluate_delivery_coverage(adopted_snapshot()).allowed


def test_adopted_admission_accepts_failed_partial_work_before_done_without_credit():
    _, implementations, tests = case()
    partial = replace(
        implementations[0],
        fact=replace(
            implementations[0].fact,
            card_status="in_progress",
            contributions=(DeliveryContribution(BINDING, "partial"),),
        ),
    )
    failed = replace(
        tests[0],
        fact=replace(tests[0].fact, result="failed", card_status="in_progress"),
    )
    snapshot = adopted_snapshot((partial,), (failed,))
    require_test_result_admission(snapshot, failed.fact)
    assert not evaluate_delivery_coverage(snapshot).allowed


@pytest.mark.parametrize(
    "mutation",
    [
        "wrong_criterion",
        "wrong_method",
        "no_scope",
        "stale_scope",
        "unobserved_card",
        "missing_context",
    ],
)
def test_admission_rejects_unobserved_or_unattested_contributions(mutation):
    _, implementations, tests = case()
    run = tests[0]
    implementation = implementations[0]
    if mutation == "wrong_criterion":
        run = replace(run, criterion_ids=("unrelated",))
    elif mutation == "wrong_method":
        run = replace(run, verification_method="inspection")
    elif mutation == "no_scope":
        implementation = replace(implementation, scopes=())
    elif mutation == "stale_scope":
        implementation = replace(
            implementation,
            scopes=(replace(implementation.scopes[0], scope_sha256="c" * 64),),
        )
    elif mutation == "unobserved_card":
        run = replace(
            run,
            fact=replace(run.fact, verified_implementation_ids=("ui", "authorization")),
        )
    snapshot = adopted_snapshot((implementation, implementations[1]), (run,))
    if mutation == "missing_context":
        snapshot = replace(snapshot, effective_context=False)
    with pytest.raises(ValueError, match="delivery_current_verified"):
        require_test_result_admission(snapshot, run.fact)
