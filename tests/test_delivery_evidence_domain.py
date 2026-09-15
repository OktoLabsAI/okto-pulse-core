from dataclasses import replace

import pytest

from okto_pulse.core.domain.delivery_evidence import (
    DeliveryBinding,
    DeliveryEvidenceSnapshot,
    DeliveryObligation,
    DeliveryPhase,
    DeliveryScope,
    DeliveryWaiverFact,
    ImplementationDeliveryFact,
    TestDeliveryFact as DeliveryTestFact,
    evaluate_delivery_coverage,
)
from okto_pulse.core.domain.enums import (
    CardStatus,
    CardType,
    TestScenarioStatus as ScenarioStatus,
)

SCOPE = DeliveryScope("board", "spec", 2)
BINDING = DeliveryBinding("functional_requirement:fr-1", "a" * 64)
OBLIGATION = DeliveryObligation(BINDING, "Store memories atomically")
IMPLEMENTATION = ImplementationDeliveryFact(
    "impl",
    SCOPE,
    "task-1",
    CardType.NORMAL,
    CardStatus.DONE,
    (BINDING,),
    "repository:neuron",
    "c" * 40,
    "src/storage.py",
    "Implements the atomic write",
    "execution-receipt",
    True,
    "agent-1",
)
TEST = DeliveryTestFact(
    "test",
    SCOPE,
    "test-card-1",
    CardType.TEST,
    CardStatus.DONE,
    (BINDING,),
    "scenario-1",
    ScenarioStatus.PASSED,
    "verified-test-receipt",
    True,
    "agent-2",
    ("impl",),
)
SNAPSHOT = DeliveryEvidenceSnapshot(
    SCOPE, (OBLIGATION,), (IMPLEMENTATION,), (TEST,), complete=True
)


def test_complete_delivery_preserves_distinct_implementation_and_test_receipts():
    result = evaluate_delivery_coverage(SNAPSHOT)
    assert result.allowed
    assert result.rows[0].implementation_ids == ("impl",)
    assert result.rows[0].test_ids == ("test",)


@pytest.mark.parametrize("card_type", [CardType.NORMAL, CardType.BUG])
def test_task_or_bug_cannot_claim_test_verification(card_type):
    result = evaluate_delivery_coverage(
        replace(SNAPSHOT, tests=(replace(TEST, card_type=card_type),))
    )
    assert not result.allowed
    assert result.blockers == ("delivery_test_result_missing",)


def test_test_card_cannot_replace_task_implementation():
    result = evaluate_delivery_coverage(
        replace(
            SNAPSHOT,
            implementations=(replace(IMPLEMENTATION, card_type=CardType.TEST),),
        )
    )
    assert result.blockers == (
        "delivery_implementation_missing",
        "delivery_test_result_missing",
    )


@pytest.mark.parametrize(
    "overrides",
    [
        {"current_verified_run": False},
        {"receipt_id": ""},
        {"scenario_id": ""},
        {"actor_id": ""},
        {"result": ScenarioStatus.FAILED},
        {"verified_implementation_ids": ()},
        {"verified_implementation_ids": ("old-impl",)},
        {"result": ScenarioStatus.AUTOMATED},
        {"card_status": CardStatus.CANCELLED},
        {"card_status": CardStatus.IN_PROGRESS},
        {"scope": DeliveryScope("other-board", "spec", 2)},
        {"scope": DeliveryScope("board", "other-spec", 2)},
        {"scope": DeliveryScope("board", "spec", 1)},
        {"bindings": (DeliveryBinding(BINDING.obligation_ref, "b" * 64),)},
    ],
)
def test_done_alone_stale_or_wrong_scope_test_is_not_proof(overrides):
    result = evaluate_delivery_coverage(
        replace(SNAPSHOT, tests=(replace(TEST, **overrides),))
    )
    assert not result.allowed
    assert result.rows[0].test_ids == ()
    assert "test" in result.rejected_record_ids


@pytest.mark.parametrize(
    "overrides",
    [
        {"current_accepted_execution": False},
        {"receipt_id": ""},
        {"result_revision": ""},
        {"source_ref": ""},
        {"relative_path": ""},
        {"explanation": "  "},
        {"card_status": CardStatus.CANCELLED},
        {"card_status": CardStatus.VALIDATION},
        {"scope": DeliveryScope("other", "spec", 2)},
        {"scope": DeliveryScope("board", "spec", 1)},
        {"bindings": (DeliveryBinding(BINDING.obligation_ref, "b" * 64),)},
    ],
)
def test_plans_empty_or_stale_implementation_cannot_satisfy_delivery(overrides):
    result = evaluate_delivery_coverage(
        replace(SNAPSHOT, implementations=(replace(IMPLEMENTATION, **overrides),))
    )
    assert not result.allowed
    assert result.rows[0].implementation_ids == ()


def test_shared_evidence_and_selective_invalidation():
    second = DeliveryBinding("technical_requirement:tr-2", "b" * 64)
    snapshot = replace(
        SNAPSHOT,
        obligations=(OBLIGATION, DeliveryObligation(second, "Bound concurrency")),
        implementations=(replace(IMPLEMENTATION, bindings=(BINDING, second)),),
        tests=(replace(TEST, bindings=(BINDING, second)),),
    )
    assert evaluate_delivery_coverage(snapshot).allowed
    changed = replace(second, semantic_sha256="d" * 64)
    result = evaluate_delivery_coverage(
        replace(
            snapshot,
            obligations=(
                OBLIGATION,
                DeliveryObligation(changed, "Bound concurrency differently"),
            ),
        )
    )
    assert result.rows[0].implementation_satisfied and result.rows[0].test_satisfied
    assert (
        not result.rows[1].implementation_satisfied
        and not result.rows[1].test_satisfied
    )


def waiver(**overrides):
    return replace(
        DeliveryWaiverFact(
            "waiver",
            SCOPE,
            BINDING,
            DeliveryPhase.TEST,
            "No executable behavior for this obligation",
            "human",
            "authorization-receipt",
            True,
        ),
        **overrides,
    )


def test_authorized_per_obligation_waiver_is_not_a_passed_test():
    result = evaluate_delivery_coverage(
        replace(SNAPSHOT, tests=(), waivers=(waiver(),))
    )
    assert result.allowed
    assert result.rows[0].test_ids == ()
    assert result.rows[0].test_waiver_ids == ("waiver",)


@pytest.mark.parametrize(
    "overrides",
    [
        {"current_authorized": False},
        {"authorization_receipt_id": ""},
        {"actor_id": ""},
        {"justification": " "},
        {"phase": DeliveryPhase.IMPLEMENTATION},
        {"phase": "test"},
        {"scope": DeliveryScope("board", "spec", 1)},
        {"binding": DeliveryBinding(BINDING.obligation_ref, "b" * 64)},
    ],
)
def test_waiver_cannot_bypass_authority_phase_scope_or_currentness(overrides):
    result = evaluate_delivery_coverage(
        replace(SNAPSHOT, tests=(), waivers=(waiver(**overrides),))
    )
    assert not result.allowed
    assert "delivery_test_result_missing" in result.blockers


def test_partial_empty_and_ambiguous_projections_fail_closed():
    assert not evaluate_delivery_coverage(replace(SNAPSHOT, complete=False)).allowed
    assert not evaluate_delivery_coverage(replace(SNAPSHOT, obligations=())).allowed
    assert not evaluate_delivery_coverage(
        replace(SNAPSHOT, obligations=(OBLIGATION, OBLIGATION))
    ).allowed
    assert not evaluate_delivery_coverage(replace(SNAPSHOT, tests=(TEST, TEST))).allowed


def test_boundaries_reject_invalid_scope_and_semantic_identity():
    with pytest.raises(ValueError):
        DeliveryScope("board", "spec", True)
    with pytest.raises(ValueError):
        DeliveryBinding("fr:1", "not-a-digest")


def test_no_status_or_policy_side_effect_and_no_provider_dependency():
    result = evaluate_delivery_coverage(replace(SNAPSHOT, implementations=(), tests=()))
    assert not result.allowed
    # The snapshot has no mutable model/status or Skip/context flags to rewrite.
    assert not hasattr(SNAPSHOT, "spec_status")
    assert not hasattr(SNAPSHOT, "skip_code_evidence_coverage")
