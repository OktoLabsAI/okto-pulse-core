from dataclasses import replace

import pytest

from okto_pulse.core.domain.delivery_evidence import require_test_result_admission, evaluate_delivery_coverage, DeliveryContribution
from okto_pulse.core.domain.enums import CardStatus, TestScenarioStatus as Result
from test_delivery_evidence_domain import SNAPSHOT, TEST, IMPLEMENTATION, BINDING, SCOPE


@pytest.mark.parametrize("result", [Result.PASSED, Result.FAILED])
def test_admission_does_not_imply_final_delivery(result):
    proof = replace(IMPLEMENTATION, card_status=CardStatus.IN_PROGRESS,
                    contributions=(DeliveryContribution(BINDING, "partial"),))
    test = replace(TEST, card_status=CardStatus.IN_PROGRESS, result=result)
    snapshot = replace(SNAPSHOT, implementations=(proof,), tests=(test,))
    require_test_result_admission(snapshot, test)
    assert not evaluate_delivery_coverage(snapshot).allowed
    completed = replace(SNAPSHOT, tests=(replace(test, card_status=CardStatus.DONE),))
    assert evaluate_delivery_coverage(completed).allowed == (result == Result.PASSED)


@pytest.mark.parametrize("change", [
    {"current_verified_run": False}, {"result": Result.DRAFT},
    {"verified_implementation_ids": ("unknown",)}, {"receipt_id": ""},
    {"verified_implementation_ids": ("impl", "impl")},
    {"scope": replace(SCOPE, board_id="foreign")},
    {"card_status": CardStatus.VALIDATION}, {"card_status": CardStatus.REJECTED},
])
def test_admission_still_requires_authenticated_exact_scope(change):
    with pytest.raises(ValueError, match="verified_test_and_implementation"):
        require_test_result_admission(SNAPSHOT, replace(TEST, **change))


@pytest.mark.parametrize("change", [
    {"current_accepted_execution": False}, {"bindings": ()},
    {"scope": replace(SCOPE, edition=3)}, {"card_status": CardStatus.CANCELLED},
])
def test_admission_requires_all_named_implementations_current(change):
    with pytest.raises(ValueError, match="verified_test_and_implementation"):
        require_test_result_admission(replace(SNAPSHOT, implementations=(replace(IMPLEMENTATION, **change),)), TEST)


def test_unknown_population_cannot_be_admitted():
    with pytest.raises(ValueError, match="verified_test_and_implementation"):
        require_test_result_admission(replace(SNAPSHOT, complete=False), TEST)
