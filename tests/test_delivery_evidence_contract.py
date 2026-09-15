from dataclasses import replace
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from okto_pulse.core.models.delivery_evidence import DeliveryEvidenceCommand
from okto_pulse.core.services.delivery_evidence import delivery_inventory
from okto_pulse.core.domain.delivery_evidence import evaluate_delivery_coverage
from test_delivery_evidence_domain import SNAPSHOT, IMPLEMENTATION, TEST


def test_all_current_implementations_require_tests_but_separate_runs_can_jointly_cover():
    second = replace(IMPLEMENTATION, id="second-impl", receipt_id="second-receipt")
    snapshot = replace(SNAPSHOT, implementations=(IMPLEMENTATION, second))
    assert not evaluate_delivery_coverage(snapshot).allowed
    second_test = replace(
        TEST, id="second-test", verified_implementation_ids=(second.id,)
    )
    assert evaluate_delivery_coverage(
        replace(snapshot, tests=(TEST, second_test))
    ).allowed


def test_inventory_selectively_invalidates_and_never_assumes_empty_is_delivered():
    spec = SimpleNamespace(
        id="spec",
        title="Delivery",
        functional_requirements=[
            {"id": "fr1", "text": "A"},
            {"id": "fr2", "text": "B"},
        ],
    )
    before = delivery_inventory(spec)
    spec.functional_requirements[0]["linked_task_ids"] = ["task"]
    assert delivery_inventory(spec) == before
    spec.functional_requirements[0]["text"] = "Changed"
    after = delivery_inventory(spec)
    assert before[0].binding != after[0].binding
    assert before[1].binding == after[1].binding
    spec.functional_requirements[0]["status"] = "revoked"
    assert delivery_inventory(spec) == (after[1],)
    spec.functional_requirements = []
    assert delivery_inventory(spec)[0].binding.obligation_ref == "spec:spec"


@pytest.mark.parametrize(
    "extra",
    [
        {"verified": True},
        {"actor_id": "owner"},
        {"expected_edition": True},
        {"obligation_refs": ["fr:1", "fr:1"]},
        {"implementation_ids": ["foreign"]},
        {"scenario_id": "scenario"},
        {"justification": " "},
    ],
)
def test_closed_command_rejects_forged_authority_and_wrong_shape(extra):
    data = dict(
        board_id="b",
        spec_id="s",
        kind="implementation",
        expected_edition=1,
        expected_version=1,
        idempotency_key="key",
        obligation_refs=["fr:1"],
        card_id="task",
        execution_id="execution",
        justification="Implemented",
    )
    with pytest.raises(ValidationError):
        DeliveryEvidenceCommand(**{**data, **extra})
