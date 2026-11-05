from dataclasses import replace
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from okto_pulse.core.models.delivery_evidence import (
    CardDeliveryEvidenceCommand,
    CardDeliveryEvidenceBatchCommand,
    DeliveryEvidenceCommand,
    DeliveryEvidenceInput,
)
from okto_pulse.core.domain.delivery_inventory import delivery_inventory
from okto_pulse.core.domain.delivery_evidence import _evaluate_delivery_facts as evaluate_delivery_coverage
from test_delivery_evidence_domain import SNAPSHOT, IMPLEMENTATION, TEST


def test_all_current_implementations_require_tests_but_separate_runs_can_jointly_cover():
    second = replace(IMPLEMENTATION, id="second-impl", executions=(replace(IMPLEMENTATION.executions[0], execution_id="second-receipt"),), contributions=tuple(replace(row, execution_ids=("second-receipt",)) for row in IMPLEMENTATION.contributions))
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
        {"expected_spec_edition": True},
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
        expected_spec_edition=1,
        expected_card_version=1,
        idempotency_key="key",
        obligation_refs=["fr:1"],
        card_id="task",
        execution_id="execution",
        justification="Implemented",
    )
    with pytest.raises(ValidationError):
        CardDeliveryEvidenceCommand(**{**data, **extra})


@pytest.mark.parametrize("kind", ["implementation", "test"])
def test_spec_exception_contract_rejects_obsolete_proof_with_remediation(kind):
    """DEI-T53: a legacy client cannot create proof outside the canonical ledger."""
    with pytest.raises(ValidationError) as caught:
        DeliveryEvidenceInput.model_validate({"kind": kind})
    assert caught.value.errors()[0]["type"] == "delivery_card_scope_required"
    assert "card-scoped" in str(caught.value)
    assert DeliveryEvidenceInput.model_json_schema()["properties"]["kind"]["enum"] == [
        "waiver", "revoke"
    ]


@pytest.mark.parametrize("kind", [[], {}, None, "unknown"])
def test_invalid_spec_exception_kind_is_validation_error(kind):
    with pytest.raises(ValidationError):
        DeliveryEvidenceInput.model_validate({"kind": kind})


@pytest.mark.parametrize("kind", ["implementation", "test"])
def test_constructed_spec_command_cannot_bypass_kind_guard(kind):
    command = DeliveryEvidenceCommand.model_construct(kind=kind)
    with pytest.raises(ValueError, match="delivery_card_scope_required"):
        command.require_exception_kind()


@pytest.mark.parametrize("value", ["old requirement text", {"text": "Missing identity"},
                                    {"id": None}, {"id": ""}, {"id": 1}])
def test_inventory_refuses_old_shapes_without_synthesizing_identity(value):
    from copy import deepcopy
    spec = SimpleNamespace(id="spec", title="Delivery", functional_requirements=[value])
    before = deepcopy(spec.functional_requirements)
    with pytest.raises(ValueError, match="delivery_obligation_inventory_invalid"):
        delivery_inventory(spec)
    assert spec.functional_requirements == before


@pytest.mark.parametrize("value", ["", False, 0, {}])
def test_inventory_does_not_convert_invalid_collection_to_empty(value):
    spec = SimpleNamespace(id="spec", title="Delivery", functional_requirements=value)
    with pytest.raises(ValueError, match="delivery_obligation_inventory_invalid"):
        delivery_inventory(spec)


@pytest.mark.parametrize('batch', [False, True])
@pytest.mark.parametrize('accepted_id', [None, 'accepted-result'])
def test_test_association_accepts_optional_exact_result_in_single_and_batch(batch, accepted_id):
    fields = dict(kind='test', obligation_refs=['fr:1'], scenario_id='scenario',
        implementation_ids=['implementation'], justification='Observe the delivered revision')
    if accepted_id is not None:
        fields['accepted_result_id'] = accepted_id
    scope = dict(board_id='board', spec_id='spec', card_id='test', expected_card_version=1,
        expected_spec_edition=1, idempotency_key='association')
    if batch:
        command = CardDeliveryEvidenceBatchCommand(**scope, contract_version='card-delivery-batch/v1',
            expected_delivery_revision=0, entries=[dict(client_ref='test', **fields)])
        actual = command.entries[0]
    else:
        actual = CardDeliveryEvidenceCommand(**scope, **fields)
    assert actual.accepted_result_id == accepted_id


@pytest.mark.parametrize('kind,fields', [
    ('implementation', dict(bindings=[dict(obligation_ref='fr:1', contribution='complete')], execution_id='execution')),
    ('revoke', dict(record_id='record')),
    ('progress', {}),
])
def test_accepted_result_identity_cannot_be_used_for_other_delivery_kinds(kind, fields):
    with pytest.raises(ValidationError, match='delivery_command_fields_invalid'):
        CardDeliveryEvidenceCommand(board_id='board', spec_id='spec', card_id='task',
            expected_card_version=1, expected_spec_edition=1, idempotency_key='invalid',
            kind=kind, accepted_result_id='accepted-result', justification='No foreign proof kind', **fields)


@pytest.mark.parametrize('batch', [False, True])
def test_exact_result_reference_counts_against_existing_bounded_command_budget(batch):
    fields = dict(kind='test', obligation_refs=['fr:1'], scenario_id='scenario',
        implementation_ids=[f'implementation-{i}' for i in range(199)], justification='Bounded references')
    scope = dict(board_id='board', spec_id='spec', card_id='test', expected_card_version=1,
        expected_spec_edition=1, idempotency_key='bounded')
    def parse(value):
        return (CardDeliveryEvidenceBatchCommand(**scope, contract_version='card-delivery-batch/v1',
            expected_delivery_revision=0, entries=[dict(client_ref='test', **value)]) if batch
            else CardDeliveryEvidenceCommand(**scope, **value))
    parse(fields)
    with pytest.raises(ValidationError, match='delivery_payload_limit'):
        parse(dict(fields, accepted_result_id='one-more-reference'))
