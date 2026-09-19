from dataclasses import replace

import pytest
from pydantic import ValidationError

from okto_pulse.core.domain.delivery_evidence import (
    DeliveryBinding,
    DeliveryContribution,
    DeliveryObligation,
    ImplementationExecutionProof,
    delivery_execution_ids,
    evaluate_delivery_coverage,
    implementation_binding_ready,
    read_delivery_contributions,
)
from okto_pulse.core.models.delivery_evidence import CardDeliveryEvidenceInput
from test_delivery_batch import batch
from test_delivery_local_references import implementation
from test_delivery_evidence_domain import BINDING, IMPLEMENTATION, SNAPSHOT


SECOND = DeliveryBinding("tr:tr", "b" * 64)
PROOFS = tuple(
    ImplementationExecutionProof(
        f"execution-{i}",
        f"target-{i}",
        1,
        "source",
        "a" * 40,
        f"src/{i}.py",
        True,
    )
    for i in (1, 2)
)
COMPOSITE = replace(
    IMPLEMENTATION,
    source_ref="",
    result_revision="",
    relative_path="",
    receipt_id="",
    bindings=(BINDING, SECOND),
    executions=PROOFS,
    contributions=(
        DeliveryContribution(BINDING, "complete", ("execution-1", "execution-2")),
        DeliveryContribution(SECOND, "complete", ("execution-1",)),
    ),
)


def entry(**changes):
    return dict(
        client_ref="composed",
        kind="implementation",
        justification="Integrated contribution",
        bindings=[
            dict(
                obligation_ref="fr:fr",
                contribution="complete",
                execution_refs=[
                    dict(execution_id="execution-1"),
                    dict(execution_id="execution-2"),
                ],
            )
        ],
        **changes,
    )


def test_exact_execution_sets_have_selective_currentness_and_test_credit():
    snapshot = replace(
        SNAPSHOT,
        obligations=(*SNAPSHOT.obligations, DeliveryObligation(SECOND, "Technical")),
        implementations=(COMPOSITE,),
    )
    result = evaluate_delivery_coverage(snapshot)
    assert [row.implementation_satisfied for row in result.rows] == [True, True]
    assert result.rows[0].test_satisfied
    changed = replace(
        COMPOSITE,
        current_accepted_execution=False,
        executions=(PROOFS[0], replace(PROOFS[1], current_accepted_execution=False)),
    )
    result = evaluate_delivery_coverage(replace(snapshot, implementations=(changed,)))
    assert [row.implementation_satisfied for row in result.rows] == [False, True]
    assert not result.rows[0].test_satisfied
    assert not implementation_binding_ready(changed, BINDING)
    assert implementation_binding_ready(changed, SECOND)


@pytest.mark.parametrize(
    "proofs",
    [
        PROOFS[:1],
        (PROOFS[0], replace(PROOFS[1], result_revision="b" * 40)),
        (PROOFS[0], replace(PROOFS[1], source_ref="other-source")),
        (PROOFS[0], replace(PROOFS[1], target_id="target-1")),
        (PROOFS[0], replace(PROOFS[1], relative_path="")),
        (PROOFS[0], replace(PROOFS[1], target_revision=0)),
    ],
)
def test_missing_incompatible_or_invalid_member_cannot_complete_set(proofs):
    assert not implementation_binding_ready(
        replace(COMPOSITE, executions=proofs), BINDING
    )


def test_partial_set_and_old_test_never_upgrade_a_new_complete_record():
    partial = replace(
        COMPOSITE,
        contributions=(
            replace(COMPOSITE.contributions[0], contribution="partial"),
            COMPOSITE.contributions[1],
        ),
    )
    assert not implementation_binding_ready(partial, BINDING)
    result = evaluate_delivery_coverage(
        replace(SNAPSHOT, implementations=(replace(COMPOSITE, id="new-record"),))
    )
    assert result.rows[0].implementation_satisfied and not result.rows[0].test_satisfied


def test_persisted_set_resolves_only_receipts_of_the_tested_binding():
    payload = dict(
        contribution_contract_version="card-binding-contribution/v2",
        bindings=[
            dict(obligation_ref=b.obligation_ref, semantic_sha256=b.semantic_sha256)
            for b in (BINDING, SECOND)
        ],
        contributions=[
            dict(
                obligation_ref=c.binding.obligation_ref,
                contribution=c.contribution,
                execution_ids=list(c.execution_ids),
            )
            for c in COMPOSITE.contributions
        ],
    )
    assert delivery_execution_ids(payload) == ("execution-1", "execution-2")
    assert delivery_execution_ids(payload, (SECOND,)) == ("execution-1",)
    assert (
        read_delivery_contributions(payload, (BINDING, SECOND))
        == COMPOSITE.contributions
    )
    payload["contributions"][0]["execution_ids"] = []
    with pytest.raises(ValueError, match="execution_set_invalid"):
        delivery_execution_ids(payload)


def test_single_command_and_batch_resolve_the_same_typed_set():
    data = entry()
    data.pop("client_ref")
    single = CardDeliveryEvidenceInput(
        **data,
        expected_card_version=1,
        expected_spec_edition=1,
        idempotency_key="single",
    )
    assert single.composite_execution and len(single.binding_execution_refs) == 2
    local = entry()
    local["bindings"][0]["execution_refs"] = [
        {"client_ref": "first"},
        {"client_ref": "second"},
    ]
    request = batch(entries=[implementation("first"), implementation("second"), local])
    resolved = request.entries[-1].resolved_fields(
        {"first": {"execution_id": "one"}, "second": {"execution_id": "two"}}
    )
    assert resolved["bindings"][0]["execution_refs"] == [
        {"execution_id": "one"},
        {"execution_id": "two"},
    ]
    with pytest.raises(ValidationError, match="requires_batch"):
        CardDeliveryEvidenceInput(
            **{k: v for k, v in local.items() if k != "client_ref"},
            expected_card_version=1,
            expected_spec_edition=1,
            idempotency_key="single",
        )


@pytest.mark.parametrize(
    "change",
    [
        {"execution_id": "outer"},
        {"execution_client_ref": "first"},
        {
            "execution_submission": {
                "target_id": "t",
                "result_investigation_receipt_id": "r",
                "disposition": "touched",
                "actual_relative_path": "src/a.py",
            }
        },
    ],
)
def test_composed_set_cannot_mix_envelope_execution(change):
    with pytest.raises(ValidationError, match="fields_invalid"):
        batch(entries=[implementation("first"), entry(**change)])


@pytest.mark.parametrize(
    "reference",
    [
        {},
        {"execution_id": "e", "client_ref": "local"},
        {"execution_id": "e", "verified": True},
    ],
)
def test_reference_shape_is_closed(reference):
    value = entry()
    value["bindings"][0]["execution_refs"] = [reference]
    with pytest.raises(ValidationError):
        batch(entries=[value])


def test_set_aliases_cannot_point_forward_or_to_a_composite_or_exceed_budget():
    value = entry()
    value["bindings"][0]["execution_refs"] = [{"client_ref": "later"}]
    with pytest.raises(ValidationError, match="local_reference_invalid"):
        batch(entries=[value, implementation("later")])
    value["bindings"][0]["execution_refs"] = [{"client_ref": "composed"}]
    value["client_ref"] = "another"
    with pytest.raises(ValidationError, match="local_reference_invalid"):
        batch(entries=[entry(), value])
    large = entry()
    large["bindings"][0]["execution_refs"] = [
        {"execution_id": str(i)} for i in range(100)
    ]
    another = {**large, "client_ref": "another"}
    with pytest.raises(ValidationError, match="batch_payload_limit"):
        batch(entries=[large, another])
