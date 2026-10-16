from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

from okto_pulse.core.domain.delivery_evidence import (
    DeliveryBinding,
    DeliveryContribution,
    DeliveryObligation,
    _evaluate_delivery_facts as evaluate_delivery_coverage,
    read_delivery_contributions,
    delivery_execution_ids,
)
from okto_pulse.core.domain.enums import CardStatus
from okto_pulse.core.models.delivery_evidence import (
    CardDeliveryEvidenceInput,
    CardDeliveryEvidenceBatchInput,
)
from okto_pulse.core.services.delivery_evidence import require_card_delivery
from test_delivery_evidence_domain import BINDING, IMPLEMENTATION, SNAPSHOT


def request(**changes):
    return dict(
        expected_card_version=1,
        expected_spec_edition=2,
        idempotency_key="declaration",
        kind="implementation",
        execution_id="execution",
        justification="Observed implementation",
        bindings=[dict(obligation_ref=BINDING.obligation_ref, contribution="partial")],
        **changes,
    )


def test_partial_is_not_credit_and_two_partials_never_accumulate_completion():
    partial = replace(
        IMPLEMENTATION, contributions=(DeliveryContribution(BINDING, "partial", ("execution-receipt",)),)
    )
    for facts in ((partial,), (partial, replace(partial, id="second"))):
        result = evaluate_delivery_coverage(replace(SNAPSHOT, implementations=facts))
        assert not result.allowed
        assert not result.rows[0].implementation_satisfied
        assert not result.rows[
            0
        ].test_satisfied  # A passing test cannot upgrade the claim.


def test_one_receipt_keeps_partial_and_complete_obligations_distinct():
    second = DeliveryBinding("tr:latency", "b" * 64)
    fact = replace(
        IMPLEMENTATION,
        bindings=(BINDING, second),
        contributions=(
            DeliveryContribution(BINDING, "partial", ("execution-receipt",)),
            DeliveryContribution(second, "complete", ("execution-receipt",)),
        ),
    )
    result = evaluate_delivery_coverage(
        replace(
            SNAPSHOT,
            obligations=(*SNAPSHOT.obligations, DeliveryObligation(second, "Latency")),
            implementations=(fact,),
            tests=(),
        )
    )
    assert [row.implementation_satisfied for row in result.rows] == [False, True]


def test_explicit_complete_preserves_all_original_proof_and_lifecycle_checks():
    complete = replace(
        IMPLEMENTATION, contributions=(DeliveryContribution(BINDING, "complete", ("execution-receipt",)),)
    )
    assert evaluate_delivery_coverage(
        replace(SNAPSHOT, implementations=(complete,))
    ).allowed
    for change in (
        {"executions": (replace(IMPLEMENTATION.executions[0], current_accepted_execution=False),)},
        {"card_status": CardStatus.IN_PROGRESS},
        {"executions": (replace(IMPLEMENTATION.executions[0], execution_id=""),)},
    ):
        assert not evaluate_delivery_coverage(
            replace(SNAPSHOT, implementations=(replace(complete, **change),))
        ).allowed


@pytest.mark.parametrize("declarations", [None, ()])
def test_absent_declaration_never_infers_completion(declarations):
    with pytest.raises(ValueError, match="delivery_contribution_payload_invalid"):
        read_delivery_contributions({}, (BINDING,))
    fact = replace(IMPLEMENTATION, contributions=declarations)
    assert not evaluate_delivery_coverage(replace(SNAPSHOT, implementations=(fact,))).allowed


def test_implementation_requires_typed_declarations_before_store_admission():
    data = request()
    data.pop("bindings")
    data["obligation_refs"] = [BINDING.obligation_ref]
    with pytest.raises(ValidationError, match="delivery_contribution_bindings_required"):
        CardDeliveryEvidenceInput.model_validate(data)


@pytest.mark.parametrize("fields", [
    dict(kind="implementation", execution_id="execution", bindings=[dict(obligation_ref="fr:a", contribution="complete")]),
    dict(kind="test", scenario_id="scenario", obligation_refs=["fr:a"], implementation_ids=["implementation"]),
    dict(kind="revoke", record_id="record"),
    dict(kind="progress", progress=dict(material_change="none", source_state=dict(workspace_state="unknown", recoverability="unknown"), remaining="Inspect")),
])
def test_current_commands_have_one_round_trip_and_canonical_defaults(fields):
    value = CardDeliveryEvidenceInput(expected_card_version=1, expected_spec_edition=1,
        idempotency_key="canonical", justification="Current declaration", **fields)
    canonical = value.model_dump(mode="json")
    omitted = {key: item for key, item in canonical.items() if item is not None}
    assert CardDeliveryEvidenceInput.model_validate(canonical).model_dump(mode="json") == canonical
    assert CardDeliveryEvidenceInput.model_validate(omitted).model_dump(mode="json") == canonical
    assert "execution_submission" in canonical and "progress_refs" in canonical
    if value.kind == "implementation":
        assert canonical["bindings"][0]["execution_refs"] == []


@pytest.mark.parametrize(
    "bindings",
    [
        None,
        [],
        [{"obligation_ref": "fr:a"}],
        [{"obligation_ref": "fr:a", "contribution": "legacy"}],
        [{"obligation_ref": "fr:a", "contribution": True}],
        [{"obligation_ref": "fr:a", "contribution": "complete", "verified": True}],
        [{"obligation_ref": "fr:a", "contribution": "partial"}] * 2,
    ],
)
def test_closed_binding_shape(bindings):
    data = request()
    data["bindings"] = bindings
    with pytest.raises(ValidationError):
        CardDeliveryEvidenceInput(**data)


def test_declarations_are_exclusive_to_implementation_and_count_in_batch_limits():
    data = request(obligation_refs=["fr:a"])
    with pytest.raises(ValidationError, match="contribution_bindings_invalid"):
        CardDeliveryEvidenceInput(**data)
    for kind in ("progress", "test", "revoke"):
        data = request()
        data["kind"] = kind
        with pytest.raises(ValidationError, match="contribution_bindings_invalid"):
            CardDeliveryEvidenceInput(**data)
    entry = {
        k: v
        for k, v in request().items()
        if k
        not in {"expected_card_version", "expected_spec_edition", "idempotency_key"}
    }
    entry["bindings"] = [
        dict(obligation_ref=f"fr:{i}", contribution="partial") for i in range(201)
    ]
    with pytest.raises(ValidationError, match="batch_payload_limit"):
        CardDeliveryEvidenceBatchInput(
            contract_version="card-delivery-batch/v1",
            expected_card_version=1,
            expected_spec_edition=2,
            expected_delivery_revision=0,
            idempotency_key="batch",
            entries=[dict(client_ref="a", **entry)],
        )


@pytest.mark.parametrize(
    "change",
    [
        {"contribution_contract_version": "unknown"},
        {"contributions": []},
        {"contributions": None},
        {"contributions": [{"obligation_ref": "foreign", "contribution": "complete"}]},
        {
            "contributions": [
                {"obligation_ref": BINDING.obligation_ref, "contribution": "invalid"}
            ]
        },
    ],
)
def test_corrupt_current_history_is_rejected(change):
    payload = dict(
        contribution_contract_version="card-binding-contribution/v2",
        contributions=[
            dict(obligation_ref=BINDING.obligation_ref, contribution="partial", execution_ids=["execution"])
        ],
    )
    payload.update(change)
    with pytest.raises(ValueError):
        read_delivery_contributions(payload, (BINDING,))


@pytest.mark.parametrize("version", [None, "card-binding-contribution/v1"])
def test_previous_storage_is_rejected_even_with_a_top_level_receipt(version):
    payload = dict(contribution_contract_version=version, execution_id="old-receipt",
        bindings=[dict(obligation_ref=BINDING.obligation_ref, semantic_sha256=BINDING.semantic_sha256)],
        contributions=[dict(obligation_ref=BINDING.obligation_ref, contribution="complete")])
    with pytest.raises(ValueError, match="delivery_contribution_payload_invalid"):
        delivery_execution_ids(payload)


def test_current_storage_resolves_only_explicit_binding_receipts():
    payload = dict(contribution_contract_version="card-binding-contribution/v2", execution_id="ignored",
        bindings=[dict(obligation_ref=BINDING.obligation_ref, semantic_sha256=BINDING.semantic_sha256)],
        contributions=[dict(obligation_ref=BINDING.obligation_ref, contribution="complete", execution_ids=["receipt"])])
    assert delivery_execution_ids(payload) == ("receipt",)
    assert delivery_execution_ids(payload, ()) == ()


@pytest.mark.asyncio
async def test_card_gate_before_done_uses_the_same_completion_predicate(monkeypatch):
    from okto_pulse.core.services import delivery_evidence as service
    from test_delivery_card_readiness import ready_snapshot

    store = SimpleNamespace(load_card_snapshot=AsyncMock())
    monkeypatch.setattr(service, "card_delivery_store", lambda _: store)
    card = SimpleNamespace(
        id="ui", board_id="board", spec_id="spec", card_type="normal"
    )
    spec = SimpleNamespace(id="spec", edition=2)
    for state in ("partial", "complete"):
        current = ready_snapshot()
        fact = replace(
            current.implementations[0],
            card_status=CardStatus.IN_PROGRESS,
            contributions=(DeliveryContribution(BINDING, state, ("execution-receipt",)),),
        )
        store.load_card_snapshot.return_value = replace(
            current, implementations=(fact,), effective_context=replace(current.effective_context,
                implementations=(replace(current.effective_context.implementations[0], fact=fact),)),
        )
        if state == "partial":
            with pytest.raises(ValueError, match="delivery_evidence_incomplete"):
                await require_card_delivery(None, card, spec)
        else:
            await require_card_delivery(None, card, spec)
