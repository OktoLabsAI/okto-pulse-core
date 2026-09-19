from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

from okto_pulse.core.domain.delivery_evidence import (
    DeliveryBinding,
    DeliveryContribution,
    DeliveryObligation,
    evaluate_delivery_coverage,
    read_delivery_contributions,
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
        IMPLEMENTATION, contributions=(DeliveryContribution(BINDING, "partial"),)
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
            DeliveryContribution(BINDING, "partial"),
            DeliveryContribution(second, "complete"),
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
        IMPLEMENTATION, contributions=(DeliveryContribution(BINDING, "complete"),)
    )
    assert evaluate_delivery_coverage(
        replace(SNAPSHOT, implementations=(complete,))
    ).allowed
    for change in (
        {"current_accepted_execution": False},
        {"card_status": CardStatus.IN_PROGRESS},
        {"receipt_id": ""},
    ):
        assert not evaluate_delivery_coverage(
            replace(SNAPSHOT, implementations=(replace(complete, **change),))
        ).allowed


def test_legacy_remains_absent_not_fabricated_complete():
    assert read_delivery_contributions({}, (BINDING,)) is None
    assert IMPLEMENTATION.contributions is None
    assert evaluate_delivery_coverage(SNAPSHOT).allowed
    old = request()
    old.pop("bindings")
    old["obligation_refs"] = [BINDING.obligation_ref]
    assert "bindings" not in CardDeliveryEvidenceInput(**old).model_dump()
    # Serialization adds no new empty field to the legacy idempotency digest.
    assert (
        CardDeliveryEvidenceInput(**old).selected_obligation_refs
        == old["obligation_refs"]
    )


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
def test_corrupt_new_history_never_falls_back_to_legacy(change):
    payload = dict(
        contribution_contract_version="card-binding-contribution/v1",
        contributions=[
            dict(obligation_ref=BINDING.obligation_ref, contribution="partial")
        ],
    )
    payload.update(change)
    with pytest.raises(ValueError):
        read_delivery_contributions(payload, (BINDING,))


@pytest.mark.asyncio
async def test_card_gate_before_done_uses_the_same_completion_predicate(monkeypatch):
    from okto_pulse.core.services import delivery_evidence as service

    store = SimpleNamespace(load_card_snapshot=AsyncMock())
    monkeypatch.setattr(service, "card_delivery_store", lambda _: store)
    card = SimpleNamespace(
        id="task-1", board_id="board", spec_id="spec", card_type="normal"
    )
    spec = SimpleNamespace(id="spec", edition=2)
    for state in ("partial", "complete"):
        fact = replace(
            IMPLEMENTATION,
            card_status=CardStatus.IN_PROGRESS,
            contributions=(DeliveryContribution(BINDING, state),),
        )
        store.load_card_snapshot.return_value = replace(
            SNAPSHOT, implementations=(fact,)
        )
        if state == "partial":
            with pytest.raises(ValueError, match="delivery_evidence_incomplete"):
                await require_card_delivery(None, card, spec)
        else:
            await require_card_delivery(None, card, spec)
