import pytest
from pydantic import ValidationError
from okto_pulse.core.models.delivery_evidence import CardDeliveryEvidenceCommand
from okto_pulse.core.models.code_traceability import (
    ImplementationTargetExecutionSubmission,
)


def command(**changes):
    return CardDeliveryEvidenceCommand.model_validate(
        dict(
            board_id="b",
            card_id="c",
            spec_id="s",
            expected_card_version=1,
            expected_spec_edition=1,
            idempotency_key="key",
            kind="implementation",
            obligation_refs=["card:c"],
            justification="Implemented",
            execution_submission=dict(
                target_id="target",
                result_investigation_receipt_id="receipt",
                disposition="touched",
            ),
            **changes,
        )
    )


@pytest.mark.parametrize(
    "extra",
    [
        "board_id",
        "card_id",
        "idempotency_key",
        "justification",
        "accepted",
        "actor_id",
        "trust_level",
    ],
)
def test_inline_owns_no_scope_identity_or_authority(extra):
    payload = command().model_dump()
    payload["execution_submission"][extra] = "forged"
    with pytest.raises(ValidationError):
        CardDeliveryEvidenceCommand.model_validate(payload)


def test_inline_and_reference_are_exclusive_and_legacy_digest_unchanged():
    with pytest.raises(ValidationError, match="fields_invalid"):
        command(execution_id="existing")
    payload = command().model_dump()
    payload.pop("execution_submission")
    payload["execution_id"] = "existing"
    legacy = CardDeliveryEvidenceCommand.model_validate(payload)
    assert "execution_submission" not in legacy.model_dump()
    assert legacy.model_dump() == payload


def test_inline_single_counts_technical_references_in_aggregate_limit():
    payload = command().model_dump()
    payload["obligation_refs"] = [f"fr:{i}" for i in range(199)]
    with pytest.raises(ValidationError, match="payload_limit"):
        CardDeliveryEvidenceCommand.model_validate(payload)


@pytest.mark.parametrize(
    "changes",
    [
        {"actual_relative_path": "../secret"},
        {"disposition": "replaced"},
        {"replacement_target_id": "different"},
    ],
)
def test_inline_reuses_origin_path_and_disposition_validators(changes):
    payload = command().model_dump()
    payload["execution_submission"].update(changes)
    with pytest.raises(ValidationError):
        CardDeliveryEvidenceCommand.model_validate(payload)
    with pytest.raises(ValidationError):
        ImplementationTargetExecutionSubmission(
            board_id="b",
            card_id="c",
            idempotency_key="key",
            justification="Implemented",
            **payload["execution_submission"],
        )
