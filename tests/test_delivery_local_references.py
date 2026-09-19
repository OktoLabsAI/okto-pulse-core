import pytest
from pydantic import ValidationError
from test_delivery_inline_execution import command as inline_command
from test_delivery_batch import batch, progress
from okto_pulse.core.models.delivery_evidence import CardDeliveryEvidenceCommand


def implementation(ref="proof", **changes):
    payload = inline_command().model_dump(
        exclude={
            "board_id",
            "card_id",
            "spec_id",
            "expected_card_version",
            "expected_spec_edition",
            "idempotency_key",
        }
    )
    payload.update(client_ref=ref, **changes)
    return payload


def alias(ref="alias", source="proof", **changes):
    payload = implementation(ref)
    payload.pop("execution_submission")
    payload.update(execution_client_ref=source, **changes)
    return payload


@pytest.mark.parametrize(
    "entries",
    [
        [alias()],  # missing
        [alias(), implementation()],  # forward
        [alias(source="alias")],  # self/cycle
        [progress("proof"), alias()],  # wrong kind
        [implementation(progress_refs=[{"client_ref": "later"}]), progress("later")],
        [
            implementation(),
            implementation("next", progress_refs=[{"client_ref": "proof"}]),
        ],
    ],
)
def test_local_references_are_typed_previous_entries_only(entries):
    with pytest.raises(ValidationError, match="local_reference_invalid"):
        batch(entries=entries)


def test_aliases_resolve_to_canonical_ids_without_authority_fields():
    value = batch(
        entries=[
            progress("p"),
            implementation(progress_refs=[{"client_ref": "p"}]),
            alias(),
        ]
    )
    result = value.entries[1].resolved_fields({"p": {"id": "progress-id"}})
    assert result["progress_refs"] == [{"record_id": "progress-id"}]
    result = value.entries[2].resolved_fields(
        {"proof": {"id": "binding-id", "execution_id": "execution-id"}}
    )
    assert result["execution_id"] == "execution-id"
    assert "execution_client_ref" not in result and "client_ref" not in result


def test_alias_cannot_be_supplied_to_single_entry_or_with_other_execution():
    payload = inline_command().model_dump()
    payload["progress_refs"] = [{"client_ref": "p"}]
    with pytest.raises(ValidationError, match="requires_batch"):
        CardDeliveryEvidenceCommand.model_validate(payload)
    with pytest.raises(ValidationError, match="fields_invalid"):
        batch(entries=[implementation(), alias(execution_id="another")])


@pytest.mark.parametrize(
    "reference",
    [
        {},
        {"record_id": "p", "client_ref": "local"},
        {"record_id": "p", "verified": True},
    ],
)
def test_progress_reference_is_closed_and_has_exactly_one_identity(reference):
    with pytest.raises(ValidationError):
        batch(entries=[implementation(progress_refs=[reference])])


def test_local_and_persisted_references_consume_aggregate_budget():
    entries = [progress("p")]
    entries += [
        implementation(
            str(i),
            progress_refs=[{"client_ref": "p"}],
            obligation_refs=[f"fr:{j}" for j in range(98)],
        )
        for i in range(2)
    ]
    with pytest.raises(ValidationError, match="batch_payload_limit"):
        batch(entries=entries)
