"""Closed inbound contracts; receipt validity and digests are server owned."""

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, model_serializer, model_validator
from pydantic_core import PydanticCustomError

from okto_pulse.core.domain.delivery_progress import DeliveryProgress
from okto_pulse.core.models.code_traceability import ImplementationTargetExecutionFields

Identity = Annotated[str, Field(min_length=1, max_length=512, pattern=r"\S")]
ClientReference = Annotated[str, Field(min_length=1, max_length=80, pattern=r"^[A-Za-z0-9_-]+$")]


class DeliveryProgressReference(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    record_id: Identity | None = None
    client_ref: ClientReference | None = None

    @model_validator(mode="after")
    def one_identity(self):
        if (self.record_id is None) == (self.client_ref is None):
            raise ValueError("delivery_progress_reference_identity_required")
        return self


class DeliveryEvidenceQuery(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    board_id: Identity
    spec_id: Identity


class DeliveryEvidenceInput(BaseModel):
    """Spec-scoped exceptions; implementation and test writes belong to cards.

    Historical proof payloads are read from the ledger, never through this
    inbound contract. Reject obsolete writers instead of accepting proof that
    the card-ledger rollup cannot consume.
    """

    model_config = ConfigDict(extra="forbid", strict=True)
    expected_edition: int = Field(ge=1)
    expected_version: int = Field(ge=1)
    idempotency_key: Identity
    kind: Literal["waiver", "revoke"]
    obligation_refs: list[Identity] = Field(default_factory=list, max_length=1000)
    phase: Literal["implementation", "test"] | None = None
    record_id: Identity | None = None
    justification: str = Field(min_length=1, max_length=20000, pattern=r"\S")

    @model_validator(mode="before")
    @classmethod
    def reject_spec_scoped_proof(cls, value):
        if isinstance(value, dict) and value.get("kind") in ("implementation", "test"):
            raise PydanticCustomError(
                "delivery_card_scope_required",
                "Implementation and test evidence must use the card-scoped "
                "delivery-evidence endpoint with expected_card_version and "
                "expected_spec_edition, or okto_pulse_record_delivery_evidence "
                "with card_id. Read okto_pulse_get_delivery_evidence for current scope.",
            )
        return value

    @model_validator(mode="after")
    def closed_shape(self):
        if len(set(self.obligation_refs)) != len(self.obligation_refs):
            raise ValueError("delivery_duplicate_obligation")
        supplied = {
            name for name in ("phase", "record_id") if getattr(self, name) is not None
        }
        required = {
            "waiver": {"phase"},
            "revoke": {"record_id"},
        }[self.kind]
        if supplied != required:
            raise ValueError("delivery_command_fields_invalid")
        if bool(self.obligation_refs) != (self.kind != "revoke"):
            raise ValueError("delivery_obligation_refs_required")
        return self


class DeliveryEvidenceCommand(DeliveryEvidenceQuery, DeliveryEvidenceInput):
    def require_exception_kind(self) -> None:
        """Defend service/store callers that bypass Pydantic construction."""
        if self.kind not in ("waiver", "revoke"):
            raise ValueError(
                "delivery_card_scope_required: use the card-scoped delivery "
                "writer with expected_card_version and expected_spec_edition"
            )


class CardDeliveryEvidenceFields(BaseModel):
    """Card-scoped recording contract: the task owns its delivery bindings.

    Waivers are deliberately absent — they remain a human-only, spec-rollup
    level surface (BR: exceptions never move to the task ledger).
    """

    model_config = ConfigDict(extra="forbid", strict=True)
    kind: Literal["implementation", "test", "revoke", "progress"]
    obligation_refs: list[Identity] = Field(default_factory=list, max_length=1000)
    execution_id: Identity | None = None
    execution_submission: ImplementationTargetExecutionFields | None = None
    execution_client_ref: ClientReference | None = None
    progress_refs: list[DeliveryProgressReference] = Field(default_factory=list, max_length=100)
    scenario_id: Identity | None = None
    implementation_ids: list[Identity] = Field(default_factory=list, max_length=1000)
    record_id: Identity | None = None
    justification: str = Field(min_length=1, max_length=20000, pattern=r"\S")
    progress: DeliveryProgress | None = None

    @model_serializer(mode="wrap")
    def preserve_legacy_request_digest(self, handler):
        result = handler(self)
        if self.progress is None:
            result.pop("progress", None)
        if self.execution_submission is None:
            result.pop("execution_submission", None)
        if self.execution_client_ref is None:
            result.pop("execution_client_ref", None)
        if not self.progress_refs:
            result.pop("progress_refs", None)
        return result

    @model_validator(mode="after")
    def closed_shape(self):
        if len(set(self.obligation_refs)) != len(self.obligation_refs):
            raise ValueError("delivery_duplicate_obligation")
        if len(set(self.implementation_ids)) != len(self.implementation_ids):
            raise ValueError("delivery_duplicate_implementation")
        supplied = {
            name
            for name in ("execution_id", "execution_submission", "execution_client_ref", "scenario_id", "record_id")
            if getattr(self, name) is not None
        }
        required = {
            "implementation": {"execution_client_ref"} if self.execution_client_ref is not None else {"execution_submission"} if self.execution_submission is not None else {"execution_id"},
            "test": {"scenario_id"},
            "revoke": {"record_id"},
            "progress": set(),
        }[self.kind]
        if supplied != required:
            raise ValueError("delivery_command_fields_invalid")
        references = [(ref.record_id, ref.client_ref) for ref in self.progress_refs]
        if len(references) != len(set(references)):
            raise ValueError("delivery_progress_reference_duplicate")
        if self.progress_refs and self.kind == "revoke":
            raise ValueError("delivery_command_fields_invalid")
        if self.kind != "progress" and bool(self.obligation_refs) != (
            self.kind != "revoke"
        ):
            raise ValueError("delivery_obligation_refs_required")
        if bool(self.implementation_ids) != (self.kind == "test"):
            raise ValueError("delivery_test_implementation_binding_required")
        if (self.progress is not None) != (self.kind == "progress"):
            raise ValueError("delivery_progress_fields_invalid")
        if self.kind == "progress" and len(self.obligation_refs) > 200:
            raise ValueError("delivery_progress_payload_limit")
        if self.execution_submission is not None and (
            len(self.obligation_refs) + len(self.progress_refs) + 2 + bool(self.execution_submission.replacement_target_id) > 200
        ):
            raise ValueError("delivery_payload_limit")
        if self.progress_refs and (
            len(self.obligation_refs) + len(self.progress_refs) + len(self.implementation_ids)
            + len(self.progress.target_ids if self.progress else ()) > 200
        ):
            raise ValueError("delivery_payload_limit")
        if len(self.model_dump_json().encode("utf-8")) > 128 * 1024:
            raise ValueError("delivery_payload_limit")
        return self


class CardDeliveryEvidenceInput(CardDeliveryEvidenceFields):
    expected_card_version: int = Field(ge=1)
    expected_spec_edition: int = Field(ge=1)
    idempotency_key: Identity

    @model_validator(mode="after")
    def aliases_require_batch(self):
        if self.execution_client_ref is not None or any(ref.client_ref is not None for ref in self.progress_refs):
            raise ValueError("delivery_local_reference_requires_batch")
        return self


class CardDeliveryEvidenceCommand(DeliveryEvidenceQuery, CardDeliveryEvidenceInput):
    card_id: Identity


class CardDeliveryEvidenceEntry(CardDeliveryEvidenceFields):
    """One existing admission contract; exceptions are never batched."""

    client_ref: ClientReference
    kind: Literal["progress", "implementation", "test"]

    def resolved_fields(self, prior_results: dict[str, dict]) -> dict:
        """Resolve references only; aliases never create or authorize an object.

        The closed batch already checks kind and order. The adapter supplies
        identities from entries admitted in this exact transaction.
        """
        fields = self.model_dump(exclude={"client_ref", "execution_client_ref"})
        if self.execution_client_ref is not None:
            fields["execution_id"] = prior_results[self.execution_client_ref]["execution_id"]
        if self.progress_refs:
            fields["progress_refs"] = [
                {"record_id": ref.record_id or prior_results[ref.client_ref]["id"]}
                for ref in self.progress_refs
            ]
        return fields


class CardDeliveryEvidenceBatchInput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    contract_version: Literal["card-delivery-batch/v1"]
    expected_card_version: int = Field(ge=1)
    expected_spec_edition: int = Field(ge=1)
    expected_delivery_revision: int = Field(ge=0)
    idempotency_key: Identity
    entries: list[CardDeliveryEvidenceEntry] = Field(min_length=1, max_length=50)

    @model_validator(mode="after")
    def aggregate_limits(self):
        refs = [entry.client_ref for entry in self.entries]
        if len(refs) != len(set(refs)):
            raise ValueError("delivery_batch_client_ref_duplicate")
        prior = {}
        for entry in self.entries:
            if entry.execution_client_ref is not None and prior.get(entry.execution_client_ref) != "implementation":
                raise ValueError("delivery_execution_local_reference_invalid")
            if any(ref.client_ref is not None and prior.get(ref.client_ref) != "progress" for ref in entry.progress_refs):
                raise ValueError("delivery_progress_local_reference_invalid")
            prior[entry.client_ref] = entry.kind
        links = sum(
            len(entry.obligation_refs)
            + len(entry.progress_refs) + bool(entry.execution_client_ref)
            + len(entry.implementation_ids)
            + len(entry.progress.target_ids if entry.progress else ())
            + (2 + bool(entry.execution_submission.replacement_target_id) if entry.execution_submission else 0)
            for entry in self.entries
        )
        if links > 200 or len(self.model_dump_json().encode("utf-8")) > 128 * 1024:
            raise ValueError("delivery_batch_payload_limit")
        return self


class CardDeliveryEvidenceBatchCommand(
    DeliveryEvidenceQuery, CardDeliveryEvidenceBatchInput
):
    card_id: Identity


CardDeliveryEvidenceWriteInput = (
    CardDeliveryEvidenceInput | CardDeliveryEvidenceBatchInput
)
CardDeliveryEvidenceWriteCommand = (
    CardDeliveryEvidenceCommand | CardDeliveryEvidenceBatchCommand
)


def card_delivery_command(
    *, board_id, card_id, spec_id, evidence
) -> CardDeliveryEvidenceWriteCommand:
    """REST and MCP share the same closed envelope and legacy parsing."""
    batch = isinstance(evidence, CardDeliveryEvidenceBatchInput) or (
        isinstance(evidence, dict) and "entries" in evidence
    )
    input_model = CardDeliveryEvidenceBatchInput if batch else CardDeliveryEvidenceInput
    command_model = (
        CardDeliveryEvidenceBatchCommand if batch else CardDeliveryEvidenceCommand
    )
    body = input_model.model_validate(evidence)
    return command_model(
        board_id=board_id, card_id=card_id, spec_id=spec_id, **body.model_dump()
    )


class DeliveryBatchEntryError(ValueError):
    """Only caller-owned identity and a bounded domain code cross transports."""

    def __init__(self, entry_index: int, client_ref: str, cause_code: str):
        self.entry_index = entry_index
        self.client_ref = client_ref
        self.cause_code = (
            cause_code
            if (
                cause_code.startswith("delivery_")
                and len(cause_code) < 100
                and all(char.isalnum() or char == "_" for char in cause_code)
            )
            else "delivery_entry_invalid"
        )
        super().__init__(
            f"delivery_batch_entry_invalid: entry {entry_index} ({client_ref}): {self.cause_code}"
        )

    def details(self):
        return {
            "entry_index": self.entry_index,
            "client_ref": self.client_ref,
            "cause_code": self.cause_code,
        }
