"""Explicit Spec contract selection; independent of adopted Architecture Designs."""

from typing import Literal
from pydantic import BaseModel, ConfigDict, Field


class SpecExecutionContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    contract_version: Literal["spec-execution-contract/v1"] = (
        "spec-execution-contract/v1"
    )
    board_id: str = Field(min_length=1)
    spec_id: str = Field(min_length=1)
    adopted_in_edition: int = Field(ge=1)
    actor_id: str = Field(min_length=1)
    origin: Literal["new_spec", "explicit_revision"]


class SpecExecutionContractAdoption(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    contract_version: Literal["spec-execution-contract/v1"] = (
        "spec-execution-contract/v1"
    )
    expected_spec_version: int = Field(ge=1)
    expected_spec_edition: int = Field(ge=1)


def execution_contract(spec: object) -> SpecExecutionContract | None:
    raw = getattr(spec, "execution_contract", None)
    if raw is None:
        return None
    contract = SpecExecutionContract.model_validate(raw)
    if (
        contract.board_id != spec.board_id
        or contract.spec_id != spec.id
        or type(spec.edition) is not int
        or contract.adopted_in_edition > spec.edition
    ):
        raise ValueError("spec_execution_contract_scope_mismatch")
    return contract


def new_execution_contract(
    *, board_id: str, spec_id: str, edition: int, actor_id: str, origin: str
) -> dict:
    return SpecExecutionContract(
        board_id=board_id,
        spec_id=spec_id,
        adopted_in_edition=edition,
        actor_id=actor_id,
        origin=origin,
    ).model_dump(mode="json")


def adopt_execution_contract(
    spec: object, request: SpecExecutionContractAdoption, *, actor_id: str
) -> dict | None:
    """Caller must hold the write fence and the existing Draft mutation authority."""
    if (spec.version, spec.edition) != (
        request.expected_spec_version,
        request.expected_spec_edition,
    ):
        raise ValueError("spec_execution_contract_version_conflict")
    if execution_contract(spec) is not None:
        return None  # Exact current adoption is a no-op; no new provenance.
    return new_execution_contract(
        board_id=spec.board_id,
        spec_id=spec.id,
        edition=spec.edition,
        actor_id=actor_id,
        origin="explicit_revision",
    )
