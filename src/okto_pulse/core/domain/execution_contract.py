"""Required Spec execution contract; independent of adopted Architecture Designs."""

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


def execution_contract(spec: object) -> SpecExecutionContract:
    raw = getattr(spec, "execution_contract", None)
    if raw is None:
        raise ValueError("spec_execution_contract_required")
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
