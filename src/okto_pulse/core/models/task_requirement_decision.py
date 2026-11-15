"""Closed transports; actor, time, scopes and audit IDs are server-owned."""
from typing import Literal
from pydantic import BaseModel, ConfigDict, Field, field_validator
from okto_pulse.core.domain.task_requirement_decision import normalize_user_note


class TaskRequirementOptionsQuery(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True)
    board_id: str = Field(min_length=1, max_length=512)
    spec_id: str = Field(min_length=1, max_length=512)
    card_ids: list[str] = Field(min_length=1, max_length=50)

    @field_validator('board_id', 'spec_id')
    @classmethod
    def nonblank(cls, value):
        if not value.strip():
            raise ValueError('task_requirement_scope_invalid')
        return value

    @field_validator('card_ids')
    @classmethod
    def unique_cards(cls, value):
        if any(not item.strip() or len(item) > 512 for item in value) or len(set(value)) != len(value):
            raise ValueError('task_requirement_selection_invalid')
        return value


class TaskRequirementDecisionInput(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True)
    contract_version: Literal['task-requirement-decision/v1']
    card_ids: list[str] = Field(min_length=1, max_length=50)
    decision: Literal['waived', 'optional', 'required']
    basis: str = Field(pattern=r'^[0-9a-f]{64}$')
    note: str | None = Field(default=None, max_length=20000)
    idempotency_key: str = Field(min_length=1, max_length=512, pattern=r'\S')

    @field_validator('card_ids')
    @classmethod
    def unique_cards(cls, value):
        return TaskRequirementOptionsQuery.unique_cards(value)

    @field_validator('note')
    @classmethod
    def normalize_note(cls, value):
        return normalize_user_note(value)


class TaskRequirementDecisionCommand(TaskRequirementDecisionInput):
    board_id: str = Field(min_length=1, max_length=512, pattern=r'\S')
    spec_id: str = Field(min_length=1, max_length=512, pattern=r'\S')


class TaskRequirementHistoryQuery(TaskRequirementOptionsQuery):
    card_ids: list[str] = Field(min_length=1, max_length=1)
    edition: int | None = Field(default=None, ge=1)
    before_revision: int | None = Field(default=None, ge=1)
    limit: int = Field(default=10, ge=1, le=20)
    record_id: str | None = Field(default=None, min_length=1, max_length=512)
