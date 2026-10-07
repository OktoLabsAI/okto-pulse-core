"""Explicit public Learning intent shared by standalone and compound authorship.

Replacement requires explicit source_bug scope in the current contract.
Incompatible requests and stored captures are refused without conversion.
"""
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from okto_pulse.core.ports.learning_capture import LearningCaptureIntent


class LearningCreateIntent(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True, frozen=True)
    kind: Literal['create']

    def command(self) -> LearningCaptureIntent:
        return LearningCaptureIntent()


class LearningTargetIntent(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True, frozen=True)
    target_node_id: str = Field(min_length=1, max_length=4096)
    target_generation: int = Field(ge=0)
    expected_fingerprint: str = Field(pattern=r'^[0-9a-f]{64}$')
    reason: str = Field(min_length=1, max_length=16384)

    @field_validator('target_node_id', 'reason')
    @classmethod
    def meaningful_text(cls, value):
        if not value.strip():
            raise ValueError('learning_capture_intent_invalid')
        return value

    def command(self) -> LearningCaptureIntent:
        return LearningCaptureIntent(**self.model_dump())


class LearningReuseIntent(LearningTargetIntent):
    kind: Literal['reuse']


class LearningSupersedeIntent(LearningTargetIntent):
    kind: Literal['supersede']
    scope: Literal['source_bug']


LearningIntentRequest = Annotated[
    LearningCreateIntent | LearningReuseIntent | LearningSupersedeIntent,
    Field(discriminator='kind'),
]
