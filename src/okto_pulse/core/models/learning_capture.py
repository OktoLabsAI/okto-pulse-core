"""Closed transport input for authored Learning creation."""

from pydantic import BaseModel, ConfigDict, Field
from okto_pulse.core.ports.learning_capture import CreateLearningCapture


class LearningCaptureCreateRequest(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True)
    board_id: str = Field(min_length=1, max_length=4096)
    capture_id: str = Field(min_length=1, max_length=4096)
    expected_source_digest: str = Field(pattern=r'^[0-9a-f]{64}$')
    expected_source_version: int = Field(ge=1)
    content: str = Field(min_length=1, max_length=65536)
    context: str = Field(min_length=1, max_length=65536)
    applicability: str = Field(min_length=1, max_length=65536)
    scenario_ids: list[str] = Field(min_length=1, max_length=128)

    def command(self, bug_id: str) -> CreateLearningCapture:
        return CreateLearningCapture(bug_id=bug_id,
            **{**self.model_dump(), 'scenario_ids': tuple(self.scenario_ids)})
