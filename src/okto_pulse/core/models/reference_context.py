"""Read-only, bounded diagnostics for the Card's current scenario references."""
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class ScenarioReferenceFindingView(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True)
    finding_id: str
    source_selector: str
    target_ref: str | None
    reason_code: Literal['parent_absent', 'target_absent', 'target_ambiguous']
    correction_surface: Literal['card_scenario_links', 'spec_test_scenarios']


class CardScenarioReferenceContext(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True)
    contract_version: Literal['card-scenario-reference-context/v1'] = 'card-scenario-reference-context/v1'
    status: Literal['available', 'not_authorized', 'unavailable']
    source_fingerprint: str | None = None
    finding_count: int | None = Field(default=None, ge=0)
    findings: list[ScenarioReferenceFindingView] = Field(default_factory=list, max_length=20)
    truncated: bool = False

    @model_validator(mode='after')
    def validate_observation(self):
        if self.status != 'available':
            if self.source_fingerprint is not None or self.finding_count is not None or self.findings or self.truncated:
                raise ValueError('unobserved_reference_context_has_no_findings_or_count')
        elif (self.source_fingerprint is None or len(self.source_fingerprint) != 64
              or self.finding_count is None or self.finding_count < len(self.findings)
              or self.truncated != (self.finding_count > len(self.findings))):
            raise ValueError('reference_context_observation_invalid')
        return self
