"""External observations for specialized verification; never self-approval.

Author/scope/current scenario binding are supplied by the authorized use case,
not this input. Parsing a report does not authenticate it or grant test credit.
"""

from datetime import datetime
import json
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, TypeAdapter, field_validator, model_validator


Reference = Annotated[str, StringConstraints(strict=True, min_length=1, max_length=2048, pattern=r'\S')]
Text = Annotated[str, StringConstraints(strict=True, min_length=1, max_length=8192, pattern=r'\S')]
Digest = Annotated[str, StringConstraints(strict=True, pattern=r'^[0-9a-f]{64}$')]
VerificationOutcome = Literal['passed', 'failed', 'inconclusive', 'aborted', 'unavailable']


class VersionedVerificationObject(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True)
    reference: Reference
    revision: Reference
    sha256: Digest


class VerificationObservation(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True)
    observation_id: Reference
    criterion_id: Reference
    observation_ref: Reference
    expected: Text
    observed: Text
    outcome: VerificationOutcome


class _Report(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True)
    schema_version: Literal['verification-report/v1']
    report_id: Reference
    observed_at: datetime
    sources: tuple[VersionedVerificationObject, ...] = Field(min_length=1, max_length=20)
    observations: tuple[VerificationObservation, ...] = Field(min_length=1, max_length=100)
    conclusion: Text
    result: VerificationOutcome

    @field_validator('observed_at', mode='before')
    @classmethod
    def timestamp_shape(cls, value):
        if type(value) not in {str, datetime}:
            raise ValueError('verification_report_timestamp_required')
        return value

    @field_validator('observed_at')
    @classmethod
    def timezone_required(cls, value):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError('verification_report_timezone_required')
        return value

    @model_validator(mode='after')
    def complete_observation(self):
        if len({item.observation_id for item in self.observations}) != len(self.observations):
            raise ValueError('verification_report_duplicate_observation')
        if len({item.reference for item in self.sources}) != len(self.sources):
            raise ValueError('verification_report_duplicate_source')
        failed = any(item.outcome == 'failed' for item in self.observations)
        outcomes = {item.outcome for item in self.observations}
        if ((self.result == 'failed') != failed
                or (self.result == 'passed' and outcomes != {'passed'})
                or (self.result not in {'passed', 'failed'} and self.result not in outcomes)):
            raise ValueError('verification_report_result_mismatch')
        if len(json.dumps(self.model_dump(mode='json'), ensure_ascii=False, allow_nan=False).encode('utf-8')) > 64 * 1024:
            raise ValueError('verification_report_aggregate_limit')
        return self


class VerificationFinding(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True)
    finding_id: Reference
    rule_id: Reference
    location: Reference
    description: Text
    severity: Literal['info', 'warning', 'error']


class StaticAnalysisReport(_Report):
    method: Literal['static_analysis']
    tool_name: Reference
    tool_version: Reference
    rules: tuple[VersionedVerificationObject, ...] = Field(min_length=1, max_length=100)
    configuration: VersionedVerificationObject
    analyzed_scope: tuple[Reference, ...] = Field(min_length=1, max_length=100)
    findings: tuple[VerificationFinding, ...] = Field(max_length=100)

    @model_validator(mode='after')
    def finding_rules(self):
        rules = {item.reference for item in self.rules}
        if (len(rules) != len(self.rules) or len(set(self.analyzed_scope)) != len(self.analyzed_scope)
                or len({item.finding_id for item in self.findings}) != len(self.findings)
                or any(item.rule_id not in rules for item in self.findings)):
            raise ValueError('verification_report_rule_scope_invalid')
        return self


class InspectionReport(_Report):
    method: Literal['inspection']
    inspection_procedure: VersionedVerificationObject


class DemonstrationReport(_Report):
    method: Literal['demonstration']
    procedure: VersionedVerificationObject
    environment: VersionedVerificationObject


VerificationReport = Annotated[StaticAnalysisReport | InspectionReport | DemonstrationReport, Field(discriminator='method')]
_ADAPTER = TypeAdapter(VerificationReport)


def parse_verification_report(value) -> StaticAnalysisReport | InspectionReport | DemonstrationReport:
    return _ADAPTER.validate_python(value)


def verification_report_scenario_status(report) -> Literal['ready', 'passed', 'failed']:
    """Keep factual outcomes in the report; incomplete attempts await a retry.

    This maps into the existing lifecycle and grants no transition authority.
    In particular an incomplete attempt is neither a failed assertion nor proof.
    """
    if not isinstance(report, (StaticAnalysisReport, InspectionReport, DemonstrationReport)):
        raise TypeError('verification_report_required')
    return report.result if report.result in {'passed', 'failed'} else 'ready'


def verification_report_passing_criteria(report) -> tuple[str, ...]:
    """Project observed criteria, not trust: callers must authenticate the report.

    Every observation of a criterion must pass. A failed or incomplete sibling
    observation cannot disappear through selection or duplicate criterion IDs.
    """
    verification_report_scenario_status(report)  # Require the closed report model.
    outcomes = {}
    for item in report.observations:
        outcomes.setdefault(item.criterion_id, set()).add(item.outcome)
    return tuple(sorted(key for key, values in outcomes.items() if values == {'passed'}))


def require_verification_report_context(report, *, method, status, criterion_ids):
    """Bind the external observation to the entire current scenario's criteria.

    The caller resolves scope, authorization, scenario freshness and applicable
    independence before issuing any authenticated receipt. No report can grant
    authority through extra author/approval fields or replace a method.
    """
    if not isinstance(report, (StaticAnalysisReport, InspectionReport, DemonstrationReport)):
        raise TypeError('verification_report_required')
    if report.method != method or verification_report_scenario_status(report) != status:
        raise ValueError('verification_report_method_or_result_mismatch')
    if (type(criterion_ids) is not tuple or not criterion_ids
            or any(type(key) is not str or not key for key in criterion_ids)
            or len(set(criterion_ids)) != len(criterion_ids)
            or {item.criterion_id for item in report.observations} != set(criterion_ids)):
        raise ValueError('verification_report_criterion_scope_mismatch')


class VerificationReportEvidence(BaseModel):
    """Authenticated-submission envelope, not independent approval or execution.

    The concrete verifier must authenticate the receipt. This model only checks
    shape; historical evidence with other classes retains its original meaning.
    """
    model_config = ConfigDict(extra='forbid', frozen=True)
    evidence_class: Literal['verification_report']
    verification_report: VerificationReport
    report_author_id: Reference
    scenario_sha256: Annotated[str, StringConstraints(strict=True, pattern=r'^sha256:[0-9a-f]{64}$')]
    execution_receipt: Annotated[str, StringConstraints(strict=True, pattern=r'^ev2r\.[0-9a-f]{32}\.[0-9a-f]{64}$')]


def require_evidence_method_binding(method: object, evidence: object) -> None:
    """No replay receipt may masquerade as a specialized observation, or vice versa."""
    report = evidence.get('verification_report') if isinstance(evidence, dict) else None
    report_class = isinstance(evidence, dict) and evidence.get('evidence_class') == 'verification_report'
    if method in {'static_analysis', 'inspection', 'demonstration'}:
        if not report_class or not isinstance(report, dict) or report.get('method') != method:
            raise ValueError('verification_report_method_mismatch')
    elif report_class or report is not None:
        raise ValueError('verification_report_explicit_method_required')
