"""Canonical factual coverage and Code Evidence Matrix projection contract."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Protocol, runtime_checkable

from okto_pulse.core.domain.code_traceability import (
    CodeEvidenceType,
    CodeEvidenceSpecRelationType,
    CodeTraceabilityLifecycleStatus,
    CodeTraceabilityWaiverEntityType,
    CodeTraceabilityWaiverReason,
    CodeTraceabilityWaiverScope,
    ImplementationTargetExecutionDisposition,
    ImplementationTargetResolutionState,
    TargetOverlapDisposition,
    TargetOverlapSeverity,
    normalize_code_source_ref,
)
from okto_pulse.core.ports.analytics_foundation import (
    ANALYTICS_FOUNDATION_CONTRACT_VERSION,
    AnalyticsExclusionSummary,
    AnalyticsFilterClause,
    AnalyticsFoundationQuery,
    AnalyticsPopulationScope,
    require_utc_datetime,
)


COVERAGE_TRACEABILITY_CONTRACT_VERSION = "1"
MAX_COVERAGE_OBLIGATION_ROWS = 20_000
MAX_COVERAGE_EVIDENCE_ROWS = 100_000
MAX_CODE_EVIDENCE_MATRIX_ROWS = 100_000

_IDENTIFIER = re.compile(r"^[^\x00-\x1f\x7f]{1,255}$")
_REASON = re.compile(r"^[a-z][a-z0-9_.-]{0,127}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


def _identifier(value: str | None, *, field: str) -> str:
    if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"coverage_traceability_{field}_invalid")
    return value


def _optional_identifier(value: str | None, *, field: str) -> str | None:
    if value is None:
        return None
    return _identifier(value, field=field)


def _reason(value: str | None, *, field: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not _REASON.fullmatch(value):
        raise ValueError(f"coverage_traceability_{field}_invalid")
    return value


def _positive_int(value: int, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"coverage_traceability_{field}_invalid")
    return value


def _utc_text(value: datetime) -> str:
    return (
        require_utc_datetime(value, field="coverage_traceability_timestamp")
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


class CoverageObligationType(str, Enum):
    ACCEPTANCE_CRITERION = "ac"
    FUNCTIONAL_REQUIREMENT = "fr"
    TEST_SCENARIO = "test_scenario"
    BUSINESS_RULE = "business_rule"
    API_CONTRACT = "api_contract"
    TECHNICAL_REQUIREMENT = "technical_requirement"
    DECISION = "decision"
    INTEGRATION_REQUIREMENT = "integration_requirement"
    OBSERVABILITY_REQUIREMENT = "observability_requirement"


class CoverageCurrentness(str, Enum):
    CURRENT = "current"
    PREVIOUS = "previous"
    STALE = "stale"


class CoverageAuthorityState(str, Enum):
    AVAILABLE = "available"
    RESTRICTED = "restricted"
    UNAVAILABLE = "unavailable"
    INCONSISTENT = "inconsistent"


class CoverageFactState(str, Enum):
    COVERED = "covered"
    UNCOVERED = "uncovered"
    NOT_APPLICABLE = "not_applicable"
    RESTRICTED = "restricted"
    UNAVAILABLE = "unavailable"
    INCONSISTENT = "inconsistent"


class CoverageAggregateState(str, Enum):
    AVAILABLE = "available"
    NOT_APPLICABLE = "not_applicable"
    RESTRICTED = "restricted"
    UNAVAILABLE = "unavailable"
    INCONSISTENT = "inconsistent"


class CoverageSkipState(str, Enum):
    NOT_SKIPPED = "not_skipped"
    SKIPPED = "skipped"
    RESTRICTED = "restricted"
    UNAVAILABLE = "unavailable"


class CoverageDeliveryState(str, Enum):
    ACTIVE = "active"
    CANCELLED = "cancelled"
    ARCHIVED = "archived"


class CoverageEvidenceEligibility(str, Enum):
    ELIGIBLE = "eligible"
    INELIGIBLE_NON_SATISFYING_RELATION = "ineligible_non_satisfying_relation"
    INELIGIBLE_CANCELLED_OR_ARCHIVED = "ineligible_cancelled_or_archived"
    INELIGIBLE_REVOKED_OR_SUPERSEDED = "ineligible_revoked_or_superseded"
    INELIGIBLE_PRIOR_EDITION = "ineligible_prior_edition"


class CodeEvidenceMatrixState(str, Enum):
    AVAILABLE = "available"
    NOT_APPLICABLE = "not_applicable"
    RESTRICTED = "restricted"
    UNAVAILABLE = "unavailable"
    INCONSISTENT = "inconsistent"


@dataclass(frozen=True, slots=True)
class CoverageObligationIdentity:
    spec_id: str
    obligation_type: CoverageObligationType
    obligation_id: str
    edition: int
    currentness: CoverageCurrentness

    def __post_init__(self) -> None:
        object.__setattr__(self, "spec_id", _identifier(self.spec_id, field="spec_id"))
        object.__setattr__(
            self,
            "obligation_id",
            _identifier(self.obligation_id, field="obligation_id"),
        )
        if not isinstance(self.obligation_type, CoverageObligationType):
            raise ValueError("coverage_traceability_obligation_type_invalid")
        object.__setattr__(
            self, "edition", _positive_int(self.edition, field="edition")
        )
        if not isinstance(self.currentness, CoverageCurrentness):
            raise ValueError("coverage_traceability_currentness_invalid")

    def canonical_dict(self) -> dict[str, object]:
        return {
            "spec_id": self.spec_id,
            "obligation_type": self.obligation_type.value,
            "obligation_id": self.obligation_id,
            "edition": self.edition,
            "currentness": self.currentness.value,
        }


@dataclass(frozen=True, slots=True)
class CoverageSkipMetadata:
    state: CoverageSkipState = CoverageSkipState.NOT_SKIPPED
    authority_ref: str | None = None
    reason_code: str | None = None
    currentness: CoverageCurrentness | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.state, CoverageSkipState):
            raise ValueError("coverage_traceability_skip_state_invalid")
        if self.state is CoverageSkipState.SKIPPED:
            object.__setattr__(
                self,
                "authority_ref",
                _identifier(self.authority_ref, field="skip_authority_ref"),
            )
            object.__setattr__(
                self,
                "reason_code",
                _reason(self.reason_code, field="skip_reason_code"),
            )
            if self.currentness is not CoverageCurrentness.CURRENT:
                raise ValueError("coverage_traceability_effective_skip_not_current")
        elif any(
            item is not None
            for item in (self.authority_ref, self.reason_code, self.currentness)
        ):
            raise ValueError(
                "coverage_traceability_non_effective_skip_details_forbidden"
            )

    @property
    def effective(self) -> bool:
        return self.state is CoverageSkipState.SKIPPED

    def canonical_dict(self) -> dict[str, object]:
        return {
            "state": self.state.value,
            "effective": self.effective,
            "authority_ref": self.authority_ref,
            "reason_code": self.reason_code,
            "currentness": self.currentness.value if self.currentness else None,
        }


@dataclass(frozen=True, slots=True)
class CoverageObligationFact:
    identity: CoverageObligationIdentity
    applicable: bool
    authority_state: CoverageAuthorityState
    authority_ref: str | None
    authority_reason: str | None = None
    skip: CoverageSkipMetadata = CoverageSkipMetadata()

    def __post_init__(self) -> None:
        if not isinstance(self.identity, CoverageObligationIdentity):
            raise ValueError("coverage_traceability_obligation_identity_invalid")
        if not isinstance(self.applicable, bool):
            raise ValueError("coverage_traceability_applicable_invalid")
        if not isinstance(self.authority_state, CoverageAuthorityState):
            raise ValueError("coverage_traceability_authority_state_invalid")
        if not isinstance(self.skip, CoverageSkipMetadata):
            raise ValueError("coverage_traceability_skip_invalid")
        if not self.applicable and self.skip.effective:
            raise ValueError("coverage_traceability_non_applicable_skip_forbidden")
        if self.authority_state is CoverageAuthorityState.AVAILABLE:
            object.__setattr__(
                self,
                "authority_ref",
                _identifier(self.authority_ref, field="obligation_authority_ref"),
            )
            if self.authority_reason is not None:
                raise ValueError("coverage_traceability_available_reason_forbidden")
        else:
            if self.authority_ref is not None:
                raise ValueError(
                    "coverage_traceability_non_available_authority_forbidden"
                )
            object.__setattr__(
                self,
                "authority_reason",
                _reason(self.authority_reason, field="obligation_authority_reason"),
            )
            if self.authority_reason is None:
                raise ValueError("coverage_traceability_authority_reason_required")
            if self.skip.effective:
                raise ValueError("coverage_traceability_unavailable_skip_forbidden")


@dataclass(frozen=True, slots=True)
class CoverageEvidenceFact:
    evidence_id: str
    evidence_type: CodeEvidenceType
    source_ref: str
    obligation: CoverageObligationIdentity
    relation_type: CodeEvidenceSpecRelationType
    evidence_content_sha256: str
    parent_card_id: str
    delivery_state: CoverageDeliveryState
    lifecycle_status: CodeTraceabilityLifecycleStatus
    currentness: CoverageCurrentness
    currentness_reason: str | None
    authority_ref: str

    def __post_init__(self) -> None:
        for name in ("evidence_id", "source_ref", "parent_card_id", "authority_ref"):
            object.__setattr__(self, name, _identifier(getattr(self, name), field=name))
        object.__setattr__(
            self, "source_ref", normalize_code_source_ref(self.source_ref)
        )
        if not isinstance(self.obligation, CoverageObligationIdentity):
            raise ValueError("coverage_traceability_evidence_obligation_invalid")
        if not isinstance(self.evidence_type, CodeEvidenceType):
            raise ValueError("coverage_traceability_evidence_type_invalid")
        if not isinstance(self.relation_type, CodeEvidenceSpecRelationType):
            raise ValueError("coverage_traceability_evidence_relation_invalid")
        if not isinstance(self.evidence_content_sha256, str) or not _SHA256.fullmatch(
            self.evidence_content_sha256
        ):
            raise ValueError("coverage_traceability_evidence_digest_invalid")
        if not isinstance(self.delivery_state, CoverageDeliveryState):
            raise ValueError("coverage_traceability_delivery_state_invalid")
        if not isinstance(self.lifecycle_status, CodeTraceabilityLifecycleStatus):
            raise ValueError("coverage_traceability_evidence_lifecycle_invalid")
        if not isinstance(self.currentness, CoverageCurrentness):
            raise ValueError("coverage_traceability_evidence_currentness_invalid")
        object.__setattr__(
            self,
            "currentness_reason",
            _reason(self.currentness_reason, field="evidence_currentness_reason"),
        )
        if (self.currentness is CoverageCurrentness.CURRENT) == (
            self.currentness_reason is not None
        ):
            raise ValueError(
                "coverage_traceability_evidence_currentness_reason_invalid"
            )


@dataclass(frozen=True, slots=True)
class CoverageEvidenceRow:
    fact: CoverageEvidenceFact
    eligibility: CoverageEvidenceEligibility

    def __post_init__(self) -> None:
        if not isinstance(self.fact, CoverageEvidenceFact):
            raise ValueError("coverage_traceability_evidence_fact_invalid")
        if not isinstance(self.eligibility, CoverageEvidenceEligibility):
            raise ValueError("coverage_traceability_evidence_eligibility_invalid")

    def canonical_dict(self) -> dict[str, object]:
        fact = self.fact
        return {
            "evidence_id": fact.evidence_id,
            "evidence_type": fact.evidence_type.value,
            "source_ref": fact.source_ref,
            "obligation": fact.obligation.canonical_dict(),
            "relation_type": fact.relation_type.value,
            "evidence_content_sha256": fact.evidence_content_sha256,
            "parent_card_id": fact.parent_card_id,
            "delivery_state": fact.delivery_state.value,
            "lifecycle_status": fact.lifecycle_status.value,
            "currentness": fact.currentness.value,
            "currentness_reason": fact.currentness_reason,
            "authority_ref": fact.authority_ref,
            "eligibility": self.eligibility.value,
        }


@dataclass(frozen=True, slots=True)
class CoverageObligationRow:
    identity: CoverageObligationIdentity
    state: CoverageFactState
    applicable: bool
    covered: bool | None
    skip: CoverageSkipMetadata
    evidence: tuple[CoverageEvidenceRow, ...]
    authority_ref: str | None = None
    reason: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.identity, CoverageObligationIdentity):
            raise ValueError("coverage_traceability_row_identity_invalid")
        if not isinstance(self.state, CoverageFactState):
            raise ValueError("coverage_traceability_row_state_invalid")
        if not isinstance(self.applicable, bool):
            raise ValueError("coverage_traceability_row_applicable_invalid")
        if not isinstance(self.skip, CoverageSkipMetadata):
            raise ValueError("coverage_traceability_row_skip_invalid")
        if (
            not isinstance(self.evidence, tuple)
            or len(self.evidence) > MAX_COVERAGE_EVIDENCE_ROWS
            or any(not isinstance(item, CoverageEvidenceRow) for item in self.evidence)
        ):
            raise ValueError("coverage_traceability_row_evidence_invalid")
        if len({item.fact.evidence_id for item in self.evidence}) != len(self.evidence):
            raise ValueError("coverage_traceability_row_evidence_duplicate")
        if self.evidence != tuple(
            sorted(
                self.evidence,
                key=lambda item: (item.fact.evidence_id, item.fact.authority_ref),
            )
        ):
            raise ValueError("coverage_traceability_row_evidence_order_invalid")
        if any(item.fact.obligation != self.identity for item in self.evidence):
            raise ValueError("coverage_traceability_row_evidence_target_mismatch")

        eligible = tuple(
            item
            for item in self.evidence
            if item.eligibility is CoverageEvidenceEligibility.ELIGIBLE
        )
        if self.state in {CoverageFactState.COVERED, CoverageFactState.UNCOVERED}:
            if (
                not self.applicable
                or self.identity.currentness is not CoverageCurrentness.CURRENT
            ):
                raise ValueError("coverage_traceability_factual_row_not_current")
            if self.covered is not (self.state is CoverageFactState.COVERED):
                raise ValueError("coverage_traceability_covered_state_mismatch")
            if bool(eligible) is not self.covered:
                raise ValueError("coverage_traceability_evidence_coverage_mismatch")
            object.__setattr__(
                self,
                "authority_ref",
                _identifier(self.authority_ref, field="row_authority_ref"),
            )
            if self.reason is not None:
                raise ValueError("coverage_traceability_available_row_reason_forbidden")
        elif self.state is CoverageFactState.NOT_APPLICABLE:
            if self.applicable or self.covered is not None or eligible:
                raise ValueError("coverage_traceability_not_applicable_row_invalid")
            object.__setattr__(
                self,
                "authority_ref",
                _identifier(self.authority_ref, field="row_authority_ref"),
            )
            object.__setattr__(self, "reason", _reason(self.reason, field="row_reason"))
            if self.reason is None:
                raise ValueError("coverage_traceability_row_reason_required")
        else:
            if (
                self.covered is not None
                or self.authority_ref is not None
                or self.evidence
            ):
                raise ValueError("coverage_traceability_non_available_row_leak")
            object.__setattr__(self, "reason", _reason(self.reason, field="row_reason"))
            if self.reason is None:
                raise ValueError("coverage_traceability_row_reason_required")

    def canonical_dict(self) -> dict[str, object]:
        return {
            "identity": self.identity.canonical_dict(),
            "state": self.state.value,
            "applicable": self.applicable,
            "covered": self.covered,
            "skip": self.skip.canonical_dict(),
            "authority_ref": self.authority_ref,
            "reason": self.reason,
            "evidence": [item.canonical_dict() for item in self.evidence],
        }


@dataclass(frozen=True, slots=True)
class CoverageCounts:
    state: CoverageAggregateState
    applicable: int | None
    covered: int | None
    uncovered: int | None
    skipped: int | None
    value: float | None
    n: int | None
    reason: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.state, CoverageAggregateState):
            raise ValueError("coverage_traceability_counts_state_invalid")
        numeric = (self.applicable, self.covered, self.uncovered, self.skipped, self.n)
        if self.state is CoverageAggregateState.AVAILABLE:
            if any(
                isinstance(item, bool) or not isinstance(item, int) for item in numeric
            ):
                raise ValueError("coverage_traceability_available_counts_required")
            assert self.applicable is not None
            assert self.covered is not None
            assert self.uncovered is not None
            assert self.skipped is not None
            if (
                self.applicable < 1
                or self.covered < 0
                or self.uncovered < 0
                or self.covered + self.uncovered != self.applicable
                or not 0 <= self.skipped <= self.applicable
                or self.n != self.applicable
            ):
                raise ValueError("coverage_traceability_available_counts_invalid")
            expected = round((self.covered / self.applicable) * 100, 6)
            if self.value != expected or self.reason is not None:
                raise ValueError("coverage_traceability_available_value_invalid")
        elif self.state is CoverageAggregateState.NOT_APPLICABLE:
            if numeric != (0, 0, 0, 0, 0) or self.value is not None:
                raise ValueError("coverage_traceability_not_applicable_counts_invalid")
            object.__setattr__(
                self, "reason", _reason(self.reason, field="counts_reason")
            )
            if self.reason is None:
                raise ValueError("coverage_traceability_counts_reason_required")
        else:
            if any(item is not None for item in numeric) or self.value is not None:
                raise ValueError("coverage_traceability_non_numeric_counts_invalid")
            object.__setattr__(
                self, "reason", _reason(self.reason, field="counts_reason")
            )
            if self.reason is None:
                raise ValueError("coverage_traceability_counts_reason_required")

    def canonical_dict(self) -> dict[str, object]:
        return {
            "state": self.state.value,
            "applicable": self.applicable,
            "covered": self.covered,
            "uncovered": self.uncovered,
            "skipped": self.skipped,
            "value": self.value,
            "n": self.n,
            "reason": self.reason,
        }


def _counts_for_rows(rows: tuple[CoverageObligationRow, ...]) -> CoverageCounts:
    precedence = (
        (CoverageFactState.INCONSISTENT, CoverageAggregateState.INCONSISTENT),
        (CoverageFactState.RESTRICTED, CoverageAggregateState.RESTRICTED),
        (CoverageFactState.UNAVAILABLE, CoverageAggregateState.UNAVAILABLE),
    )
    for fact_state, aggregate_state in precedence:
        if any(item.state is fact_state for item in rows):
            return CoverageCounts(
                state=aggregate_state,
                applicable=None,
                covered=None,
                uncovered=None,
                skipped=None,
                value=None,
                n=None,
                reason=f"coverage_{aggregate_state.value}",
            )
    applicable_rows = tuple(item for item in rows if item.applicable)
    if not applicable_rows:
        return CoverageCounts(
            state=CoverageAggregateState.NOT_APPLICABLE,
            applicable=0,
            covered=0,
            uncovered=0,
            skipped=0,
            value=None,
            n=0,
            reason="zero_applicable_obligations",
        )
    covered = sum(item.covered is True for item in applicable_rows)
    applicable = len(applicable_rows)
    return CoverageCounts(
        state=CoverageAggregateState.AVAILABLE,
        applicable=applicable,
        covered=covered,
        uncovered=applicable - covered,
        skipped=sum(item.skip.effective for item in applicable_rows),
        value=round((covered / applicable) * 100, 6),
        n=applicable,
    )


@dataclass(frozen=True, slots=True)
class CoverageTypeProjection:
    obligation_type: CoverageObligationType
    counts: CoverageCounts
    rows: tuple[CoverageObligationRow, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.obligation_type, CoverageObligationType):
            raise ValueError("coverage_traceability_type_projection_invalid")
        if not isinstance(self.counts, CoverageCounts):
            raise ValueError("coverage_traceability_type_counts_invalid")
        if not isinstance(self.rows, tuple) or any(
            not isinstance(item, CoverageObligationRow) for item in self.rows
        ):
            raise ValueError("coverage_traceability_type_rows_invalid")
        if any(
            item.identity.obligation_type is not self.obligation_type
            for item in self.rows
        ):
            raise ValueError("coverage_traceability_type_row_mismatch")
        if self.rows != tuple(
            sorted(
                self.rows,
                key=lambda item: (
                    item.identity.spec_id,
                    item.identity.edition,
                    item.identity.obligation_id,
                ),
            )
        ):
            raise ValueError("coverage_traceability_type_row_order_invalid")
        if self.counts != _counts_for_rows(self.rows):
            raise ValueError("coverage_traceability_type_counts_mismatch")

    def canonical_dict(self) -> dict[str, object]:
        return {
            "obligation_type": self.obligation_type.value,
            **self.counts.canonical_dict(),
            "rows": [item.canonical_dict() for item in self.rows],
        }


@dataclass(frozen=True, slots=True)
class CodeEvidenceTargetFact:
    target_id: str
    card_id: str
    source_ref: str
    revision: int
    lifecycle_status: CodeTraceabilityLifecycleStatus
    delivery_state: CoverageDeliveryState
    currentness: CoverageCurrentness
    currentness_reason: str | None
    current_resolution_id: str | None

    def __post_init__(self) -> None:
        for name in ("target_id", "card_id", "source_ref"):
            object.__setattr__(self, name, _identifier(getattr(self, name), field=name))
        object.__setattr__(
            self, "source_ref", normalize_code_source_ref(self.source_ref)
        )
        object.__setattr__(
            self, "revision", _positive_int(self.revision, field="target_revision")
        )
        if not isinstance(self.lifecycle_status, CodeTraceabilityLifecycleStatus):
            raise ValueError("coverage_traceability_target_lifecycle_invalid")
        if not isinstance(self.delivery_state, CoverageDeliveryState):
            raise ValueError("coverage_traceability_target_delivery_state_invalid")
        if not isinstance(self.currentness, CoverageCurrentness):
            raise ValueError("coverage_traceability_target_currentness_invalid")
        object.__setattr__(
            self,
            "currentness_reason",
            _reason(self.currentness_reason, field="target_currentness_reason"),
        )
        if (self.currentness is CoverageCurrentness.CURRENT) == (
            self.currentness_reason is not None
        ):
            raise ValueError("coverage_traceability_target_currentness_reason_invalid")
        object.__setattr__(
            self,
            "current_resolution_id",
            _optional_identifier(
                self.current_resolution_id, field="target_current_resolution_id"
            ),
        )

    def canonical_dict(self) -> dict[str, object]:
        return {
            "target_id": self.target_id,
            "card_id": self.card_id,
            "source_ref": self.source_ref,
            "revision": self.revision,
            "lifecycle_status": self.lifecycle_status.value,
            "delivery_state": self.delivery_state.value,
            "currentness": self.currentness.value,
            "currentness_reason": self.currentness_reason,
            "current_resolution_id": self.current_resolution_id,
        }


@dataclass(frozen=True, slots=True)
class CodeEvidenceResolutionFact:
    resolution_id: str
    target_id: str
    target_revision: int
    state: ImplementationTargetResolutionState
    currentness: CoverageCurrentness
    currentness_reason: str | None
    authority_ref: str

    def __post_init__(self) -> None:
        for name in ("resolution_id", "target_id", "authority_ref"):
            object.__setattr__(self, name, _identifier(getattr(self, name), field=name))
        object.__setattr__(
            self,
            "target_revision",
            _positive_int(self.target_revision, field="resolution_target_revision"),
        )
        if not isinstance(self.state, ImplementationTargetResolutionState):
            raise ValueError("coverage_traceability_resolution_state_invalid")
        if not isinstance(self.currentness, CoverageCurrentness):
            raise ValueError("coverage_traceability_resolution_currentness_invalid")
        object.__setattr__(
            self,
            "currentness_reason",
            _reason(self.currentness_reason, field="resolution_currentness_reason"),
        )
        if (self.currentness is CoverageCurrentness.CURRENT) == (
            self.currentness_reason is not None
        ):
            raise ValueError(
                "coverage_traceability_resolution_currentness_reason_invalid"
            )

    def canonical_dict(self) -> dict[str, object]:
        return {
            "resolution_id": self.resolution_id,
            "target_id": self.target_id,
            "target_revision": self.target_revision,
            "state": self.state.value,
            "currentness": self.currentness.value,
            "currentness_reason": self.currentness_reason,
            "authority_ref": self.authority_ref,
        }


@dataclass(frozen=True, slots=True)
class CodeEvidenceExecutionFact:
    execution_id: str
    target_id: str
    target_revision: int
    disposition: ImplementationTargetExecutionDisposition
    currentness: CoverageCurrentness
    currentness_reason: str | None
    authority_ref: str

    def __post_init__(self) -> None:
        for name in ("execution_id", "target_id", "authority_ref"):
            object.__setattr__(self, name, _identifier(getattr(self, name), field=name))
        object.__setattr__(
            self,
            "target_revision",
            _positive_int(self.target_revision, field="execution_target_revision"),
        )
        if not isinstance(self.disposition, ImplementationTargetExecutionDisposition):
            raise ValueError("coverage_traceability_execution_disposition_invalid")
        if not isinstance(self.currentness, CoverageCurrentness):
            raise ValueError("coverage_traceability_execution_currentness_invalid")
        object.__setattr__(
            self,
            "currentness_reason",
            _reason(self.currentness_reason, field="execution_currentness_reason"),
        )
        if (self.currentness is CoverageCurrentness.CURRENT) == (
            self.currentness_reason is not None
        ):
            raise ValueError(
                "coverage_traceability_execution_currentness_reason_invalid"
            )

    def canonical_dict(self) -> dict[str, object]:
        return {
            "execution_id": self.execution_id,
            "target_id": self.target_id,
            "target_revision": self.target_revision,
            "disposition": self.disposition.value,
            "currentness": self.currentness.value,
            "currentness_reason": self.currentness_reason,
            "authority_ref": self.authority_ref,
        }


@dataclass(frozen=True, slots=True)
class CodeEvidenceOverlapFact:
    overlap_id: str
    target_a_id: str
    target_b_id: str
    resolution_a_id: str
    resolution_b_id: str
    severity: TargetOverlapSeverity
    disposition: TargetOverlapDisposition | None
    currentness: CoverageCurrentness
    currentness_reason: str | None

    def __post_init__(self) -> None:
        for name in (
            "overlap_id",
            "target_a_id",
            "target_b_id",
            "resolution_a_id",
            "resolution_b_id",
        ):
            object.__setattr__(self, name, _identifier(getattr(self, name), field=name))
        if self.target_a_id == self.target_b_id:
            raise ValueError("coverage_traceability_overlap_targets_duplicate")
        if not isinstance(self.severity, TargetOverlapSeverity):
            raise ValueError("coverage_traceability_overlap_severity_invalid")
        if self.disposition is not None and not isinstance(
            self.disposition, TargetOverlapDisposition
        ):
            raise ValueError("coverage_traceability_overlap_disposition_invalid")
        if not isinstance(self.currentness, CoverageCurrentness):
            raise ValueError("coverage_traceability_overlap_currentness_invalid")
        object.__setattr__(
            self,
            "currentness_reason",
            _reason(self.currentness_reason, field="overlap_currentness_reason"),
        )
        if (self.currentness is CoverageCurrentness.CURRENT) == (
            self.currentness_reason is not None
        ):
            raise ValueError("coverage_traceability_overlap_currentness_reason_invalid")

    def canonical_dict(self) -> dict[str, object]:
        return {
            "overlap_id": self.overlap_id,
            "target_a_id": self.target_a_id,
            "target_b_id": self.target_b_id,
            "resolution_a_id": self.resolution_a_id,
            "resolution_b_id": self.resolution_b_id,
            "severity": self.severity.value,
            "disposition": self.disposition.value if self.disposition else None,
            "currentness": self.currentness.value,
            "currentness_reason": self.currentness_reason,
        }


@dataclass(frozen=True, slots=True)
class CodeEvidenceWaiverFact:
    waiver_id: str
    entity_type: CodeTraceabilityWaiverEntityType
    entity_id: str
    scope: CodeTraceabilityWaiverScope
    reason_code: CodeTraceabilityWaiverReason
    active: bool
    currentness: CoverageCurrentness
    currentness_reason: str | None
    authority_ref: str

    def __post_init__(self) -> None:
        for name in ("waiver_id", "entity_id", "authority_ref"):
            object.__setattr__(self, name, _identifier(getattr(self, name), field=name))
        if not isinstance(self.entity_type, CodeTraceabilityWaiverEntityType):
            raise ValueError("coverage_traceability_waiver_entity_type_invalid")
        if not isinstance(self.scope, CodeTraceabilityWaiverScope):
            raise ValueError("coverage_traceability_waiver_scope_invalid")
        if not isinstance(self.reason_code, CodeTraceabilityWaiverReason):
            raise ValueError("coverage_traceability_waiver_reason_invalid")
        if not isinstance(self.active, bool):
            raise ValueError("coverage_traceability_waiver_active_invalid")
        if not isinstance(self.currentness, CoverageCurrentness):
            raise ValueError("coverage_traceability_waiver_currentness_invalid")
        object.__setattr__(
            self,
            "currentness_reason",
            _reason(self.currentness_reason, field="waiver_currentness_reason"),
        )
        if (self.currentness is CoverageCurrentness.CURRENT) == (
            self.currentness_reason is not None
        ):
            raise ValueError("coverage_traceability_waiver_currentness_reason_invalid")
        if self.active and self.currentness is not CoverageCurrentness.CURRENT:
            raise ValueError("coverage_traceability_active_waiver_not_current")

    def canonical_dict(self) -> dict[str, object]:
        return {
            "waiver_id": self.waiver_id,
            "entity_type": self.entity_type.value,
            "entity_id": self.entity_id,
            "scope": self.scope.value,
            "reason_code": self.reason_code.value,
            "active": self.active,
            "currentness": self.currentness.value,
            "currentness_reason": self.currentness_reason,
            "authority_ref": self.authority_ref,
        }


@dataclass(frozen=True, slots=True)
class CodeEvidenceMatrix:
    state: CodeEvidenceMatrixState
    targets: tuple[CodeEvidenceTargetFact, ...] = ()
    resolutions: tuple[CodeEvidenceResolutionFact, ...] = ()
    executions: tuple[CodeEvidenceExecutionFact, ...] = ()
    overlaps: tuple[CodeEvidenceOverlapFact, ...] = ()
    waivers: tuple[CodeEvidenceWaiverFact, ...] = ()
    reason: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.state, CodeEvidenceMatrixState):
            raise ValueError("coverage_traceability_matrix_state_invalid")
        collections = (
            (self.targets, CodeEvidenceTargetFact, "target_id"),
            (self.resolutions, CodeEvidenceResolutionFact, "resolution_id"),
            (self.executions, CodeEvidenceExecutionFact, "execution_id"),
            (self.overlaps, CodeEvidenceOverlapFact, "overlap_id"),
            (self.waivers, CodeEvidenceWaiverFact, "waiver_id"),
        )
        if (
            sum(len(items) for items, _, _ in collections)
            > MAX_CODE_EVIDENCE_MATRIX_ROWS
        ):
            raise ValueError("coverage_traceability_matrix_rows_too_many")
        for items, item_type, identity_field in collections:
            if not isinstance(items, tuple) or any(
                not isinstance(item, item_type) for item in items
            ):
                raise ValueError("coverage_traceability_matrix_rows_invalid")
            if len({getattr(item, identity_field) for item in items}) != len(items):
                raise ValueError("coverage_traceability_matrix_row_duplicate")
            if items != tuple(
                sorted(items, key=lambda item: getattr(item, identity_field))
            ):
                raise ValueError("coverage_traceability_matrix_row_order_invalid")
        if self.state is CodeEvidenceMatrixState.AVAILABLE:
            current_targets = {
                item.target_id: item
                for item in self.targets
                if item.currentness is CoverageCurrentness.CURRENT
            }
            current_resolutions = {
                item.resolution_id: item
                for item in self.resolutions
                if item.currentness is CoverageCurrentness.CURRENT
            }
            if self.reason is not None or not current_targets:
                raise ValueError("coverage_traceability_available_matrix_invalid")
            if any(
                target.lifecycle_status is not CodeTraceabilityLifecycleStatus.ACTIVE
                or target.delivery_state is not CoverageDeliveryState.ACTIVE
                or target.current_resolution_id is None
                for target in current_targets.values()
            ):
                raise ValueError("coverage_traceability_available_target_invalid")
            pointed = {
                target.current_resolution_id for target in current_targets.values()
            }
            if set(current_resolutions) != pointed or any(
                resolution.target_id != target.target_id
                or resolution.target_revision != target.revision
                or resolution.state
                not in {
                    ImplementationTargetResolutionState.RESOLVED,
                    ImplementationTargetResolutionState.MOVED,
                }
                for target in current_targets.values()
                for resolution in (
                    current_resolutions.get(target.current_resolution_id),
                )
                if resolution is not None
            ):
                raise ValueError("coverage_traceability_available_resolution_invalid")
            if any(
                current_resolutions.get(target.current_resolution_id) is None
                for target in current_targets.values()
            ):
                raise ValueError("coverage_traceability_available_resolution_invalid")
            if any(
                item.target_id not in current_targets
                or item.target_revision != current_targets[item.target_id].revision
                for item in self.executions
                if item.currentness is CoverageCurrentness.CURRENT
            ):
                raise ValueError("coverage_traceability_available_execution_invalid")
            if any(
                item.target_a_id not in current_targets
                or item.target_b_id not in current_targets
                or item.resolution_a_id
                != current_targets[item.target_a_id].current_resolution_id
                or item.resolution_b_id
                != current_targets[item.target_b_id].current_resolution_id
                for item in self.overlaps
                if item.currentness is CoverageCurrentness.CURRENT
            ):
                raise ValueError("coverage_traceability_available_overlap_invalid")
            all_targets = {item.target_id: item for item in self.targets}
            all_resolutions = {item.resolution_id: item for item in self.resolutions}
            if (
                any(
                    item.target_id not in all_targets
                    or item.target_revision > all_targets[item.target_id].revision
                    for item in self.resolutions
                )
                or any(
                    item.target_id not in all_targets
                    or item.target_revision > all_targets[item.target_id].revision
                    for item in self.executions
                )
                or any(
                    item.target_a_id not in all_targets
                    or item.target_b_id not in all_targets
                    or item.resolution_a_id not in all_resolutions
                    or item.resolution_b_id not in all_resolutions
                    for item in self.overlaps
                )
            ):
                raise ValueError("coverage_traceability_available_history_invalid")
        elif self.state is CodeEvidenceMatrixState.NOT_APPLICABLE:
            factual_rows = (
                self.targets,
                self.resolutions,
                self.executions,
                self.overlaps,
            )
            if any(
                item.currentness is CoverageCurrentness.CURRENT
                for items in factual_rows
                for item in items
            ):
                raise ValueError("coverage_traceability_not_applicable_matrix_invalid")
            object.__setattr__(
                self, "reason", _reason(self.reason, field="matrix_reason")
            )
            if self.reason is None:
                raise ValueError("coverage_traceability_matrix_reason_required")
        elif self.state is CodeEvidenceMatrixState.RESTRICTED:
            if any(items for items, _, _ in collections):
                raise ValueError("coverage_traceability_restricted_matrix_leak")
            object.__setattr__(
                self, "reason", _reason(self.reason, field="matrix_reason")
            )
            if self.reason is None:
                raise ValueError("coverage_traceability_matrix_reason_required")
        else:
            object.__setattr__(
                self, "reason", _reason(self.reason, field="matrix_reason")
            )
            if self.reason is None:
                raise ValueError("coverage_traceability_matrix_reason_required")

    def canonical_dict(self) -> dict[str, object]:
        return {
            "state": self.state.value,
            "reason": self.reason,
            "targets": [item.canonical_dict() for item in self.targets],
            "resolutions": [item.canonical_dict() for item in self.resolutions],
            "executions": [item.canonical_dict() for item in self.executions],
            "overlaps": [item.canonical_dict() for item in self.overlaps],
            "waivers": [item.canonical_dict() for item in self.waivers],
        }


@dataclass(frozen=True, slots=True)
class CoverageTraceabilityProjection:
    contract_version: str
    foundation_version: str
    query_fingerprint: str
    filters: tuple[AnalyticsFilterClause, ...]
    as_of: datetime
    population_scope: AnalyticsPopulationScope
    exclusions: AnalyticsExclusionSummary
    evidence_population_scope: AnalyticsPopulationScope
    evidence_exclusions: AnalyticsExclusionSummary
    totals: CoverageCounts
    coverage: tuple[CoverageTypeProjection, ...]
    code_evidence: CodeEvidenceMatrix
    next_cursor: str | None = None

    def __post_init__(self) -> None:
        if self.contract_version != COVERAGE_TRACEABILITY_CONTRACT_VERSION:
            raise ValueError("coverage_traceability_contract_version_unsupported")
        if self.foundation_version != ANALYTICS_FOUNDATION_CONTRACT_VERSION:
            raise ValueError("coverage_traceability_foundation_version_unsupported")
        if not isinstance(self.query_fingerprint, str) or not _SHA256.fullmatch(
            self.query_fingerprint
        ):
            raise ValueError("coverage_traceability_query_fingerprint_invalid")
        object.__setattr__(
            self,
            "as_of",
            require_utc_datetime(self.as_of, field="coverage_traceability_as_of"),
        )
        if not isinstance(self.filters, tuple) or any(
            not isinstance(item, AnalyticsFilterClause) for item in self.filters
        ):
            raise ValueError("coverage_traceability_filters_invalid")
        if not isinstance(
            self.population_scope, AnalyticsPopulationScope
        ) or not isinstance(self.evidence_population_scope, AnalyticsPopulationScope):
            raise ValueError("coverage_traceability_population_scope_invalid")
        if not isinstance(self.exclusions, AnalyticsExclusionSummary) or not isinstance(
            self.evidence_exclusions, AnalyticsExclusionSummary
        ):
            raise ValueError("coverage_traceability_exclusions_invalid")
        if self.population_scope.excluded_count != self.exclusions.excluded_count:
            raise ValueError("coverage_traceability_exclusion_population_mismatch")
        if (
            self.evidence_population_scope.excluded_count
            != self.evidence_exclusions.excluded_count
        ):
            raise ValueError(
                "coverage_traceability_evidence_exclusion_population_mismatch"
            )
        if not isinstance(self.totals, CoverageCounts):
            raise ValueError("coverage_traceability_totals_invalid")
        if (
            not isinstance(self.coverage, tuple)
            or any(
                not isinstance(item, CoverageTypeProjection) for item in self.coverage
            )
            or tuple(item.obligation_type for item in self.coverage)
            != tuple(CoverageObligationType)
        ):
            raise ValueError("coverage_traceability_type_set_invalid")
        rows = tuple(row for item in self.coverage for row in item.rows)
        if len(rows) > MAX_COVERAGE_OBLIGATION_ROWS:
            raise ValueError("coverage_traceability_rows_too_many")
        if sum(len(item.evidence) for item in rows) > MAX_COVERAGE_EVIDENCE_ROWS:
            raise ValueError("coverage_traceability_evidence_rows_too_many")
        identities = tuple(item.identity for item in rows)
        if len(set(identities)) != len(identities):
            raise ValueError("coverage_traceability_obligation_duplicate")
        if any(
            item.currentness is not CoverageCurrentness.CURRENT for item in identities
        ):
            raise ValueError("coverage_traceability_current_population_required")
        if len(rows) != self.population_scope.accessible_count:
            raise ValueError("coverage_traceability_population_count_mismatch")
        if self.totals != _counts_for_rows(rows):
            raise ValueError("coverage_traceability_board_totals_mismatch")
        if not isinstance(self.code_evidence, CodeEvidenceMatrix):
            raise ValueError("coverage_traceability_matrix_invalid")
        matrix_row_count = sum(
            len(items)
            for items in (
                self.code_evidence.targets,
                self.code_evidence.resolutions,
                self.code_evidence.executions,
                self.code_evidence.overlaps,
                self.code_evidence.waivers,
            )
        )
        if (
            sum(len(item.evidence) for item in rows) + matrix_row_count
            != self.evidence_population_scope.accessible_count
        ):
            raise ValueError("coverage_traceability_evidence_population_count_mismatch")
        object.__setattr__(
            self,
            "next_cursor",
            _optional_identifier(self.next_cursor, field="next_cursor"),
        )

    def canonical_dict(self) -> dict[str, object]:
        return {
            "contract_version": self.contract_version,
            "foundation_version": self.foundation_version,
            "query_fingerprint": self.query_fingerprint,
            "filters": [item.canonical_dict() for item in self.filters],
            "as_of": _utc_text(self.as_of),
            "population_scope": self.population_scope.canonical_dict(),
            "exclusions": self.exclusions.canonical_dict(),
            "evidence_population_scope": self.evidence_population_scope.canonical_dict(),
            "evidence_exclusions": self.evidence_exclusions.canonical_dict(),
            "totals": self.totals.canonical_dict(),
            "coverage": [item.canonical_dict() for item in self.coverage],
            "code_evidence": self.code_evidence.canonical_dict(),
            "next_cursor": self.next_cursor,
        }


@runtime_checkable
class CoverageTraceabilityProjectionPort(Protocol):
    async def project_coverage_traceability(
        self,
        context: object,
        query: AnalyticsFoundationQuery,
    ) -> CoverageTraceabilityProjection: ...


__all__ = [
    "COVERAGE_TRACEABILITY_CONTRACT_VERSION",
    "MAX_CODE_EVIDENCE_MATRIX_ROWS",
    "MAX_COVERAGE_EVIDENCE_ROWS",
    "MAX_COVERAGE_OBLIGATION_ROWS",
    "CodeEvidenceExecutionFact",
    "CodeEvidenceMatrix",
    "CodeEvidenceMatrixState",
    "CodeEvidenceOverlapFact",
    "CodeEvidenceResolutionFact",
    "CodeEvidenceTargetFact",
    "CodeEvidenceWaiverFact",
    "CoverageAggregateState",
    "CoverageAuthorityState",
    "CoverageCounts",
    "CoverageCurrentness",
    "CoverageDeliveryState",
    "CoverageEvidenceEligibility",
    "CoverageEvidenceFact",
    "CoverageEvidenceRow",
    "CoverageFactState",
    "CoverageObligationFact",
    "CoverageObligationIdentity",
    "CoverageObligationRow",
    "CoverageObligationType",
    "CoverageSkipMetadata",
    "CoverageSkipState",
    "CoverageTraceabilityProjection",
    "CoverageTraceabilityProjectionPort",
    "CoverageTypeProjection",
]
