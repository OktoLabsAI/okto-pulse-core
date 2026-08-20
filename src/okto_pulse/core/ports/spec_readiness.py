"""Edition-aware projection contract for Spec validation readiness."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Protocol, runtime_checkable

from okto_pulse.core.ports.analytics_foundation import (
    ANALYTICS_FOUNDATION_CONTRACT_VERSION,
    AnalyticsEvidenceCurrentness,
    AnalyticsExclusionSummary,
    AnalyticsFilterClause,
    AnalyticsFoundationQuery,
    AnalyticsPopulationScope,
    require_utc_datetime,
)


SPEC_READINESS_CONTRACT_VERSION = "1"
MAX_SPEC_READINESS_CHECKS = 4096
MAX_SPEC_READINESS_ROWS = 10_000

_IDENTIFIER = re.compile(r"^[^\x00-\x1f\x7f]{1,255}$")
_CHECK_ID = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_.:/-]{0,254}$")
_SEVERITY = re.compile(r"^[a-z][a-z0-9_.-]{0,63}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class SpecReadinessEvidenceState(str, Enum):
    CURRENT = AnalyticsEvidenceCurrentness.CURRENT.value
    PREVIOUS = AnalyticsEvidenceCurrentness.PREVIOUS.value
    MISSING = AnalyticsEvidenceCurrentness.MISSING.value
    STALE = AnalyticsEvidenceCurrentness.STALE.value
    RESTRICTED = "restricted"


class SpecReadinessCheckKind(str, Enum):
    CURATED_CHECKLIST = "curated_checklist"
    REQUIREMENT_LINT = "requirement_lint"


def _score(value: int | None, *, field: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 100:
        raise ValueError(f"spec_readiness_{field}_invalid")
    return value


def _identifier(value: str, *, field: str) -> str:
    if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"spec_readiness_{field}_invalid")
    return value


def _utc_text(value: datetime) -> str:
    return (
        require_utc_datetime(value, field="spec_readiness_timestamp")
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


@dataclass(frozen=True, slots=True)
class SpecValidationMeasures:
    confidence: int | None = None
    clarity: int | None = None
    assertiveness: int | None = None
    decidability: int | None = None
    ambiguity: int | None = None
    legacy_completeness: int | None = None

    def __post_init__(self) -> None:
        for field in (
            "confidence",
            "clarity",
            "assertiveness",
            "decidability",
            "ambiguity",
            "legacy_completeness",
        ):
            object.__setattr__(self, field, _score(getattr(self, field), field=field))

    @property
    def canonical_complete(self) -> bool:
        return all(
            value is not None
            for value in (
                self.confidence,
                self.clarity,
                self.assertiveness,
                self.decidability,
                self.ambiguity,
            )
        )

    @property
    def canonical_present_count(self) -> int:
        return sum(
            value is not None
            for value in (
                self.confidence,
                self.clarity,
                self.assertiveness,
                self.decidability,
                self.ambiguity,
            )
        )

    def canonical_dict(self) -> dict[str, int | None]:
        return {
            "confidence": self.confidence,
            "clarity": self.clarity,
            "assertiveness": self.assertiveness,
            "decidability": self.decidability,
            "ambiguity": self.ambiguity,
            "legacy_completeness": self.legacy_completeness,
        }


@dataclass(frozen=True, slots=True)
class SpecValidationAttemptFacts:
    attempts: int
    first_pass: bool | None
    revalidation_count: int

    def __post_init__(self) -> None:
        if (
            isinstance(self.attempts, bool)
            or not isinstance(self.attempts, int)
            or self.attempts < 0
        ):
            raise ValueError("spec_readiness_attempts_invalid")
        if (
            isinstance(self.revalidation_count, bool)
            or not isinstance(self.revalidation_count, int)
            or self.revalidation_count < 0
        ):
            raise ValueError("spec_readiness_revalidation_count_invalid")
        if self.attempts == 0:
            if self.first_pass is not None or self.revalidation_count != 0:
                raise ValueError("spec_readiness_empty_attempt_shape_invalid")
        elif (
            not isinstance(self.first_pass, bool)
            or self.revalidation_count != self.attempts - 1
        ):
            raise ValueError("spec_readiness_attempt_shape_invalid")

    def canonical_dict(self) -> dict[str, object]:
        return {
            "attempts": self.attempts,
            "first_pass": self.first_pass,
            "revalidation_count": self.revalidation_count,
        }


@dataclass(frozen=True, slots=True)
class SpecValidationReadiness:
    state: SpecReadinessEvidenceState
    measures: SpecValidationMeasures
    attempts: SpecValidationAttemptFacts
    validation_id: str | None = None
    authority_ref: str | None = None
    evidence_edition: int | None = None
    lifecycle_ready: bool | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.state, SpecReadinessEvidenceState):
            raise ValueError("spec_readiness_validation_state_invalid")
        if not isinstance(self.measures, SpecValidationMeasures):
            raise ValueError("spec_readiness_validation_measures_invalid")
        if not isinstance(self.attempts, SpecValidationAttemptFacts):
            raise ValueError("spec_readiness_validation_attempts_invalid")
        if self.evidence_edition is not None and (
            isinstance(self.evidence_edition, bool)
            or not isinstance(self.evidence_edition, int)
            or self.evidence_edition < 1
        ):
            raise ValueError("spec_readiness_evidence_edition_invalid")

        if self.state in {
            SpecReadinessEvidenceState.MISSING,
            SpecReadinessEvidenceState.RESTRICTED,
        }:
            if (
                any(
                    value is not None
                    for value in (
                        self.validation_id,
                        self.authority_ref,
                        self.evidence_edition,
                        self.lifecycle_ready,
                    )
                )
                or self.measures.canonical_present_count
                or self.measures.legacy_completeness is not None
            ):
                raise ValueError("spec_readiness_absent_validation_shape_invalid")
            if self.attempts.attempts:
                raise ValueError("spec_readiness_absent_validation_attempts_invalid")
            return

        object.__setattr__(
            self,
            "validation_id",
            _identifier(self.validation_id, field="validation_id"),
        )
        object.__setattr__(
            self,
            "authority_ref",
            _identifier(self.authority_ref, field="validation_authority_ref"),
        )
        if self.evidence_edition is None:
            raise ValueError("spec_readiness_evidence_edition_required")
        if self.state is SpecReadinessEvidenceState.CURRENT:
            if not isinstance(self.lifecycle_ready, bool):
                raise ValueError("spec_readiness_lifecycle_authority_required")
            if not self.measures.canonical_complete and self.lifecycle_ready:
                raise ValueError("spec_readiness_incomplete_validation_cannot_be_ready")
        elif self.lifecycle_ready is not None:
            raise ValueError("spec_readiness_historical_lifecycle_authority_forbidden")

    def canonical_dict(self) -> dict[str, object]:
        return {
            "state": self.state.value,
            "validation_id": self.validation_id,
            "authority_ref": self.authority_ref,
            "evidence_edition": self.evidence_edition,
            "measures": self.measures.canonical_dict(),
            **self.attempts.canonical_dict(),
            "lifecycle_ready": self.lifecycle_ready,
        }


@dataclass(frozen=True, slots=True)
class SpecReadinessCheck:
    kind: SpecReadinessCheckKind
    check_id: str
    severity: str
    state: SpecReadinessEvidenceState
    evidence_ref: str | None = None
    evidence_edition: int | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.kind, SpecReadinessCheckKind):
            raise ValueError("spec_readiness_check_kind_invalid")
        if not isinstance(self.check_id, str) or not _CHECK_ID.fullmatch(self.check_id):
            raise ValueError("spec_readiness_check_id_invalid")
        if not isinstance(self.severity, str) or not _SEVERITY.fullmatch(self.severity):
            raise ValueError("spec_readiness_check_severity_invalid")
        if not isinstance(self.state, SpecReadinessEvidenceState):
            raise ValueError("spec_readiness_check_state_invalid")
        absent = self.state in {
            SpecReadinessEvidenceState.MISSING,
            SpecReadinessEvidenceState.RESTRICTED,
        }
        if absent:
            if self.evidence_ref is not None or self.evidence_edition is not None:
                raise ValueError("spec_readiness_absent_check_evidence_forbidden")
        else:
            object.__setattr__(
                self,
                "evidence_ref",
                _identifier(self.evidence_ref, field="check_evidence_ref"),
            )
            if (
                isinstance(self.evidence_edition, bool)
                or not isinstance(self.evidence_edition, int)
                or self.evidence_edition < 1
            ):
                raise ValueError("spec_readiness_check_evidence_edition_invalid")

    def canonical_dict(self) -> dict[str, object]:
        return {
            "kind": self.kind.value,
            "check_id": self.check_id,
            "severity": self.severity,
            "state": self.state.value,
            "evidence_ref": self.evidence_ref,
            "evidence_edition": self.evidence_edition,
        }


@dataclass(frozen=True, slots=True)
class SpecReadinessRow:
    spec_id: str
    edition: int
    validation: SpecValidationReadiness
    checklist: tuple[SpecReadinessCheck, ...]
    requirement_lint: tuple[SpecReadinessCheck, ...]
    spec_pending_validation: bool | None

    def __post_init__(self) -> None:
        object.__setattr__(self, "spec_id", _identifier(self.spec_id, field="spec_id"))
        if (
            isinstance(self.edition, bool)
            or not isinstance(self.edition, int)
            or self.edition < 1
        ):
            raise ValueError("spec_readiness_spec_edition_invalid")
        if not isinstance(self.validation, SpecValidationReadiness):
            raise ValueError("spec_readiness_validation_invalid")
        if not isinstance(self.checklist, tuple) or not isinstance(
            self.requirement_lint, tuple
        ):
            raise ValueError("spec_readiness_checks_invalid")
        checks = self.checklist + self.requirement_lint
        if len(checks) > MAX_SPEC_READINESS_CHECKS or any(
            not isinstance(item, SpecReadinessCheck) for item in checks
        ):
            raise ValueError("spec_readiness_checks_invalid")
        if any(
            item.kind is not SpecReadinessCheckKind.CURATED_CHECKLIST
            for item in self.checklist
        ) or any(
            item.kind is not SpecReadinessCheckKind.REQUIREMENT_LINT
            for item in self.requirement_lint
        ):
            raise ValueError("spec_readiness_check_bucket_mismatch")
        identities = tuple((item.kind, item.check_id) for item in checks)
        if len(set(identities)) != len(identities):
            raise ValueError("spec_readiness_check_duplicate")
        if any(
            item.state is SpecReadinessEvidenceState.CURRENT
            and item.evidence_edition != self.edition
            for item in checks
        ):
            raise ValueError("spec_readiness_current_check_edition_mismatch")

        if self.validation.state is SpecReadinessEvidenceState.RESTRICTED:
            expected_pending: bool | None = None
        elif self.validation.state is SpecReadinessEvidenceState.CURRENT:
            if self.validation.evidence_edition != self.edition:
                raise ValueError("spec_readiness_current_validation_edition_mismatch")
            expected_pending = not self.validation.lifecycle_ready
        else:
            expected_pending = True
        if self.spec_pending_validation is not expected_pending:
            raise ValueError("spec_readiness_pending_validation_mismatch")

    def canonical_dict(self) -> dict[str, object]:
        return {
            "spec_id": self.spec_id,
            "edition": self.edition,
            "validation": self.validation.canonical_dict(),
            "checklist": [item.canonical_dict() for item in self.checklist],
            "requirement_lint": [
                item.canonical_dict() for item in self.requirement_lint
            ],
            "lifecycle": {"spec_pending_validation": self.spec_pending_validation},
        }


@dataclass(frozen=True, slots=True)
class SpecReadinessProjection:
    contract_version: str
    foundation_version: str
    query_fingerprint: str
    filters: tuple[AnalyticsFilterClause, ...]
    as_of: datetime
    population_scope: AnalyticsPopulationScope
    exclusions: AnalyticsExclusionSummary
    specs: tuple[SpecReadinessRow, ...]
    next_cursor: str | None = None

    def __post_init__(self) -> None:
        if self.contract_version != SPEC_READINESS_CONTRACT_VERSION:
            raise ValueError("spec_readiness_contract_version_unsupported")
        if self.foundation_version != ANALYTICS_FOUNDATION_CONTRACT_VERSION:
            raise ValueError("spec_readiness_foundation_version_unsupported")
        if not isinstance(self.query_fingerprint, str) or not _SHA256.fullmatch(
            self.query_fingerprint
        ):
            raise ValueError("spec_readiness_query_fingerprint_invalid")
        object.__setattr__(
            self,
            "as_of",
            require_utc_datetime(self.as_of, field="spec_readiness_as_of"),
        )
        if not isinstance(self.population_scope, AnalyticsPopulationScope):
            raise ValueError("spec_readiness_population_scope_invalid")
        if not isinstance(self.exclusions, AnalyticsExclusionSummary):
            raise ValueError("spec_readiness_exclusions_invalid")
        if self.population_scope.excluded_count != self.exclusions.excluded_count:
            raise ValueError("spec_readiness_exclusion_population_mismatch")
        if (
            not isinstance(self.specs, tuple)
            or len(self.specs) > MAX_SPEC_READINESS_ROWS
            or any(not isinstance(item, SpecReadinessRow) for item in self.specs)
        ):
            raise ValueError("spec_readiness_rows_invalid")
        if len({(item.spec_id, item.edition) for item in self.specs}) != len(
            self.specs
        ):
            raise ValueError("spec_readiness_row_duplicate")
        if len(self.specs) != self.population_scope.accessible_count:
            raise ValueError("spec_readiness_population_count_mismatch")

    def canonical_dict(self) -> dict[str, object]:
        return {
            "contract_version": self.contract_version,
            "foundation_version": self.foundation_version,
            "query_fingerprint": self.query_fingerprint,
            "filters": [item.canonical_dict() for item in self.filters],
            "as_of": _utc_text(self.as_of),
            "population_scope": self.population_scope.canonical_dict(),
            "exclusions": self.exclusions.canonical_dict(),
            "specs": [item.canonical_dict() for item in self.specs],
            "next_cursor": self.next_cursor,
        }


@runtime_checkable
class SpecReadinessProjectionPort(Protocol):
    async def project_spec_readiness(
        self,
        context: object,
        query: AnalyticsFoundationQuery,
    ) -> SpecReadinessProjection: ...


__all__ = [
    "MAX_SPEC_READINESS_CHECKS",
    "MAX_SPEC_READINESS_ROWS",
    "SPEC_READINESS_CONTRACT_VERSION",
    "SpecReadinessCheck",
    "SpecReadinessCheckKind",
    "SpecReadinessEvidenceState",
    "SpecReadinessProjection",
    "SpecReadinessProjectionPort",
    "SpecReadinessRow",
    "SpecValidationAttemptFacts",
    "SpecValidationMeasures",
    "SpecValidationReadiness",
]
