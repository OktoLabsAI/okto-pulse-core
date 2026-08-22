"""Read-only contract for policy and Resource Gate readiness analytics."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Protocol, runtime_checkable

from okto_pulse.core.ports.analytics_foundation import (
    ANALYTICS_FOUNDATION_CONTRACT_VERSION,
    AnalyticsExclusionSummary,
    AnalyticsFilterClause,
    AnalyticsFoundationQuery,
    AnalyticsPopulationScope,
    require_utc_datetime,
)


POLICY_RESOURCE_READINESS_CONTRACT_VERSION = "1"
MAX_POLICY_RESOURCE_READINESS_ROWS = 10_000
MAX_POLICY_FACTS_PER_SPEC = 4096
MAX_RESOURCE_EVIDENCE_REFS = 4096

_IDENTIFIER = re.compile(r"^[^\x00-\x1f\x7f]{1,255}$")
_REASON = re.compile(r"^[a-z][a-z0-9_.:-]{0,127}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


def _identifier(value: str, *, field: str) -> str:
    if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"policy_resource_{field}_invalid")
    return value


def _reason(value: str | None, *, field: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not _REASON.fullmatch(value):
        raise ValueError(f"policy_resource_{field}_invalid")
    return value


def _count(value: int, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"policy_resource_{field}_invalid")
    return value


def _utc_text(value: datetime) -> str:
    return (
        require_utc_datetime(value, field="policy_resource_timestamp")
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


class PolicyReadinessState(str, Enum):
    NATIVE_PASS = "native_pass"
    BLOCKING_PENDING = "blocking_pending"
    BLOCKING_FAILED = "blocking_failed"
    ADVISORY_FAILED = "advisory_failed"
    WAIVED = "waived"
    SKIPPED = "skipped"
    INCONSISTENT = "inconsistent"
    STALE = "stale"
    RESTRICTED = "restricted"
    NOT_APPLICABLE = "not_applicable"


class PolicyAuthority(str, Enum):
    BLOCKING = "blocking"
    ADVISORY = "advisory"


class PolicyExceptionKind(str, Enum):
    WAIVER = "waiver"
    SKIP = "skip"


class GovernanceCurrentness(str, Enum):
    CURRENT = "current"
    STALE = "stale"
    INCONSISTENT = "inconsistent"


class ResourceType(str, Enum):
    ARCHITECTURE = "architecture"
    MOCKUP = "mockup"
    KNOWLEDGE_BASE = "knowledge_base"


class ResourceL1State(str, Enum):
    PROVIDED = "provided"
    NOT_APPLICABLE = "not_applicable"
    MISSING = "missing"
    RESTRICTED = "restricted"
    INCONSISTENT = "inconsistent"


class ResourceL2State(str, Enum):
    COVERED = "covered"
    UNCOVERED = "uncovered"
    NOT_APPLICABLE = "not_applicable"
    RESTRICTED = "restricted"
    INCONSISTENT = "inconsistent"


@dataclass(frozen=True, slots=True)
class PolicyExceptionFact:
    kind: PolicyExceptionKind
    authority_ref: str
    reason_code: str
    currentness: GovernanceCurrentness
    effective_at: datetime
    age_seconds: int
    expires_at: datetime | None = None
    currentness_reason: str | None = None
    impact_backlog_count: int = 0

    def __post_init__(self) -> None:
        if not isinstance(self.kind, PolicyExceptionKind):
            raise ValueError("policy_resource_exception_kind_invalid")
        if not isinstance(self.currentness, GovernanceCurrentness):
            raise ValueError("policy_resource_exception_currentness_invalid")
        object.__setattr__(
            self,
            "authority_ref",
            _identifier(self.authority_ref, field="exception_authority_ref"),
        )
        object.__setattr__(
            self,
            "reason_code",
            _reason(self.reason_code, field="exception_reason_code"),
        )
        object.__setattr__(
            self,
            "effective_at",
            require_utc_datetime(self.effective_at, field="exception_effective_at"),
        )
        object.__setattr__(
            self, "age_seconds", _count(self.age_seconds, field="exception_age")
        )
        object.__setattr__(
            self,
            "impact_backlog_count",
            _count(self.impact_backlog_count, field="exception_impact_backlog"),
        )
        if self.expires_at is not None:
            expires_at = require_utc_datetime(
                self.expires_at, field="exception_expires_at"
            )
            if expires_at <= self.effective_at:
                raise ValueError("policy_resource_exception_expiry_invalid")
            object.__setattr__(self, "expires_at", expires_at)
        reason = _reason(
            self.currentness_reason,
            field="exception_currentness_reason",
        )
        if self.currentness is not GovernanceCurrentness.CURRENT and reason is None:
            raise ValueError("policy_resource_exception_currentness_reason_required")
        object.__setattr__(self, "currentness_reason", reason)

    def canonical_dict(self) -> dict[str, object]:
        return {
            "kind": self.kind.value,
            "authority_ref": self.authority_ref,
            "reason_code": self.reason_code,
            "currentness": self.currentness.value,
            "effective_at": _utc_text(self.effective_at),
            "age_seconds": self.age_seconds,
            "expires_at": (
                _utc_text(self.expires_at) if self.expires_at is not None else None
            ),
            "currentness_reason": self.currentness_reason,
            "impact_backlog_count": self.impact_backlog_count,
        }


@dataclass(frozen=True, slots=True)
class PolicyReadinessFact:
    policy_id: str
    state: PolicyReadinessState
    authority: PolicyAuthority
    authority_ref: str | None = None
    evidence_ref: str | None = None
    currentness_reason: str | None = None
    exception: PolicyExceptionFact | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "policy_id", _identifier(self.policy_id, field="policy_id")
        )
        if not isinstance(self.state, PolicyReadinessState):
            raise ValueError("policy_resource_policy_state_invalid")
        if not isinstance(self.authority, PolicyAuthority):
            raise ValueError("policy_resource_policy_authority_invalid")
        authority_ref = (
            _identifier(self.authority_ref, field="policy_authority_ref")
            if self.authority_ref is not None
            else None
        )
        evidence_ref = (
            _identifier(self.evidence_ref, field="policy_evidence_ref")
            if self.evidence_ref is not None
            else None
        )
        currentness_reason = _reason(
            self.currentness_reason, field="policy_currentness_reason"
        )
        if self.state is PolicyReadinessState.RESTRICTED:
            if any(
                value is not None
                for value in (
                    authority_ref,
                    evidence_ref,
                    currentness_reason,
                    self.exception,
                )
            ):
                raise ValueError("policy_resource_restricted_policy_leaks_evidence")
        elif authority_ref is None:
            raise ValueError("policy_resource_policy_authority_ref_required")
        if (
            self.state is PolicyReadinessState.NATIVE_PASS
            and self.exception is not None
        ):
            raise ValueError("policy_resource_native_pass_exception_forbidden")
        if (
            self.state
            in {
                PolicyReadinessState.BLOCKING_PENDING,
                PolicyReadinessState.BLOCKING_FAILED,
            }
            and self.authority is not PolicyAuthority.BLOCKING
        ):
            raise ValueError("policy_resource_blocking_state_authority_mismatch")
        if (
            self.state is PolicyReadinessState.ADVISORY_FAILED
            and self.authority is not PolicyAuthority.ADVISORY
        ):
            raise ValueError("policy_resource_advisory_state_authority_mismatch")
        expected_kind = {
            PolicyReadinessState.WAIVED: PolicyExceptionKind.WAIVER,
            PolicyReadinessState.SKIPPED: PolicyExceptionKind.SKIP,
        }.get(self.state)
        if expected_kind is not None and (
            self.exception is None
            or self.exception.kind is not expected_kind
            or self.exception.currentness is not GovernanceCurrentness.CURRENT
        ):
            raise ValueError("policy_resource_policy_exception_binding_invalid")
        if (
            self.state
            in {
                PolicyReadinessState.STALE,
                PolicyReadinessState.INCONSISTENT,
            }
            and currentness_reason is None
        ):
            raise ValueError("policy_resource_policy_currentness_reason_required")
        object.__setattr__(self, "authority_ref", authority_ref)
        object.__setattr__(self, "evidence_ref", evidence_ref)
        object.__setattr__(self, "currentness_reason", currentness_reason)

    @property
    def applicable(self) -> bool:
        return self.state not in {
            PolicyReadinessState.NOT_APPLICABLE,
            PolicyReadinessState.RESTRICTED,
        }

    @property
    def completed(self) -> bool:
        return self.state in {
            PolicyReadinessState.NATIVE_PASS,
            PolicyReadinessState.BLOCKING_FAILED,
            PolicyReadinessState.ADVISORY_FAILED,
            PolicyReadinessState.WAIVED,
            PolicyReadinessState.SKIPPED,
        }

    def canonical_dict(self) -> dict[str, object]:
        return {
            "policy_id": self.policy_id,
            "state": self.state.value,
            "authority": self.authority.value,
            "authority_ref": self.authority_ref,
            "evidence_ref": self.evidence_ref,
            "currentness_reason": self.currentness_reason,
            "exception": (
                self.exception.canonical_dict() if self.exception is not None else None
            ),
        }


@dataclass(frozen=True, slots=True)
class PolicyReadinessTotals:
    applicable: int
    completed: int
    native_pass: int
    blocking_pending: int
    blocking_failed: int
    advisory_failed: int
    waived: int
    skipped: int
    inconsistent: int
    stale: int
    restricted: int
    not_applicable: int

    def __post_init__(self) -> None:
        for field in self.__dataclass_fields__:
            object.__setattr__(self, field, _count(getattr(self, field), field=field))
        if self.completed != sum(
            (
                self.native_pass,
                self.blocking_failed,
                self.advisory_failed,
                self.waived,
                self.skipped,
            )
        ):
            raise ValueError("policy_resource_completed_total_invalid")

    def canonical_dict(self) -> dict[str, int]:
        return {field: getattr(self, field) for field in self.__dataclass_fields__}

    @classmethod
    def from_facts(
        cls,
        policies: tuple[PolicyReadinessFact, ...],
    ) -> PolicyReadinessTotals:
        if not isinstance(policies, tuple) or any(
            not isinstance(item, PolicyReadinessFact) for item in policies
        ):
            raise ValueError("policy_resource_policy_facts_invalid")
        by_state = {
            state: sum(item.state is state for item in policies)
            for state in PolicyReadinessState
        }
        return cls(
            applicable=sum(item.applicable for item in policies),
            completed=sum(item.completed for item in policies),
            native_pass=by_state[PolicyReadinessState.NATIVE_PASS],
            blocking_pending=by_state[PolicyReadinessState.BLOCKING_PENDING],
            blocking_failed=by_state[PolicyReadinessState.BLOCKING_FAILED],
            advisory_failed=by_state[PolicyReadinessState.ADVISORY_FAILED],
            waived=by_state[PolicyReadinessState.WAIVED],
            skipped=by_state[PolicyReadinessState.SKIPPED],
            inconsistent=by_state[PolicyReadinessState.INCONSISTENT],
            stale=by_state[PolicyReadinessState.STALE],
            restricted=by_state[PolicyReadinessState.RESTRICTED],
            not_applicable=by_state[PolicyReadinessState.NOT_APPLICABLE],
        )


@dataclass(frozen=True, slots=True)
class ResourceL1Fact:
    resource_type: ResourceType
    state: ResourceL1State
    authority: PolicyAuthority
    authority_ref: str | None = None
    evidence_ref: str | None = None
    currentness_reason: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.resource_type, ResourceType):
            raise ValueError("policy_resource_resource_type_invalid")
        if not isinstance(self.state, ResourceL1State):
            raise ValueError("policy_resource_l1_state_invalid")
        if not isinstance(self.authority, PolicyAuthority):
            raise ValueError("policy_resource_l1_authority_invalid")
        authority_ref = (
            _identifier(self.authority_ref, field="l1_authority_ref")
            if self.authority_ref is not None
            else None
        )
        evidence_ref = (
            _identifier(self.evidence_ref, field="l1_evidence_ref")
            if self.evidence_ref is not None
            else None
        )
        reason = _reason(self.currentness_reason, field="l1_currentness_reason")
        if self.state is ResourceL1State.RESTRICTED:
            if (
                authority_ref is not None
                or evidence_ref is not None
                or reason is not None
            ):
                raise ValueError("policy_resource_restricted_l1_leaks_evidence")
        elif authority_ref is None:
            raise ValueError("policy_resource_l1_authority_ref_required")
        if self.state is ResourceL1State.PROVIDED and evidence_ref is None:
            raise ValueError("policy_resource_l1_evidence_ref_required")
        if (
            self.state in {ResourceL1State.MISSING, ResourceL1State.NOT_APPLICABLE}
            and evidence_ref is not None
        ):
            raise ValueError("policy_resource_l1_evidence_ref_forbidden")
        if (
            self.state in {ResourceL1State.NOT_APPLICABLE, ResourceL1State.INCONSISTENT}
            and reason is None
        ):
            raise ValueError("policy_resource_l1_reason_required")
        object.__setattr__(self, "authority_ref", authority_ref)
        object.__setattr__(self, "evidence_ref", evidence_ref)
        object.__setattr__(self, "currentness_reason", reason)

    def canonical_dict(self) -> dict[str, object]:
        return {
            "resource_type": self.resource_type.value,
            "state": self.state.value,
            "authority": self.authority.value,
            "authority_ref": self.authority_ref,
            "evidence_ref": self.evidence_ref,
            "currentness_reason": self.currentness_reason,
        }


@dataclass(frozen=True, slots=True)
class ResourceL2Fact:
    resource_type: ResourceType
    state: ResourceL2State
    eligible_card_count: int | None
    covered_card_count: int | None
    cancelled_or_archived_link_count: int | None
    evidence_refs: tuple[str, ...] = ()
    currentness_reason: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.resource_type, ResourceType):
            raise ValueError("policy_resource_resource_type_invalid")
        if not isinstance(self.state, ResourceL2State):
            raise ValueError("policy_resource_l2_state_invalid")
        reason = _reason(self.currentness_reason, field="l2_currentness_reason")
        if (
            not isinstance(self.evidence_refs, tuple)
            or len(self.evidence_refs) > MAX_RESOURCE_EVIDENCE_REFS
        ):
            raise ValueError("policy_resource_l2_evidence_refs_invalid")
        refs = tuple(
            _identifier(value, field="l2_evidence_ref") for value in self.evidence_refs
        )
        if len(set(refs)) != len(refs):
            raise ValueError("policy_resource_l2_evidence_ref_duplicate")
        if self.state is ResourceL2State.RESTRICTED:
            if (
                any(
                    value is not None
                    for value in (
                        self.eligible_card_count,
                        self.covered_card_count,
                        self.cancelled_or_archived_link_count,
                        reason,
                    )
                )
                or refs
            ):
                raise ValueError("policy_resource_restricted_l2_leaks_evidence")
        else:
            if (
                self.eligible_card_count is None
                or self.covered_card_count is None
                or self.cancelled_or_archived_link_count is None
            ):
                raise ValueError("policy_resource_l2_counts_required")
            eligible = _count(self.eligible_card_count, field="l2_eligible_cards")
            covered = _count(self.covered_card_count, field="l2_covered_cards")
            _count(
                self.cancelled_or_archived_link_count,
                field="l2_cancelled_or_archived_links",
            )
            if covered > eligible:
                raise ValueError("policy_resource_l2_covered_population_invalid")
            if self.state is ResourceL2State.COVERED and not (
                eligible > 0 and covered > 0 and refs
            ):
                raise ValueError("policy_resource_l2_covered_shape_invalid")
            if self.state is ResourceL2State.UNCOVERED and not (
                eligible > 0 and covered == 0 and not refs
            ):
                raise ValueError("policy_resource_l2_uncovered_shape_invalid")
            if self.state is ResourceL2State.NOT_APPLICABLE and not (
                eligible == 0 and covered == 0 and not refs
            ):
                raise ValueError("policy_resource_l2_not_applicable_shape_invalid")
            if self.state is ResourceL2State.INCONSISTENT and reason is None:
                raise ValueError("policy_resource_l2_reason_required")
        object.__setattr__(self, "evidence_refs", refs)
        object.__setattr__(self, "currentness_reason", reason)

    @property
    def covered_only_by_cancelled_task(self) -> bool | None:
        if self.state is ResourceL2State.RESTRICTED:
            return None
        return bool(
            self.covered_card_count == 0
            and self.cancelled_or_archived_link_count
            and self.cancelled_or_archived_link_count > 0
        )

    def canonical_dict(self) -> dict[str, object]:
        return {
            "resource_type": self.resource_type.value,
            "state": self.state.value,
            "eligible_card_count": self.eligible_card_count,
            "covered_card_count": self.covered_card_count,
            "cancelled_or_archived_link_count": self.cancelled_or_archived_link_count,
            "covered_only_by_cancelled_task": self.covered_only_by_cancelled_task,
            "evidence_refs": list(self.evidence_refs),
            "currentness_reason": self.currentness_reason,
        }


@dataclass(frozen=True, slots=True)
class PolicyResourceReadinessRow:
    spec_id: str
    edition: int
    policies: tuple[PolicyReadinessFact, ...]
    policy_totals: PolicyReadinessTotals
    resources_l1: tuple[ResourceL1Fact, ...]
    resources_l2: tuple[ResourceL2Fact, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "spec_id", _identifier(self.spec_id, field="spec_id"))
        if (
            isinstance(self.edition, bool)
            or not isinstance(self.edition, int)
            or self.edition < 1
        ):
            raise ValueError("policy_resource_edition_invalid")
        if (
            not isinstance(self.policies, tuple)
            or len(self.policies) > MAX_POLICY_FACTS_PER_SPEC
            or any(not isinstance(item, PolicyReadinessFact) for item in self.policies)
        ):
            raise ValueError("policy_resource_policy_facts_invalid")
        if len({item.policy_id for item in self.policies}) != len(self.policies):
            raise ValueError("policy_resource_policy_duplicate")
        if not isinstance(self.policy_totals, PolicyReadinessTotals):
            raise ValueError("policy_resource_policy_totals_invalid")
        if self.policy_totals != PolicyReadinessTotals.from_facts(self.policies):
            raise ValueError("policy_resource_policy_totals_mismatch")
        if not isinstance(self.resources_l1, tuple) or any(
            not isinstance(item, ResourceL1Fact) for item in self.resources_l1
        ):
            raise ValueError("policy_resource_l1_facts_invalid")
        if not isinstance(self.resources_l2, tuple) or any(
            not isinstance(item, ResourceL2Fact) for item in self.resources_l2
        ):
            raise ValueError("policy_resource_l2_facts_invalid")
        if len({item.resource_type for item in self.resources_l1}) != len(
            self.resources_l1
        ):
            raise ValueError("policy_resource_l1_duplicate")
        if len({item.resource_type for item in self.resources_l2}) != len(
            self.resources_l2
        ):
            raise ValueError("policy_resource_l2_duplicate")
        expected_resource_types = set(ResourceType)
        if {
            item.resource_type for item in self.resources_l1
        } != expected_resource_types:
            raise ValueError("policy_resource_l1_authority_set_incomplete")
        if {
            item.resource_type for item in self.resources_l2
        } != expected_resource_types:
            raise ValueError("policy_resource_l2_authority_set_incomplete")

    def canonical_dict(self) -> dict[str, object]:
        return {
            "spec_id": self.spec_id,
            "edition": self.edition,
            "policy": {
                "totals": self.policy_totals.canonical_dict(),
                "facts": [item.canonical_dict() for item in self.policies],
            },
            "resources": {
                "l1": [item.canonical_dict() for item in self.resources_l1],
                "l2": [item.canonical_dict() for item in self.resources_l2],
                "covered_only_by_cancelled_task": sum(
                    item.covered_only_by_cancelled_task is True
                    for item in self.resources_l2
                ),
            },
        }


@dataclass(frozen=True, slots=True)
class PolicyResourceReadinessProjection:
    contract_version: str
    foundation_version: str
    query_fingerprint: str
    filters: tuple[AnalyticsFilterClause, ...]
    as_of: datetime
    population_scope: AnalyticsPopulationScope
    exclusions: AnalyticsExclusionSummary
    specs: tuple[PolicyResourceReadinessRow, ...]
    next_cursor: str | None = None

    def __post_init__(self) -> None:
        if self.contract_version != POLICY_RESOURCE_READINESS_CONTRACT_VERSION:
            raise ValueError("policy_resource_contract_version_unsupported")
        if self.foundation_version != ANALYTICS_FOUNDATION_CONTRACT_VERSION:
            raise ValueError("policy_resource_foundation_version_unsupported")
        if not isinstance(self.query_fingerprint, str) or not _SHA256.fullmatch(
            self.query_fingerprint
        ):
            raise ValueError("policy_resource_query_fingerprint_invalid")
        object.__setattr__(
            self,
            "as_of",
            require_utc_datetime(self.as_of, field="policy_resource_as_of"),
        )
        if not isinstance(self.population_scope, AnalyticsPopulationScope):
            raise ValueError("policy_resource_population_scope_invalid")
        if not isinstance(self.exclusions, AnalyticsExclusionSummary):
            raise ValueError("policy_resource_exclusions_invalid")
        if self.population_scope.excluded_count != self.exclusions.excluded_count:
            raise ValueError("policy_resource_exclusion_population_mismatch")
        if (
            not isinstance(self.specs, tuple)
            or len(self.specs) > MAX_POLICY_RESOURCE_READINESS_ROWS
            or any(
                not isinstance(item, PolicyResourceReadinessRow) for item in self.specs
            )
        ):
            raise ValueError("policy_resource_rows_invalid")
        if len({(item.spec_id, item.edition) for item in self.specs}) != len(
            self.specs
        ):
            raise ValueError("policy_resource_row_duplicate")
        if len(self.specs) != self.population_scope.accessible_count:
            raise ValueError("policy_resource_population_count_mismatch")

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
class PolicyResourceReadinessProjectionPort(Protocol):
    async def project_policy_resource_readiness(
        self,
        context: object,
        query: AnalyticsFoundationQuery,
    ) -> PolicyResourceReadinessProjection: ...


__all__ = [
    "GovernanceCurrentness",
    "MAX_POLICY_FACTS_PER_SPEC",
    "MAX_POLICY_RESOURCE_READINESS_ROWS",
    "MAX_RESOURCE_EVIDENCE_REFS",
    "POLICY_RESOURCE_READINESS_CONTRACT_VERSION",
    "PolicyAuthority",
    "PolicyExceptionFact",
    "PolicyExceptionKind",
    "PolicyReadinessFact",
    "PolicyReadinessState",
    "PolicyReadinessTotals",
    "PolicyResourceReadinessProjection",
    "PolicyResourceReadinessProjectionPort",
    "PolicyResourceReadinessRow",
    "ResourceL1Fact",
    "ResourceL1State",
    "ResourceL2Fact",
    "ResourceL2State",
    "ResourceType",
]
