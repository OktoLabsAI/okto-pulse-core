"""Persistence-neutral contracts for governed flow-health analytics."""

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
    AnalyticsSourceAuthority,
    require_utc_datetime,
)


FLOW_HEALTH_CONTRACT_VERSION = "1"
FLOW_HEALTH_DEFAULT_GENERAL_STALE_HOURS = 72
FLOW_HEALTH_DEFAULT_REJECTED_STALE_HOURS = 96
MAX_FLOW_HEALTH_ITEMS = 10_000
MAX_FLOW_HEALTH_REASONS = 32
MAX_FLOW_HEALTH_REWORK_EPISODES = 256

_SHA256 = re.compile(r"^[0-9a-f]{64}$")


def _text(value: str, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"flow_health_{field}_required")
    return value.strip()


def _whole_hours(value: int, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"flow_health_{field}_invalid")
    return value


def _utc_text(value: datetime) -> str:
    return value.isoformat(timespec="microseconds").replace("+00:00", "Z")


class FlowSubjectType(str, Enum):
    BOARD = "board"
    SPEC = "spec"
    CARD = "card"


class FlowLifecycleState(str, Enum):
    BACKLOG = "backlog"
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    REJECTED = "rejected"
    DONE = "done"
    CANCELLED = "cancelled"
    ARCHIVED = "archived"


class FlowHealthState(str, Enum):
    HEALTHY = "healthy"
    AT_RISK = "at_risk"
    BLOCKED = "blocked"
    STALE = "stale"
    RESTRICTED = "restricted"
    UNAVAILABLE = "unavailable"
    INCONSISTENT = "inconsistent"


class FlowThresholdProvenance(str, Enum):
    DEFAULT = "default"
    OVERRIDE = "override"


class FlowAuthorityState(str, Enum):
    CURRENT = "current"
    MISSING = "missing"
    STALE = "stale"
    INCONSISTENT = "inconsistent"
    RESTRICTED = "restricted"


class FlowBlockerCode(str, Enum):
    SPEC_PENDING_VALIDATION = "spec_pending_validation"
    UNCOVERED_TEST = "uncovered_test"


@dataclass(frozen=True, slots=True)
class FlowSubjectRef:
    subject_type: FlowSubjectType
    subject_id: str

    def __post_init__(self) -> None:
        if not isinstance(self.subject_type, FlowSubjectType):
            raise ValueError("flow_health_subject_type_invalid")
        object.__setattr__(
            self, "subject_id", _text(self.subject_id, field="subject_id")
        )

    def canonical_dict(self) -> dict[str, str]:
        return {"type": self.subject_type.value, "id": self.subject_id}

    @property
    def sort_key(self) -> tuple[str, str]:
        return self.subject_type.value, self.subject_id


@dataclass(frozen=True, slots=True)
class FlowPolicyOverride:
    state: FlowLifecycleState
    stale_hours: int

    def __post_init__(self) -> None:
        if not isinstance(self.state, FlowLifecycleState):
            raise ValueError("flow_health_policy_override_state_invalid")
        if self.state in {FlowLifecycleState.CANCELLED, FlowLifecycleState.ARCHIVED}:
            raise ValueError("flow_health_policy_override_inactive_state")
        object.__setattr__(
            self,
            "stale_hours",
            _whole_hours(self.stale_hours, field="policy_override_hours"),
        )


@dataclass(frozen=True, slots=True)
class FlowHealthPolicy:
    version: int
    authority_ref: str
    general_stale_hours: int = FLOW_HEALTH_DEFAULT_GENERAL_STALE_HOURS
    rejected_stale_hours: int = FLOW_HEALTH_DEFAULT_REJECTED_STALE_HOURS
    overrides: tuple[FlowPolicyOverride, ...] = ()

    def __post_init__(self) -> None:
        if (
            isinstance(self.version, bool)
            or not isinstance(self.version, int)
            or self.version < 1
        ):
            raise ValueError("flow_health_policy_version_invalid")
        object.__setattr__(
            self,
            "authority_ref",
            _text(self.authority_ref, field="policy_authority_ref"),
        )
        object.__setattr__(
            self,
            "general_stale_hours",
            _whole_hours(self.general_stale_hours, field="general_stale_hours"),
        )
        object.__setattr__(
            self,
            "rejected_stale_hours",
            _whole_hours(self.rejected_stale_hours, field="rejected_stale_hours"),
        )
        if not isinstance(self.overrides, tuple) or any(
            not isinstance(item, FlowPolicyOverride) for item in self.overrides
        ):
            raise ValueError("flow_health_policy_overrides_invalid")
        states = tuple(item.state for item in self.overrides)
        if len(set(states)) != len(states):
            raise ValueError("flow_health_policy_override_duplicate")
        if (
            tuple(sorted(self.overrides, key=lambda item: item.state.value))
            != self.overrides
        ):
            raise ValueError("flow_health_policy_overrides_out_of_order")


@dataclass(frozen=True, slots=True)
class EffectiveFlowThreshold:
    state: FlowLifecycleState
    stale_hours: int
    provenance: FlowThresholdProvenance
    policy_version: int
    authority_ref: str

    def __post_init__(self) -> None:
        if not isinstance(self.state, FlowLifecycleState):
            raise ValueError("flow_health_threshold_state_invalid")
        object.__setattr__(
            self,
            "stale_hours",
            _whole_hours(self.stale_hours, field="effective_stale_hours"),
        )
        if not isinstance(self.provenance, FlowThresholdProvenance):
            raise ValueError("flow_health_threshold_provenance_invalid")
        if (
            isinstance(self.policy_version, bool)
            or not isinstance(self.policy_version, int)
            or self.policy_version < 1
        ):
            raise ValueError("flow_health_threshold_policy_version_invalid")
        object.__setattr__(
            self,
            "authority_ref",
            _text(self.authority_ref, field="threshold_authority_ref"),
        )

    def canonical_dict(self) -> dict[str, object]:
        return {
            "state": self.state.value,
            "stale_hours": self.stale_hours,
            "provenance": self.provenance.value,
            "policy_version": self.policy_version,
            "authority_ref": self.authority_ref,
        }


@dataclass(frozen=True, slots=True)
class FlowLifecycleEvent:
    event_id: str
    subject: FlowSubjectRef
    sequence: int
    from_state: FlowLifecycleState | None
    to_state: FlowLifecycleState
    occurred_at: datetime
    authority_ref: str
    rejection_kind: str | None = None
    rejection_code: str | None = None
    rejection_summary: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "event_id", _text(self.event_id, field="event_id"))
        if not isinstance(self.subject, FlowSubjectRef):
            raise ValueError("flow_health_event_subject_invalid")
        if (
            isinstance(self.sequence, bool)
            or not isinstance(self.sequence, int)
            or self.sequence < 1
        ):
            raise ValueError("flow_health_event_sequence_invalid")
        if self.from_state is not None and not isinstance(
            self.from_state, FlowLifecycleState
        ):
            raise ValueError("flow_health_event_from_state_invalid")
        if not isinstance(self.to_state, FlowLifecycleState):
            raise ValueError("flow_health_event_to_state_invalid")
        object.__setattr__(
            self,
            "occurred_at",
            require_utc_datetime(self.occurred_at, field="flow_event_at"),
        )
        object.__setattr__(
            self,
            "authority_ref",
            _text(self.authority_ref, field="event_authority_ref"),
        )
        detail = (self.rejection_kind, self.rejection_code, self.rejection_summary)
        if self.to_state is FlowLifecycleState.REJECTED:
            if any(item is None for item in detail):
                raise ValueError("flow_health_rejection_detail_required")
            for field, value in zip(
                ("rejection_kind", "rejection_code", "rejection_summary"),
                detail,
                strict=True,
            ):
                object.__setattr__(self, field, _text(value, field=field))
        elif any(item is not None for item in detail):
            raise ValueError("flow_health_rejection_detail_unexpected")


@dataclass(frozen=True, slots=True)
class FlowCurrentEpisode:
    state: FlowLifecycleState
    entered_at: datetime
    age_seconds: int
    entry_event_id: str
    authority_ref: str

    def __post_init__(self) -> None:
        if not isinstance(self.state, FlowLifecycleState):
            raise ValueError("flow_health_episode_state_invalid")
        object.__setattr__(
            self,
            "entered_at",
            require_utc_datetime(self.entered_at, field="episode_entered_at"),
        )
        if (
            isinstance(self.age_seconds, bool)
            or not isinstance(self.age_seconds, int)
            or self.age_seconds < 0
        ):
            raise ValueError("flow_health_episode_age_invalid")
        object.__setattr__(
            self, "entry_event_id", _text(self.entry_event_id, field="entry_event_id")
        )
        object.__setattr__(
            self,
            "authority_ref",
            _text(self.authority_ref, field="episode_authority_ref"),
        )

    def canonical_dict(self) -> dict[str, object]:
        return {
            "state": self.state.value,
            "entered_at": _utc_text(self.entered_at),
            "age_seconds": self.age_seconds,
            "entry_event_id": self.entry_event_id,
            "authority_ref": self.authority_ref,
        }


@dataclass(frozen=True, slots=True)
class FlowReworkEpisode:
    attempt: int
    rejected_at: datetime
    rejection_event_id: str
    rejection_kind: str
    rejection_code: str
    rejection_summary: str
    resumed_at: datetime | None = None
    completed_at: datetime | None = None

    def __post_init__(self) -> None:
        if (
            isinstance(self.attempt, bool)
            or not isinstance(self.attempt, int)
            or self.attempt < 1
        ):
            raise ValueError("flow_health_rework_attempt_invalid")
        object.__setattr__(
            self,
            "rejected_at",
            require_utc_datetime(self.rejected_at, field="rework_rejected_at"),
        )
        for field in (
            "rejection_event_id",
            "rejection_kind",
            "rejection_code",
            "rejection_summary",
        ):
            object.__setattr__(self, field, _text(getattr(self, field), field=field))
        previous = self.rejected_at
        for field in ("resumed_at", "completed_at"):
            value = getattr(self, field)
            if value is None:
                continue
            canonical = require_utc_datetime(value, field=f"rework_{field}")
            if canonical < previous:
                raise ValueError("flow_health_rework_time_regressed")
            object.__setattr__(self, field, canonical)
            previous = canonical
        if self.completed_at is not None and self.resumed_at is None:
            raise ValueError("flow_health_rework_completion_without_resume")

    def canonical_dict(self) -> dict[str, object]:
        return {
            "attempt": self.attempt,
            "rejected_at": _utc_text(self.rejected_at),
            "rejection_event_id": self.rejection_event_id,
            "rejection_kind": self.rejection_kind,
            "rejection_code": self.rejection_code,
            "rejection_summary": self.rejection_summary,
            "resumed_at": _utc_text(self.resumed_at) if self.resumed_at else None,
            "completed_at": _utc_text(self.completed_at) if self.completed_at else None,
        }


@dataclass(frozen=True, slots=True)
class FlowBlockerFact:
    code: FlowBlockerCode
    authority_state: FlowAuthorityState
    authority_ref: str | None
    effective_skip: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.code, FlowBlockerCode):
            raise ValueError("flow_health_blocker_code_invalid")
        if not isinstance(self.authority_state, FlowAuthorityState):
            raise ValueError("flow_health_blocker_authority_state_invalid")
        if not isinstance(self.effective_skip, bool):
            raise ValueError("flow_health_blocker_skip_invalid")
        if self.authority_state is FlowAuthorityState.CURRENT:
            object.__setattr__(
                self,
                "authority_ref",
                _text(self.authority_ref, field="blocker_authority_ref"),
            )
        elif self.authority_ref is not None:
            object.__setattr__(
                self,
                "authority_ref",
                _text(self.authority_ref, field="blocker_authority_ref"),
            )
        if self.effective_skip and self.code is not FlowBlockerCode.UNCOVERED_TEST:
            raise ValueError("flow_health_skip_blocker_invalid")


@dataclass(frozen=True, slots=True)
class FlowHealthItem:
    subject: FlowSubjectRef
    state: FlowHealthState
    reason_codes: tuple[str, ...]
    threshold: EffectiveFlowThreshold | None
    current_episode: FlowCurrentEpisode | None
    rework: tuple[FlowReworkEpisode, ...]
    blockers: tuple[FlowBlockerFact, ...]
    source_authority: AnalyticsSourceAuthority

    def __post_init__(self) -> None:
        if not isinstance(self.subject, FlowSubjectRef):
            raise ValueError("flow_health_item_subject_invalid")
        if not isinstance(self.state, FlowHealthState):
            raise ValueError("flow_health_item_state_invalid")
        if (
            not isinstance(self.reason_codes, tuple)
            or len(self.reason_codes) > MAX_FLOW_HEALTH_REASONS
        ):
            raise ValueError("flow_health_reason_codes_invalid")
        reasons = tuple(_text(item, field="reason_code") for item in self.reason_codes)
        if len(set(reasons)) != len(reasons) or tuple(sorted(reasons)) != reasons:
            raise ValueError("flow_health_reason_codes_not_canonical")
        object.__setattr__(self, "reason_codes", reasons)
        if self.threshold is not None and not isinstance(
            self.threshold, EffectiveFlowThreshold
        ):
            raise ValueError("flow_health_item_threshold_invalid")
        if self.current_episode is not None and not isinstance(
            self.current_episode, FlowCurrentEpisode
        ):
            raise ValueError("flow_health_item_episode_invalid")
        if not isinstance(self.rework, tuple) or any(
            not isinstance(item, FlowReworkEpisode) for item in self.rework
        ):
            raise ValueError("flow_health_item_rework_invalid")
        if len(self.rework) > MAX_FLOW_HEALTH_REWORK_EPISODES:
            raise ValueError("flow_health_item_rework_too_many")
        if tuple(item.attempt for item in self.rework) != tuple(
            range(1, len(self.rework) + 1)
        ):
            raise ValueError("flow_health_rework_attempts_not_contiguous")
        if not isinstance(self.blockers, tuple) or any(
            not isinstance(item, FlowBlockerFact) for item in self.blockers
        ):
            raise ValueError("flow_health_item_blockers_invalid")
        if not isinstance(self.source_authority, AnalyticsSourceAuthority):
            raise ValueError("flow_health_source_authority_invalid")
        if self.state is FlowHealthState.RESTRICTED:
            raise ValueError("flow_health_restricted_item_must_be_aggregate_only")
        if self.state in {
            FlowHealthState.UNAVAILABLE,
            FlowHealthState.INCONSISTENT,
        }:
            if self.current_episode is not None or self.rework or self.blockers:
                raise ValueError("flow_health_nonavailable_item_leaks_facts")
        elif self.current_episode is None or self.threshold is None:
            raise ValueError("flow_health_available_item_facts_required")

    def canonical_dict(self) -> dict[str, object]:
        return {
            "subject": self.subject.canonical_dict(),
            "state": self.state.value,
            "reason_codes": list(self.reason_codes),
            "threshold": self.threshold.canonical_dict() if self.threshold else None,
            "current_episode": self.current_episode.canonical_dict()
            if self.current_episode
            else None,
            "rework": [item.canonical_dict() for item in self.rework],
            "blockers": [
                {
                    "code": item.code.value,
                    "authority_state": item.authority_state.value,
                    "authority_ref": item.authority_ref,
                    "effective_skip": item.effective_skip,
                }
                for item in self.blockers
            ],
            "source_authority": self.source_authority.canonical_dict(),
        }


@dataclass(frozen=True, slots=True)
class FlowHealthSummary:
    healthy: int
    at_risk: int
    blocked: int
    stale: int
    restricted: int
    unavailable: int
    inconsistent: int

    def __post_init__(self) -> None:
        for field in (
            "healthy",
            "at_risk",
            "blocked",
            "stale",
            "restricted",
            "unavailable",
            "inconsistent",
        ):
            value = getattr(self, field)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError("flow_health_summary_count_invalid")

    @property
    def total(self) -> int:
        return sum(
            getattr(self, field)
            for field in (
                "healthy",
                "at_risk",
                "blocked",
                "stale",
                "restricted",
                "unavailable",
                "inconsistent",
            )
        )


@dataclass(frozen=True, slots=True)
class FlowHealthProjection:
    contract_version: str
    foundation_version: str
    query_fingerprint: str
    filters: tuple[AnalyticsFilterClause, ...]
    as_of: datetime
    policy: FlowHealthPolicy
    population_scope: AnalyticsPopulationScope
    exclusions: AnalyticsExclusionSummary
    summary: FlowHealthSummary
    items: tuple[FlowHealthItem, ...]
    next_cursor: str | None = None

    def __post_init__(self) -> None:
        if self.contract_version != FLOW_HEALTH_CONTRACT_VERSION:
            raise ValueError("flow_health_contract_version_unsupported")
        if self.foundation_version != ANALYTICS_FOUNDATION_CONTRACT_VERSION:
            raise ValueError("flow_health_foundation_version_unsupported")
        if not isinstance(self.query_fingerprint, str) or not _SHA256.fullmatch(
            self.query_fingerprint
        ):
            raise ValueError("flow_health_query_fingerprint_invalid")
        object.__setattr__(
            self, "as_of", require_utc_datetime(self.as_of, field="flow_health_as_of")
        )
        if not isinstance(self.policy, FlowHealthPolicy):
            raise ValueError("flow_health_policy_invalid")
        if not isinstance(self.population_scope, AnalyticsPopulationScope):
            raise ValueError("flow_health_population_scope_invalid")
        if not isinstance(self.exclusions, AnalyticsExclusionSummary):
            raise ValueError("flow_health_exclusions_invalid")
        if not isinstance(self.summary, FlowHealthSummary):
            raise ValueError("flow_health_summary_invalid")
        if not isinstance(self.items, tuple) or any(
            not isinstance(item, FlowHealthItem) for item in self.items
        ):
            raise ValueError("flow_health_items_invalid")
        if len(self.items) > MAX_FLOW_HEALTH_ITEMS:
            raise ValueError("flow_health_items_too_many")
        identities = tuple(item.subject for item in self.items)
        if (
            len(set(identities)) != len(identities)
            or tuple(sorted(identities, key=lambda item: item.sort_key)) != identities
        ):
            raise ValueError("flow_health_items_not_canonical")
        if self.summary.total != len(self.items) + self.summary.restricted:
            raise ValueError("flow_health_summary_total_mismatch")
        if self.population_scope.accessible_count != len(self.items):
            raise ValueError("flow_health_population_total_mismatch")
        if self.summary.restricted != self.exclusions.restricted_count:
            raise ValueError("flow_health_restricted_total_mismatch")
        observed = {state: 0 for state in FlowHealthState}
        for item in self.items:
            observed[item.state] += 1
        expected = {
            FlowHealthState.HEALTHY: self.summary.healthy,
            FlowHealthState.AT_RISK: self.summary.at_risk,
            FlowHealthState.BLOCKED: self.summary.blocked,
            FlowHealthState.STALE: self.summary.stale,
            FlowHealthState.RESTRICTED: 0,
            FlowHealthState.UNAVAILABLE: self.summary.unavailable,
            FlowHealthState.INCONSISTENT: self.summary.inconsistent,
        }
        if observed != expected:
            raise ValueError("flow_health_summary_state_mismatch")
        if self.next_cursor is not None:
            object.__setattr__(
                self, "next_cursor", _text(self.next_cursor, field="next_cursor")
            )

    def canonical_dict(self) -> dict[str, object]:
        return {
            "contract_version": self.contract_version,
            "foundation_version": self.foundation_version,
            "query_fingerprint": self.query_fingerprint,
            "filters": [item.canonical_dict() for item in self.filters],
            "as_of": _utc_text(self.as_of),
            "effective_policy": {
                "version": self.policy.version,
                "authority_ref": self.policy.authority_ref,
                "general_stale_hours": self.policy.general_stale_hours,
                "rejected_stale_hours": self.policy.rejected_stale_hours,
                "overrides": [
                    {"state": item.state.value, "stale_hours": item.stale_hours}
                    for item in self.policy.overrides
                ],
            },
            "population_scope": self.population_scope.canonical_dict(),
            "exclusions": self.exclusions.canonical_dict(),
            "summary": {
                "healthy": self.summary.healthy,
                "at_risk": self.summary.at_risk,
                "blocked": self.summary.blocked,
                "stale": self.summary.stale,
                "restricted": self.summary.restricted,
                "unavailable": self.summary.unavailable,
                "inconsistent": self.summary.inconsistent,
            },
            "items": [item.canonical_dict() for item in self.items],
            "next_cursor": self.next_cursor,
        }


@runtime_checkable
class FlowHealthProjectionPort(Protocol):
    def project(
        self, context: object, query: AnalyticsFoundationQuery
    ) -> FlowHealthProjection: ...


__all__ = [
    "EffectiveFlowThreshold",
    "FLOW_HEALTH_CONTRACT_VERSION",
    "FLOW_HEALTH_DEFAULT_GENERAL_STALE_HOURS",
    "FLOW_HEALTH_DEFAULT_REJECTED_STALE_HOURS",
    "FlowAuthorityState",
    "FlowBlockerCode",
    "FlowBlockerFact",
    "FlowCurrentEpisode",
    "FlowHealthItem",
    "FlowHealthPolicy",
    "FlowHealthProjection",
    "FlowHealthProjectionPort",
    "FlowHealthState",
    "FlowHealthSummary",
    "FlowLifecycleEvent",
    "FlowLifecycleState",
    "FlowPolicyOverride",
    "FlowReworkEpisode",
    "FlowSubjectRef",
    "FlowSubjectType",
    "FlowThresholdProvenance",
]
