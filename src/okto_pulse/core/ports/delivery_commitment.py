"""Read-only Analytics contract for Sprint delivery commitment."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol, runtime_checkable

from okto_pulse.core.ports.analytics_foundation import (
    ANALYTICS_FOUNDATION_CONTRACT_VERSION,
    AnalyticsExclusionSummary,
    AnalyticsFilterClause,
    AnalyticsPopulationScope,
    require_utc_datetime,
)
from okto_pulse.core.ports.sprint_activation_baseline import SprintCommitmentState


DELIVERY_COMMITMENT_CONTRACT_VERSION = "1"
MAX_DELIVERY_COMMITMENT_SPRINTS = 10_000
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


def _text(value: str, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"delivery_commitment_{field}_required")
    return value.strip()


def _count(value: int | None, *, field: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"delivery_commitment_{field}_invalid")
    return value


def _utc_text(value: datetime) -> str:
    return value.isoformat(timespec="microseconds").replace("+00:00", "Z")


@dataclass(frozen=True, slots=True)
class SprintCommitmentSlice:
    sprint_id: str
    state: SprintCommitmentState
    baseline_ref: str | None
    activated_at: datetime | None
    original_member_count: int | None
    current_member_count: int | None
    added_count: int | None
    removed_count: int | None
    unavailable_reason: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "sprint_id", _text(self.sprint_id, field="sprint_id"))
        if not isinstance(self.state, SprintCommitmentState):
            raise ValueError("delivery_commitment_state_invalid")
        numeric_fields = (
            "original_member_count",
            "current_member_count",
            "added_count",
            "removed_count",
        )
        for field in numeric_fields:
            object.__setattr__(self, field, _count(getattr(self, field), field=field))
        if self.state is SprintCommitmentState.AVAILABLE:
            if self.baseline_ref is None or self.activated_at is None:
                raise ValueError("delivery_commitment_available_authority_required")
            object.__setattr__(
                self, "baseline_ref", _text(self.baseline_ref, field="baseline_ref")
            )
            object.__setattr__(
                self,
                "activated_at",
                require_utc_datetime(
                    self.activated_at, field="commitment_activated_at"
                ),
            )
            if any(getattr(self, field) is None for field in numeric_fields):
                raise ValueError("delivery_commitment_available_counts_required")
            if self.unavailable_reason is not None:
                raise ValueError("delivery_commitment_available_reason_unexpected")
        else:
            if (
                self.baseline_ref is not None
                or self.activated_at is not None
                or any(getattr(self, field) is not None for field in numeric_fields)
            ):
                raise ValueError("delivery_commitment_legacy_numeric_leak")
            object.__setattr__(
                self,
                "unavailable_reason",
                _text(self.unavailable_reason, field="unavailable_reason"),
            )

    def canonical_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "sprint_id": self.sprint_id,
            "state": self.state.value,
            "baseline_ref": self.baseline_ref,
            "unavailable_reason": self.unavailable_reason,
        }
        if self.state is SprintCommitmentState.AVAILABLE:
            payload.update(
                {
                    "activated_at": _utc_text(self.activated_at),  # type: ignore[arg-type]
                    "original_member_count": self.original_member_count,
                    "current_member_count": self.current_member_count,
                    "added_count": self.added_count,
                    "removed_count": self.removed_count,
                }
            )
        return payload


@dataclass(frozen=True, slots=True)
class DeliveryCommitmentProjection:
    contract_version: str
    foundation_version: str
    query_fingerprint: str
    filters: tuple[AnalyticsFilterClause, ...]
    as_of: datetime
    population_scope: AnalyticsPopulationScope
    exclusions: AnalyticsExclusionSummary
    sprints: tuple[SprintCommitmentSlice, ...]
    next_cursor: str | None = None

    def __post_init__(self) -> None:
        if self.contract_version != DELIVERY_COMMITMENT_CONTRACT_VERSION:
            raise ValueError("delivery_commitment_contract_version_unsupported")
        if self.foundation_version != ANALYTICS_FOUNDATION_CONTRACT_VERSION:
            raise ValueError("delivery_commitment_foundation_version_unsupported")
        if not isinstance(self.query_fingerprint, str) or not _SHA256.fullmatch(
            self.query_fingerprint
        ):
            raise ValueError("delivery_commitment_query_fingerprint_invalid")
        object.__setattr__(
            self,
            "as_of",
            require_utc_datetime(self.as_of, field="delivery_commitment_as_of"),
        )
        if not isinstance(self.population_scope, AnalyticsPopulationScope):
            raise ValueError("delivery_commitment_population_scope_invalid")
        if not isinstance(self.exclusions, AnalyticsExclusionSummary):
            raise ValueError("delivery_commitment_exclusions_invalid")
        if not isinstance(self.sprints, tuple) or any(
            not isinstance(item, SprintCommitmentSlice) for item in self.sprints
        ):
            raise ValueError("delivery_commitment_sprints_invalid")
        if len(self.sprints) > MAX_DELIVERY_COMMITMENT_SPRINTS:
            raise ValueError("delivery_commitment_sprints_too_many")
        ids = tuple(item.sprint_id for item in self.sprints)
        if len(set(ids)) != len(ids) or tuple(sorted(ids)) != ids:
            raise ValueError("delivery_commitment_sprints_not_canonical")
        if self.population_scope.accessible_count != len(self.sprints):
            raise ValueError("delivery_commitment_population_total_mismatch")
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
            "population_scope": self.population_scope.canonical_dict(),
            "exclusions": self.exclusions.canonical_dict(),
            "sprints": [item.canonical_dict() for item in self.sprints],
            "next_cursor": self.next_cursor,
        }


@runtime_checkable
class DeliveryCommitmentProjectionPort(Protocol):
    def project(
        self, context: object, query: object
    ) -> DeliveryCommitmentProjection: ...


__all__ = [
    "DELIVERY_COMMITMENT_CONTRACT_VERSION",
    "DeliveryCommitmentProjection",
    "DeliveryCommitmentProjectionPort",
    "SprintCommitmentSlice",
]
