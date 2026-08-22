"""Closed provenance/currentness contract shared by governed Analytics reads."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from okto_pulse.core.ports.analytics_foundation import (
    AnalyticsSourceAuthority,
    require_utc_datetime,
)


class AnalyticsProjectionCurrentness(str, Enum):
    CURRENT = "current"
    PARTIAL = "partial"
    STALE = "stale"
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True, slots=True)
class AnalyticsProjectionProvenance:
    observed_at: datetime
    currentness: AnalyticsProjectionCurrentness
    sources: tuple[AnalyticsSourceAuthority, ...]
    reason: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "observed_at",
            require_utc_datetime(self.observed_at, field="provenance_observed_at"),
        )
        if not isinstance(self.currentness, AnalyticsProjectionCurrentness):
            raise ValueError("analytics_provenance_currentness_invalid")
        if not isinstance(self.sources, tuple) or any(
            not isinstance(item, AnalyticsSourceAuthority) for item in self.sources
        ):
            raise ValueError("analytics_provenance_sources_invalid")
        canonical = tuple(
            sorted(
                self.sources,
                key=lambda item: (
                    item.authority,
                    item.reference,
                    item.timestamp_field,
                ),
            )
        )
        if canonical != self.sources or len(set(canonical)) != len(canonical):
            raise ValueError("analytics_provenance_sources_not_canonical")
        if self.currentness is AnalyticsProjectionCurrentness.CURRENT:
            if not self.sources or self.reason is not None:
                raise ValueError("analytics_provenance_current_shape_invalid")
        elif not isinstance(self.reason, str) or not self.reason.strip():
            raise ValueError("analytics_provenance_reason_required")
        elif self.sources and self.currentness is AnalyticsProjectionCurrentness.UNAVAILABLE:
            raise ValueError("analytics_provenance_unavailable_source_leak")
        if self.reason is not None:
            object.__setattr__(self, "reason", self.reason.strip())

    def canonical_dict(self) -> dict[str, object]:
        return {
            "observed_at": self.observed_at.isoformat(timespec="microseconds").replace(
                "+00:00", "Z"
            ),
            "currentness": self.currentness.value,
            "reason": self.reason,
            "sources": [item.canonical_dict() for item in self.sources],
        }


__all__ = [
    "AnalyticsProjectionCurrentness",
    "AnalyticsProjectionProvenance",
]
