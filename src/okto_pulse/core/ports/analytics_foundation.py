"""Persistence-neutral contract for the canonical Analytics foundation.

This module is intentionally limited to immutable DTOs and a projection
protocol. Core services own metric semantics; editions provide bounded data
access without redefining result states or temporal rules.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Protocol, runtime_checkable


# Version 3 is the coordinated replacement for the legacy analytics v2 DTOs.
ANALYTICS_FOUNDATION_CONTRACT_VERSION = "3"
MAX_ANALYTICS_EXCLUSION_REASONS = 32
MAX_ANALYTICS_FILTERS = 64
MAX_ANALYTICS_FILTER_VALUES = 256

_METRIC_ID = re.compile(r"^[a-z][a-z0-9_.-]{0,127}$")
_REASON_CODE = re.compile(r"^[a-z][a-z0-9_.-]{0,127}$")
_FILTER_OPERATORS = frozenset(
    {"eq", "ne", "in", "not_in", "gte", "gt", "lte", "lt", "contains"}
)

AnalyticsScalar = str | int | float | bool | None
AnalyticsFilterValue = AnalyticsScalar | tuple[AnalyticsScalar, ...]
AnalyticsNumber = int | float


class AnalyticsResultState(str, Enum):
    AVAILABLE = "available"
    EMPTY = "empty"
    NOT_APPLICABLE = "not_applicable"
    RESTRICTED = "restricted"
    UNAVAILABLE = "unavailable"
    STALE = "stale"


class AnalyticsMetricFamily(str, Enum):
    SNAPSHOT = "snapshot"
    EVENT_FLOW = "event_flow"
    COHORT = "cohort"


class AnalyticsEvidenceCurrentness(str, Enum):
    CURRENT = "current"
    PREVIOUS = "previous"
    MISSING = "missing"
    STALE = "stale"


def require_utc_datetime(value: datetime, *, field: str) -> datetime:
    """Return ``value`` in canonical UTC or reject before a query runs."""

    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError(f"analytics_{field}_must_be_timezone_aware_utc")
    if value.utcoffset() != timedelta(0):
        raise ValueError(f"analytics_{field}_must_be_utc")
    return value.astimezone(timezone.utc)


def _utc_text(value: datetime) -> str:
    return value.isoformat(timespec="microseconds").replace("+00:00", "Z")


def _require_nonempty(value: str, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"analytics_{field}_required")
    return value.strip()


def _require_number(value: AnalyticsNumber, *, field: str) -> AnalyticsNumber:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"analytics_{field}_must_be_numeric")
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError(f"analytics_{field}_must_be_finite")
    return value


def _canonical_scalar(value: AnalyticsScalar) -> AnalyticsScalar:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float) and math.isfinite(value):
        return value
    raise ValueError("analytics_filter_value_invalid")


@dataclass(frozen=True, slots=True)
class AnalyticsUtcWindow:
    from_inclusive: datetime
    to_exclusive: datetime

    def __post_init__(self) -> None:
        start = require_utc_datetime(self.from_inclusive, field="window_from")
        end = require_utc_datetime(self.to_exclusive, field="window_to")
        if start >= end:
            raise ValueError("analytics_temporal_query_invalid")
        object.__setattr__(self, "from_inclusive", start)
        object.__setattr__(self, "to_exclusive", end)

    def contains(self, instant: datetime) -> bool:
        candidate = require_utc_datetime(instant, field="event_timestamp")
        return self.from_inclusive <= candidate < self.to_exclusive

    def canonical_dict(self) -> dict[str, str]:
        return {
            "from": _utc_text(self.from_inclusive),
            "to": _utc_text(self.to_exclusive),
        }


@dataclass(frozen=True, slots=True)
class AnalyticsFilterClause:
    field: str
    operator: str
    value: AnalyticsFilterValue = None

    def __post_init__(self) -> None:
        field = _require_nonempty(self.field, field="filter_field")
        operator = _require_nonempty(self.operator, field="filter_operator")
        if operator not in _FILTER_OPERATORS:
            raise ValueError("analytics_filter_operator_unsupported")
        raw = self.value
        if isinstance(raw, tuple):
            if len(raw) > MAX_ANALYTICS_FILTER_VALUES:
                raise ValueError("analytics_filter_values_too_many")
            value: AnalyticsFilterValue = tuple(_canonical_scalar(item) for item in raw)
        else:
            value = _canonical_scalar(raw)
        if operator in {"in", "not_in"} and not isinstance(value, tuple):
            raise ValueError("analytics_filter_collection_required")
        if operator not in {"in", "not_in"} and isinstance(value, tuple):
            raise ValueError("analytics_filter_scalar_required")
        object.__setattr__(self, "field", field)
        object.__setattr__(self, "operator", operator)
        object.__setattr__(self, "value", value)

    def canonical_dict(self) -> dict[str, object]:
        return {
            "field": self.field,
            "operator": self.operator,
            "value": list(self.value) if isinstance(self.value, tuple) else self.value,
        }


@dataclass(frozen=True, slots=True)
class AnalyticsFoundationQuery:
    board_id: str
    actor_scope_ref: str
    window: AnalyticsUtcWindow
    filters: tuple[AnalyticsFilterClause, ...] = ()
    as_of: datetime | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "board_id", _require_nonempty(self.board_id, field="board_id")
        )
        object.__setattr__(
            self,
            "actor_scope_ref",
            _require_nonempty(self.actor_scope_ref, field="actor_scope_ref"),
        )
        if not isinstance(self.window, AnalyticsUtcWindow):
            raise ValueError("analytics_window_required")
        if not isinstance(self.filters, tuple) or any(
            not isinstance(item, AnalyticsFilterClause) for item in self.filters
        ):
            raise ValueError("analytics_filters_invalid")
        if len(self.filters) > MAX_ANALYTICS_FILTERS:
            raise ValueError("analytics_filters_too_many")
        if self.as_of is not None:
            object.__setattr__(
                self, "as_of", require_utc_datetime(self.as_of, field="as_of")
            )

    @property
    def fingerprint(self) -> str:
        filters = sorted(
            (item.canonical_dict() for item in self.filters),
            key=lambda item: json.dumps(item, sort_keys=True, separators=(",", ":")),
        )
        payload = {
            "contract_version": ANALYTICS_FOUNDATION_CONTRACT_VERSION,
            "board_id": self.board_id,
            "actor_scope_ref": self.actor_scope_ref,
            "window": self.window.canonical_dict(),
            "filters": filters,
            "as_of": (_utc_text(self.as_of) if self.as_of is not None else None),
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
        return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True, slots=True)
class AnalyticsExclusion:
    reason: str
    count: int

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "reason", _require_nonempty(self.reason, field="exclusion_reason")
        )
        if not _REASON_CODE.fullmatch(self.reason):
            raise ValueError("analytics_exclusion_reason_invalid")
        if (
            isinstance(self.count, bool)
            or not isinstance(self.count, int)
            or self.count < 0
        ):
            raise ValueError("analytics_exclusion_count_invalid")

    def canonical_dict(self) -> dict[str, object]:
        return {"reason": self.reason, "count": self.count}


@dataclass(frozen=True, slots=True)
class AnalyticsExclusionSummary:
    restricted_count: int = 0
    excluded_count: int = 0
    reasons: tuple[AnalyticsExclusion, ...] = ()

    def __post_init__(self) -> None:
        for name, value in (
            ("restricted_count", self.restricted_count),
            ("excluded_count", self.excluded_count),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"analytics_{name}_invalid")
        if len(self.reasons) > MAX_ANALYTICS_EXCLUSION_REASONS:
            raise ValueError("analytics_exclusion_reasons_too_many")
        if any(not isinstance(item, AnalyticsExclusion) for item in self.reasons):
            raise ValueError("analytics_exclusion_reasons_invalid")
        if sum(item.count for item in self.reasons) > self.excluded_count:
            raise ValueError("analytics_exclusion_reason_count_exceeds_total")
        if self.restricted_count > self.excluded_count:
            raise ValueError("analytics_restricted_count_exceeds_excluded")

    def canonical_dict(self) -> dict[str, object]:
        return {
            "restricted_count": self.restricted_count,
            "excluded_count": self.excluded_count,
            "reasons": [item.canonical_dict() for item in self.reasons],
        }


@dataclass(frozen=True, slots=True)
class AnalyticsPopulationScope:
    scope_ref: str
    accessible_count: int
    excluded_count: int = 0

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "scope_ref",
            _require_nonempty(self.scope_ref, field="population_scope"),
        )
        for name, value in (
            ("accessible_count", self.accessible_count),
            ("excluded_count", self.excluded_count),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"analytics_population_{name}_invalid")

    def canonical_dict(self) -> dict[str, object]:
        return {
            "scope_ref": self.scope_ref,
            "accessible_count": self.accessible_count,
            "excluded_count": self.excluded_count,
        }


@dataclass(frozen=True, slots=True)
class AnalyticsSourceAuthority:
    authority: str
    reference: str
    timestamp_field: str

    def __post_init__(self) -> None:
        for name in ("authority", "reference", "timestamp_field"):
            object.__setattr__(
                self,
                name,
                _require_nonempty(getattr(self, name), field=f"source_{name}"),
            )

    def canonical_dict(self) -> dict[str, str]:
        return {
            "authority": self.authority,
            "reference": self.reference,
            "timestamp_field": self.timestamp_field,
        }


@dataclass(frozen=True, slots=True)
class AnalyticsTemporalSemantics:
    family: AnalyticsMetricFamily
    authoritative_timestamp_field: str
    window: AnalyticsUtcWindow
    as_of: datetime | None = None
    event_source: str | None = None
    cohort_id: str | None = None
    membership_cut: datetime | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.family, AnalyticsMetricFamily):
            raise ValueError("analytics_metric_family_invalid")
        object.__setattr__(
            self,
            "authoritative_timestamp_field",
            _require_nonempty(
                self.authoritative_timestamp_field,
                field="authoritative_timestamp_field",
            ),
        )
        if not isinstance(self.window, AnalyticsUtcWindow):
            raise ValueError("analytics_temporal_window_required")
        if self.as_of is not None:
            object.__setattr__(
                self, "as_of", require_utc_datetime(self.as_of, field="metric_as_of")
            )
        if self.membership_cut is not None:
            object.__setattr__(
                self,
                "membership_cut",
                require_utc_datetime(self.membership_cut, field="membership_cut"),
            )

        if self.family is AnalyticsMetricFamily.SNAPSHOT:
            if (
                self.as_of is None
                or self.event_source is not None
                or self.cohort_id is not None
                or self.membership_cut is not None
            ):
                raise ValueError("analytics_snapshot_semantics_invalid")
        elif self.family is AnalyticsMetricFamily.EVENT_FLOW:
            if (
                not self.event_source
                or self.as_of is not None
                or self.cohort_id is not None
                or self.membership_cut is not None
            ):
                raise ValueError("analytics_event_flow_semantics_invalid")
            object.__setattr__(
                self,
                "event_source",
                _require_nonempty(self.event_source, field="event_source"),
            )
        elif self.family is AnalyticsMetricFamily.COHORT:
            if (
                not self.cohort_id
                or self.membership_cut is None
                or self.as_of is not None
                or self.event_source is not None
            ):
                raise ValueError("analytics_cohort_semantics_invalid")
            object.__setattr__(
                self,
                "cohort_id",
                _require_nonempty(self.cohort_id, field="cohort_id"),
            )

    def canonical_dict(self) -> dict[str, object]:
        return {
            "family": self.family.value,
            "authoritative_timestamp_field": self.authoritative_timestamp_field,
            "window": self.window.canonical_dict(),
            "as_of": _utc_text(self.as_of) if self.as_of is not None else None,
            "event_source": self.event_source,
            "cohort_id": self.cohort_id,
            "membership_cut": (
                _utc_text(self.membership_cut)
                if self.membership_cut is not None
                else None
            ),
        }


@dataclass(frozen=True, slots=True)
class AnalyticsMetricEnvelope:
    metric_id: str
    family: AnalyticsMetricFamily
    state: AnalyticsResultState
    numerator: AnalyticsNumber | None
    denominator: AnalyticsNumber | None
    value: AnalyticsNumber | None
    n: int
    exclusions: AnalyticsExclusionSummary
    as_of: datetime
    filters: tuple[AnalyticsFilterClause, ...]
    source: AnalyticsSourceAuthority
    population_scope: AnalyticsPopulationScope
    temporal_semantics: AnalyticsTemporalSemantics
    reason: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.metric_id, str) or not _METRIC_ID.fullmatch(
            self.metric_id
        ):
            raise ValueError("analytics_metric_id_invalid")
        if not isinstance(self.family, AnalyticsMetricFamily):
            raise ValueError("analytics_metric_family_invalid")
        if not isinstance(self.state, AnalyticsResultState):
            raise ValueError("analytics_result_state_invalid")
        if not isinstance(self.temporal_semantics, AnalyticsTemporalSemantics):
            raise ValueError("analytics_temporal_semantics_invalid")
        if self.family is not self.temporal_semantics.family:
            raise ValueError("analytics_metric_family_semantics_mismatch")
        object.__setattr__(
            self, "as_of", require_utc_datetime(self.as_of, field="metric_as_of")
        )
        if isinstance(self.n, bool) or not isinstance(self.n, int) or self.n < 0:
            raise ValueError("analytics_sample_size_invalid")
        if not isinstance(self.filters, tuple) or any(
            not isinstance(item, AnalyticsFilterClause) for item in self.filters
        ):
            raise ValueError("analytics_metric_filters_invalid")
        if not isinstance(self.exclusions, AnalyticsExclusionSummary):
            raise ValueError("analytics_exclusions_invalid")
        if not isinstance(self.source, AnalyticsSourceAuthority):
            raise ValueError("analytics_source_invalid")
        if not isinstance(self.population_scope, AnalyticsPopulationScope):
            raise ValueError("analytics_population_scope_invalid")
        if self.exclusions.excluded_count != self.population_scope.excluded_count:
            raise ValueError("analytics_exclusion_population_mismatch")
        if self.reason is not None:
            object.__setattr__(
                self, "reason", _require_nonempty(self.reason, field="result_reason")
            )
            if not _REASON_CODE.fullmatch(self.reason):
                raise ValueError("analytics_result_reason_invalid")

        numeric = (self.numerator, self.denominator, self.value)
        if self.state is AnalyticsResultState.AVAILABLE:
            if any(item is None for item in numeric):
                raise ValueError("analytics_available_numeric_fields_required")
            numerator = _require_number(self.numerator, field="numerator")
            denominator = _require_number(self.denominator, field="denominator")
            _require_number(self.value, field="value")
            if denominator <= 0 or numerator < 0:
                raise ValueError("analytics_available_ratio_invalid")
        elif self.state is AnalyticsResultState.NOT_APPLICABLE:
            if (self.numerator, self.denominator, self.value, self.n) != (
                0,
                0,
                None,
                0,
            ):
                raise ValueError("analytics_not_applicable_shape_invalid")
            if self.reason is None:
                raise ValueError("analytics_not_applicable_reason_required")
        elif self.state is AnalyticsResultState.EMPTY:
            if numeric != (None, None, None) or self.n != 0:
                raise ValueError("analytics_empty_shape_invalid")
        else:
            if numeric != (None, None, None) or self.n != 0:
                raise ValueError("analytics_non_numeric_state_shape_invalid")
            if self.reason is None:
                raise ValueError("analytics_non_numeric_state_reason_required")

    def canonical_dict(self) -> dict[str, object]:
        return {
            "metric_id": self.metric_id,
            "family": self.family.value,
            "state": self.state.value,
            "numerator": self.numerator,
            "denominator": self.denominator,
            "value": self.value,
            "n": self.n,
            "exclusions": self.exclusions.canonical_dict(),
            "as_of": _utc_text(self.as_of),
            "filters": [item.canonical_dict() for item in self.filters],
            "source": self.source.canonical_dict(),
            "population_scope": self.population_scope.canonical_dict(),
            "temporal_semantics": self.temporal_semantics.canonical_dict(),
            "reason": self.reason,
        }


@dataclass(frozen=True, slots=True)
class AnalyticsFoundationProjection:
    contract_version: str
    query_fingerprint: str
    filters: tuple[AnalyticsFilterClause, ...]
    as_of: datetime
    metrics: tuple[AnalyticsMetricEnvelope, ...]

    def __post_init__(self) -> None:
        if self.contract_version != ANALYTICS_FOUNDATION_CONTRACT_VERSION:
            raise ValueError("analytics_contract_version_unsupported")
        if not re.fullmatch(r"[0-9a-f]{64}", self.query_fingerprint):
            raise ValueError("analytics_query_fingerprint_invalid")
        object.__setattr__(
            self, "as_of", require_utc_datetime(self.as_of, field="projection_as_of")
        )
        if not isinstance(self.filters, tuple) or any(
            not isinstance(item, AnalyticsFilterClause) for item in self.filters
        ):
            raise ValueError("analytics_projection_filters_invalid")
        if len(self.filters) > MAX_ANALYTICS_FILTERS:
            raise ValueError("analytics_filters_too_many")
        if not isinstance(self.metrics, tuple) or any(
            not isinstance(item, AnalyticsMetricEnvelope) for item in self.metrics
        ):
            raise ValueError("analytics_metrics_invalid")
        if any(item.filters != self.filters for item in self.metrics):
            raise ValueError("analytics_metric_filter_mismatch")
        if any(item.as_of != self.as_of for item in self.metrics):
            raise ValueError("analytics_metric_as_of_mismatch")

    def canonical_dict(self) -> dict[str, object]:
        return {
            "contract_version": self.contract_version,
            "query_fingerprint": self.query_fingerprint,
            "filters": [item.canonical_dict() for item in self.filters],
            "as_of": _utc_text(self.as_of),
            "metrics": [item.canonical_dict() for item in self.metrics],
        }


@runtime_checkable
class AnalyticsFoundationProjectionPort(Protocol):
    async def project(
        self,
        context: object,
        query: AnalyticsFoundationQuery,
    ) -> AnalyticsFoundationProjection: ...


__all__ = [
    "ANALYTICS_FOUNDATION_CONTRACT_VERSION",
    "MAX_ANALYTICS_EXCLUSION_REASONS",
    "MAX_ANALYTICS_FILTERS",
    "MAX_ANALYTICS_FILTER_VALUES",
    "AnalyticsEvidenceCurrentness",
    "AnalyticsExclusion",
    "AnalyticsExclusionSummary",
    "AnalyticsFilterClause",
    "AnalyticsFoundationProjection",
    "AnalyticsFoundationProjectionPort",
    "AnalyticsFoundationQuery",
    "AnalyticsMetricEnvelope",
    "AnalyticsMetricFamily",
    "AnalyticsNumber",
    "AnalyticsPopulationScope",
    "AnalyticsResultState",
    "AnalyticsSourceAuthority",
    "AnalyticsTemporalSemantics",
    "AnalyticsUtcWindow",
    "require_utc_datetime",
]
