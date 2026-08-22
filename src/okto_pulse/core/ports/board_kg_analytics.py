"""Canonical Board KG Analytics output contract."""

from __future__ import annotations

import hashlib
import json
import math
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
    AnalyticsUtcWindow,
    require_utc_datetime,
)
from okto_pulse.core.ports.analytics_provenance import (
    AnalyticsProjectionCurrentness,
    AnalyticsProjectionProvenance,
)


BOARD_KG_ANALYTICS_CONTRACT_VERSION = "2"
LEGACY_BOARD_KG_ANALYTICS_CONTRACT_VERSION = "1"
MAX_BOARD_KG_COMPONENTS = 32
MAX_BOARD_KG_REASON_CODES = 64
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


def _text(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"board_kg_analytics_{field}_required")
    return value.strip()


def _count(value: int | None, *, field: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"board_kg_analytics_{field}_invalid")
    return value


def _utc_text(value: datetime) -> str:
    return value.isoformat(timespec="microseconds").replace("+00:00", "Z")


class BoardKgHealthState(str, Enum):
    HEALTHY = "healthy"
    AT_RISK = "at_risk"
    BACKPRESSURE = "backpressure"
    RECOVERY_NEEDED = "recovery_needed"
    QUARANTINED = "quarantined"


class BoardKgClassificationState(str, Enum):
    """Fail-safe product classification for the complete v2 projection.

    The operational KG health vocabulary remains authoritative for individual
    components.  The product classification is deliberately separate so a
    missing required metric can never be serialized as ``healthy``.
    """

    HEALTHY = "healthy"
    AT_RISK = "at_risk"
    BLOCKING = "blocking"
    UNAVAILABLE = "unavailable"
    RESTRICTED = "restricted"
    ERROR = "error"


class BoardKgAnalyticsResultState(str, Enum):
    AVAILABLE = "available"
    PARTIAL = "partial"
    RESTRICTED = "restricted"
    UNAVAILABLE = "unavailable"
    EMPTY = "empty"
    ERROR = "error"


@dataclass(frozen=True, slots=True)
class BoardKgHealthComponent:
    component: str
    health_state: BoardKgHealthState
    result_state: BoardKgAnalyticsResultState
    classification_reason: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "component", _text(self.component, field="component"))
        if not isinstance(self.health_state, BoardKgHealthState):
            raise ValueError("board_kg_analytics_component_health_invalid")
        if not isinstance(self.result_state, BoardKgAnalyticsResultState):
            raise ValueError("board_kg_analytics_component_result_invalid")
        object.__setattr__(
            self,
            "classification_reason",
            _text(self.classification_reason, field="classification_reason"),
        )

    def canonical_dict(self) -> dict[str, str]:
        return {
            "component": self.component,
            "health_state": self.health_state.value,
            "result_state": self.result_state.value,
            "classification_reason": self.classification_reason,
        }


@dataclass(frozen=True, slots=True)
class BoardKgHealthEvidenceSnapshot:
    """Strict public projection of the health facts consumed by Analytics."""

    board_id: str
    health_state: BoardKgHealthState
    result_state: BoardKgAnalyticsResultState
    classification_reason: str
    reason_codes: tuple[str, ...]
    components: tuple[BoardKgHealthComponent, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "board_id", _text(self.board_id, field="board_id"))
        if not isinstance(self.health_state, BoardKgHealthState):
            raise ValueError("board_kg_analytics_health_state_invalid")
        if not isinstance(self.result_state, BoardKgAnalyticsResultState):
            raise ValueError("board_kg_analytics_health_result_state_invalid")
        object.__setattr__(
            self,
            "classification_reason",
            _text(self.classification_reason, field="classification_reason"),
        )
        if (
            not isinstance(self.reason_codes, tuple)
            or len(self.reason_codes) > MAX_BOARD_KG_REASON_CODES
        ):
            raise ValueError("board_kg_analytics_reason_codes_invalid")
        reasons = tuple(_text(item, field="reason_code") for item in self.reason_codes)
        if tuple(sorted(set(reasons))) != reasons:
            raise ValueError("board_kg_analytics_reason_codes_not_canonical")
        object.__setattr__(self, "reason_codes", reasons)
        if not isinstance(self.components, tuple) or any(
            not isinstance(item, BoardKgHealthComponent) for item in self.components
        ):
            raise ValueError("board_kg_analytics_components_invalid")
        if len(self.components) > MAX_BOARD_KG_COMPONENTS:
            raise ValueError("board_kg_analytics_components_too_many")
        component_names = tuple(item.component for item in self.components)
        if tuple(sorted(set(component_names))) != component_names:
            raise ValueError("board_kg_analytics_components_not_canonical")

    def canonical_dict(self) -> dict[str, object]:
        return {
            "board_id": self.board_id,
            "health_state": self.health_state.value,
            "result_state": self.result_state.value,
            "classification_reason": self.classification_reason,
            "reason_codes": list(self.reason_codes),
            "components": [item.canonical_dict() for item in self.components],
        }


@dataclass(frozen=True, slots=True)
class BoardKgDebtDomains:
    result_state: BoardKgAnalyticsResultState
    active_queue_count: int | None
    technical_dlq_count: int | None
    canonical_debt_count: int | None

    def __post_init__(self) -> None:
        if not isinstance(self.result_state, BoardKgAnalyticsResultState):
            raise ValueError("board_kg_analytics_debt_result_state_invalid")
        for field in (
            "active_queue_count",
            "technical_dlq_count",
            "canonical_debt_count",
        ):
            object.__setattr__(self, field, _count(getattr(self, field), field=field))
        values = (
            self.active_queue_count,
            self.technical_dlq_count,
            self.canonical_debt_count,
        )
        if self.result_state is BoardKgAnalyticsResultState.AVAILABLE:
            if any(value is None for value in values):
                raise ValueError("board_kg_analytics_debt_counts_required")
        elif any(value is not None for value in values):
            raise ValueError("board_kg_analytics_debt_unavailable_count_leak")

    def canonical_dict(self) -> dict[str, object]:
        return {
            "result_state": self.result_state.value,
            "active_queue_count": self.active_queue_count,
            "technical_dlq_count": self.technical_dlq_count,
            "canonical_debt_count": self.canonical_debt_count,
        }


@dataclass(frozen=True, slots=True)
class CognitiveEffectivenessSlice:
    result_state: BoardKgAnalyticsResultState
    cognitively_effective: bool | None
    denominator: int | None
    attempted_count: int | None
    persisted_count: int | None
    technical_dlq_count: int | None
    persistence_gap_count: int | None

    def __post_init__(self) -> None:
        if not isinstance(self.result_state, BoardKgAnalyticsResultState):
            raise ValueError("board_kg_analytics_effectiveness_result_state_invalid")
        fields = (
            "denominator",
            "attempted_count",
            "persisted_count",
            "technical_dlq_count",
            "persistence_gap_count",
        )
        for field in fields:
            object.__setattr__(self, field, _count(getattr(self, field), field=field))
        values = tuple(getattr(self, field) for field in fields)
        if self.result_state in {
            BoardKgAnalyticsResultState.AVAILABLE,
            BoardKgAnalyticsResultState.EMPTY,
        }:
            if not isinstance(self.cognitively_effective, bool) or any(
                value is None for value in values
            ):
                raise ValueError("board_kg_analytics_effectiveness_counts_required")
            if self.result_state is BoardKgAnalyticsResultState.EMPTY and any(values):
                raise ValueError("board_kg_analytics_effectiveness_empty_nonzero")
            if self.attempted_count > self.denominator:  # type: ignore[operator]
                raise ValueError("board_kg_analytics_effectiveness_denominator_invalid")
        elif self.cognitively_effective is not None or any(
            value is not None for value in values
        ):
            raise ValueError("board_kg_analytics_effectiveness_unavailable_fact_leak")

    def canonical_dict(self) -> dict[str, object]:
        return {
            "result_state": self.result_state.value,
            "cognitively_effective": self.cognitively_effective,
            "denominator": self.denominator,
            "attempted_count": self.attempted_count,
            "persisted_count": self.persisted_count,
            "technical_dlq_count": self.technical_dlq_count,
            "persistence_gap_count": self.persistence_gap_count,
        }


@dataclass(frozen=True, slots=True)
class LegacyBoardKgAnalyticsProjection:
    contract_version: str
    foundation_version: str
    query_fingerprint: str
    filters: tuple[AnalyticsFilterClause, ...]
    as_of: datetime
    board_id: str
    result_state: BoardKgAnalyticsResultState
    health_state: BoardKgHealthState
    classification_reason: str
    reason_codes: tuple[str, ...]
    components: tuple[BoardKgHealthComponent, ...]
    debt_domains: BoardKgDebtDomains
    cognitive_effectiveness: CognitiveEffectivenessSlice
    population_scope: AnalyticsPopulationScope
    exclusions: AnalyticsExclusionSummary

    def __post_init__(self) -> None:
        if self.contract_version != LEGACY_BOARD_KG_ANALYTICS_CONTRACT_VERSION:
            raise ValueError("board_kg_analytics_contract_version_unsupported")
        if self.foundation_version != ANALYTICS_FOUNDATION_CONTRACT_VERSION:
            raise ValueError("board_kg_analytics_foundation_version_unsupported")
        if not isinstance(self.query_fingerprint, str) or not _SHA256.fullmatch(
            self.query_fingerprint
        ):
            raise ValueError("board_kg_analytics_query_fingerprint_invalid")
        object.__setattr__(
            self, "as_of", require_utc_datetime(self.as_of, field="board_kg_as_of")
        )
        object.__setattr__(self, "board_id", _text(self.board_id, field="board_id"))
        if not isinstance(self.result_state, BoardKgAnalyticsResultState):
            raise ValueError("board_kg_analytics_result_state_invalid")
        if not isinstance(self.health_state, BoardKgHealthState):
            raise ValueError("board_kg_analytics_health_state_invalid")
        object.__setattr__(
            self,
            "classification_reason",
            _text(self.classification_reason, field="classification_reason"),
        )
        if (
            not isinstance(self.reason_codes, tuple)
            or len(self.reason_codes) > MAX_BOARD_KG_REASON_CODES
        ):
            raise ValueError("board_kg_analytics_reason_codes_invalid")
        reasons = tuple(_text(item, field="reason_code") for item in self.reason_codes)
        if len(set(reasons)) != len(reasons) or tuple(sorted(reasons)) != reasons:
            raise ValueError("board_kg_analytics_reason_codes_not_canonical")
        object.__setattr__(self, "reason_codes", reasons)
        if not isinstance(self.components, tuple) or any(
            not isinstance(item, BoardKgHealthComponent) for item in self.components
        ):
            raise ValueError("board_kg_analytics_components_invalid")
        if len(self.components) > MAX_BOARD_KG_COMPONENTS:
            raise ValueError("board_kg_analytics_components_too_many")
        names = tuple(item.component for item in self.components)
        if len(set(names)) != len(names) or tuple(sorted(names)) != names:
            raise ValueError("board_kg_analytics_components_not_canonical")
        if not isinstance(self.debt_domains, BoardKgDebtDomains):
            raise ValueError("board_kg_analytics_debt_domains_invalid")
        if not isinstance(self.cognitive_effectiveness, CognitiveEffectivenessSlice):
            raise ValueError("board_kg_analytics_effectiveness_invalid")
        if not isinstance(self.population_scope, AnalyticsPopulationScope):
            raise ValueError("board_kg_analytics_population_scope_invalid")
        if not isinstance(self.exclusions, AnalyticsExclusionSummary):
            raise ValueError("board_kg_analytics_exclusions_invalid")

    def canonical_dict(self) -> dict[str, object]:
        return {
            "contract_version": self.contract_version,
            "foundation_version": self.foundation_version,
            "query_fingerprint": self.query_fingerprint,
            "filters": [item.canonical_dict() for item in self.filters],
            "as_of": _utc_text(self.as_of),
            "board_id": self.board_id,
            "result_state": self.result_state.value,
            "health": {
                "state": self.health_state.value,
                "classification_reason": self.classification_reason,
                "reason_codes": list(self.reason_codes),
                "components": [item.canonical_dict() for item in self.components],
            },
            "debt_domains": self.debt_domains.canonical_dict(),
            "cognitive_effectiveness": self.cognitive_effectiveness.canonical_dict(),
            "population_scope": self.population_scope.canonical_dict(),
            "exclusions": self.exclusions.canonical_dict(),
        }


class BoardKgDomain(str, Enum):
    ACTIVE_QUEUE = "active_queue"
    TECHNICAL_DLQ = "technical_dlq"
    CANONICAL_DEBT = "canonical_debt"
    POLICY_PROJECTION_DEBT = "policy_projection_debt"
    COGNITIVE_BACKLOG = "cognitive_backlog"


class BoardKgDomainSeverity(str, Enum):
    INFORMATIONAL = "informational"
    AT_RISK = "at_risk"
    BLOCKING = "blocking"


class BoardKgCognitiveStatus(str, Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    CONSOLIDATED = "consolidated"
    SKIPPED = "skipped"
    FAILED = "failed"
    NO_ACTION = "no_action"


class BoardKgProvenanceKind(str, Enum):
    DETERMINISTIC = "deterministic"
    COGNITIVE = "cognitive"
    FALLBACK = "fallback"
    LEGACY = "legacy"


class BoardKgEffectivenessState(str, Enum):
    AVAILABLE = "available"
    EMPTY = "empty"
    UNAVAILABLE = "unavailable"
    RESTRICTED = "restricted"


class BoardKgAnalyticsError(RuntimeError):
    def __init__(self, code: str, message: str, *, http_status: int) -> None:
        super().__init__(message)
        self.code = _text(code, field="error_code")
        self.message = _text(message, field="error_message")
        if isinstance(http_status, bool) or not isinstance(http_status, int):
            raise ValueError("board_kg_analytics_http_status_invalid")
        self.http_status = http_status

    def canonical_dict(self) -> dict[str, object]:
        return {
            "error": self.code,
            "message": self.message,
            "status_code": self.http_status,
        }


class BoardKgAnalyticsContractMismatch(BoardKgAnalyticsError):
    def __init__(self, message: str = "Board KG Analytics contract mismatch.") -> None:
        super().__init__("kg_analytics_contract_mismatch", message, http_status=409)


class BoardKgMetricUnavailable(BoardKgAnalyticsError):
    def __init__(self, message: str = "Board KG metric is unavailable.") -> None:
        super().__init__("board_kg_metric_unavailable", message, http_status=503)


class BoardKgHistoricalAsOfUnsupported(BoardKgAnalyticsError):
    def __init__(
        self, message: str = "Historical as_of is unsupported by this live projection."
    ) -> None:
        super().__init__(
            "analytics_historical_as_of_unsupported", message, http_status=409
        )


def _number(value: float | int | None, *, field: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"board_kg_analytics_{field}_invalid")
    result = float(value)
    if not math.isfinite(result) or result < 0:
        raise ValueError(f"board_kg_analytics_{field}_invalid")
    return result


@dataclass(frozen=True, slots=True)
class BoardKgAnalyticsQuery:
    foundation: AnalyticsFoundationQuery
    cognitive_status: tuple[BoardKgCognitiveStatus, ...] = ()
    artifact_types: tuple[str, ...] = ()
    cursor: str | None = None
    limit: int = 100
    historical_as_of: datetime | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.foundation, AnalyticsFoundationQuery):
            raise ValueError("board_kg_analytics_foundation_query_required")
        if not isinstance(self.cognitive_status, tuple) or any(
            not isinstance(item, BoardKgCognitiveStatus)
            for item in self.cognitive_status
        ):
            raise ValueError("board_kg_analytics_cognitive_status_invalid")
        if tuple(sorted(set(self.cognitive_status), key=lambda item: item.value)) != (
            self.cognitive_status
        ):
            raise ValueError("board_kg_analytics_cognitive_status_not_canonical")
        if not isinstance(self.artifact_types, tuple):
            raise ValueError("board_kg_analytics_artifact_types_invalid")
        artifact_types = tuple(
            _text(item, field="artifact_type") for item in self.artifact_types
        )
        if tuple(sorted(set(artifact_types))) != artifact_types:
            raise ValueError("board_kg_analytics_artifact_types_not_canonical")
        object.__setattr__(self, "artifact_types", artifact_types)
        if self.cursor is not None:
            object.__setattr__(self, "cursor", _text(self.cursor, field="cursor"))
        if (
            isinstance(self.limit, bool)
            or not isinstance(self.limit, int)
            or not 1 <= self.limit <= 500
        ):
            raise ValueError("board_kg_analytics_limit_invalid")
        if self.historical_as_of is not None:
            object.__setattr__(
                self,
                "historical_as_of",
                require_utc_datetime(
                    self.historical_as_of, field="board_kg_historical_as_of"
                ),
            )

    @property
    def fingerprint(self) -> str:
        payload = {
            "contract_version": BOARD_KG_ANALYTICS_CONTRACT_VERSION,
            "foundation_fingerprint": self.foundation.fingerprint,
            "cognitive_status": [item.value for item in self.cognitive_status],
            "artifact_types": list(self.artifact_types),
            "cursor": self.cursor,
            "limit": self.limit,
            "historical_as_of": (
                _utc_text(self.historical_as_of)
                if self.historical_as_of is not None
                else None
            ),
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()

    @property
    def board_id(self) -> str:
        return self.foundation.board_id

    @property
    def actor_scope_ref(self) -> str:
        return self.foundation.actor_scope_ref

    def canonical_echo(self) -> dict[str, object]:
        return {
            "window": self.foundation.window.canonical_dict(),
            "cognitive_status": [item.value for item in self.cognitive_status],
            "artifact_types": list(self.artifact_types),
            "cursor": self.cursor,
            "limit": self.limit,
        }


@dataclass(frozen=True, slots=True)
class BoardKgDomainAge:
    result_state: BoardKgAnalyticsResultState
    sample_count: int
    p50_hours: float | None
    p95_hours: float | None
    oldest_hours: float | None
    reason: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.result_state, BoardKgAnalyticsResultState):
            raise ValueError("board_kg_analytics_domain_age_state_invalid")
        object.__setattr__(
            self, "sample_count", _count(self.sample_count, field="age_sample_count")
        )
        for field in ("p50_hours", "p95_hours", "oldest_hours"):
            object.__setattr__(self, field, _number(getattr(self, field), field=field))
        measures = (self.p50_hours, self.p95_hours, self.oldest_hours)
        if self.result_state is BoardKgAnalyticsResultState.AVAILABLE:
            if self.sample_count < 1 or any(item is None for item in measures):
                raise ValueError(
                    "board_kg_analytics_domain_age_available_shape_invalid"
                )
            if self.p50_hours > self.p95_hours or self.p95_hours > self.oldest_hours:  # type: ignore[operator]
                raise ValueError("board_kg_analytics_domain_age_order_invalid")
            if self.reason is not None:
                raise ValueError("board_kg_analytics_domain_age_reason_unexpected")
        elif self.result_state is BoardKgAnalyticsResultState.EMPTY:
            if self.sample_count != 0 or any(item is not None for item in measures):
                raise ValueError("board_kg_analytics_domain_age_empty_shape_invalid")
            if self.reason is None:
                object.__setattr__(self, "reason", "no_age_samples")
        else:
            if self.sample_count != 0 or any(item is not None for item in measures):
                raise ValueError("board_kg_analytics_domain_age_unavailable_leak")
            object.__setattr__(
                self, "reason", _text(self.reason, field="domain_age_reason")
            )

    def canonical_dict(self) -> dict[str, object]:
        return {
            "result_state": self.result_state.value,
            "sample_count": self.sample_count,
            "p50_hours": self.p50_hours,
            "p95_hours": self.p95_hours,
            "oldest_hours": self.oldest_hours,
            "reason": self.reason,
        }


@dataclass(frozen=True, slots=True)
class BoardKgDrillDown:
    allowed: bool
    target: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.allowed, bool):
            raise ValueError("board_kg_analytics_drill_down_allowed_invalid")
        if self.allowed:
            target = _text(self.target, field="drill_down_target")
            lowered = target.lower()
            if any(
                token in lowered
                for token in ("repair", "reprocess", "consolidat", "rebuild", "mutat")
            ):
                raise ValueError("board_kg_analytics_mutating_drill_down_forbidden")
            object.__setattr__(self, "target", target)
        elif self.target is not None:
            raise ValueError("board_kg_analytics_disallowed_drill_down_target")

    def canonical_dict(self) -> dict[str, object]:
        return {"allowed": self.allowed, "target": self.target}


@dataclass(frozen=True, slots=True)
class BoardKgOperationalDomain:
    domain: BoardKgDomain
    result_state: BoardKgAnalyticsResultState
    count: int | None
    severity: BoardKgDomainSeverity | None
    age: BoardKgDomainAge
    drill_down: BoardKgDrillDown
    reason: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.domain, BoardKgDomain):
            raise ValueError("board_kg_analytics_domain_invalid")
        if not isinstance(self.result_state, BoardKgAnalyticsResultState):
            raise ValueError("board_kg_analytics_domain_result_state_invalid")
        if not isinstance(self.age, BoardKgDomainAge):
            raise ValueError("board_kg_analytics_domain_age_invalid")
        if not isinstance(self.drill_down, BoardKgDrillDown):
            raise ValueError("board_kg_analytics_domain_drill_down_invalid")
        if self.result_state in {
            BoardKgAnalyticsResultState.AVAILABLE,
            BoardKgAnalyticsResultState.EMPTY,
        }:
            object.__setattr__(self, "count", _count(self.count, field="domain_count"))
            if not isinstance(self.severity, BoardKgDomainSeverity):
                raise ValueError("board_kg_analytics_domain_severity_required")
            if (
                self.result_state is BoardKgAnalyticsResultState.EMPTY
                and self.count != 0
            ):
                raise ValueError("board_kg_analytics_domain_empty_count_invalid")
            if self.reason is not None:
                object.__setattr__(
                    self, "reason", _text(self.reason, field="domain_reason")
                )
        else:
            if self.count is not None or self.severity is not None:
                raise ValueError("board_kg_analytics_domain_unavailable_fact_leak")
            object.__setattr__(
                self, "reason", _text(self.reason, field="domain_reason")
            )
            if self.drill_down.allowed:
                raise ValueError("board_kg_analytics_domain_unavailable_drill_down")

    def canonical_dict(self) -> dict[str, object]:
        return {
            "domain": self.domain.value,
            "result_state": self.result_state.value,
            "count": self.count,
            "severity": self.severity.value if self.severity is not None else None,
            "age": self.age.canonical_dict(),
            "drill_down": self.drill_down.canonical_dict(),
            "reason": self.reason,
        }


@dataclass(frozen=True, slots=True)
class BoardKgCognitiveItemFact:
    artifact_id: str
    cognitive_item_id: str
    status: BoardKgCognitiveStatus
    provenance: BoardKgProvenanceKind
    opened_at: datetime
    candidate_materialized: bool
    persisted: bool
    outcome_materialized: bool
    consolidated_at: datetime | None = None
    overdue_revisit: bool = False
    blocker_codes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "artifact_id", _text(self.artifact_id, field="artifact_id")
        )
        object.__setattr__(
            self,
            "cognitive_item_id",
            _text(self.cognitive_item_id, field="cognitive_item_id"),
        )
        if not isinstance(self.status, BoardKgCognitiveStatus):
            raise ValueError("board_kg_analytics_cognitive_status_invalid")
        if not isinstance(self.provenance, BoardKgProvenanceKind):
            raise ValueError("board_kg_analytics_provenance_kind_invalid")
        object.__setattr__(
            self,
            "opened_at",
            require_utc_datetime(self.opened_at, field="cognitive_item_opened_at"),
        )
        if self.consolidated_at is not None:
            object.__setattr__(
                self,
                "consolidated_at",
                require_utc_datetime(
                    self.consolidated_at, field="cognitive_item_consolidated_at"
                ),
            )
            if self.consolidated_at < self.opened_at:
                raise ValueError("board_kg_analytics_negative_consolidation_time")
        for field in (
            "candidate_materialized",
            "persisted",
            "outcome_materialized",
            "overdue_revisit",
        ):
            if not isinstance(getattr(self, field), bool):
                raise ValueError(f"board_kg_analytics_{field}_invalid")
        if self.outcome_materialized and not self.persisted:
            raise ValueError("board_kg_analytics_materialized_without_persisted")
        if self.persisted and not self.candidate_materialized:
            raise ValueError("board_kg_analytics_persisted_without_candidate")
        blockers = tuple(
            _text(item, field="blocker_code") for item in self.blocker_codes
        )
        if tuple(sorted(set(blockers))) != blockers:
            raise ValueError("board_kg_analytics_blocker_codes_not_canonical")
        object.__setattr__(self, "blocker_codes", blockers)


@dataclass(frozen=True, slots=True)
class BoardKgTiming:
    state: BoardKgEffectivenessState
    sample_count: int
    p50_hours: float | None
    p95_hours: float | None
    reason: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.state, BoardKgEffectivenessState):
            raise ValueError("board_kg_analytics_timing_state_invalid")
        object.__setattr__(
            self, "sample_count", _count(self.sample_count, field="timing_sample_count")
        )
        object.__setattr__(
            self, "p50_hours", _number(self.p50_hours, field="timing_p50")
        )
        object.__setattr__(
            self, "p95_hours", _number(self.p95_hours, field="timing_p95")
        )
        if self.state is BoardKgEffectivenessState.AVAILABLE:
            if (
                self.sample_count < 1
                or self.p50_hours is None
                or self.p95_hours is None
            ):
                raise ValueError("board_kg_analytics_timing_available_shape_invalid")
            if self.p50_hours > self.p95_hours:
                raise ValueError("board_kg_analytics_timing_order_invalid")
            if self.reason is not None:
                raise ValueError("board_kg_analytics_timing_reason_unexpected")
        else:
            if (
                self.sample_count != 0
                or self.p50_hours is not None
                or self.p95_hours is not None
            ):
                raise ValueError("board_kg_analytics_timing_unavailable_fact_leak")
            object.__setattr__(
                self, "reason", _text(self.reason, field="timing_reason")
            )

    def canonical_dict(self) -> dict[str, object]:
        return {
            "state": self.state.value,
            "sample_count": self.sample_count,
            "p50_hours": self.p50_hours,
            "p95_hours": self.p95_hours,
            "reason": self.reason,
        }


@dataclass(frozen=True, slots=True)
class BoardKgStatusCount:
    status: BoardKgCognitiveStatus
    count: int

    def __post_init__(self) -> None:
        if not isinstance(self.status, BoardKgCognitiveStatus):
            raise ValueError("board_kg_analytics_inventory_status_invalid")
        object.__setattr__(self, "count", _count(self.count, field="inventory_count"))

    def canonical_dict(self) -> dict[str, object]:
        return {"status": self.status.value, "count": self.count}


@dataclass(frozen=True, slots=True)
class BoardKgCognitiveInventory:
    result_state: BoardKgAnalyticsResultState
    by_status: tuple[BoardKgStatusCount, ...]
    total: int | None
    overdue_revisits: int | None
    age: BoardKgDomainAge
    reason: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.result_state, BoardKgAnalyticsResultState):
            raise ValueError("board_kg_analytics_inventory_result_state_invalid")
        if not isinstance(self.by_status, tuple) or any(
            not isinstance(item, BoardKgStatusCount) for item in self.by_status
        ):
            raise ValueError("board_kg_analytics_inventory_counts_invalid")
        statuses = tuple(item.status for item in self.by_status)
        expected = tuple(BoardKgCognitiveStatus)
        if not isinstance(self.age, BoardKgDomainAge):
            raise ValueError("board_kg_analytics_inventory_age_invalid")
        if self.result_state in {
            BoardKgAnalyticsResultState.AVAILABLE,
            BoardKgAnalyticsResultState.EMPTY,
        }:
            if statuses != expected:
                raise ValueError("board_kg_analytics_inventory_statuses_incomplete")
            object.__setattr__(
                self, "total", _count(self.total, field="inventory_total")
            )
            object.__setattr__(
                self,
                "overdue_revisits",
                _count(self.overdue_revisits, field="overdue_revisits"),
            )
            if sum(item.count for item in self.by_status) != self.total:
                raise ValueError("board_kg_analytics_inventory_total_mismatch")
            if (
                self.result_state is BoardKgAnalyticsResultState.EMPTY
                and self.total != 0
            ):
                raise ValueError("board_kg_analytics_inventory_empty_nonzero")
        else:
            if self.by_status:
                raise ValueError("board_kg_analytics_inventory_unavailable_count_leak")
            if self.total is not None or self.overdue_revisits is not None:
                raise ValueError("board_kg_analytics_inventory_unavailable_fact_leak")
            object.__setattr__(
                self, "reason", _text(self.reason, field="inventory_reason")
            )

    def canonical_dict(self) -> dict[str, object]:
        return {
            "result_state": self.result_state.value,
            "by_status": {item.status.value: item.count for item in self.by_status},
            "total": self.total,
            "overdue_revisits": self.overdue_revisits,
            "age": self.age.canonical_dict(),
            "reason": self.reason,
        }


@dataclass(frozen=True, slots=True)
class BoardKgCognitiveEffectiveness:
    state: BoardKgEffectivenessState
    numerator: int | None
    denominator: int | None
    rate: float | None
    candidate_count: int | None
    persisted_count: int | None
    conversion_rate: float | None
    method_version: str
    sample_period: AnalyticsUtcWindow
    timing: BoardKgTiming
    reason: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.state, BoardKgEffectivenessState):
            raise ValueError("board_kg_analytics_effectiveness_state_invalid")
        object.__setattr__(
            self, "method_version", _text(self.method_version, field="method_version")
        )
        if not isinstance(self.sample_period, AnalyticsUtcWindow):
            raise ValueError("board_kg_analytics_sample_period_invalid")
        if not isinstance(self.timing, BoardKgTiming):
            raise ValueError("board_kg_analytics_timing_invalid")
        numeric = (
            "numerator",
            "denominator",
            "candidate_count",
            "persisted_count",
        )
        if self.state in {
            BoardKgEffectivenessState.AVAILABLE,
            BoardKgEffectivenessState.EMPTY,
        }:
            for field in numeric:
                object.__setattr__(
                    self, field, _count(getattr(self, field), field=field)
                )
            if self.numerator > self.denominator:  # type: ignore[operator]
                raise ValueError("board_kg_analytics_effectiveness_denominator_invalid")
            if self.persisted_count > self.candidate_count:  # type: ignore[operator]
                raise ValueError("board_kg_analytics_conversion_denominator_invalid")
            if self.state is BoardKgEffectivenessState.EMPTY:
                if any(getattr(self, field) for field in numeric):
                    raise ValueError("board_kg_analytics_effectiveness_empty_nonzero")
                if self.rate is not None or self.conversion_rate is not None:
                    raise ValueError("board_kg_analytics_effectiveness_empty_rate_leak")
            else:
                object.__setattr__(
                    self, "rate", _number(self.rate, field="effectiveness_rate")
                )
                if self.rate is None or not 0 <= self.rate <= 1:
                    raise ValueError("board_kg_analytics_effectiveness_rate_invalid")
                if self.candidate_count == 0:
                    if self.conversion_rate is not None:
                        raise ValueError("board_kg_analytics_conversion_rate_leak")
                else:
                    object.__setattr__(
                        self,
                        "conversion_rate",
                        _number(self.conversion_rate, field="conversion_rate"),
                    )
                    if not 0 <= self.conversion_rate <= 1:  # type: ignore[operator]
                        raise ValueError("board_kg_analytics_conversion_rate_invalid")
        else:
            if any(
                getattr(self, field) is not None
                for field in (*numeric, "rate", "conversion_rate")
            ):
                raise ValueError(
                    "board_kg_analytics_effectiveness_unavailable_fact_leak"
                )
            object.__setattr__(
                self, "reason", _text(self.reason, field="effectiveness_reason")
            )

    def canonical_dict(self) -> dict[str, object]:
        return {
            "state": self.state.value,
            "numerator": self.numerator,
            "denominator": self.denominator,
            "rate": self.rate,
            "candidate_count": self.candidate_count,
            "persisted_count": self.persisted_count,
            "conversion_rate": self.conversion_rate,
            "method_version": self.method_version,
            "sample_period": self.sample_period.canonical_dict(),
            "timing": self.timing.canonical_dict(),
            "reason": self.reason,
        }


@dataclass(frozen=True, slots=True)
class BoardKgProvenanceSlice:
    kind: BoardKgProvenanceKind
    count: int
    rate: float | None

    def __post_init__(self) -> None:
        if not isinstance(self.kind, BoardKgProvenanceKind):
            raise ValueError("board_kg_analytics_provenance_kind_invalid")
        object.__setattr__(self, "count", _count(self.count, field="provenance_count"))
        if self.rate is not None:
            object.__setattr__(
                self, "rate", _number(self.rate, field="provenance_rate")
            )
            if not 0 <= self.rate <= 1:
                raise ValueError("board_kg_analytics_provenance_rate_invalid")

    def canonical_dict(self) -> dict[str, object]:
        return {"kind": self.kind.value, "count": self.count, "rate": self.rate}


@dataclass(frozen=True, slots=True)
class BoardKgProvenanceMix:
    result_state: BoardKgAnalyticsResultState
    total: int | None
    slices: tuple[BoardKgProvenanceSlice, ...]
    reason: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.result_state, BoardKgAnalyticsResultState):
            raise ValueError("board_kg_analytics_provenance_result_state_invalid")
        if not isinstance(self.slices, tuple) or any(
            not isinstance(item, BoardKgProvenanceSlice) for item in self.slices
        ):
            raise ValueError("board_kg_analytics_provenance_slices_invalid")
        if self.result_state in {
            BoardKgAnalyticsResultState.AVAILABLE,
            BoardKgAnalyticsResultState.EMPTY,
        }:
            if tuple(item.kind for item in self.slices) != tuple(BoardKgProvenanceKind):
                raise ValueError("board_kg_analytics_provenance_kinds_incomplete")
            object.__setattr__(
                self, "total", _count(self.total, field="provenance_total")
            )
            if sum(item.count for item in self.slices) != self.total:
                raise ValueError("board_kg_analytics_provenance_total_mismatch")
        elif self.total is not None or self.slices:
            raise ValueError("board_kg_analytics_provenance_unavailable_fact_leak")

    def canonical_dict(self) -> dict[str, object]:
        return {
            "result_state": self.result_state.value,
            "total": self.total,
            "by_kind": {
                item.kind.value: {"count": item.count, "rate": item.rate}
                for item in self.slices
            },
            "reason": self.reason,
        }


@dataclass(frozen=True, slots=True)
class BoardKgDiagnostic:
    domain: str
    severity: BoardKgDomainSeverity
    reason: str
    next_step: BoardKgDrillDown

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "domain", _text(self.domain, field="diagnostic_domain")
        )
        if not isinstance(self.severity, BoardKgDomainSeverity):
            raise ValueError("board_kg_analytics_diagnostic_severity_invalid")
        object.__setattr__(
            self, "reason", _text(self.reason, field="diagnostic_reason")
        )
        if not isinstance(self.next_step, BoardKgDrillDown):
            raise ValueError("board_kg_analytics_diagnostic_next_step_invalid")

    def canonical_dict(self) -> dict[str, object]:
        return {
            "domain": self.domain,
            "severity": self.severity.value,
            "reason": self.reason,
            "next_step": self.next_step.canonical_dict(),
        }


@dataclass(frozen=True, slots=True)
class BoardKgAnalyticsEvidence:
    board_id: str
    foundation_contract_version: str
    observed_at: datetime
    health_state: BoardKgHealthState
    health_result_state: BoardKgAnalyticsResultState
    classification_reason: str
    reason_codes: tuple[str, ...]
    components: tuple[BoardKgHealthComponent, ...]
    domains: tuple[BoardKgOperationalDomain, ...]
    cognitive_items: tuple[BoardKgCognitiveItemFact, ...]
    diagnostics: tuple[BoardKgDiagnostic, ...]
    redactions: tuple[str, ...]
    population_scope: AnalyticsPopulationScope
    exclusions: AnalyticsExclusionSummary
    currentness: AnalyticsProjectionCurrentness
    sources: tuple[AnalyticsSourceAuthority, ...]
    next_cursor: str | None = None
    currentness_reason: str | None = None
    historical_as_of_supported: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "board_id", _text(self.board_id, field="board_id"))
        object.__setattr__(
            self,
            "observed_at",
            require_utc_datetime(
                self.observed_at, field="board_kg_evidence_observed_at"
            ),
        )
        if not isinstance(self.health_state, BoardKgHealthState):
            raise ValueError("board_kg_analytics_health_state_invalid")
        if not isinstance(self.health_result_state, BoardKgAnalyticsResultState):
            raise ValueError("board_kg_analytics_health_result_state_invalid")
        object.__setattr__(
            self,
            "classification_reason",
            _text(self.classification_reason, field="classification_reason"),
        )
        reasons = tuple(_text(item, field="reason_code") for item in self.reason_codes)
        if tuple(sorted(set(reasons))) != reasons:
            raise ValueError("board_kg_analytics_reason_codes_not_canonical")
        object.__setattr__(self, "reason_codes", reasons)
        if not isinstance(self.components, tuple) or any(
            not isinstance(item, BoardKgHealthComponent) for item in self.components
        ):
            raise ValueError("board_kg_analytics_components_invalid")
        component_names = tuple(item.component for item in self.components)
        if tuple(sorted(set(component_names))) != component_names:
            raise ValueError("board_kg_analytics_components_not_canonical")
        if not isinstance(self.domains, tuple) or any(
            not isinstance(item, BoardKgOperationalDomain) for item in self.domains
        ):
            raise ValueError("board_kg_analytics_domains_invalid")
        if tuple(item.domain for item in self.domains) != tuple(BoardKgDomain):
            raise ValueError("board_kg_analytics_domains_incomplete")
        if not isinstance(self.cognitive_items, tuple) or any(
            not isinstance(item, BoardKgCognitiveItemFact)
            for item in self.cognitive_items
        ):
            raise ValueError("board_kg_analytics_cognitive_items_invalid")
        item_keys = tuple(
            (item.artifact_id, item.cognitive_item_id) for item in self.cognitive_items
        )
        if tuple(sorted(set(item_keys))) != item_keys:
            raise ValueError("board_kg_analytics_cognitive_items_not_canonical")
        if not isinstance(self.diagnostics, tuple) or any(
            not isinstance(item, BoardKgDiagnostic) for item in self.diagnostics
        ):
            raise ValueError("board_kg_analytics_diagnostics_invalid")
        redactions = tuple(_text(item, field="redaction") for item in self.redactions)
        if tuple(sorted(set(redactions))) != redactions:
            raise ValueError("board_kg_analytics_redactions_not_canonical")
        object.__setattr__(self, "redactions", redactions)
        if not isinstance(self.population_scope, AnalyticsPopulationScope):
            raise ValueError("board_kg_analytics_population_scope_invalid")
        if not isinstance(self.exclusions, AnalyticsExclusionSummary):
            raise ValueError("board_kg_analytics_exclusions_invalid")
        if not isinstance(self.currentness, AnalyticsProjectionCurrentness):
            raise ValueError("board_kg_analytics_currentness_invalid")
        if not isinstance(self.sources, tuple) or any(
            not isinstance(item, AnalyticsSourceAuthority) for item in self.sources
        ):
            raise ValueError("board_kg_analytics_sources_invalid")
        if self.currentness is AnalyticsProjectionCurrentness.CURRENT:
            if self.currentness_reason is not None or not self.sources:
                raise ValueError("board_kg_analytics_currentness_shape_invalid")
        else:
            object.__setattr__(
                self,
                "currentness_reason",
                _text(self.currentness_reason, field="currentness_reason"),
            )
        if self.next_cursor is not None:
            object.__setattr__(
                self, "next_cursor", _text(self.next_cursor, field="next_cursor")
            )
        if not isinstance(self.historical_as_of_supported, bool):
            raise ValueError("board_kg_analytics_historical_support_invalid")


@dataclass(frozen=True, slots=True)
class BoardKgEffectivenessProjection:
    contract_version: str
    foundation_version: str
    query: BoardKgAnalyticsQuery
    as_of: datetime
    board_id: str
    result_state: BoardKgAnalyticsResultState
    provenance: AnalyticsProjectionProvenance
    health_state: BoardKgClassificationState
    classification_reason: str
    reason_codes: tuple[str, ...]
    components: tuple[BoardKgHealthComponent, ...]
    domains: tuple[BoardKgOperationalDomain, ...]
    cognitive_inventory: BoardKgCognitiveInventory
    effectiveness: BoardKgCognitiveEffectiveness
    provenance_mix: BoardKgProvenanceMix
    diagnostics: tuple[BoardKgDiagnostic, ...]
    redactions: tuple[str, ...]
    population_scope: AnalyticsPopulationScope
    exclusions: AnalyticsExclusionSummary
    next_cursor: str | None = None

    def __post_init__(self) -> None:
        if self.contract_version != BOARD_KG_ANALYTICS_CONTRACT_VERSION:
            raise ValueError("board_kg_analytics_contract_version_unsupported")
        if self.foundation_version != ANALYTICS_FOUNDATION_CONTRACT_VERSION:
            raise ValueError("board_kg_analytics_foundation_version_unsupported")
        if not isinstance(self.query, BoardKgAnalyticsQuery):
            raise ValueError("board_kg_analytics_query_invalid")
        object.__setattr__(
            self, "as_of", require_utc_datetime(self.as_of, field="board_kg_as_of")
        )
        object.__setattr__(self, "board_id", _text(self.board_id, field="board_id"))
        if self.board_id != self.query.foundation.board_id:
            raise ValueError("board_kg_analytics_board_mismatch")
        if not isinstance(self.result_state, BoardKgAnalyticsResultState):
            raise ValueError("board_kg_analytics_result_state_invalid")
        if not isinstance(self.provenance, AnalyticsProjectionProvenance):
            raise ValueError("board_kg_analytics_provenance_invalid")
        if not isinstance(self.health_state, BoardKgClassificationState):
            raise ValueError("board_kg_analytics_health_state_invalid")
        object.__setattr__(
            self,
            "classification_reason",
            _text(self.classification_reason, field="classification_reason"),
        )
        if tuple(sorted(set(self.reason_codes))) != self.reason_codes:
            raise ValueError("board_kg_analytics_reason_codes_not_canonical")
        if tuple(item.domain for item in self.domains) != tuple(BoardKgDomain):
            raise ValueError("board_kg_analytics_domains_incomplete")
        if not isinstance(self.cognitive_inventory, BoardKgCognitiveInventory):
            raise ValueError("board_kg_analytics_inventory_invalid")
        if not isinstance(self.effectiveness, BoardKgCognitiveEffectiveness):
            raise ValueError("board_kg_analytics_effectiveness_invalid")
        if not isinstance(self.provenance_mix, BoardKgProvenanceMix):
            raise ValueError("board_kg_analytics_provenance_mix_invalid")
        if not isinstance(self.population_scope, AnalyticsPopulationScope):
            raise ValueError("board_kg_analytics_population_scope_invalid")
        if not isinstance(self.exclusions, AnalyticsExclusionSummary):
            raise ValueError("board_kg_analytics_exclusions_invalid")

    def canonical_dict(self) -> dict[str, object]:
        health_availability = {
            item.component: item.result_state.value for item in self.components
        }
        health_availability.update(
            {item.domain.value: item.result_state.value for item in self.domains}
        )
        return {
            "contract_version": self.contract_version,
            "foundation_version": self.foundation_version,
            "query_fingerprint": self.query.fingerprint,
            "query": self.query.canonical_echo(),
            "filters": [
                item.canonical_dict() for item in self.query.foundation.filters
            ],
            "as_of": _utc_text(self.as_of),
            "board_id": self.board_id,
            "result_state": self.result_state.value,
            "provenance": self.provenance.canonical_dict(),
            "health": {
                "state": self.health_state.value,
                "classification_reason": self.classification_reason,
                "reason_codes": list(self.reason_codes),
                "availability": health_availability,
                "components": [item.canonical_dict() for item in self.components],
            },
            "domains": [item.canonical_dict() for item in self.domains],
            "cognitive_inventory": self.cognitive_inventory.canonical_dict(),
            "effectiveness": self.effectiveness.canonical_dict(),
            "provenance_mix": self.provenance_mix.canonical_dict(),
            "diagnostics": [item.canonical_dict() for item in self.diagnostics],
            "redactions": list(self.redactions),
            "population_scope": self.population_scope.canonical_dict(),
            "exclusions": self.exclusions.canonical_dict(),
            "next_cursor": self.next_cursor,
        }


@runtime_checkable
class BoardKgAnalyticsEvidencePort(Protocol):
    async def load(
        self, context: object, *, query: BoardKgAnalyticsQuery
    ) -> BoardKgAnalyticsEvidence: ...


# Public v2 name.  The former compact DTO remains implementation-only so no
# second contract can advertise version 2 with an incomplete payload.
BoardKgAnalyticsProjection = BoardKgEffectivenessProjection


@runtime_checkable
class BoardKgAnalyticsProjectionPort(Protocol):
    async def project(
        self, context: object, *, query: BoardKgAnalyticsQuery
    ) -> BoardKgEffectivenessProjection: ...


__all__ = [
    "BOARD_KG_ANALYTICS_CONTRACT_VERSION",
    "LEGACY_BOARD_KG_ANALYTICS_CONTRACT_VERSION",
    "BoardKgAnalyticsEvidence",
    "BoardKgAnalyticsEvidencePort",
    "BoardKgAnalyticsContractMismatch",
    "BoardKgAnalyticsError",
    "BoardKgHistoricalAsOfUnsupported",
    "BoardKgAnalyticsProjection",
    "BoardKgAnalyticsProjectionPort",
    "BoardKgAnalyticsQuery",
    "BoardKgAnalyticsResultState",
    "BoardKgCognitiveEffectiveness",
    "BoardKgCognitiveInventory",
    "BoardKgCognitiveItemFact",
    "BoardKgCognitiveStatus",
    "BoardKgClassificationState",
    "BoardKgDebtDomains",
    "BoardKgDiagnostic",
    "BoardKgDomain",
    "BoardKgDomainAge",
    "BoardKgDomainSeverity",
    "BoardKgDrillDown",
    "BoardKgEffectivenessProjection",
    "BoardKgEffectivenessState",
    "BoardKgHealthComponent",
    "BoardKgHealthEvidenceSnapshot",
    "BoardKgHealthState",
    "BoardKgMetricUnavailable",
    "BoardKgOperationalDomain",
    "BoardKgProvenanceKind",
    "BoardKgProvenanceMix",
    "BoardKgProvenanceSlice",
    "BoardKgStatusCount",
    "BoardKgTiming",
    "CognitiveEffectivenessSlice",
]
