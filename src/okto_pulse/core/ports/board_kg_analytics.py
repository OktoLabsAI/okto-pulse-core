"""Canonical Board KG Analytics output contract."""

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
    AnalyticsPopulationScope,
    require_utc_datetime,
)


BOARD_KG_ANALYTICS_CONTRACT_VERSION = "1"
MAX_BOARD_KG_COMPONENTS = 32
MAX_BOARD_KG_REASON_CODES = 64
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


def _text(value: str, *, field: str) -> str:
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


class BoardKgAnalyticsResultState(str, Enum):
    AVAILABLE = "available"
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
class BoardKgAnalyticsProjection:
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
        if self.contract_version != BOARD_KG_ANALYTICS_CONTRACT_VERSION:
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


@runtime_checkable
class BoardKgAnalyticsProjectionPort(Protocol):
    async def project(
        self, context: object, query: object
    ) -> BoardKgAnalyticsProjection: ...


__all__ = [
    "BOARD_KG_ANALYTICS_CONTRACT_VERSION",
    "BoardKgAnalyticsProjection",
    "BoardKgAnalyticsProjectionPort",
    "BoardKgAnalyticsResultState",
    "BoardKgDebtDomains",
    "BoardKgHealthComponent",
    "BoardKgHealthState",
    "CognitiveEffectivenessSlice",
]
