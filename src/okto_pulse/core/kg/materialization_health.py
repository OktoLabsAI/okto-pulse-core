"""Edition-neutral materialization evidence and fail-closed health policy.

This module owns no filesystem, graph backend, database session, clock or
worker. Edition adapters collect one generation-fenced evidence snapshot and
the policy composes it with the established KG health state machine.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from types import MappingProxyType
from typing import Mapping

from okto_pulse.core.kg.health_state import HealthState, MetricStatus
from okto_pulse.core.kg.interfaces.graph_runtime_store import (
    GraphRuntimeObservationState,
    GraphRuntimeState,
)


class CensusStatus(str, Enum):
    AVAILABLE = "available"
    UNAVAILABLE = "unavailable"


class MaterializationState(str, Enum):
    NOT_MATERIALIZED = "not_materialized"
    MATERIALIZED = "materialized"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class HealthProbeDeadline:
    """One monotonic deadline shared by every probe in a health request."""

    deadline_at: float

    def __post_init__(self) -> None:
        value = float(self.deadline_at)
        if value < 0:
            raise ValueError("health_probe_deadline_must_be_non_negative")
        object.__setattr__(self, "deadline_at", value)

    def remaining_seconds(self, *, now: float) -> float:
        return max(0.0, self.deadline_at - float(now))

    def expired(self, *, now: float) -> bool:
        return self.remaining_seconds(now=now) <= 0.0


@dataclass(frozen=True, slots=True)
class MaterializationEvidenceRequest:
    board_id: str
    generation: str
    deadline: HealthProbeDeadline

    def __post_init__(self) -> None:
        board_id = str(self.board_id).strip()
        generation = str(self.generation).strip()
        if not board_id:
            raise ValueError("materialization_evidence_board_id_required")
        if not generation:
            raise ValueError("materialization_evidence_generation_required")
        object.__setattr__(self, "board_id", board_id)
        object.__setattr__(self, "generation", generation)


@dataclass(frozen=True, slots=True)
class BoardHealthCensus:
    """Board-scoped relational counts observed under one generation."""

    generation: str | None
    status: CensusStatus
    source_count: int | None
    queue_depth: int | None
    active_queue_count: int | None
    dead_letter_count: int | None
    global_outbox_dead_letter_count: int | None
    reason_code: str
    observed_at: datetime

    def __post_init__(self) -> None:
        status = CensusStatus(self.status)
        reason_code = str(self.reason_code).strip()
        generation = (
            str(self.generation).strip() if self.generation is not None else None
        )
        if generation == "":
            generation = None
        if not reason_code:
            raise ValueError("board_health_census_reason_code_required")
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "reason_code", reason_code)
        object.__setattr__(self, "generation", generation)

        counts = self.counts
        for name, value in counts.items():
            if value is None:
                if status is CensusStatus.AVAILABLE:
                    raise ValueError(f"board_health_census_{name}_required")
                continue
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"board_health_census_{name}_invalid")

    @property
    def counts(self) -> Mapping[str, int | None]:
        return MappingProxyType(
            {
                "source_count": self.source_count,
                "queue_depth": self.queue_depth,
                "active_queue_count": self.active_queue_count,
                "dead_letter_count": self.dead_letter_count,
                "global_outbox_dead_letter_count": (
                    self.global_outbox_dead_letter_count
                ),
            }
        )

    @property
    def is_confirmed_zero(self) -> bool:
        return self.status is CensusStatus.AVAILABLE and all(
            value == 0 for value in self.counts.values()
        )


@dataclass(frozen=True, slots=True)
class MaterializationEvidence:
    board_store: GraphRuntimeState
    census: BoardHealthCensus
    discovery_store: GraphRuntimeState


@dataclass(frozen=True, slots=True)
class KnownEmptyBoardMetrics:
    """Only the board metrics proven zero/null by strict absence evidence."""

    queue_depth: int = 0
    active_queue_count: int = 0
    dead_letter_count: int = 0
    global_outbox_dead_letter_count: int = 0
    total_nodes: int = 0
    default_score_count: int = 0
    default_score_ratio: float = 0.0
    avg_relevance: float = 0.0
    canonical_layer_count: int = 0
    working_layer_count: int = 0
    source_count: int = 0
    board_storage_total_bytes: int = 0
    oldest_pending_age_s: float | None = None
    oldest_dead_letter_age_s: float | None = None
    high_water_mark_pct: float | None = None
    last_decay_tick_at: str | None = None
    graph_schema_version: str | None = None


@dataclass(frozen=True, slots=True)
class MaterializationHealthBaseline:
    """Existing health classification before materialization composition."""

    graph_state: HealthState
    discovery_state: HealthState
    overall_state: HealthState
    metric_status: MetricStatus
    classification_reasons: tuple[str, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        object.__setattr__(self, "graph_state", HealthState(self.graph_state))
        object.__setattr__(
            self,
            "discovery_state",
            HealthState(self.discovery_state),
        )
        object.__setattr__(self, "overall_state", HealthState(self.overall_state))
        object.__setattr__(self, "metric_status", MetricStatus(self.metric_status))
        object.__setattr__(
            self,
            "classification_reasons",
            tuple(str(reason) for reason in self.classification_reasons if reason),
        )


@dataclass(frozen=True, slots=True)
class MaterializationHealthSnapshot:
    materialization_state: MaterializationState
    materialization_generation: str | None
    graph_state: HealthState
    discovery_state: HealthState
    overall_state: HealthState
    metric_status: MetricStatus
    classification_reason: str
    classification_reasons: tuple[str, ...]
    probe_reason_codes: Mapping[str, str]
    known_empty_metrics: KnownEmptyBoardMetrics | None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "probe_reason_codes",
            MappingProxyType(dict(self.probe_reason_codes)),
        )


_HEALTH_SEVERITY = {
    HealthState.HEALTHY: 0,
    HealthState.AT_RISK: 1,
    HealthState.BACKPRESSURE: 2,
    HealthState.RECOVERY_NEEDED: 3,
    HealthState.QUARANTINED: 4,
}


def _worst_state(*states: HealthState) -> HealthState:
    return max(states, key=_HEALTH_SEVERITY.__getitem__)


def _at_least_at_risk(state: HealthState) -> HealthState:
    return _worst_state(state, HealthState.AT_RISK)


def _stable_reason(value: str | None, fallback: str) -> str:
    normalized = str(value or "").strip()
    return normalized or fallback


def _merge_reasons(primary: str, baseline: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(dict.fromkeys((primary, *baseline)))


@dataclass(frozen=True, slots=True)
class MaterializationHealthPolicy:
    """Compose four-state evidence without weakening canonical health."""

    def evaluate(
        self,
        *,
        board_store: GraphRuntimeState,
        census: BoardHealthCensus,
        discovery_store: GraphRuntimeState,
        baseline: MaterializationHealthBaseline,
    ) -> MaterializationHealthSnapshot:
        board_observation = board_store.normalized_state
        discovery_observation = discovery_store.normalized_state
        probe_reason_codes = {
            "board_graph": _stable_reason(
                board_store.reason_code or board_store.unavailable_reason,
                "board_graph_observation_unclassified",
            ),
            "board_census": _stable_reason(
                census.reason_code,
                "board_census_unclassified",
            ),
            "global_discovery": _stable_reason(
                discovery_store.reason_code or discovery_store.unavailable_reason,
                "global_discovery_observation_unclassified",
            ),
        }

        discovery_failed = discovery_observation in {
            GraphRuntimeObservationState.PRESENT_UNREADABLE_OR_ERROR,
            GraphRuntimeObservationState.PROVIDER_UNAVAILABLE,
        }
        discovery_state = (
            _at_least_at_risk(baseline.discovery_state)
            if discovery_failed
            else baseline.discovery_state
        )

        if board_observation is GraphRuntimeObservationState.PRESENT_READABLE_CANDIDATE:
            reason = _stable_reason(board_store.reason_code, "board_graph_present")
            return self._snapshot(
                state=MaterializationState.MATERIALIZED,
                generation=board_store.generation,
                graph_state=baseline.graph_state,
                discovery_state=discovery_state,
                metric_status=(
                    MetricStatus.UNAVAILABLE
                    if discovery_failed
                    else baseline.metric_status
                ),
                reason=reason,
                baseline=baseline,
                probe_reason_codes=probe_reason_codes,
            )

        generations_match = (
            board_store.generation is not None
            and board_store.generation == census.generation
        )
        if (
            board_observation is GraphRuntimeObservationState.CONFIRMED_ABSENT
            and census.status is CensusStatus.AVAILABLE
            and not generations_match
        ):
            reason = (
                "materialization_generation_unbound"
                if board_store.generation is None and census.generation is None
                else "materialization_generation_changed"
            )
            return self._unknown(
                reason=reason,
                baseline=baseline,
                discovery_state=discovery_state,
                probe_reason_codes=probe_reason_codes,
            )

        if (
            board_observation is GraphRuntimeObservationState.CONFIRMED_ABSENT
            and generations_match
            and census.is_confirmed_zero
        ):
            reason = "empty_board_not_materialized"
            graph_state = HealthState.HEALTHY
            metric_status = (
                MetricStatus.UNAVAILABLE
                if discovery_failed
                else MetricStatus.AVAILABLE
            )
            return self._snapshot(
                state=MaterializationState.NOT_MATERIALIZED,
                generation=board_store.generation,
                graph_state=graph_state,
                discovery_state=discovery_state,
                metric_status=metric_status,
                reason=reason,
                baseline=baseline,
                probe_reason_codes=probe_reason_codes,
                known_empty_metrics=KnownEmptyBoardMetrics(),
            )

        if board_observation is not GraphRuntimeObservationState.CONFIRMED_ABSENT:
            reason = probe_reason_codes["board_graph"]
        elif census.status is not CensusStatus.AVAILABLE:
            reason = probe_reason_codes["board_census"]
        else:
            reason = "board_census_nonzero"
        return self._unknown(
            reason=reason,
            baseline=baseline,
            discovery_state=discovery_state,
            probe_reason_codes=probe_reason_codes,
        )

    def _unknown(
        self,
        *,
        reason: str,
        baseline: MaterializationHealthBaseline,
        discovery_state: HealthState,
        probe_reason_codes: Mapping[str, str],
    ) -> MaterializationHealthSnapshot:
        return self._snapshot(
            state=MaterializationState.UNKNOWN,
            generation=None,
            graph_state=_at_least_at_risk(baseline.graph_state),
            discovery_state=discovery_state,
            metric_status=MetricStatus.UNAVAILABLE,
            reason=reason,
            baseline=baseline,
            probe_reason_codes=probe_reason_codes,
        )

    @staticmethod
    def _snapshot(
        *,
        state: MaterializationState,
        generation: str | None,
        graph_state: HealthState,
        discovery_state: HealthState,
        metric_status: MetricStatus,
        reason: str,
        baseline: MaterializationHealthBaseline,
        probe_reason_codes: Mapping[str, str],
        known_empty_metrics: KnownEmptyBoardMetrics | None = None,
    ) -> MaterializationHealthSnapshot:
        old_component_worst = _worst_state(
            baseline.graph_state,
            baseline.discovery_state,
        )
        if (
            _HEALTH_SEVERITY[baseline.overall_state]
            > _HEALTH_SEVERITY[old_component_worst]
        ):
            overall_state = baseline.overall_state
        else:
            overall_state = _worst_state(graph_state, discovery_state)
        return MaterializationHealthSnapshot(
            materialization_state=state,
            materialization_generation=generation,
            graph_state=graph_state,
            discovery_state=discovery_state,
            overall_state=overall_state,
            metric_status=metric_status,
            classification_reason=reason,
            classification_reasons=_merge_reasons(
                reason,
                baseline.classification_reasons,
            ),
            probe_reason_codes=probe_reason_codes,
            known_empty_metrics=known_empty_metrics,
        )


__all__ = [
    "BoardHealthCensus",
    "CensusStatus",
    "HealthProbeDeadline",
    "KnownEmptyBoardMetrics",
    "MaterializationEvidence",
    "MaterializationEvidenceRequest",
    "MaterializationHealthBaseline",
    "MaterializationHealthPolicy",
    "MaterializationHealthSnapshot",
    "MaterializationState",
]
