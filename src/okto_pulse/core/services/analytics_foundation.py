"""Pure semantic service for the canonical Analytics foundation contract."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from okto_pulse.core.ports.analytics_foundation import (
    ANALYTICS_FOUNDATION_CONTRACT_VERSION,
    AnalyticsEvidenceCurrentness,
    AnalyticsExclusionSummary,
    AnalyticsFilterClause,
    AnalyticsFoundationProjection,
    AnalyticsFoundationQuery,
    AnalyticsMetricEnvelope,
    AnalyticsMetricFamily,
    AnalyticsNumber,
    AnalyticsPopulationScope,
    AnalyticsResultState,
    AnalyticsSourceAuthority,
    AnalyticsTemporalSemantics,
    require_utc_datetime,
)


@dataclass(frozen=True, slots=True)
class AnalyticsEvidenceBinding:
    subject_id: str
    lifecycle_edition: str
    result_type: str
    authority_ref: str

    def __post_init__(self) -> None:
        for name in (
            "subject_id",
            "lifecycle_edition",
            "result_type",
            "authority_ref",
        ):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"analytics_evidence_{name}_required")
            object.__setattr__(self, name, value.strip())


@dataclass(frozen=True, slots=True)
class AnalyticsLifecycleEvent:
    episode_id: str
    event_type: str
    occurred_at: datetime

    def __post_init__(self) -> None:
        if not isinstance(self.episode_id, str) or not self.episode_id.strip():
            raise ValueError("analytics_episode_id_required")
        if not isinstance(self.event_type, str) or not self.event_type.strip():
            raise ValueError("analytics_event_type_required")
        object.__setattr__(self, "episode_id", self.episode_id.strip())
        object.__setattr__(self, "event_type", self.event_type.strip())
        object.__setattr__(
            self,
            "occurred_at",
            require_utc_datetime(self.occurred_at, field="lifecycle_event_timestamp"),
        )


def classify_evidence_currentness(
    *,
    expected: AnalyticsEvidenceBinding,
    evidence: AnalyticsEvidenceBinding | None,
    accepted_previous_authority_refs: frozenset[str] = frozenset(),
) -> AnalyticsEvidenceCurrentness:
    """Classify evidence without consulting mutable lifecycle status/version."""

    if evidence is None:
        return AnalyticsEvidenceCurrentness.MISSING
    if (
        evidence.subject_id != expected.subject_id
        or evidence.result_type != expected.result_type
    ):
        return AnalyticsEvidenceCurrentness.STALE
    if (
        evidence.lifecycle_edition == expected.lifecycle_edition
        and evidence.authority_ref == expected.authority_ref
    ):
        return AnalyticsEvidenceCurrentness.CURRENT
    if (
        evidence.lifecycle_edition != expected.lifecycle_edition
        and evidence.authority_ref in accepted_previous_authority_refs
    ):
        return AnalyticsEvidenceCurrentness.PREVIOUS
    return AnalyticsEvidenceCurrentness.STALE


def governed_episode_duration(
    events: tuple[AnalyticsLifecycleEvent, ...],
    *,
    episode_id: str,
    start_event_type: str,
    end_event_type: str,
) -> timedelta:
    """Calculate duration exclusively from ordered events of one episode."""

    if not events:
        raise ValueError("analytics_lifecycle_events_required")
    ordered = sorted(events, key=lambda item: item.occurred_at)
    if tuple(ordered) != events:
        raise ValueError("analytics_lifecycle_events_out_of_order")

    start: datetime | None = None
    for event in events:
        if event.episode_id != episode_id:
            continue
        if start is None and event.event_type == start_event_type:
            start = event.occurred_at
            continue
        if start is not None and event.event_type == end_event_type:
            return event.occurred_at - start
    raise ValueError("analytics_lifecycle_episode_incomplete")


class AnalyticsFoundationService:
    """Build canonical envelopes after authorization and fact projection."""

    @staticmethod
    def ratio_metric(
        *,
        metric_id: str,
        numerator: AnalyticsNumber,
        denominator: AnalyticsNumber,
        n: int,
        exclusions: AnalyticsExclusionSummary,
        as_of: datetime,
        filters: tuple[AnalyticsFilterClause, ...],
        source: AnalyticsSourceAuthority,
        population_scope: AnalyticsPopulationScope,
        temporal_semantics: AnalyticsTemporalSemantics,
        zero_denominator_reason: str,
    ) -> AnalyticsMetricEnvelope:
        if denominator == 0:
            if numerator != 0 or n != 0:
                raise ValueError("analytics_zero_denominator_population_invalid")
            return AnalyticsMetricEnvelope(
                metric_id=metric_id,
                family=temporal_semantics.family,
                state=AnalyticsResultState.NOT_APPLICABLE,
                numerator=0,
                denominator=0,
                value=None,
                n=0,
                exclusions=exclusions,
                as_of=as_of,
                filters=filters,
                source=source,
                population_scope=population_scope,
                temporal_semantics=temporal_semantics,
                reason=zero_denominator_reason,
            )
        return AnalyticsMetricEnvelope(
            metric_id=metric_id,
            family=temporal_semantics.family,
            state=AnalyticsResultState.AVAILABLE,
            numerator=numerator,
            denominator=denominator,
            value=numerator / denominator,
            n=n,
            exclusions=exclusions,
            as_of=as_of,
            filters=filters,
            source=source,
            population_scope=population_scope,
            temporal_semantics=temporal_semantics,
        )

    @staticmethod
    def non_numeric_metric(
        *,
        metric_id: str,
        family: AnalyticsMetricFamily,
        state: AnalyticsResultState,
        reason: str | None,
        exclusions: AnalyticsExclusionSummary,
        as_of: datetime,
        filters: tuple[AnalyticsFilterClause, ...],
        source: AnalyticsSourceAuthority,
        population_scope: AnalyticsPopulationScope,
        temporal_semantics: AnalyticsTemporalSemantics,
    ) -> AnalyticsMetricEnvelope:
        if state in {
            AnalyticsResultState.AVAILABLE,
            AnalyticsResultState.NOT_APPLICABLE,
        }:
            raise ValueError("analytics_non_numeric_state_invalid")
        return AnalyticsMetricEnvelope(
            metric_id=metric_id,
            family=family,
            state=state,
            numerator=None,
            denominator=None,
            value=None,
            n=0,
            exclusions=exclusions,
            as_of=as_of,
            filters=filters,
            source=source,
            population_scope=population_scope,
            temporal_semantics=temporal_semantics,
            reason=reason,
        )

    @staticmethod
    def projection(
        *,
        query: AnalyticsFoundationQuery,
        as_of: datetime,
        metrics: tuple[AnalyticsMetricEnvelope, ...],
    ) -> AnalyticsFoundationProjection:
        cut = require_utc_datetime(as_of, field="projection_as_of")
        seen: set[str] = set()
        for metric in metrics:
            if metric.metric_id in seen:
                raise ValueError("analytics_metric_id_duplicate")
            seen.add(metric.metric_id)
            if metric.filters != query.filters:
                raise ValueError("analytics_metric_filter_mismatch")
            if metric.as_of != cut:
                raise ValueError("analytics_metric_as_of_mismatch")
            if metric.temporal_semantics.window != query.window:
                raise ValueError("analytics_metric_window_mismatch")
        return AnalyticsFoundationProjection(
            contract_version=ANALYTICS_FOUNDATION_CONTRACT_VERSION,
            query_fingerprint=query.fingerprint,
            filters=query.filters,
            as_of=cut,
            metrics=metrics,
        )


__all__ = [
    "AnalyticsEvidenceBinding",
    "AnalyticsFoundationService",
    "AnalyticsLifecycleEvent",
    "classify_evidence_currentness",
    "governed_episode_duration",
]
