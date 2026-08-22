"""Deterministic readiness, forecast and backtest policy."""

from __future__ import annotations

import math

from okto_pulse.core.ports.analytics_foundation import (
    ANALYTICS_FOUNDATION_CONTRACT_VERSION,
)
from okto_pulse.core.ports.analytics_provenance import (
    AnalyticsProjectionCurrentness,
    AnalyticsProjectionProvenance,
)
from okto_pulse.core.ports.delivery_commitment import (
    DELIVERY_COMMITMENT_CONTRACT_VERSION,
)
from okto_pulse.core.ports.delivery_forecast import (
    DELIVERY_FORECAST_CONTRACT_VERSION,
    DeliveryForecastEvidence,
    DeliveryForecastEvidencePort,
    DeliveryForecastProjection,
    DeliveryForecastResultState,
    ForecastBacktest,
    ForecastBacktestState,
    ForecastDependencyContractMismatch,
    ForecastEstimate,
    ForecastInputState,
    ForecastReadiness,
    ForecastReadinessQuery,
    ForecastReadinessState,
    ForecastSourcePeriod,
    HistoricalAnalyticsAsOfUnsupported,
)


_REMEDIATION = {
    ForecastReadinessState.INSUFFICIENT_HISTORY: (
        "Complete more comparable sprints with current activation baselines."
    ),
    ForecastReadinessState.UNAVAILABLE: (
        "Restore complete, current delivery timestamps and retry."
    ),
    ForecastReadinessState.RESTRICTED: (
        "Request access to the required aggregate delivery evidence."
    ),
    ForecastReadinessState.EMPTY: (
        "Select a period containing eligible completed delivery observations."
    ),
}


def _quantile(values: tuple[int, ...], probability: float) -> float:
    ordered = tuple(sorted(values))
    if len(ordered) == 1:
        return float(ordered[0])
    position = (len(ordered) - 1) * probability
    lower_index = math.floor(position)
    upper_index = math.ceil(position)
    if lower_index == upper_index:
        return float(ordered[lower_index])
    fraction = position - lower_index
    return float(
        ordered[lower_index] + (ordered[upper_index] - ordered[lower_index]) * fraction
    )


def _readiness(
    evidence: DeliveryForecastEvidence, query: ForecastReadinessQuery
) -> tuple[ForecastReadiness, tuple]:
    required = evidence.minimum_observations
    if evidence.input_state is ForecastInputState.RESTRICTED:
        state = ForecastReadinessState.RESTRICTED
        actual = 0
        observations = ()
        reason = evidence.reason or "forecast_input_restricted"
    elif evidence.input_state is ForecastInputState.UNAVAILABLE:
        state = ForecastReadinessState.UNAVAILABLE
        actual = 0
        observations = ()
        reason = evidence.reason or "forecast_input_unavailable"
    elif evidence.input_state is ForecastInputState.EMPTY or not evidence.observations:
        state = ForecastReadinessState.EMPTY
        actual = 0
        observations = ()
        reason = evidence.reason or "forecast_input_empty"
    elif evidence.currentness is not AnalyticsProjectionCurrentness.CURRENT:
        state = ForecastReadinessState.UNAVAILABLE
        actual = 0
        observations = ()
        reason = "forecast_input_not_current"
    elif any(item.completed_at is None for item in evidence.observations):
        state = ForecastReadinessState.UNAVAILABLE
        actual = 0
        observations = ()
        reason = "required_completion_timestamp_missing"
    elif any(
        not query.foundation.window.contains(item.completed_at)
        for item in evidence.observations
        if item.completed_at is not None
    ):
        state = ForecastReadinessState.UNAVAILABLE
        actual = 0
        observations = ()
        reason = "required_completion_timestamp_outside_effective_window"
    else:
        observations = tuple(item for item in evidence.observations if item.comparable)
        actual = len(observations)
        if actual < required:
            state = ForecastReadinessState.INSUFFICIENT_HISTORY
            reason = "minimum_comparable_observations_not_met"
        else:
            state = ForecastReadinessState.READY
            reason = None
    return (
        ForecastReadiness(
            ready=state is ForecastReadinessState.READY,
            state=state,
            reason=reason,
            remediation=None
            if state is ForecastReadinessState.READY
            else _REMEDIATION[state],
            actual_observations=actual,
            required_observations=required,
            rule_version=evidence.readiness_rule_version,
        ),
        observations,
    )


def _estimate(query: ForecastReadinessQuery, observations: tuple) -> ForecastEstimate:
    completed_at = tuple(item.completed_at for item in observations)
    if any(item is None for item in completed_at):
        raise ValueError("delivery_forecast_complete_timestamps_required")
    counts = tuple(item.delivered_count for item in observations)
    tail = (1 - query.confidence_level) / 2
    return ForecastEstimate(
        point=round(_quantile(counts, 0.5), 6),
        lower_bound=round(_quantile(counts, tail), 6),
        upper_bound=round(_quantile(counts, 1 - tail), 6),
        confidence_level=query.confidence_level,
        horizon=query.horizon,
        assumptions=(
            "Authorized completed observations are comparable across the selected window.",
            "The empirical delivery distribution remains informative for the requested horizon.",
            "No nominative contribution signal is used.",
        ),
        sample_size=len(observations),
        source_period=ForecastSourcePeriod(
            min(completed_at),  # type: ignore[arg-type]
            max(completed_at),  # type: ignore[arg-type]
        ),
        method_version=query.method_version,
    )


def _backtest(
    evidence: DeliveryForecastEvidence, query: ForecastReadinessQuery
) -> ForecastBacktest:
    outcomes = tuple(
        item
        for item in evidence.backtest_outcomes
        if item.method_version == query.method_version
    )
    if not outcomes:
        return ForecastBacktest(
            state=ForecastBacktestState.EMPTY,
            method_version=query.method_version,
            sample_size=0,
            evaluation_window=None,
            reason="no_comparable_backtest_outcomes",
        )
    absolute_errors = tuple(
        abs(item.forecast_value - item.actual_value) for item in outcomes
    )
    covered = sum(
        1
        for item in outcomes
        if item.lower_bound <= item.actual_value <= item.upper_bound
    )
    return ForecastBacktest(
        state=ForecastBacktestState.AVAILABLE,
        method_version=query.method_version,
        sample_size=len(outcomes),
        evaluation_window=ForecastSourcePeriod(
            min(item.evaluated_at for item in outcomes),
            max(item.evaluated_at for item in outcomes),
        ),
        error=round(sum(absolute_errors) / len(absolute_errors), 6),
        calibration=round(covered / len(outcomes), 6),
    )


def _result_state(
    readiness: ForecastReadiness, backtest: ForecastBacktest
) -> DeliveryForecastResultState:
    if readiness.state is ForecastReadinessState.READY:
        return (
            DeliveryForecastResultState.AVAILABLE
            if backtest.state is ForecastBacktestState.AVAILABLE
            else DeliveryForecastResultState.PARTIAL
        )
    return {
        ForecastReadinessState.INSUFFICIENT_HISTORY: DeliveryForecastResultState.PARTIAL,
        ForecastReadinessState.UNAVAILABLE: DeliveryForecastResultState.UNAVAILABLE,
        ForecastReadinessState.RESTRICTED: DeliveryForecastResultState.RESTRICTED,
        ForecastReadinessState.EMPTY: DeliveryForecastResultState.EMPTY,
    }[readiness.state]


class DeliveryForecastService:
    @staticmethod
    async def project(
        context: object,
        *,
        query: ForecastReadinessQuery,
        evidence_port: DeliveryForecastEvidencePort,
    ) -> DeliveryForecastProjection:
        if query.historical_as_of is not None:
            raise HistoricalAnalyticsAsOfUnsupported()
        evidence = await evidence_port.load(context, query=query)
        if evidence.board_id != query.foundation.board_id:
            raise ValueError("delivery_forecast_board_scope_mismatch")
        if (
            evidence.foundation_contract_version
            != ANALYTICS_FOUNDATION_CONTRACT_VERSION
            or evidence.delivery_contract_version
            != DELIVERY_COMMITMENT_CONTRACT_VERSION
        ):
            raise ForecastDependencyContractMismatch()
        if evidence.historical_as_of_supported:
            # The v1 public route has no snapshot authority. Advertising it from
            # an adapter would otherwise allow a current read to masquerade as history.
            raise ForecastDependencyContractMismatch(
                "Forecast evidence advertises unsupported historical authority."
            )
        if evidence.population_scope.scope_ref != query.foundation.actor_scope_ref:
            raise ValueError("delivery_forecast_population_scope_mismatch")
        if query.foundation.as_of is None:
            raise ValueError("delivery_forecast_projection_as_of_required")
        if evidence.observed_at > query.foundation.as_of:
            raise ValueError("delivery_forecast_evidence_from_future")
        readiness, observations = _readiness(evidence, query)
        forecast = _estimate(query, observations) if readiness.ready else None
        backtest = _backtest(evidence, query)
        if readiness.state in {
            ForecastReadinessState.UNAVAILABLE,
            ForecastReadinessState.RESTRICTED,
            ForecastReadinessState.EMPTY,
        }:
            backtest = ForecastBacktest(
                state=(
                    ForecastBacktestState.EMPTY
                    if readiness.state is ForecastReadinessState.EMPTY
                    else ForecastBacktestState.UNAVAILABLE
                ),
                method_version=query.method_version,
                sample_size=0,
                evaluation_window=None,
                reason="forecast_input_not_ready",
            )
        provenance = AnalyticsProjectionProvenance(
            observed_at=evidence.observed_at,
            currentness=evidence.currentness,
            sources=evidence.sources,
            reason=(
                None
                if evidence.currentness is AnalyticsProjectionCurrentness.CURRENT
                else evidence.reason or "forecast_evidence_not_fully_current"
            ),
        )
        return DeliveryForecastProjection(
            contract_version=DELIVERY_FORECAST_CONTRACT_VERSION,
            foundation_version=ANALYTICS_FOUNDATION_CONTRACT_VERSION,
            delivery_phase_1_version=DELIVERY_COMMITMENT_CONTRACT_VERSION,
            query_fingerprint=query.fingerprint,
            filters=query.foundation.filters,
            as_of=query.foundation.as_of,
            board_id=query.foundation.board_id,
            result_state=_result_state(readiness, backtest),
            provenance=provenance,
            readiness=readiness,
            forecast=forecast,
            backtest=backtest,
            population_scope=evidence.population_scope,
            exclusions=evidence.exclusions,
        )


__all__ = ["DeliveryForecastService"]
