from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from okto_pulse.core.ports.analytics_foundation import (
    ANALYTICS_FOUNDATION_CONTRACT_VERSION,
    AnalyticsExclusionSummary,
    AnalyticsFoundationQuery,
    AnalyticsPopulationScope,
    AnalyticsSourceAuthority,
    AnalyticsUtcWindow,
)
from okto_pulse.core.ports.analytics_provenance import (
    AnalyticsProjectionCurrentness,
)
from okto_pulse.core.ports.delivery_commitment import (
    DELIVERY_COMMITMENT_CONTRACT_VERSION,
)
from okto_pulse.core.ports.delivery_forecast import (
    DEFAULT_FORECAST_METHOD_VERSION,
    DeliveryForecastEvidence,
    DeliveryForecastResultState,
    ForecastBacktestOutcome,
    ForecastDependencyContractMismatch,
    ForecastInputState,
    ForecastObservation,
    ForecastReadinessQuery,
    HistoricalAnalyticsAsOfUnsupported,
)
from okto_pulse.core.services.delivery_forecast import DeliveryForecastService


NOW = datetime(2026, 8, 21, 12, tzinfo=UTC)


class _EvidencePort:
    def __init__(self, evidence: DeliveryForecastEvidence) -> None:
        self.evidence = evidence
        self.called = False

    async def load(self, context: object, *, query: ForecastReadinessQuery):  # noqa: ANN201, ARG002
        self.called = True
        return self.evidence


def _query(*, historical_as_of: datetime | None = None) -> ForecastReadinessQuery:
    return ForecastReadinessQuery(
        foundation=AnalyticsFoundationQuery(
            board_id="board-1",
            actor_scope_ref="actor:user-1",
            window=AnalyticsUtcWindow(NOW - timedelta(days=90), NOW),
            as_of=NOW,
        ),
        historical_as_of=historical_as_of,
    )


def _source() -> tuple[AnalyticsSourceAuthority, ...]:
    return (
        AnalyticsSourceAuthority(
            "delivery_commitment_projection",
            "board:board-1:delivery-commitment:v1",
            "completed_at",
        ),
    )


def _observations(count: int, *, missing_timestamp: bool = False):
    return tuple(
        ForecastObservation(
            observation_id=f"sprint-{index:02d}",
            delivered_count=8 + index,
            source_ref=f"sprint:sprint-{index:02d}:activation-baseline",
            completed_at=(
                None
                if missing_timestamp and index == 0
                else NOW - timedelta(days=(count - index) * 7)
            ),
        )
        for index in range(count)
    )


def _backtests():
    return tuple(
        ForecastBacktestOutcome(
            outcome_id=f"backtest-{index:02d}",
            method_version=DEFAULT_FORECAST_METHOD_VERSION,
            forecast_value=10,
            lower_bound=8,
            upper_bound=12,
            actual_value=9 + index,
            evaluated_at=NOW - timedelta(days=(2 - index) * 7),
        )
        for index in range(2)
    )


def _evidence(
    *,
    state: ForecastInputState = ForecastInputState.AVAILABLE,
    observations=None,
    backtests=None,
    foundation_version: str = ANALYTICS_FOUNDATION_CONTRACT_VERSION,
    currentness: AnalyticsProjectionCurrentness = (
        AnalyticsProjectionCurrentness.CURRENT
    ),
    historical_as_of_supported: bool = False,
    reason: str | None = None,
) -> DeliveryForecastEvidence:
    return DeliveryForecastEvidence(
        board_id="board-1",
        foundation_contract_version=foundation_version,
        delivery_contract_version=DELIVERY_COMMITMENT_CONTRACT_VERSION,
        observed_at=NOW - timedelta(seconds=1),
        input_state=state,
        minimum_observations=3,
        readiness_rule_version="forecast-readiness-v1",
        observations=_observations(4) if observations is None else observations,
        backtest_outcomes=_backtests() if backtests is None else backtests,
        population_scope=AnalyticsPopulationScope("actor:user-1", 4),
        exclusions=AnalyticsExclusionSummary(),
        currentness=currentness,
        sources=_source(),
        reason=reason,
        historical_as_of_supported=historical_as_of_supported,
    )


@pytest.mark.asyncio
async def test_ready_forecast_is_deterministic_and_fully_disclosed() -> None:
    port = _EvidencePort(_evidence())

    first = await DeliveryForecastService.project(
        None, query=_query(), evidence_port=port
    )
    second = await DeliveryForecastService.project(
        None, query=_query(), evidence_port=port
    )

    assert first.canonical_dict() == second.canonical_dict()
    payload = first.canonical_dict()
    assert payload["result_state"] == "available"
    assert payload["readiness"]["state"] == "ready"
    assert payload["forecast"] == {
        "point": 9.5,
        "lower_bound": 8.3,
        "upper_bound": 10.7,
        "confidence_level": 0.8,
        "horizon": "next_sprint",
        "assumptions": [
            "Authorized completed observations are comparable across the selected window.",
            "The empirical delivery distribution remains informative for the requested horizon.",
            "No nominative contribution signal is used.",
        ],
        "sample_size": 4,
        "source_period": {
            "from": "2026-07-24T12:00:00.000000Z",
            "to": "2026-08-14T12:00:00.000000Z",
        },
        "method_version": DEFAULT_FORECAST_METHOD_VERSION,
    }
    assert payload["backtest"]["state"] == "available"


@pytest.mark.asyncio
async def test_insufficient_history_omits_forecast_key_entirely() -> None:
    projection = await DeliveryForecastService.project(
        None,
        query=_query(),
        evidence_port=_EvidencePort(_evidence(observations=_observations(2))),
    )

    payload = projection.canonical_dict()
    assert projection.result_state is DeliveryForecastResultState.PARTIAL
    assert payload["readiness"]["state"] == "insufficient_history"
    assert payload["readiness"]["actual_observations"] == 2
    assert "forecast" not in payload


@pytest.mark.asyncio
async def test_incomplete_required_timestamp_is_unavailable_not_zero() -> None:
    projection = await DeliveryForecastService.project(
        None,
        query=_query(),
        evidence_port=_EvidencePort(
            _evidence(observations=_observations(4, missing_timestamp=True))
        ),
    )

    payload = projection.canonical_dict()
    assert payload["result_state"] == "unavailable"
    assert payload["readiness"]["reason"] == "required_completion_timestamp_missing"
    assert "forecast" not in payload


@pytest.mark.asyncio
async def test_partial_currentness_cannot_emit_a_numeric_forecast() -> None:
    projection = await DeliveryForecastService.project(
        None,
        query=_query(),
        evidence_port=_EvidencePort(
            _evidence(currentness=AnalyticsProjectionCurrentness.PARTIAL)
        ),
    )

    payload = projection.canonical_dict()
    assert payload["result_state"] == "unavailable"
    assert payload["readiness"]["reason"] == "forecast_input_not_current"
    assert "forecast" not in payload


@pytest.mark.asyncio
async def test_observation_outside_effective_window_fails_closed() -> None:
    observation = ForecastObservation(
        "sprint-outside-window",
        12,
        "sprint:sprint-outside-window:activation-baseline",
        NOW - timedelta(days=91),
    )
    projection = await DeliveryForecastService.project(
        None,
        query=_query(),
        evidence_port=_EvidencePort(_evidence(observations=(observation,))),
    )

    payload = projection.canonical_dict()
    assert payload["result_state"] == "unavailable"
    assert payload["readiness"]["reason"] == (
        "required_completion_timestamp_outside_effective_window"
    )
    assert "forecast" not in payload


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("input_state", "result_state"),
    [
        (ForecastInputState.RESTRICTED, "restricted"),
        (ForecastInputState.EMPTY, "empty"),
        (ForecastInputState.UNAVAILABLE, "unavailable"),
    ],
)
async def test_nonavailable_input_has_explicit_state_without_estimate(
    input_state: ForecastInputState, result_state: str
) -> None:
    projection = await DeliveryForecastService.project(
        None,
        query=_query(),
        evidence_port=_EvidencePort(
            _evidence(
                state=input_state,
                observations=(),
                backtests=(),
                reason=f"forecast_input_{input_state.value}",
            )
        ),
    )

    payload = projection.canonical_dict()
    assert payload["result_state"] == result_state
    assert "forecast" not in payload


@pytest.mark.asyncio
async def test_dependency_version_mismatch_is_typed_409() -> None:
    with pytest.raises(ForecastDependencyContractMismatch) as caught:
        await DeliveryForecastService.project(
            None,
            query=_query(),
            evidence_port=_EvidencePort(_evidence(foundation_version="999")),
        )

    assert caught.value.code == "forecast_dependency_contract_mismatch"
    assert caught.value.http_status == 409


@pytest.mark.asyncio
async def test_historical_as_of_is_rejected_before_current_evidence_read() -> None:
    port = _EvidencePort(_evidence())

    with pytest.raises(HistoricalAnalyticsAsOfUnsupported) as caught:
        await DeliveryForecastService.project(
            None,
            query=_query(historical_as_of=NOW - timedelta(days=1)),
            evidence_port=port,
        )

    assert caught.value.code == "analytics_historical_as_of_unsupported"
    assert port.called is False


@pytest.mark.asyncio
async def test_adapter_cannot_advertise_unsupported_historical_authority() -> None:
    with pytest.raises(ForecastDependencyContractMismatch) as caught:
        await DeliveryForecastService.project(
            None,
            query=_query(),
            evidence_port=_EvidencePort(_evidence(historical_as_of_supported=True)),
        )

    assert caught.value.code == "forecast_dependency_contract_mismatch"
