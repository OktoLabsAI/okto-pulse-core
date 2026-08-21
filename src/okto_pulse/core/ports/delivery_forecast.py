"""Governed, read-only Delivery Forecast contracts.

The forecast boundary is deliberately separate from the Phase 1 delivery
commitment contract.  Editions provide authorized source evidence; Core owns
readiness, estimator selection and the closed public result union.
"""

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
    require_utc_datetime,
)
from okto_pulse.core.ports.analytics_provenance import (
    AnalyticsProjectionCurrentness,
    AnalyticsProjectionProvenance,
)
from okto_pulse.core.ports.delivery_commitment import (
    DELIVERY_COMMITMENT_CONTRACT_VERSION,
)


DELIVERY_FORECAST_CONTRACT_VERSION = "1"
FORECAST_READINESS_RULE_VERSION = "forecast-readiness-v1"
DEFAULT_FORECAST_METHOD_VERSION = "empirical-quantile-v1"
DEFAULT_FORECAST_HORIZON = "next_sprint"
DEFAULT_FORECAST_CONFIDENCE_LEVEL = 0.8
DEFAULT_FORECAST_MINIMUM_OBSERVATIONS = 8
MAX_FORECAST_OBSERVATIONS = 10_000
MAX_FORECAST_BACKTEST_OUTCOMES = 10_000
_TOKEN = re.compile(r"^[a-z][a-z0-9_.-]{0,127}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


def _text(value: str, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"delivery_forecast_{field}_required")
    return value.strip()


def _token(value: str, *, field: str) -> str:
    normalized = _text(value, field=field)
    if not _TOKEN.fullmatch(normalized):
        raise ValueError(f"delivery_forecast_{field}_invalid")
    return normalized


def _count(value: int, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"delivery_forecast_{field}_invalid")
    return value


def _number(value: object, *, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"delivery_forecast_{field}_invalid")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"delivery_forecast_{field}_invalid")
    return number


def _utc_text(value: datetime) -> str:
    return value.isoformat(timespec="microseconds").replace("+00:00", "Z")


class ForecastReadinessState(str, Enum):
    READY = "ready"
    INSUFFICIENT_HISTORY = "insufficient_history"
    UNAVAILABLE = "unavailable"
    RESTRICTED = "restricted"
    EMPTY = "empty"


class DeliveryForecastResultState(str, Enum):
    AVAILABLE = "available"
    PARTIAL = "partial"
    UNAVAILABLE = "unavailable"
    RESTRICTED = "restricted"
    EMPTY = "empty"
    ERROR = "error"


class ForecastBacktestState(str, Enum):
    AVAILABLE = "available"
    UNAVAILABLE = "unavailable"
    EMPTY = "empty"


class ForecastInputState(str, Enum):
    AVAILABLE = "available"
    UNAVAILABLE = "unavailable"
    RESTRICTED = "restricted"
    EMPTY = "empty"


class DeliveryForecastError(RuntimeError):
    def __init__(self, code: str, message: str, *, http_status: int) -> None:
        super().__init__(message)
        self.code = _token(code, field="error_code")
        self.message = _text(message, field="error_message")
        if isinstance(http_status, bool) or not isinstance(http_status, int):
            raise ValueError("delivery_forecast_http_status_invalid")
        self.http_status = http_status

    def canonical_dict(self) -> dict[str, object]:
        return {
            "error": self.code,
            "message": self.message,
            "status_code": self.http_status,
        }


class ForecastDependencyContractMismatch(DeliveryForecastError):
    def __init__(self, message: str = "Forecast dependency contract mismatch.") -> None:
        super().__init__(
            "forecast_dependency_contract_mismatch", message, http_status=409
        )


class ForecastInputUnavailable(DeliveryForecastError):
    def __init__(self, message: str = "Forecast input is unavailable.") -> None:
        super().__init__("forecast_input_unavailable", message, http_status=503)


class HistoricalAnalyticsAsOfUnsupported(DeliveryForecastError):
    def __init__(
        self, message: str = "Historical as_of is unsupported by this live projection."
    ) -> None:
        super().__init__(
            "analytics_historical_as_of_unsupported", message, http_status=409
        )


@dataclass(frozen=True, slots=True)
class ForecastReadinessQuery:
    foundation: AnalyticsFoundationQuery
    horizon: str = DEFAULT_FORECAST_HORIZON
    confidence_level: float = DEFAULT_FORECAST_CONFIDENCE_LEVEL
    method_version: str = DEFAULT_FORECAST_METHOD_VERSION
    historical_as_of: datetime | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.foundation, AnalyticsFoundationQuery):
            raise ValueError("delivery_forecast_foundation_query_required")
        object.__setattr__(self, "horizon", _token(self.horizon, field="horizon"))
        confidence = _number(self.confidence_level, field="confidence_level")
        if not 0 < confidence < 1:
            raise ValueError("delivery_forecast_confidence_level_invalid")
        object.__setattr__(self, "confidence_level", confidence)
        object.__setattr__(
            self,
            "method_version",
            _token(self.method_version, field="method_version"),
        )
        if self.historical_as_of is not None:
            object.__setattr__(
                self,
                "historical_as_of",
                require_utc_datetime(
                    self.historical_as_of, field="forecast_historical_as_of"
                ),
            )

    @property
    def fingerprint(self) -> str:
        payload = {
            "contract_version": DELIVERY_FORECAST_CONTRACT_VERSION,
            "foundation_fingerprint": self.foundation.fingerprint,
            "horizon": self.horizon,
            "confidence_level": self.confidence_level,
            "method_version": self.method_version,
            "historical_as_of": (
                _utc_text(self.historical_as_of)
                if self.historical_as_of is not None
                else None
            ),
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()


@dataclass(frozen=True, slots=True)
class ForecastObservation:
    observation_id: str
    delivered_count: int
    source_ref: str
    completed_at: datetime | None
    comparable: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "observation_id",
            _text(self.observation_id, field="observation_id"),
        )
        object.__setattr__(
            self,
            "delivered_count",
            _count(self.delivered_count, field="delivered_count"),
        )
        object.__setattr__(
            self, "source_ref", _text(self.source_ref, field="source_ref")
        )
        if self.completed_at is not None:
            object.__setattr__(
                self,
                "completed_at",
                require_utc_datetime(
                    self.completed_at, field="forecast_observation_completed_at"
                ),
            )
        if not isinstance(self.comparable, bool):
            raise ValueError("delivery_forecast_comparable_invalid")


@dataclass(frozen=True, slots=True)
class ForecastBacktestOutcome:
    outcome_id: str
    method_version: str
    forecast_value: float
    lower_bound: float
    upper_bound: float
    actual_value: float
    evaluated_at: datetime

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "outcome_id", _text(self.outcome_id, field="outcome_id")
        )
        object.__setattr__(
            self, "method_version", _token(self.method_version, field="method_version")
        )
        for field in ("forecast_value", "lower_bound", "upper_bound", "actual_value"):
            object.__setattr__(self, field, _number(getattr(self, field), field=field))
        if (
            self.lower_bound > self.forecast_value
            or self.forecast_value > self.upper_bound
        ):
            raise ValueError("delivery_forecast_backtest_interval_invalid")
        object.__setattr__(
            self,
            "evaluated_at",
            require_utc_datetime(self.evaluated_at, field="backtest_evaluated_at"),
        )


@dataclass(frozen=True, slots=True)
class DeliveryForecastEvidence:
    board_id: str
    foundation_contract_version: str
    delivery_contract_version: str
    observed_at: datetime
    input_state: ForecastInputState
    minimum_observations: int
    readiness_rule_version: str
    observations: tuple[ForecastObservation, ...]
    backtest_outcomes: tuple[ForecastBacktestOutcome, ...]
    population_scope: AnalyticsPopulationScope
    exclusions: AnalyticsExclusionSummary
    currentness: AnalyticsProjectionCurrentness
    sources: tuple[AnalyticsSourceAuthority, ...]
    reason: str | None = None
    historical_as_of_supported: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "board_id", _text(self.board_id, field="board_id"))
        object.__setattr__(
            self,
            "foundation_contract_version",
            _text(
                self.foundation_contract_version, field="foundation_contract_version"
            ),
        )
        object.__setattr__(
            self,
            "delivery_contract_version",
            _text(self.delivery_contract_version, field="delivery_contract_version"),
        )
        object.__setattr__(
            self,
            "observed_at",
            require_utc_datetime(
                self.observed_at, field="forecast_evidence_observed_at"
            ),
        )
        if not isinstance(self.input_state, ForecastInputState):
            raise ValueError("delivery_forecast_input_state_invalid")
        minimum = _count(self.minimum_observations, field="minimum_observations")
        if minimum < 1:
            raise ValueError("delivery_forecast_minimum_observations_invalid")
        object.__setattr__(self, "minimum_observations", minimum)
        object.__setattr__(
            self,
            "readiness_rule_version",
            _token(self.readiness_rule_version, field="readiness_rule_version"),
        )
        if not isinstance(self.observations, tuple) or any(
            not isinstance(item, ForecastObservation) for item in self.observations
        ):
            raise ValueError("delivery_forecast_observations_invalid")
        if len(self.observations) > MAX_FORECAST_OBSERVATIONS:
            raise ValueError("delivery_forecast_observations_too_many")
        identities = tuple(item.observation_id for item in self.observations)
        if (
            len(set(identities)) != len(identities)
            or tuple(sorted(identities)) != identities
        ):
            raise ValueError("delivery_forecast_observations_not_canonical")
        if not isinstance(self.backtest_outcomes, tuple) or any(
            not isinstance(item, ForecastBacktestOutcome)
            for item in self.backtest_outcomes
        ):
            raise ValueError("delivery_forecast_backtests_invalid")
        if len(self.backtest_outcomes) > MAX_FORECAST_BACKTEST_OUTCOMES:
            raise ValueError("delivery_forecast_backtests_too_many")
        backtest_ids = tuple(item.outcome_id for item in self.backtest_outcomes)
        if (
            len(set(backtest_ids)) != len(backtest_ids)
            or tuple(sorted(backtest_ids)) != backtest_ids
        ):
            raise ValueError("delivery_forecast_backtests_not_canonical")
        if not isinstance(self.population_scope, AnalyticsPopulationScope):
            raise ValueError("delivery_forecast_population_scope_invalid")
        if not isinstance(self.exclusions, AnalyticsExclusionSummary):
            raise ValueError("delivery_forecast_exclusions_invalid")
        if not isinstance(self.currentness, AnalyticsProjectionCurrentness):
            raise ValueError("delivery_forecast_currentness_invalid")
        if not isinstance(self.sources, tuple) or any(
            not isinstance(item, AnalyticsSourceAuthority) for item in self.sources
        ):
            raise ValueError("delivery_forecast_sources_invalid")
        if not isinstance(self.historical_as_of_supported, bool):
            raise ValueError("delivery_forecast_historical_support_invalid")
        if self.input_state is ForecastInputState.AVAILABLE:
            if self.reason is not None or not self.sources:
                raise ValueError("delivery_forecast_available_evidence_shape_invalid")
        else:
            if not isinstance(self.reason, str) or not self.reason.strip():
                raise ValueError("delivery_forecast_input_reason_required")
            if self.observations or self.backtest_outcomes:
                raise ValueError("delivery_forecast_nonavailable_evidence_leak")
            object.__setattr__(self, "reason", self.reason.strip())


@dataclass(frozen=True, slots=True)
class ForecastSourcePeriod:
    from_inclusive: datetime
    to_inclusive: datetime

    def __post_init__(self) -> None:
        start = require_utc_datetime(self.from_inclusive, field="forecast_source_from")
        end = require_utc_datetime(self.to_inclusive, field="forecast_source_to")
        if start > end:
            raise ValueError("delivery_forecast_source_period_invalid")
        object.__setattr__(self, "from_inclusive", start)
        object.__setattr__(self, "to_inclusive", end)

    def canonical_dict(self) -> dict[str, str]:
        return {
            "from": _utc_text(self.from_inclusive),
            "to": _utc_text(self.to_inclusive),
        }


@dataclass(frozen=True, slots=True)
class ForecastReadiness:
    ready: bool
    state: ForecastReadinessState
    actual_observations: int
    required_observations: int
    rule_version: str
    reason: str | None = None
    remediation: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.ready, bool) or not isinstance(
            self.state, ForecastReadinessState
        ):
            raise ValueError("delivery_forecast_readiness_invalid")
        if self.ready != (self.state is ForecastReadinessState.READY):
            raise ValueError("delivery_forecast_readiness_state_mismatch")
        object.__setattr__(
            self,
            "actual_observations",
            _count(self.actual_observations, field="actual_observations"),
        )
        required = _count(self.required_observations, field="required_observations")
        if required < 1:
            raise ValueError("delivery_forecast_required_observations_invalid")
        object.__setattr__(self, "required_observations", required)
        object.__setattr__(
            self, "rule_version", _token(self.rule_version, field="rule_version")
        )
        if self.ready:
            if self.reason is not None or self.remediation is not None:
                raise ValueError("delivery_forecast_ready_reason_unexpected")
        elif not all(
            isinstance(item, str) and item.strip()
            for item in (self.reason, self.remediation)
        ):
            raise ValueError("delivery_forecast_nonready_guidance_required")

    def canonical_dict(self) -> dict[str, object]:
        return {
            "ready": self.ready,
            "state": self.state.value,
            "reason": self.reason,
            "remediation": self.remediation,
            "actual_observations": self.actual_observations,
            "required_observations": self.required_observations,
            "rule_version": self.rule_version,
        }


@dataclass(frozen=True, slots=True)
class ForecastEstimate:
    point: float
    lower_bound: float
    upper_bound: float
    confidence_level: float
    horizon: str
    assumptions: tuple[str, ...]
    sample_size: int
    source_period: ForecastSourcePeriod
    method_version: str

    def __post_init__(self) -> None:
        for field in ("point", "lower_bound", "upper_bound", "confidence_level"):
            object.__setattr__(self, field, _number(getattr(self, field), field=field))
        if self.lower_bound > self.point or self.point > self.upper_bound:
            raise ValueError("delivery_forecast_estimate_interval_invalid")
        if not 0 < self.confidence_level < 1:
            raise ValueError("delivery_forecast_confidence_level_invalid")
        object.__setattr__(self, "horizon", _token(self.horizon, field="horizon"))
        if not isinstance(self.assumptions, tuple) or not self.assumptions:
            raise ValueError("delivery_forecast_assumptions_required")
        assumptions = tuple(
            _text(item, field="assumption") for item in self.assumptions
        )
        if len(set(assumptions)) != len(assumptions):
            raise ValueError("delivery_forecast_assumptions_duplicate")
        object.__setattr__(self, "assumptions", assumptions)
        if _count(self.sample_size, field="sample_size") < 1:
            raise ValueError("delivery_forecast_sample_size_invalid")
        if not isinstance(self.source_period, ForecastSourcePeriod):
            raise ValueError("delivery_forecast_source_period_required")
        object.__setattr__(
            self, "method_version", _token(self.method_version, field="method_version")
        )

    def canonical_dict(self) -> dict[str, object]:
        return {
            "point": self.point,
            "lower_bound": self.lower_bound,
            "upper_bound": self.upper_bound,
            "confidence_level": self.confidence_level,
            "horizon": self.horizon,
            "assumptions": list(self.assumptions),
            "sample_size": self.sample_size,
            "source_period": self.source_period.canonical_dict(),
            "method_version": self.method_version,
        }


@dataclass(frozen=True, slots=True)
class ForecastBacktest:
    state: ForecastBacktestState
    method_version: str
    sample_size: int
    evaluation_window: ForecastSourcePeriod | None
    error: float | None = None
    calibration: float | None = None
    reason: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.state, ForecastBacktestState):
            raise ValueError("delivery_forecast_backtest_state_invalid")
        object.__setattr__(
            self, "method_version", _token(self.method_version, field="method_version")
        )
        object.__setattr__(
            self, "sample_size", _count(self.sample_size, field="sample_size")
        )
        if self.state is ForecastBacktestState.AVAILABLE:
            if self.sample_size < 1 or self.evaluation_window is None:
                raise ValueError("delivery_forecast_backtest_available_shape_invalid")
            error = _number(self.error, field="backtest_error")
            calibration = _number(self.calibration, field="calibration")
            object.__setattr__(self, "error", error)
            object.__setattr__(self, "calibration", calibration)
            if error < 0 or not 0 <= calibration <= 1:
                raise ValueError("delivery_forecast_backtest_measure_invalid")
            if self.reason is not None:
                raise ValueError("delivery_forecast_backtest_reason_unexpected")
        else:
            if self.error is not None or self.calibration is not None:
                raise ValueError("delivery_forecast_backtest_numeric_leak")
            if not isinstance(self.reason, str) or not self.reason.strip():
                raise ValueError("delivery_forecast_backtest_reason_required")
            if self.state is ForecastBacktestState.EMPTY and self.sample_size != 0:
                raise ValueError("delivery_forecast_backtest_empty_sample_invalid")

    def canonical_dict(self) -> dict[str, object]:
        return {
            "state": self.state.value,
            "error": self.error,
            "calibration": self.calibration,
            "method_version": self.method_version,
            "sample_size": self.sample_size,
            "evaluation_window": (
                self.evaluation_window.canonical_dict()
                if self.evaluation_window is not None
                else None
            ),
            "reason": self.reason,
        }


@dataclass(frozen=True, slots=True)
class DeliveryForecastProjection:
    contract_version: str
    foundation_version: str
    delivery_phase_1_version: str
    query_fingerprint: str
    filters: tuple[AnalyticsFilterClause, ...]
    as_of: datetime
    board_id: str
    result_state: DeliveryForecastResultState
    provenance: AnalyticsProjectionProvenance
    readiness: ForecastReadiness
    forecast: ForecastEstimate | None
    backtest: ForecastBacktest
    population_scope: AnalyticsPopulationScope
    exclusions: AnalyticsExclusionSummary

    def __post_init__(self) -> None:
        if self.contract_version != DELIVERY_FORECAST_CONTRACT_VERSION:
            raise ValueError("delivery_forecast_contract_version_unsupported")
        if self.foundation_version != ANALYTICS_FOUNDATION_CONTRACT_VERSION:
            raise ValueError("delivery_forecast_foundation_version_unsupported")
        if self.delivery_phase_1_version != DELIVERY_COMMITMENT_CONTRACT_VERSION:
            raise ValueError("delivery_forecast_phase_1_version_unsupported")
        if not isinstance(self.query_fingerprint, str) or not _SHA256.fullmatch(
            self.query_fingerprint
        ):
            raise ValueError("delivery_forecast_query_fingerprint_invalid")
        if not isinstance(self.filters, tuple) or any(
            not isinstance(item, AnalyticsFilterClause) for item in self.filters
        ):
            raise ValueError("delivery_forecast_filters_invalid")
        object.__setattr__(
            self, "as_of", require_utc_datetime(self.as_of, field="forecast_as_of")
        )
        object.__setattr__(self, "board_id", _text(self.board_id, field="board_id"))
        if not isinstance(self.result_state, DeliveryForecastResultState):
            raise ValueError("delivery_forecast_result_state_invalid")
        if not isinstance(self.provenance, AnalyticsProjectionProvenance):
            raise ValueError("delivery_forecast_provenance_invalid")
        if not isinstance(self.readiness, ForecastReadiness):
            raise ValueError("delivery_forecast_readiness_invalid")
        if self.readiness.ready != (self.forecast is not None):
            raise ValueError("delivery_forecast_readiness_estimate_mismatch")
        if self.forecast is not None and not isinstance(
            self.forecast, ForecastEstimate
        ):
            raise ValueError("delivery_forecast_estimate_invalid")
        if not isinstance(self.backtest, ForecastBacktest):
            raise ValueError("delivery_forecast_backtest_invalid")
        if not isinstance(self.population_scope, AnalyticsPopulationScope):
            raise ValueError("delivery_forecast_population_scope_invalid")
        if not isinstance(self.exclusions, AnalyticsExclusionSummary):
            raise ValueError("delivery_forecast_exclusions_invalid")

    def canonical_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "contract_version": self.contract_version,
            "dependency_versions": {
                "analytics_foundation": self.foundation_version,
                "delivery_phase_1": self.delivery_phase_1_version,
            },
            "query_fingerprint": self.query_fingerprint,
            "filters": [item.canonical_dict() for item in self.filters],
            "as_of": _utc_text(self.as_of),
            "board_id": self.board_id,
            "result_state": self.result_state.value,
            "provenance": self.provenance.canonical_dict(),
            "readiness": self.readiness.canonical_dict(),
            "backtest": self.backtest.canonical_dict(),
            "population_scope": self.population_scope.canonical_dict(),
            "exclusions": self.exclusions.canonical_dict(),
        }
        if self.forecast is not None:
            payload["forecast"] = self.forecast.canonical_dict()
        return payload


@runtime_checkable
class DeliveryForecastEvidencePort(Protocol):
    async def load(
        self, context: object, *, query: ForecastReadinessQuery
    ) -> DeliveryForecastEvidence: ...


@runtime_checkable
class DeliveryForecastProjectionPort(Protocol):
    async def project(
        self, context: object, *, query: ForecastReadinessQuery
    ) -> DeliveryForecastProjection: ...


__all__ = [
    "DEFAULT_FORECAST_CONFIDENCE_LEVEL",
    "DEFAULT_FORECAST_HORIZON",
    "DEFAULT_FORECAST_METHOD_VERSION",
    "DEFAULT_FORECAST_MINIMUM_OBSERVATIONS",
    "DELIVERY_FORECAST_CONTRACT_VERSION",
    "FORECAST_READINESS_RULE_VERSION",
    "DeliveryForecastError",
    "DeliveryForecastEvidence",
    "DeliveryForecastEvidencePort",
    "DeliveryForecastProjection",
    "DeliveryForecastProjectionPort",
    "DeliveryForecastResultState",
    "ForecastBacktest",
    "ForecastBacktestOutcome",
    "ForecastBacktestState",
    "ForecastDependencyContractMismatch",
    "ForecastEstimate",
    "ForecastInputState",
    "ForecastInputUnavailable",
    "ForecastObservation",
    "ForecastReadiness",
    "ForecastReadinessQuery",
    "ForecastReadinessState",
    "ForecastSourcePeriod",
    "HistoricalAnalyticsAsOfUnsupported",
]
