"""Compose public KG health/effectiveness contracts for Board Analytics."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any

from okto_pulse.core.ports.analytics_foundation import (
    ANALYTICS_FOUNDATION_CONTRACT_VERSION,
    AnalyticsExclusionSummary,
    AnalyticsFoundationQuery,
    AnalyticsPopulationScope,
    require_utc_datetime,
)
from okto_pulse.core.ports.board_kg_analytics import (
    BOARD_KG_ANALYTICS_CONTRACT_VERSION,
    BoardKgAnalyticsProjection,
    BoardKgAnalyticsResultState,
    BoardKgDebtDomains,
    BoardKgHealthComponent,
    BoardKgHealthState,
    CognitiveEffectivenessSlice,
)


_RESULT_SEVERITY = {
    BoardKgAnalyticsResultState.AVAILABLE: 0,
    BoardKgAnalyticsResultState.EMPTY: 1,
    BoardKgAnalyticsResultState.UNAVAILABLE: 2,
    BoardKgAnalyticsResultState.RESTRICTED: 3,
    BoardKgAnalyticsResultState.ERROR: 4,
}


def _mapping(value: object, *, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"board_kg_analytics_{field}_invalid")
    return value


def _health_state(value: object, *, field: str) -> BoardKgHealthState:
    try:
        return BoardKgHealthState(str(value))
    except ValueError as exc:
        raise ValueError(f"board_kg_analytics_{field}_invalid") from exc


def _int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"board_kg_analytics_{field}_invalid")
    return value


class BoardKgAnalyticsService:
    """Keep health classification orthogonal to evidence availability."""

    @staticmethod
    def _health_result_state(payload: Mapping[str, Any]) -> BoardKgAnalyticsResultState:
        raw = str(payload.get("metric_status") or "").lower()
        if raw in {"available", "ok"}:
            return BoardKgAnalyticsResultState.AVAILABLE
        if raw == "restricted":
            return BoardKgAnalyticsResultState.RESTRICTED
        if raw == "error":
            return BoardKgAnalyticsResultState.ERROR
        return BoardKgAnalyticsResultState.UNAVAILABLE

    @staticmethod
    def _components(
        payload: Mapping[str, Any], result_state: BoardKgAnalyticsResultState
    ) -> tuple[BoardKgHealthComponent, ...]:
        reason = str(
            payload.get("classification_reason") or "classification_unavailable"
        )
        components: list[BoardKgHealthComponent] = []
        for name, field in (
            ("discovery", "discovery_state"),
            ("graph", "graph_state"),
        ):
            if field not in payload:
                continue
            components.append(
                BoardKgHealthComponent(
                    component=name,
                    health_state=_health_state(payload[field], field=field),
                    result_state=result_state,
                    classification_reason=reason,
                )
            )
        return tuple(sorted(components, key=lambda item: item.component))

    @staticmethod
    def _debt_domains(
        payload: Mapping[str, Any], result_state: BoardKgAnalyticsResultState
    ) -> BoardKgDebtDomains:
        if result_state is not BoardKgAnalyticsResultState.AVAILABLE:
            return BoardKgDebtDomains(result_state, None, None, None)
        canonical = _mapping(payload.get("canonical_debt"), field="canonical_debt")
        return BoardKgDebtDomains(
            result_state=result_state,
            active_queue_count=_int(payload.get("queue_depth"), field="queue_depth"),
            technical_dlq_count=_int(
                payload.get("dead_letter_count"), field="dead_letter_count"
            ),
            canonical_debt_count=_int(
                canonical.get("open_count"), field="canonical_debt_count"
            ),
        )

    @staticmethod
    def _effectiveness(
        payload: Mapping[str, Any],
        result_state: BoardKgAnalyticsResultState | None = None,
    ) -> CognitiveEffectivenessSlice:
        effective_state = result_state
        if effective_state is None:
            if payload.get("kg_projection_available") is not True:
                effective_state = BoardKgAnalyticsResultState.UNAVAILABLE
            else:
                artifacts = payload.get("artifacts")
                if not isinstance(artifacts, list):
                    raise ValueError("board_kg_analytics_artifacts_invalid")
                effective_state = (
                    BoardKgAnalyticsResultState.EMPTY
                    if not artifacts
                    else BoardKgAnalyticsResultState.AVAILABLE
                )
        if effective_state not in {
            BoardKgAnalyticsResultState.AVAILABLE,
            BoardKgAnalyticsResultState.EMPTY,
        }:
            return CognitiveEffectivenessSlice(
                effective_state, None, None, None, None, None, None
            )
        totals = _mapping(payload.get("totals"), field="effectiveness_totals")
        denominator = len(payload.get("artifacts") or [])
        return CognitiveEffectivenessSlice(
            result_state=effective_state,
            cognitively_effective=bool(payload.get("cognitively_effective")),
            denominator=denominator,
            attempted_count=_int(totals.get("attempted", 0), field="attempted_count"),
            persisted_count=_int(
                totals.get("persisted_or_consolidated", 0), field="persisted_count"
            ),
            technical_dlq_count=_int(totals.get("dlq", 0), field="cognitive_dlq_count"),
            persistence_gap_count=_int(
                totals.get("extractor_triggered_but_not_persisted", 0),
                field="persistence_gap_count",
            ),
        )

    @staticmethod
    def compose(
        *,
        query: AnalyticsFoundationQuery,
        as_of: datetime,
        population_scope: AnalyticsPopulationScope,
        exclusions: AnalyticsExclusionSummary,
        health_payload: Mapping[str, Any],
        effectiveness_payload: Mapping[str, Any],
        effectiveness_result_state: BoardKgAnalyticsResultState | None = None,
    ) -> BoardKgAnalyticsProjection:
        observed_at = require_utc_datetime(as_of, field="board_kg_projection_as_of")
        if query.as_of is not None and query.as_of != observed_at:
            raise ValueError("board_kg_analytics_as_of_mismatch")
        if population_scope.scope_ref != query.actor_scope_ref:
            raise ValueError("board_kg_analytics_population_scope_mismatch")
        if population_scope.accessible_count != 1:
            raise ValueError("board_kg_analytics_board_population_invalid")
        health = _mapping(health_payload, field="health_payload")
        effectiveness = _mapping(effectiveness_payload, field="effectiveness_payload")
        if (
            str(health.get("board_id")) != query.board_id
            or str(effectiveness.get("board_id")) != query.board_id
        ):
            raise ValueError("board_kg_analytics_board_mismatch")
        health_state = _health_state(health.get("overall_state"), field="health_state")
        health_result = BoardKgAnalyticsService._health_result_state(health)
        cognitive = BoardKgAnalyticsService._effectiveness(
            effectiveness, effectiveness_result_state
        )
        result_state = max(
            (health_result, cognitive.result_state), key=_RESULT_SEVERITY.__getitem__
        )
        raw_reasons = health.get("classification_reasons") or ()
        if not isinstance(raw_reasons, (list, tuple)):
            raise ValueError("board_kg_analytics_reason_codes_invalid")
        reasons = tuple(
            sorted({str(item).strip() for item in raw_reasons if str(item).strip()})
        )
        classification_reason = str(
            health.get("classification_reason") or "classification_unavailable"
        )
        return BoardKgAnalyticsProjection(
            contract_version=BOARD_KG_ANALYTICS_CONTRACT_VERSION,
            foundation_version=ANALYTICS_FOUNDATION_CONTRACT_VERSION,
            query_fingerprint=query.fingerprint,
            filters=query.filters,
            as_of=observed_at,
            board_id=query.board_id,
            result_state=result_state,
            health_state=health_state,
            classification_reason=classification_reason,
            reason_codes=reasons,
            components=BoardKgAnalyticsService._components(health, health_result),
            debt_domains=BoardKgAnalyticsService._debt_domains(health, health_result),
            cognitive_effectiveness=cognitive,
            population_scope=population_scope,
            exclusions=exclusions,
        )

    @staticmethod
    async def project_from_public_services(
        context: object,
        *,
        query: AnalyticsFoundationQuery,
        as_of: datetime,
        population_scope: AnalyticsPopulationScope,
        exclusions: AnalyticsExclusionSummary,
    ) -> BoardKgAnalyticsProjection:
        # These imports are intentionally public service boundaries. No graph
        # adapter, private registry or persistence mechanism is reachable here.
        from okto_pulse.core.services.cognitive_effectiveness_service import (
            build_cognitive_effectiveness_inventory,
        )
        from okto_pulse.core.services.kg_health_service import get_kg_health

        health = await get_kg_health(query.board_id, context)
        effectiveness = await build_cognitive_effectiveness_inventory(
            context, query.board_id
        )
        return BoardKgAnalyticsService.compose(
            query=query,
            as_of=as_of,
            population_scope=population_scope,
            exclusions=exclusions,
            health_payload=health,
            effectiveness_payload=effectiveness,
        )


__all__ = ["BoardKgAnalyticsService"]
