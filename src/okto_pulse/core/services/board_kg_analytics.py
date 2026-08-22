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
from okto_pulse.core.ports.analytics_provenance import (
    AnalyticsProjectionCurrentness,
    AnalyticsProjectionProvenance,
)
from okto_pulse.core.ports.board_kg_analytics import (
    BOARD_KG_ANALYTICS_CONTRACT_VERSION,
    LEGACY_BOARD_KG_ANALYTICS_CONTRACT_VERSION,
    BoardKgAnalyticsContractMismatch,
    BoardKgAnalyticsEvidencePort,
    BoardKgAnalyticsQuery,
    BoardKgAnalyticsResultState,
    BoardKgCognitiveEffectiveness,
    BoardKgCognitiveInventory,
    BoardKgCognitiveStatus,
    BoardKgClassificationState,
    BoardKgDebtDomains,
    BoardKgDomain,
    BoardKgDomainAge,
    BoardKgEffectivenessProjection,
    BoardKgEffectivenessState,
    BoardKgHealthComponent,
    BoardKgHealthEvidenceSnapshot,
    BoardKgHealthState,
    BoardKgHistoricalAsOfUnsupported,
    LegacyBoardKgAnalyticsProjection,
    BoardKgProvenanceKind,
    BoardKgProvenanceMix,
    BoardKgProvenanceSlice,
    BoardKgStatusCount,
    BoardKgTiming,
    CognitiveEffectivenessSlice,
)


_RESULT_SEVERITY = {
    BoardKgAnalyticsResultState.AVAILABLE: 0,
    BoardKgAnalyticsResultState.EMPTY: 1,
    BoardKgAnalyticsResultState.PARTIAL: 2,
    BoardKgAnalyticsResultState.UNAVAILABLE: 3,
    BoardKgAnalyticsResultState.RESTRICTED: 4,
    BoardKgAnalyticsResultState.ERROR: 5,
}


def _text(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"board_kg_analytics_{field}_required")
    return value.strip()


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


def _percentile(values: tuple[float, ...], probability: float) -> float:
    ordered = tuple(sorted(values))
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * probability
    lower_index = int(position)
    upper_index = min(lower_index + 1, len(ordered) - 1)
    fraction = position - lower_index
    return (
        ordered[lower_index] + (ordered[upper_index] - ordered[lower_index]) * fraction
    )


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
    ) -> LegacyBoardKgAnalyticsProjection:
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
        return LegacyBoardKgAnalyticsProjection(
            contract_version=LEGACY_BOARD_KG_ANALYTICS_CONTRACT_VERSION,
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
    ) -> LegacyBoardKgAnalyticsProjection:
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


async def read_board_kg_health_evidence(
    context: object,
    *,
    board_id: str,
) -> BoardKgHealthEvidenceSnapshot:
    """Read KG health through one public, validated Analytics facade."""

    from okto_pulse.core.services.kg_health_service import get_kg_health

    canonical_board_id = _text(board_id, field="board_id")
    payload = _mapping(
        await get_kg_health(canonical_board_id, context),
        field="health_payload",
    )
    if str(payload.get("board_id") or "") != canonical_board_id:
        raise ValueError("board_kg_analytics_board_mismatch")

    health_state = _health_state(payload.get("overall_state"), field="health_state")
    result_state = BoardKgAnalyticsService._health_result_state(payload)
    reason = _text(
        payload.get("classification_reason"),
        field="classification_reason",
    )
    raw_reasons = payload.get("classification_reasons") or (reason,)
    if not isinstance(raw_reasons, (list, tuple)):
        raise ValueError("board_kg_analytics_reason_codes_invalid")
    reasons = tuple(sorted({_text(item, field="reason_code") for item in raw_reasons}))
    return BoardKgHealthEvidenceSnapshot(
        board_id=canonical_board_id,
        health_state=health_state,
        result_state=result_state,
        classification_reason=reason,
        reason_codes=reasons,
        components=BoardKgAnalyticsService._components(payload, result_state),
    )


def resolve_board_kg_cognitive_status(
    *,
    ledger_status: object,
    outcome_type: object,
) -> BoardKgCognitiveStatus | None:
    """Map the internal cognitive ledger vocabulary into the public A6 status."""

    from okto_pulse.core.kg.query_contract import CognitiveOutcomeType

    raw_outcome = getattr(outcome_type, "value", outcome_type)
    if raw_outcome == CognitiveOutcomeType.NO_ACTION_REQUIRED.value:
        return BoardKgCognitiveStatus.NO_ACTION
    raw_status = getattr(ledger_status, "value", ledger_status)
    try:
        return BoardKgCognitiveStatus(str(raw_status))
    except ValueError:
        return None


class BoardKgEffectivenessService:
    """Build the complete A6 projection from a board-scoped public evidence port."""

    @staticmethod
    def _filtered_items(evidence, query: BoardKgAnalyticsQuery):  # noqa: ANN001, ANN205
        statuses = set(query.cognitive_status)
        artifact_types = set(query.artifact_types)
        return tuple(
            item
            for item in evidence.cognitive_items
            if (not statuses or item.status in statuses)
            and (
                not artifact_types
                or item.artifact_id.partition(":")[0] in artifact_types
            )
        )

    @staticmethod
    def _age(
        *, observed_at: datetime, items: tuple, empty_state: BoardKgAnalyticsResultState
    ) -> BoardKgDomainAge:
        ages = tuple(
            max(0.0, (observed_at - item.opened_at).total_seconds() / 3600)
            for item in items
        )
        if not ages:
            return BoardKgDomainAge(empty_state, 0, None, None, None)
        return BoardKgDomainAge(
            BoardKgAnalyticsResultState.AVAILABLE,
            len(ages),
            round(_percentile(ages, 0.5), 6),
            round(_percentile(ages, 0.95), 6),
            round(max(ages), 6),
        )

    @staticmethod
    def _unavailable_inventory(
        state: BoardKgAnalyticsResultState, *, reason: str
    ) -> BoardKgCognitiveInventory:
        return BoardKgCognitiveInventory(
            result_state=state,
            by_status=(),
            total=None,
            overdue_revisits=None,
            age=BoardKgDomainAge(state, 0, None, None, None, reason),
            reason=reason,
        )

    @staticmethod
    def _inventory(*, observed_at: datetime, items: tuple) -> BoardKgCognitiveInventory:
        counts = {
            status: sum(1 for item in items if item.status is status)
            for status in BoardKgCognitiveStatus
        }
        state = (
            BoardKgAnalyticsResultState.EMPTY
            if not items
            else BoardKgAnalyticsResultState.AVAILABLE
        )
        active = tuple(
            item
            for item in items
            if item.status
            in {
                BoardKgCognitiveStatus.PENDING,
                BoardKgCognitiveStatus.IN_PROGRESS,
                BoardKgCognitiveStatus.FAILED,
            }
        )
        return BoardKgCognitiveInventory(
            result_state=state,
            by_status=tuple(
                BoardKgStatusCount(status, counts[status])
                for status in BoardKgCognitiveStatus
            ),
            total=len(items),
            overdue_revisits=sum(1 for item in items if item.overdue_revisit),
            age=BoardKgEffectivenessService._age(
                observed_at=observed_at,
                items=active,
                empty_state=BoardKgAnalyticsResultState.EMPTY,
            ),
        )

    @staticmethod
    def _timing(items: tuple) -> BoardKgTiming:
        durations_by_artifact: dict[str, list[float]] = {}
        for item in items:
            if item.consolidated_at is None:
                continue
            durations_by_artifact.setdefault(item.artifact_id, []).append(
                (item.consolidated_at - item.opened_at).total_seconds() / 3600
            )
        # One timing sample per effectiveness denominator identity.  Multiple
        # ledger/projection rows for an artifact cannot weight the percentile.
        durations = tuple(
            min(durations_by_artifact[artifact_id])
            for artifact_id in sorted(durations_by_artifact)
        )
        if not durations:
            return BoardKgTiming(
                BoardKgEffectivenessState.UNAVAILABLE,
                0,
                None,
                None,
                "insufficient_consolidation_timing_evidence",
            )
        return BoardKgTiming(
            BoardKgEffectivenessState.AVAILABLE,
            len(durations),
            round(_percentile(durations, 0.5), 6),
            round(_percentile(durations, 0.95), 6),
        )

    @staticmethod
    def _unavailable_effectiveness(
        state: BoardKgEffectivenessState,
        *,
        query: BoardKgAnalyticsQuery,
        reason: str,
    ) -> BoardKgCognitiveEffectiveness:
        return BoardKgCognitiveEffectiveness(
            state=state,
            numerator=None,
            denominator=None,
            rate=None,
            candidate_count=None,
            persisted_count=None,
            conversion_rate=None,
            method_version="cognitive-effectiveness-v1",
            sample_period=query.foundation.window,
            timing=BoardKgTiming(state, 0, None, None, reason),
            reason=reason,
        )

    @staticmethod
    def _effectiveness(
        *, query: BoardKgAnalyticsQuery, items: tuple
    ) -> BoardKgCognitiveEffectiveness:
        artifacts = {item.artifact_id for item in items}
        if not artifacts:
            return BoardKgCognitiveEffectiveness(
                state=BoardKgEffectivenessState.EMPTY,
                numerator=0,
                denominator=0,
                rate=None,
                candidate_count=0,
                persisted_count=0,
                conversion_rate=None,
                method_version="cognitive-effectiveness-v1",
                sample_period=query.foundation.window,
                timing=BoardKgTiming(
                    BoardKgEffectivenessState.EMPTY,
                    0,
                    None,
                    None,
                    "empty_cognitive_denominator",
                ),
                reason="empty_cognitive_denominator",
            )
        materialized = {item.artifact_id for item in items if item.outcome_materialized}
        candidates = {item.artifact_id for item in items if item.candidate_materialized}
        persisted = {item.artifact_id for item in items if item.persisted}
        return BoardKgCognitiveEffectiveness(
            state=BoardKgEffectivenessState.AVAILABLE,
            numerator=len(materialized),
            denominator=len(artifacts),
            rate=round(len(materialized) / len(artifacts), 6),
            candidate_count=len(candidates),
            persisted_count=len(persisted),
            conversion_rate=(
                round(len(persisted) / len(candidates), 6) if candidates else None
            ),
            method_version="cognitive-effectiveness-v1",
            sample_period=query.foundation.window,
            timing=BoardKgEffectivenessService._timing(items),
        )

    @staticmethod
    def _unavailable_provenance(
        state: BoardKgAnalyticsResultState, *, reason: str
    ) -> BoardKgProvenanceMix:
        return BoardKgProvenanceMix(
            result_state=state,
            total=None,
            slices=(),
            reason=reason,
        )

    @staticmethod
    def _provenance_mix(items: tuple) -> BoardKgProvenanceMix:
        total = len(items)
        state = (
            BoardKgAnalyticsResultState.EMPTY
            if total == 0
            else BoardKgAnalyticsResultState.AVAILABLE
        )
        return BoardKgProvenanceMix(
            result_state=state,
            total=total,
            slices=tuple(
                BoardKgProvenanceSlice(
                    kind,
                    (count := sum(1 for item in items if item.provenance is kind)),
                    round(count / total, 6) if total else None,
                )
                for kind in BoardKgProvenanceKind
            ),
            reason="empty_cognitive_inventory" if total == 0 else None,
        )

    @staticmethod
    def _result_state(
        *,
        evidence,
        inventory: BoardKgCognitiveInventory,
        effectiveness: BoardKgCognitiveEffectiveness,
    ) -> BoardKgAnalyticsResultState:
        health_states = {
            evidence.health_result_state,
            *(item.result_state for item in evidence.components),
        }
        if BoardKgAnalyticsResultState.ERROR in health_states:
            return BoardKgAnalyticsResultState.ERROR
        if BoardKgAnalyticsResultState.RESTRICTED in health_states:
            return BoardKgAnalyticsResultState.RESTRICTED
        if BoardKgAnalyticsResultState.UNAVAILABLE in health_states:
            return BoardKgAnalyticsResultState.UNAVAILABLE
        if evidence.health_result_state in {
            BoardKgAnalyticsResultState.ERROR,
            BoardKgAnalyticsResultState.RESTRICTED,
            BoardKgAnalyticsResultState.UNAVAILABLE,
        }:
            return evidence.health_result_state
        if evidence.currentness is AnalyticsProjectionCurrentness.UNAVAILABLE:
            return BoardKgAnalyticsResultState.UNAVAILABLE
        partial = evidence.currentness in {
            AnalyticsProjectionCurrentness.PARTIAL,
            AnalyticsProjectionCurrentness.STALE,
        }
        partial = partial or evidence.health_result_state in {
            BoardKgAnalyticsResultState.PARTIAL,
            BoardKgAnalyticsResultState.EMPTY,
        }
        partial = partial or any(
            item.result_state
            not in {
                BoardKgAnalyticsResultState.AVAILABLE,
                BoardKgAnalyticsResultState.EMPTY,
            }
            for item in evidence.domains
        )
        partial = partial or inventory.result_state not in {
            BoardKgAnalyticsResultState.AVAILABLE,
            BoardKgAnalyticsResultState.EMPTY,
        }
        partial = partial or effectiveness.state in {
            BoardKgEffectivenessState.RESTRICTED,
            BoardKgEffectivenessState.UNAVAILABLE,
        }
        partial = partial or (
            effectiveness.state is BoardKgEffectivenessState.AVAILABLE
            and effectiveness.timing.state is BoardKgEffectivenessState.UNAVAILABLE
        )
        return (
            BoardKgAnalyticsResultState.PARTIAL
            if partial
            else BoardKgAnalyticsResultState.AVAILABLE
        )

    @staticmethod
    def _classification_state(
        *,
        evidence,
        result_state: BoardKgAnalyticsResultState,
    ) -> BoardKgClassificationState:
        if result_state is BoardKgAnalyticsResultState.ERROR:
            return BoardKgClassificationState.ERROR
        if result_state is BoardKgAnalyticsResultState.RESTRICTED:
            return BoardKgClassificationState.RESTRICTED
        if result_state is BoardKgAnalyticsResultState.UNAVAILABLE:
            return BoardKgClassificationState.UNAVAILABLE
        if evidence.health_state in {
            BoardKgHealthState.BACKPRESSURE,
            BoardKgHealthState.RECOVERY_NEEDED,
            BoardKgHealthState.QUARANTINED,
        }:
            return BoardKgClassificationState.BLOCKING
        if (
            result_state is BoardKgAnalyticsResultState.PARTIAL
            or evidence.health_state is BoardKgHealthState.AT_RISK
        ):
            return BoardKgClassificationState.AT_RISK
        return BoardKgClassificationState.HEALTHY

    @staticmethod
    async def project(
        context: object,
        *,
        query: BoardKgAnalyticsQuery,
        evidence_port: BoardKgAnalyticsEvidencePort,
    ) -> BoardKgEffectivenessProjection:
        if query.historical_as_of is not None:
            raise BoardKgHistoricalAsOfUnsupported()
        evidence = await evidence_port.load(context, query=query)
        if (
            evidence.foundation_contract_version
            != ANALYTICS_FOUNDATION_CONTRACT_VERSION
        ):
            raise BoardKgAnalyticsContractMismatch()
        if evidence.historical_as_of_supported:
            raise BoardKgAnalyticsContractMismatch(
                "Board KG evidence advertises unsupported historical authority."
            )
        if evidence.board_id != query.foundation.board_id:
            raise ValueError("board_kg_analytics_board_scope_mismatch")
        if evidence.population_scope.scope_ref != query.foundation.actor_scope_ref:
            raise ValueError("board_kg_analytics_population_scope_mismatch")
        if query.foundation.as_of is None:
            raise ValueError("board_kg_analytics_projection_as_of_required")
        if evidence.observed_at > query.foundation.as_of:
            raise ValueError("board_kg_analytics_evidence_from_future")
        cognitive_domain = next(
            item
            for item in evidence.domains
            if item.domain is BoardKgDomain.COGNITIVE_BACKLOG
        )
        items = BoardKgEffectivenessService._filtered_items(evidence, query)
        if cognitive_domain.result_state in {
            BoardKgAnalyticsResultState.RESTRICTED,
            BoardKgAnalyticsResultState.UNAVAILABLE,
            BoardKgAnalyticsResultState.ERROR,
        }:
            if items:
                raise ValueError("board_kg_analytics_restricted_cognitive_fact_leak")
            reason = cognitive_domain.reason or "cognitive_evidence_unavailable"
            inventory = BoardKgEffectivenessService._unavailable_inventory(
                cognitive_domain.result_state, reason=reason
            )
            effectiveness = BoardKgEffectivenessService._unavailable_effectiveness(
                (
                    BoardKgEffectivenessState.RESTRICTED
                    if cognitive_domain.result_state
                    is BoardKgAnalyticsResultState.RESTRICTED
                    else BoardKgEffectivenessState.UNAVAILABLE
                ),
                query=query,
                reason=reason,
            )
            provenance_mix = BoardKgEffectivenessService._unavailable_provenance(
                cognitive_domain.result_state, reason=reason
            )
        else:
            inventory = BoardKgEffectivenessService._inventory(
                observed_at=evidence.observed_at, items=items
            )
            effectiveness = BoardKgEffectivenessService._effectiveness(
                query=query, items=items
            )
            provenance_mix = BoardKgEffectivenessService._provenance_mix(items)
        provenance = AnalyticsProjectionProvenance(
            observed_at=evidence.observed_at,
            currentness=evidence.currentness,
            sources=evidence.sources,
            reason=evidence.currentness_reason,
        )
        result_state = BoardKgEffectivenessService._result_state(
            evidence=evidence,
            inventory=inventory,
            effectiveness=effectiveness,
        )
        return BoardKgEffectivenessProjection(
            contract_version=BOARD_KG_ANALYTICS_CONTRACT_VERSION,
            foundation_version=ANALYTICS_FOUNDATION_CONTRACT_VERSION,
            query=query,
            as_of=query.foundation.as_of,
            board_id=query.foundation.board_id,
            result_state=result_state,
            provenance=provenance,
            health_state=BoardKgEffectivenessService._classification_state(
                evidence=evidence,
                result_state=result_state,
            ),
            classification_reason=evidence.classification_reason,
            reason_codes=evidence.reason_codes,
            components=evidence.components,
            domains=evidence.domains,
            cognitive_inventory=inventory,
            effectiveness=effectiveness,
            provenance_mix=provenance_mix,
            diagnostics=evidence.diagnostics,
            redactions=evidence.redactions,
            population_scope=evidence.population_scope,
            exclusions=evidence.exclusions,
            next_cursor=evidence.next_cursor,
        )


__all__ = [
    "BoardKgAnalyticsService",
    "BoardKgEffectivenessService",
    "read_board_kg_health_evidence",
    "resolve_board_kg_cognitive_status",
]
