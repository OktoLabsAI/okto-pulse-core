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
from okto_pulse.core.ports.board_kg_analytics import (
    BOARD_KG_ANALYTICS_CONTRACT_VERSION,
    BoardKgAnalyticsContractMismatch,
    BoardKgAnalyticsEvidence,
    BoardKgAnalyticsQuery,
    BoardKgAnalyticsResultState,
    BoardKgClassificationState,
    BoardKgCognitiveItemFact,
    BoardKgCognitiveStatus,
    BoardKgDiagnostic,
    BoardKgDomain,
    BoardKgDomainAge,
    BoardKgDomainSeverity,
    BoardKgDrillDown,
    BoardKgEffectivenessState,
    BoardKgHealthComponent,
    BoardKgHealthState,
    BoardKgHistoricalAsOfUnsupported,
    BoardKgOperationalDomain,
    BoardKgProvenanceKind,
)
from okto_pulse.core.services.board_kg_analytics import (
    BoardKgEffectivenessService,
)


NOW = datetime(2026, 8, 21, 12, tzinfo=UTC)


class _EvidencePort:
    def __init__(self, evidence: BoardKgAnalyticsEvidence) -> None:
        self.evidence = evidence
        self.called = False

    async def load(self, context, *, query):  # noqa: ANN001, ANN201, ARG002
        self.called = True
        return self.evidence


def _query(*, historical_as_of: datetime | None = None) -> BoardKgAnalyticsQuery:
    return BoardKgAnalyticsQuery(
        foundation=AnalyticsFoundationQuery(
            board_id="board-1",
            actor_scope_ref="actor:user-1",
            window=AnalyticsUtcWindow(NOW - timedelta(days=90), NOW),
            as_of=NOW,
        ),
        historical_as_of=historical_as_of,
    )


def _age(count: int) -> BoardKgDomainAge:
    if count == 0:
        return BoardKgDomainAge(
            BoardKgAnalyticsResultState.EMPTY,
            0,
            None,
            None,
            None,
        )
    return BoardKgDomainAge(
        BoardKgAnalyticsResultState.AVAILABLE,
        count,
        2.0,
        4.0,
        6.0,
    )


def _domain(
    domain: BoardKgDomain,
    count: int,
    *,
    severity: BoardKgDomainSeverity = BoardKgDomainSeverity.INFORMATIONAL,
) -> BoardKgOperationalDomain:
    state = (
        BoardKgAnalyticsResultState.EMPTY
        if count == 0
        else BoardKgAnalyticsResultState.AVAILABLE
    )
    return BoardKgOperationalDomain(
        domain=domain,
        result_state=state,
        count=count,
        severity=severity,
        age=_age(count),
        drill_down=BoardKgDrillDown(
            True,
            f"/boards/board-1/analytics/kg?domain={domain.value}",
        ),
    )


def _domains(
    *,
    cognitive: BoardKgOperationalDomain | None = None,
) -> tuple[BoardKgOperationalDomain, ...]:
    values = {
        BoardKgDomain.ACTIVE_QUEUE: _domain(BoardKgDomain.ACTIVE_QUEUE, 2),
        BoardKgDomain.TECHNICAL_DLQ: _domain(
            BoardKgDomain.TECHNICAL_DLQ,
            1,
            severity=BoardKgDomainSeverity.BLOCKING,
        ),
        BoardKgDomain.CANONICAL_DEBT: _domain(
            BoardKgDomain.CANONICAL_DEBT,
            3,
            severity=BoardKgDomainSeverity.AT_RISK,
        ),
        BoardKgDomain.POLICY_PROJECTION_DEBT: _domain(
            BoardKgDomain.POLICY_PROJECTION_DEBT,
            4,
            severity=BoardKgDomainSeverity.AT_RISK,
        ),
        BoardKgDomain.COGNITIVE_BACKLOG: cognitive
        or _domain(BoardKgDomain.COGNITIVE_BACKLOG, 5),
    }
    return tuple(values[domain] for domain in BoardKgDomain)


def _items(count: int = 50) -> tuple[BoardKgCognitiveItemFact, ...]:
    statuses = tuple(BoardKgCognitiveStatus)
    provenance = tuple(BoardKgProvenanceKind)
    return tuple(
        BoardKgCognitiveItemFact(
            artifact_id=f"spec:{index:03d}",
            cognitive_item_id=f"item:{index:03d}",
            status=statuses[index % len(statuses)],
            provenance=provenance[index % len(provenance)],
            opened_at=NOW - timedelta(days=10, hours=index),
            candidate_materialized=index < 45,
            persisted=index < 40,
            outcome_materialized=index < 34,
            consolidated_at=(NOW - timedelta(days=1) if index < 34 else None),
            overdue_revisit=index % 7 == 0,
            blocker_codes=("awaiting_closeout",) if index % 11 == 0 else (),
        )
        for index in range(count)
    )


def _evidence(
    *,
    items: tuple[BoardKgCognitiveItemFact, ...] | None = None,
    domains: tuple[BoardKgOperationalDomain, ...] | None = None,
    health_state: BoardKgHealthState = BoardKgHealthState.HEALTHY,
    health_result_state: BoardKgAnalyticsResultState = (
        BoardKgAnalyticsResultState.AVAILABLE
    ),
    foundation_version: str = ANALYTICS_FOUNDATION_CONTRACT_VERSION,
) -> BoardKgAnalyticsEvidence:
    return BoardKgAnalyticsEvidence(
        board_id="board-1",
        foundation_contract_version=foundation_version,
        observed_at=NOW - timedelta(seconds=1),
        health_state=health_state,
        health_result_state=health_result_state,
        classification_reason="canonical_board_health",
        reason_codes=("canonical_health",),
        components=(
            BoardKgHealthComponent(
                "graph",
                health_state,
                health_result_state,
                "canonical_graph_health",
            ),
            BoardKgHealthComponent(
                "metric",
                health_state,
                health_result_state,
                "canonical_metric_health",
            ),
        ),
        domains=_domains() if domains is None else domains,
        cognitive_items=_items() if items is None else items,
        diagnostics=(
            BoardKgDiagnostic(
                "canonical_debt",
                BoardKgDomainSeverity.AT_RISK,
                "Open canonical debt requires review.",
                BoardKgDrillDown(
                    True,
                    "/boards/board-1/analytics/kg?domain=canonical_debt",
                ),
            ),
        ),
        redactions=("global_operational_signals",),
        population_scope=AnalyticsPopulationScope("actor:user-1", 50),
        exclusions=AnalyticsExclusionSummary(),
        currentness=AnalyticsProjectionCurrentness.CURRENT,
        sources=(
            AnalyticsSourceAuthority(
                "board_kg_evidence",
                "board:board-1:kg-effectiveness",
                "observed_at",
            ),
        ),
    )


@pytest.mark.asyncio
async def test_complete_projection_separates_domains_and_reconciles_effectiveness() -> (
    None
):
    projection = await BoardKgEffectivenessService.project(
        None,
        query=_query(),
        evidence_port=_EvidencePort(_evidence()),
    )

    payload = projection.canonical_dict()
    assert payload["contract_version"] == BOARD_KG_ANALYTICS_CONTRACT_VERSION
    assert payload["result_state"] == "available"
    assert payload["health"]["state"] == "healthy"
    assert payload["health"]["availability"] == {
        "graph": "available",
        "metric": "available",
        "active_queue": "available",
        "technical_dlq": "available",
        "canonical_debt": "available",
        "policy_projection_debt": "available",
        "cognitive_backlog": "available",
    }
    assert [item["domain"] for item in payload["domains"]] == [
        domain.value for domain in BoardKgDomain
    ]
    assert payload["cognitive_inventory"]["total"] == 50
    assert sum(payload["cognitive_inventory"]["by_status"].values()) == 50
    assert (
        payload["effectiveness"]
        | {
            "numerator": 34,
            "denominator": 50,
            "rate": 0.68,
            "candidate_count": 45,
            "persisted_count": 40,
            "conversion_rate": 0.888889,
        }
        == payload["effectiveness"]
    )
    assert payload["effectiveness"]["timing"]["sample_count"] == 34
    assert payload["diagnostics"][0]["domain"] == "canonical_debt"
    assert "global_operational_signals" in payload["redactions"]


@pytest.mark.asyncio
async def test_effectiveness_deduplicates_artifacts_and_timing_samples() -> None:
    items = (
        BoardKgCognitiveItemFact(
            "spec:001",
            "item:001",
            BoardKgCognitiveStatus.CONSOLIDATED,
            BoardKgProvenanceKind.COGNITIVE,
            NOW - timedelta(days=4),
            True,
            True,
            True,
            NOW - timedelta(days=2),
        ),
        BoardKgCognitiveItemFact(
            "spec:001",
            "item:002",
            BoardKgCognitiveStatus.CONSOLIDATED,
            BoardKgProvenanceKind.COGNITIVE,
            NOW - timedelta(days=3),
            True,
            True,
            True,
            NOW - timedelta(days=1),
        ),
    )

    projection = await BoardKgEffectivenessService.project(
        None,
        query=_query(),
        evidence_port=_EvidencePort(_evidence(items=items)),
    )

    assert projection.effectiveness.denominator == 1
    assert projection.effectiveness.numerator == 1
    assert projection.effectiveness.candidate_count == 1
    assert projection.effectiveness.persisted_count == 1
    assert projection.effectiveness.timing.sample_count == 1


@pytest.mark.asyncio
async def test_empty_denominator_is_explicit_and_never_success() -> None:
    projection = await BoardKgEffectivenessService.project(
        None,
        query=_query(),
        evidence_port=_EvidencePort(_evidence(items=())),
    )

    payload = projection.canonical_dict()
    assert payload["effectiveness"]["state"] == "empty"
    assert payload["effectiveness"]["numerator"] == 0
    assert payload["effectiveness"]["denominator"] == 0
    assert payload["effectiveness"]["rate"] is None


@pytest.mark.asyncio
async def test_missing_timing_is_unavailable_and_prevents_healthy_presentation() -> (
    None
):
    item = BoardKgCognitiveItemFact(
        "spec:001",
        "item:001",
        BoardKgCognitiveStatus.IN_PROGRESS,
        BoardKgProvenanceKind.DETERMINISTIC,
        NOW - timedelta(days=1),
        True,
        True,
        True,
    )

    projection = await BoardKgEffectivenessService.project(
        None,
        query=_query(),
        evidence_port=_EvidencePort(_evidence(items=(item,))),
    )

    assert (
        projection.effectiveness.timing.state is BoardKgEffectivenessState.UNAVAILABLE
    )
    assert projection.result_state is BoardKgAnalyticsResultState.PARTIAL
    assert projection.health_state is BoardKgClassificationState.AT_RISK


@pytest.mark.asyncio
async def test_required_unavailable_health_component_cannot_serialize_healthy() -> None:
    projection = await BoardKgEffectivenessService.project(
        None,
        query=_query(),
        evidence_port=_EvidencePort(
            _evidence(
                health_result_state=BoardKgAnalyticsResultState.UNAVAILABLE,
            )
        ),
    )

    assert projection.result_state is BoardKgAnalyticsResultState.UNAVAILABLE
    assert projection.health_state is BoardKgClassificationState.UNAVAILABLE
    assert projection.canonical_dict()["health"]["state"] == "unavailable"


@pytest.mark.asyncio
async def test_restricted_cognitive_domain_has_no_hidden_aggregate() -> None:
    restricted = BoardKgOperationalDomain(
        BoardKgDomain.COGNITIVE_BACKLOG,
        BoardKgAnalyticsResultState.RESTRICTED,
        None,
        None,
        BoardKgDomainAge(
            BoardKgAnalyticsResultState.RESTRICTED,
            0,
            None,
            None,
            None,
            "cognitive_evidence_restricted",
        ),
        BoardKgDrillDown(False),
        "cognitive_evidence_restricted",
    )
    projection = await BoardKgEffectivenessService.project(
        None,
        query=_query(),
        evidence_port=_EvidencePort(
            _evidence(items=(), domains=_domains(cognitive=restricted))
        ),
    )

    assert (
        projection.cognitive_inventory.result_state
        is BoardKgAnalyticsResultState.RESTRICTED
    )
    assert projection.cognitive_inventory.total is None
    assert projection.effectiveness.state is BoardKgEffectivenessState.RESTRICTED
    assert projection.effectiveness.denominator is None
    assert projection.provenance_mix.total is None


@pytest.mark.asyncio
async def test_historical_as_of_is_rejected_before_live_evidence_read() -> None:
    port = _EvidencePort(_evidence())

    with pytest.raises(BoardKgHistoricalAsOfUnsupported) as caught:
        await BoardKgEffectivenessService.project(
            None,
            query=_query(historical_as_of=NOW - timedelta(days=1)),
            evidence_port=port,
        )

    assert caught.value.code == "analytics_historical_as_of_unsupported"
    assert port.called is False


@pytest.mark.asyncio
async def test_dependency_contract_mismatch_is_typed_409() -> None:
    with pytest.raises(BoardKgAnalyticsContractMismatch) as caught:
        await BoardKgEffectivenessService.project(
            None,
            query=_query(),
            evidence_port=_EvidencePort(_evidence(foundation_version="999")),
        )

    assert caught.value.code == "kg_analytics_contract_mismatch"
    assert caught.value.http_status == 409


def test_mutating_drill_down_is_forbidden_by_contract() -> None:
    with pytest.raises(ValueError, match="mutating_drill_down_forbidden"):
        BoardKgDrillDown(True, "/boards/board-1/kg/reprocess")
