from datetime import UTC, datetime, timedelta

import pytest

from okto_pulse.core.ports.analytics_foundation import (
    AnalyticsExclusionSummary,
    AnalyticsFoundationQuery,
    AnalyticsPopulationScope,
    AnalyticsUtcWindow,
)
from okto_pulse.core.ports.board_kg_analytics import (
    BoardKgAnalyticsResultState,
    BoardKgCognitiveStatus,
    BoardKgHealthEvidenceSnapshot,
    BoardKgHealthState,
)
from okto_pulse.core.services.board_kg_analytics import (
    BoardKgAnalyticsService,
    read_board_kg_health_evidence,
    resolve_board_kg_cognitive_status,
)


NOW = datetime(2026, 8, 20, 12, tzinfo=UTC)


def _query() -> AnalyticsFoundationQuery:
    return AnalyticsFoundationQuery(
        board_id="board-1",
        actor_scope_ref="actor:user-1",
        window=AnalyticsUtcWindow(NOW - timedelta(days=1), NOW + timedelta(seconds=1)),
        as_of=NOW,
    )


def _health(state="healthy", metric_status="available"):
    return {
        "board_id": "board-1",
        "overall_state": state,
        "graph_state": state,
        "discovery_state": state,
        "metric_status": metric_status,
        "classification_reason": f"canonical:{state}",
        "classification_reasons": [f"canonical_{state}"],
        "queue_depth": 2,
        "dead_letter_count": 1,
        "canonical_debt": {"open_count": 3},
    }


def _effectiveness(*, available=True, artifacts=2):
    return {
        "board_id": "board-1",
        "kg_projection_available": available,
        "cognitively_effective": True,
        "artifacts": [{"id": index} for index in range(artifacts)],
        "totals": {
            "attempted": artifacts,
            "persisted_or_consolidated": artifacts,
            "dlq": 0,
            "extractor_triggered_but_not_persisted": 0,
        },
    }


def _compose(health, effectiveness, *, effectiveness_state=None):
    return BoardKgAnalyticsService.compose(
        query=_query(),
        as_of=NOW,
        population_scope=AnalyticsPopulationScope("actor:user-1", 1),
        exclusions=AnalyticsExclusionSummary(),
        health_payload=health,
        effectiveness_payload=effectiveness,
        effectiveness_result_state=effectiveness_state,
    )


@pytest.mark.parametrize("state", tuple(item.value for item in BoardKgHealthState))
def test_every_canonical_health_state_is_preserved_exactly(state):
    result = _compose(_health(state), _effectiveness())

    assert result.health_state.value == state
    assert result.result_state is BoardKgAnalyticsResultState.AVAILABLE
    assert tuple(item.health_state.value for item in result.components) == (
        state,
        state,
    )


@pytest.mark.parametrize(
    "result_state",
    tuple(BoardKgAnalyticsResultState),
)
def test_health_and_metric_availability_are_orthogonal(result_state):
    result = _compose(
        _health("healthy"),
        _effectiveness(
            artifacts=0 if result_state is BoardKgAnalyticsResultState.EMPTY else 2
        ),
        effectiveness_state=result_state,
    )

    assert result.health_state is BoardKgHealthState.HEALTHY
    assert result.result_state is result_state
    if result_state in {
        BoardKgAnalyticsResultState.AVAILABLE,
        BoardKgAnalyticsResultState.EMPTY,
    }:
        assert result.cognitive_effectiveness.denominator is not None
    else:
        assert result.cognitive_effectiveness.denominator is None


def test_unavailable_effectiveness_never_masquerades_as_healthy_metric():
    result = _compose(_health("healthy"), _effectiveness(available=False))

    assert result.health_state is BoardKgHealthState.HEALTHY
    assert result.result_state is BoardKgAnalyticsResultState.UNAVAILABLE
    assert (
        result.cognitive_effectiveness.result_state
        is BoardKgAnalyticsResultState.UNAVAILABLE
    )
    assert result.cognitive_effectiveness.cognitively_effective is None
    assert result.cognitive_effectiveness.denominator is None


def test_operational_debt_domains_remain_separate_and_reconcile():
    result = _compose(_health("backpressure"), _effectiveness())

    assert result.debt_domains.active_queue_count == 2
    assert result.debt_domains.technical_dlq_count == 1
    assert result.debt_domains.canonical_debt_count == 3
    assert result.cognitive_effectiveness.technical_dlq_count == 0


def test_empty_effectiveness_has_explicit_zero_denominator():
    result = _compose(_health(), _effectiveness(artifacts=0))

    assert result.result_state is BoardKgAnalyticsResultState.EMPTY
    assert result.cognitive_effectiveness.denominator == 0
    assert result.cognitive_effectiveness.attempted_count == 0


def test_invented_health_state_fails_closed():
    with pytest.raises(ValueError, match="health_state_invalid"):
        _compose(_health("unavailable"), _effectiveness())


def test_board_scope_mismatch_fails_before_projection():
    payload = _effectiveness()
    payload["board_id"] = "other-board"

    with pytest.raises(ValueError, match="board_mismatch"):
        _compose(_health(), payload)


@pytest.mark.asyncio
async def test_public_health_evidence_facade_returns_strict_board_scoped_contract(
    monkeypatch,
):
    from okto_pulse.core.services import kg_health_service

    async def fake_health(board_id, context):
        assert board_id == "board-1"
        assert context is sentinel
        return _health("backpressure", metric_status="restricted")

    sentinel = object()
    monkeypatch.setattr(kg_health_service, "get_kg_health", fake_health)

    evidence = await read_board_kg_health_evidence(sentinel, board_id="board-1")

    assert isinstance(evidence, BoardKgHealthEvidenceSnapshot)
    assert evidence.board_id == "board-1"
    assert evidence.health_state is BoardKgHealthState.BACKPRESSURE
    assert evidence.result_state is BoardKgAnalyticsResultState.RESTRICTED
    assert evidence.reason_codes == ("canonical_backpressure",)
    assert tuple(item.component for item in evidence.components) == (
        "discovery",
        "graph",
    )


@pytest.mark.asyncio
async def test_public_health_evidence_facade_rejects_cross_board_payload(monkeypatch):
    from okto_pulse.core.services import kg_health_service

    async def fake_health(_board_id, _context):
        payload = _health()
        payload["board_id"] = "other-board"
        return payload

    monkeypatch.setattr(kg_health_service, "get_kg_health", fake_health)

    with pytest.raises(ValueError, match="board_mismatch"):
        await read_board_kg_health_evidence(object(), board_id="board-1")


@pytest.mark.parametrize(
    ("ledger_status", "outcome_type", "expected"),
    (
        ("pending", None, BoardKgCognitiveStatus.PENDING),
        ("skipped", "no_action_required", BoardKgCognitiveStatus.NO_ACTION),
        ("future_status", "relation_created", None),
    ),
)
def test_public_cognitive_status_facade_hides_internal_ledger_vocabulary(
    ledger_status,
    outcome_type,
    expected,
):
    assert (
        resolve_board_kg_cognitive_status(
            ledger_status=ledger_status,
            outcome_type=outcome_type,
        )
        is expected
    )


def test_projection_uses_public_service_boundaries_without_adapter_reach_in():
    import inspect

    source = inspect.getsource(BoardKgAnalyticsService.project_from_public_services)
    assert "services.kg_health_service import get_kg_health" in source
    assert "build_cognitive_effectiveness_inventory" in source
    assert "community" not in source
    assert "get_kg_registry" not in source
    assert "adapters" not in source


def test_transport_payload_keeps_result_state_outside_health_enum():
    payload = _compose(
        _health("recovery_needed"), _effectiveness(available=False)
    ).canonical_dict()

    assert payload["result_state"] == "unavailable"
    assert payload["health"]["state"] == "recovery_needed"
    assert "availability" not in payload["health"]


def _readable_health():
    return {
        **_health("at_risk", metric_status="unavailable"),
        "discovery_state": "healthy",
        "board_graph_queryable": True,
        "board_graph_recovery_required": False,
        "discovery_recovery_required": False,
        "probe_diagnostics": {
            name: {"status": "available"}
            for name in (
                "graph_snapshot",
                "graph_metrics",
                "discovery_snapshot",
                "discovery_telemetry",
            )
        },
    }


@pytest.mark.asyncio
async def test_partial_telemetry_preserves_independent_component_evidence(monkeypatch):
    from okto_pulse.core.services import kg_health_service

    async def health(_board_id, _context):
        return _readable_health()

    monkeypatch.setattr(kg_health_service, "get_kg_health", health)
    evidence = await read_board_kg_health_evidence(None, board_id="board-1")
    assert evidence.result_state is BoardKgAnalyticsResultState.PARTIAL
    assert evidence.health_state is BoardKgHealthState.AT_RISK
    components = {item.component: item for item in evidence.components}
    assert components["discovery"].result_state is BoardKgAnalyticsResultState.AVAILABLE
    assert components["graph"].result_state is BoardKgAnalyticsResultState.PARTIAL
    assert components["graph"].classification_reason == "health_telemetry_incomplete"


@pytest.mark.parametrize("probe", ("graph_snapshot", "discovery_snapshot"))
def test_stale_component_stays_partial_even_when_legacy_metrics_are_available(probe):
    payload = _readable_health()
    payload["metric_status"] = "available"
    payload["probe_diagnostics"][probe]["status"] = "stale"
    assert (
        BoardKgAnalyticsService._health_result_state(payload)
        is BoardKgAnalyticsResultState.PARTIAL
    )


@pytest.mark.parametrize("state", ("restricted", "error"))
@pytest.mark.parametrize(
    "source", ("metric_status", "graph_snapshot", "discovery_telemetry")
)
def test_access_and_error_signals_are_not_downgraded_to_partial(state, source):
    payload = _readable_health()
    if source == "metric_status":
        payload[source] = state
    else:
        payload["probe_diagnostics"][source]["status"] = state
    assert BoardKgAnalyticsService._health_result_state(payload).value == state


@pytest.mark.parametrize("status", ("unavailable", "future_status", None))
def test_absent_or_unknown_probes_do_not_manufacture_authority(status):
    payload = _readable_health()
    for probe in payload["probe_diagnostics"].values():
        probe["status"] = status
    assert (
        BoardKgAnalyticsService._health_result_state(payload)
        is BoardKgAnalyticsResultState.UNAVAILABLE
    )


@pytest.mark.parametrize("queryable", (False, None, "true", 1))
def test_graph_queryability_must_be_explicit_not_truthy(queryable):
    payload = _readable_health()
    payload["board_graph_queryable"] = queryable
    components = {
        item.component: item for item in BoardKgAnalyticsService._components(payload)
    }
    assert components["graph"].result_state is BoardKgAnalyticsResultState.UNAVAILABLE
    assert components["discovery"].result_state is BoardKgAnalyticsResultState.AVAILABLE
    assert (
        BoardKgAnalyticsService._health_result_state(payload)
        is BoardKgAnalyticsResultState.PARTIAL
    )


def test_confirmed_empty_board_does_not_require_a_materialized_graph():
    payload = _readable_health()
    payload.update(
        metric_status="available",
        board_graph_queryable=False,
        materialization_state="not_materialized",
    )
    assert (
        BoardKgAnalyticsService._health_result_state(payload)
        is BoardKgAnalyticsResultState.AVAILABLE
    )


def test_legacy_payload_without_probe_evidence_remains_unavailable():
    assert (
        BoardKgAnalyticsService._health_result_state(
            _health(metric_status="unavailable")
        )
        is BoardKgAnalyticsResultState.UNAVAILABLE
    )


def test_recovery_overrides_cached_readable_evidence():
    payload = _readable_health()
    payload["board_graph_recovery_required"] = True
    components = {
        item.component: item for item in BoardKgAnalyticsService._components(payload)
    }
    assert components["graph"].result_state is BoardKgAnalyticsResultState.UNAVAILABLE
    assert components["graph"].classification_reason == "board_graph_recovery_required"
