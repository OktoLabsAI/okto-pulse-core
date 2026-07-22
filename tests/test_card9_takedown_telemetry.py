"""Card 9 — governed-takedown telemetry contract and SLO policy.

These tests stay transport- and persistence-free: Core validates the selector,
state vocabulary, stable ordering, strict breach thresholds and runtime port
registration while editions remain responsible for durable queries.
"""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from okto_pulse.core.application.kg_operations import CoreKnowledgeGraphOperations
from okto_pulse.core.application.processors import consolidation
from okto_pulse.core.ports.takedown_telemetry import (
    TAKEDOWN_NORMAL_SLO_SECONDS,
    TAKEDOWN_P95_WINDOW_SECONDS,
    TAKEDOWN_RECOVERY_SLO_SECONDS,
    TAKEDOWN_SLO_RUNBOOK,
    TAKEDOWN_STATE_RANK,
    TakedownAggregates,
    TakedownSloEvaluation,
    TakedownSloEvaluationStatus,
    TakedownState,
    TakedownTelemetryQuery,
    TakedownTelemetrySnapshot,
    TakedownTransition,
    build_takedown_slo_alert,
    get_takedown_telemetry_read_port,
    register_takedown_telemetry_read_port,
    reset_takedown_telemetry_read_port_for_tests,
)
from okto_pulse.core.runtime_context import (
    RuntimeValueRegistry,
    runtime_value_scope,
)


NOW = datetime(2026, 7, 21, 18, 0, tzinfo=timezone.utc)
BOARD_ID = "board-card9"
DELETE_EVENT_ID = "delete-card9-g1"
DELIVERY_KEY = "gd_parity:board-card9:spec:spec-card9:1"


@pytest.fixture(autouse=True)
def _isolated_runtime_registry():
    """Keep registry reset assertions isolated from suite-wide providers."""

    with runtime_value_scope(RuntimeValueRegistry()):
        yield


def _transition(
    state: TakedownState,
    *,
    occurred_at: datetime = NOW,
    attempt: int | None = None,
    delete_event_id: str = DELETE_EVENT_ID,
) -> TakedownTransition:
    return TakedownTransition(
        delete_event_id=delete_event_id,
        board_id="board-card9",
        artifact_type="spec",
        artifact_id="spec-card9",
        generation=1,
        state=state,
        occurred_at=occurred_at,
        delivery_key=(
            None if state is TakedownState.INTENT_CREATED else DELIVERY_KEY
        ),
        attempt=attempt,
    )


@pytest.mark.parametrize(
    ("kwargs", "selected_field", "selected_value"),
    [
        (
            {"delete_event_id": DELETE_EVENT_ID},
            "delete_event_id",
            DELETE_EVENT_ID,
        ),
        (
            {"delivery_key": DELIVERY_KEY},
            "delivery_key",
            DELIVERY_KEY,
        ),
    ],
    ids=("delete_event_id", "delivery_key"),
)
def test_query_selector_accepts_exactly_one_identity(
    kwargs: dict[str, str],
    selected_field: str,
    selected_value: str,
) -> None:
    query = TakedownTelemetryQuery(now=NOW, board_id=BOARD_ID, **kwargs)

    assert getattr(query, selected_field) == selected_value
    assert query.board_id == BOARD_ID


@pytest.mark.parametrize(
    "kwargs",
    [
        {},
        {
            "delete_event_id": DELETE_EVENT_ID,
            "delivery_key": DELIVERY_KEY,
        },
        {"delete_event_id": ""},
        {"delivery_key": " delivery-key-with-whitespace "},
    ],
)
def test_query_selector_rejects_missing_ambiguous_or_invalid_identity(
    kwargs: dict[str, str],
) -> None:
    with pytest.raises(ValueError):
        TakedownTelemetryQuery(now=NOW, board_id=BOARD_ID, **kwargs)


def test_query_rejects_non_datetime_observation_time() -> None:
    with pytest.raises(ValueError, match="takedown_telemetry_now_invalid"):
        TakedownTelemetryQuery(  # type: ignore[arg-type]
            now="2026-07-21T18:00:00Z",
            board_id=BOARD_ID,
            delete_event_id=DELETE_EVENT_ID,
        )


def test_transition_contract_requires_attempt_for_delivery_states() -> None:
    intent = _transition(TakedownState.INTENT_CREATED)
    graph = _transition(TakedownState.GRAPH_DEMOTED)

    assert intent.attempt is None
    assert intent.delivery_key is None
    assert intent.transition_key == f"takedown:{DELETE_EVENT_ID}:intent_created"
    assert graph.attempt is None
    assert graph.transition_key == f"takedown:{DELIVERY_KEY}:graph_demoted"

    for state in (
        TakedownState.OUTBOX_PERSISTED,
        TakedownState.DELIVERED,
        TakedownState.DELIVERY_DEBT,
    ):
        with pytest.raises(ValueError, match="takedown_telemetry_attempt_invalid"):
            _transition(state)

        transition = _transition(state, attempt=0)
        assert transition.attempt == 0
        assert transition.transition_key == (
            f"takedown:{DELIVERY_KEY}:{state.value}:attempt:0"
        )


@pytest.mark.parametrize("attempt", [-1, True, 1.5])
def test_transition_rejects_invalid_attempt_values(attempt: object) -> None:
    with pytest.raises(ValueError, match="takedown_telemetry_attempt_invalid"):
        _transition(  # type: ignore[arg-type]
            TakedownState.OUTBOX_PERSISTED,
            attempt=attempt,
        )


def test_transition_rejects_unknown_state_and_missing_delivery_key() -> None:
    with pytest.raises(ValueError, match="takedown_telemetry_state_invalid"):
        TakedownTransition(
            delete_event_id=DELETE_EVENT_ID,
            board_id="board-card9",
            artifact_type="spec",
            artifact_id="spec-card9",
            generation=1,
            state="unknown",  # type: ignore[arg-type]
            occurred_at=NOW,
        )


def test_transition_rejects_identity_fields_outside_their_state_domain() -> None:
    with pytest.raises(
        ValueError,
        match="takedown_telemetry_delivery_key_forbidden",
    ):
        TakedownTransition(
            delete_event_id=DELETE_EVENT_ID,
            delivery_key=DELIVERY_KEY,
            board_id="board-card9",
            artifact_type="spec",
            artifact_id="spec-card9",
            generation=1,
            state=TakedownState.INTENT_CREATED,
            occurred_at=NOW,
        )

    for state in (TakedownState.INTENT_CREATED, TakedownState.GRAPH_DEMOTED):
        with pytest.raises(ValueError, match="takedown_telemetry_attempt_invalid"):
            TakedownTransition(
                delete_event_id=DELETE_EVENT_ID,
                delivery_key=(
                    DELIVERY_KEY
                    if state is TakedownState.GRAPH_DEMOTED
                    else None
                ),
                board_id="board-card9",
                artifact_type="spec",
                artifact_id="spec-card9",
                generation=1,
                state=state,
                occurred_at=NOW,
                attempt=0,
            )

    with pytest.raises(
        ValueError,
        match="takedown_telemetry_delivery_key_required",
    ):
        TakedownTransition(
            delete_event_id=DELETE_EVENT_ID,
            board_id="board-card9",
            artifact_type="spec",
            artifact_id="spec-card9",
            generation=1,
            state=TakedownState.GRAPH_DEMOTED,
            occurred_at=NOW,
        )


def test_state_rank_is_stable_and_orders_equal_timestamp_transitions() -> None:
    expected_order = (
        TakedownState.INTENT_CREATED,
        TakedownState.GRAPH_DEMOTED,
        TakedownState.OUTBOX_PERSISTED,
        TakedownState.DELIVERY_DEBT,
        TakedownState.DELIVERED,
    )
    assert tuple(
        sorted(TakedownState, key=TAKEDOWN_STATE_RANK.__getitem__)
    ) == expected_order
    assert [TAKEDOWN_STATE_RANK[state] for state in expected_order] == list(
        range(len(expected_order))
    )

    shuffled = [
        _transition(TakedownState.DELIVERED, attempt=1),
        _transition(TakedownState.INTENT_CREATED),
        _transition(TakedownState.DELIVERY_DEBT, attempt=0),
        _transition(TakedownState.GRAPH_DEMOTED),
        _transition(TakedownState.OUTBOX_PERSISTED, attempt=0),
    ]
    ordered = sorted(
        shuffled,
        key=lambda item: (
            item.occurred_at,
            TAKEDOWN_STATE_RANK[item.state],
            item.attempt if item.attempt is not None else -1,
        ),
    )
    assert tuple(item.state for item in ordered) == expected_order


def _snapshot_from_states(
    states: tuple[TakedownTransition, ...],
    *,
    delivery_key: str | None = DELIVERY_KEY,
) -> TakedownTelemetrySnapshot:
    return TakedownTelemetrySnapshot(
        board_id="board-card9",
        delete_event_id=DELETE_EVENT_ID,
        delivery_key=delivery_key,
        artifact_type="spec",
        artifact_id="spec-card9",
        generation=1,
        states=states,
        aggregates=_aggregates(p95=30.0, oldest_debt=None),
    )


@pytest.mark.parametrize(
    "transition",
    (
        replace(_transition(TakedownState.INTENT_CREATED), board_id="other-board"),
        replace(
            _transition(TakedownState.INTENT_CREATED),
            artifact_type="card",
        ),
        replace(
            _transition(TakedownState.INTENT_CREATED),
            artifact_id="other-spec",
        ),
        replace(_transition(TakedownState.INTENT_CREATED), generation=2),
        replace(
            _transition(TakedownState.INTENT_CREATED),
            delete_event_id="other-delete-event",
        ),
    ),
    ids=("board", "artifact_type", "artifact_id", "generation", "delete_event"),
)
def test_snapshot_rejects_mixed_immutable_timeline_identity(
    transition: TakedownTransition,
) -> None:
    with pytest.raises(
        ValueError,
        match="takedown_telemetry_state_identity_mismatch",
    ):
        _snapshot_from_states((transition,), delivery_key=None)


def test_snapshot_rejects_delivery_identity_mismatch_and_unordered_states() -> None:
    intent = _transition(TakedownState.INTENT_CREATED)
    other_delivery = TakedownTransition(
        delete_event_id=DELETE_EVENT_ID,
        delivery_key="gd_parity:board-card9:spec:spec-card9:other",
        board_id="board-card9",
        artifact_type="spec",
        artifact_id="spec-card9",
        generation=1,
        state=TakedownState.GRAPH_DEMOTED,
        occurred_at=NOW + timedelta(seconds=1),
    )
    with pytest.raises(
        ValueError,
        match="takedown_telemetry_delivery_identity_mismatch",
    ):
        _snapshot_from_states((intent, other_delivery))

    delivered = _transition(
        TakedownState.DELIVERED,
        occurred_at=NOW + timedelta(seconds=1),
        attempt=0,
    )
    with pytest.raises(
        ValueError,
        match="takedown_telemetry_states_order_invalid",
    ):
        _snapshot_from_states((delivered, intent))


@pytest.mark.parametrize("states", ((), [_transition(TakedownState.INTENT_CREATED)]))
def test_snapshot_requires_nonempty_immutable_state_tuple(states: object) -> None:
    with pytest.raises(ValueError, match="takedown_telemetry_states_invalid"):
        _snapshot_from_states(states, delivery_key=None)  # type: ignore[arg-type]


def test_reconcile_run_metrics_survive_the_boolean_worker_boundary() -> None:
    details = consolidation._stale_reconcile_telemetry_details(
        {
            "scanned": 7,
            "demoted": [{"node_id": "n1"}, {"node_id": "n2"}],
            "routed_to_debt": [{"node_id": "learning-1"}],
            "incomplete": False,
            "incomplete_cause": None,
            "failed_types": [],
            "target_identity_count": 1,
            "target_found_count": 1,
            "target_demoted_count": 1,
            "target_already_converged_count": 0,
            "target_skipped_cognitive_count": 0,
            "target_preserved_canonical_count": 0,
        },
        SimpleNamespace(attempts=3),
    )

    assert details == {
        "queue_attempt": 3,
        "scanned": 7,
        "demoted_count": 2,
        "routed_to_debt_count": 1,
        "incomplete": False,
        "incomplete_cause": None,
        "failed_types": [],
        "target_identity_count": 1,
        "target_found_count": 1,
        "target_demoted_count": 1,
        "target_already_converged_count": 0,
        "target_skipped_cognitive_count": 0,
        "target_preserved_canonical_count": 0,
    }


def _aggregates(
    *,
    p95: float | None = TAKEDOWN_NORMAL_SLO_SECONDS,
    sample_count: int = 20,
    oldest_debt: float | None = TAKEDOWN_RECOVERY_SLO_SECONDS,
) -> TakedownAggregates:
    return TakedownAggregates(
        delivery_debt_backlog=1 if oldest_debt is not None else 0,
        oldest_debt_age_seconds=oldest_debt,
        circuit_breaker_state="closed",
        circuit_breaker_reason="global_outbox_terminal_backlog_absent",
        p95_seconds_1h=p95,
        p95_sample_count=sample_count,
    )


def test_slo_threshold_boundaries_are_strict() -> None:
    at_threshold = _aggregates()
    assert at_threshold.breach_reasons == ()
    assert build_takedown_slo_alert(at_threshold) is None

    p95_breach = _aggregates(p95=TAKEDOWN_NORMAL_SLO_SECONDS + 0.001)
    assert p95_breach.breach_reasons == (
        "takedown_p95_above_120_seconds",
    )

    debt_breach = _aggregates(
        oldest_debt=TAKEDOWN_RECOVERY_SLO_SECONDS + 0.001,
    )
    assert debt_breach.breach_reasons == (
        "oldest_delivery_debt_above_26_hours",
    )

    both = _aggregates(
        p95=TAKEDOWN_NORMAL_SLO_SECONDS + 1,
        oldest_debt=TAKEDOWN_RECOVERY_SLO_SECONDS + 1,
    )
    assert both.breach_reasons == (
        "takedown_p95_above_120_seconds",
        "oldest_delivery_debt_above_26_hours",
    )

    no_samples = _aggregates(
        p95=TAKEDOWN_NORMAL_SLO_SECONDS + 1,
        sample_count=0,
        oldest_debt=None,
    )
    assert no_samples.breach_reasons == ()
    assert build_takedown_slo_alert(no_samples) is None


@pytest.mark.parametrize(
    ("field_name", "value"),
    (
        ("p95_seconds_1h", float("nan")),
        ("p95_seconds_1h", float("inf")),
        ("oldest_debt_age_seconds", float("nan")),
        ("oldest_debt_age_seconds", float("inf")),
    ),
)
def test_slo_metrics_reject_non_finite_values(
    field_name: str,
    value: float,
) -> None:
    values = {
        "delivery_debt_backlog": 0,
        "oldest_debt_age_seconds": None,
        "circuit_breaker_state": "closed",
        "circuit_breaker_reason": "healthy",
        "p95_seconds_1h": None,
        "p95_sample_count": 0,
    }
    values[field_name] = value

    with pytest.raises(ValueError, match=field_name):
        TakedownAggregates(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "aggregates",
    (
        _aggregates(p95=None, sample_count=1, oldest_debt=None),
        TakedownAggregates(
            delivery_debt_backlog=1,
            oldest_debt_age_seconds=None,
            circuit_breaker_state="closed",
            circuit_breaker_reason="healthy",
            p95_seconds_1h=30.0,
            p95_sample_count=1,
        ),
    ),
    ids=("missing_p95", "missing_oldest_debt_age"),
)
def test_slo_missing_required_metric_is_insufficient_not_healthy(
    aggregates: TakedownAggregates,
) -> None:
    evaluation = TakedownSloEvaluation(
        board_id="board-card9",
        observed_at=NOW,
        transaction_state="committed",
        aggregates=aggregates,
    )

    assert evaluation.status is TakedownSloEvaluationStatus.INSUFFICIENT_DATA
    assert evaluation.breached is False


def test_slo_alert_and_snapshot_publish_exact_runbook_contract() -> None:
    aggregates = _aggregates(
        p95=TAKEDOWN_NORMAL_SLO_SECONDS + 1,
        oldest_debt=None,
    )
    alert = build_takedown_slo_alert(aggregates)

    assert alert is not None
    assert alert["event"] == "kg.takedown.slo_breach"
    assert alert["severity"] == "critical"
    assert alert["runbook"] == [
        "okto_pulse_kg_global_outbox_dead_letter_reprocess",
        "okto_pulse_kg_stale_canonical_parity_list",
    ]
    assert tuple(alert["runbook"]) == TAKEDOWN_SLO_RUNBOOK
    assert alert["thresholds"] == {
        "normal_p95_seconds": 120.0,
        "recovery_oldest_debt_seconds": 26.0 * 60.0 * 60.0,
        "p95_window_seconds": 60 * 60,
    }

    states = (
        _transition(TakedownState.INTENT_CREATED, occurred_at=NOW),
        _transition(
            TakedownState.GRAPH_DEMOTED,
            occurred_at=NOW + timedelta(seconds=10),
        ),
        _transition(
            TakedownState.OUTBOX_PERSISTED,
            occurred_at=NOW + timedelta(seconds=11),
            attempt=0,
        ),
        _transition(
            TakedownState.DELIVERED,
            occurred_at=NOW + timedelta(seconds=121),
            attempt=0,
        ),
    )
    snapshot = TakedownTelemetrySnapshot(
        board_id="board-card9",
        delete_event_id=DELETE_EVENT_ID,
        delivery_key=DELIVERY_KEY,
        artifact_type="spec",
        artifact_id="spec-card9",
        generation=1,
        states=states,
        aggregates=aggregates,
    ).to_dict()

    assert snapshot["slo"] == {
        "normal_threshold_seconds": TAKEDOWN_NORMAL_SLO_SECONDS,
        "recovery_threshold_seconds": TAKEDOWN_RECOVERY_SLO_SECONDS,
        "window_seconds": TAKEDOWN_P95_WINDOW_SECONDS,
        "breached": True,
        "breach_reasons": ["takedown_p95_above_120_seconds"],
        "runbook": list(TAKEDOWN_SLO_RUNBOOK),
        "health_predicate": "delivered_state_and_evaluable_parity_probe",
    }
    assert snapshot["states"][-1]["attempt"] == 0


class _FakeTakedownTelemetryReader:
    async def query_takedown_telemetry(self, context, query):
        del context, query
        return None


def test_read_port_registry_register_get_and_reset() -> None:
    reset_takedown_telemetry_read_port_for_tests()
    with pytest.raises(
        RuntimeError,
        match="takedown_telemetry_read_port_not_configured",
    ):
        get_takedown_telemetry_read_port()

    reader = _FakeTakedownTelemetryReader()
    register_takedown_telemetry_read_port(reader)
    assert get_takedown_telemetry_read_port() is reader

    reset_takedown_telemetry_read_port_for_tests()
    with pytest.raises(
        RuntimeError,
        match="takedown_telemetry_read_port_not_configured",
    ):
        get_takedown_telemetry_read_port()


def _snapshot() -> TakedownTelemetrySnapshot:
    return TakedownTelemetrySnapshot(
        board_id="board-card9",
        delete_event_id=DELETE_EVENT_ID,
        delivery_key=DELIVERY_KEY,
        artifact_type="spec",
        artifact_id="spec-card9",
        generation=1,
        states=(
            _transition(TakedownState.INTENT_CREATED),
            _transition(
                TakedownState.DELIVERED,
                occurred_at=NOW + timedelta(seconds=30),
                attempt=0,
            ),
        ),
        aggregates=_aggregates(p95=30.0, oldest_debt=None),
    )


class _RecordingTakedownTelemetryReader:
    def __init__(self, snapshot: TakedownTelemetrySnapshot | None) -> None:
        self.snapshot = snapshot
        self.calls: list[tuple[object, TakedownTelemetryQuery]] = []

    async def query_takedown_telemetry(
        self,
        context: object,
        query: TakedownTelemetryQuery,
    ) -> TakedownTelemetrySnapshot | None:
        self.calls.append((context, query))
        return self.snapshot


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "selector",
    (
        {"delete_event_id": DELETE_EVENT_ID},
        {"delivery_key": DELIVERY_KEY},
    ),
    ids=("delete_event_id", "delivery_key"),
)
async def test_kg_operations_query_takedown_telemetry_is_serializable_and_clocked(
    selector: dict[str, str],
) -> None:
    context = object()
    reader = _RecordingTakedownTelemetryReader(_snapshot())
    register_takedown_telemetry_read_port(reader)
    clock_calls = 0

    def _clock() -> datetime:
        nonlocal clock_calls
        clock_calls += 1
        return NOW

    operations = CoreKnowledgeGraphOperations(context, clock=_clock)
    payload = await operations.query_takedown_telemetry(
        board_id=BOARD_ID,
        **selector,
    )

    assert clock_calls == 1
    assert len(reader.calls) == 1
    assert reader.calls[0][0] is context
    assert reader.calls[0][1].now == NOW
    assert reader.calls[0][1].board_id == BOARD_ID
    assert payload["found"] is True
    assert payload["selector"] == selector
    assert payload["observed_at"] == NOW.isoformat()
    assert payload["states"][-1]["state"] == "delivered"
    assert json.loads(json.dumps(payload))["delete_event_id"] == DELETE_EVENT_ID


@pytest.mark.asyncio
@pytest.mark.parametrize(
    (
        "stale_items",
        "expected_healthy",
        "expected_clean",
        "expected_matching_count",
        "expected_reasons",
    ),
    (
        ([], True, True, 0, []),
        (
            [
                {
                    "source_artifact_ref": "spec:spec-card9",
                    "node_id": "criterion-card9",
                }
            ],
            False,
            False,
            1,
            ["stale_canonical_parity_detected"],
        ),
        (
            [
                {
                    "source_artifact_ref": "spec:other-spec",
                    "node_id": "other-node",
                }
            ],
            False,
            False,
            0,
            ["stale_canonical_parity_detected"],
        ),
    ),
    ids=("delivered_and_clean", "matching_stale", "board_stale"),
)
async def test_kg_operations_health_uses_delivered_and_independent_parity_probe(
    monkeypatch: pytest.MonkeyPatch,
    stale_items: list[dict[str, str]],
    expected_healthy: bool,
    expected_clean: bool,
    expected_matching_count: int,
    expected_reasons: list[str],
) -> None:
    from okto_pulse.core.kg import stale_canonical_parity

    async def _probe(context, *, board_id, limit, offset):
        assert context is relational_context
        assert (board_id, limit, offset) == ("board-card9", 200, 0)
        return {
            "board_id": board_id,
            "items": stale_items,
            "count": len(stale_items),
            "mutation_allowed": False,
            "global_discovery_evaluation": "evaluated",
            "global_discovery_stale_digest_count": 0,
        }

    monkeypatch.setattr(
        stale_canonical_parity,
        "list_stale_canonical_parity",
        _probe,
    )
    register_takedown_telemetry_read_port(
        _RecordingTakedownTelemetryReader(_snapshot())
    )
    relational_context = object()

    payload = await CoreKnowledgeGraphOperations(
        relational_context, clock=lambda: NOW
    ).query_takedown_telemetry(
        board_id=BOARD_ID,
        delete_event_id=DELETE_EVENT_ID,
    )

    health = payload["e2e_health"]
    assert health == {
        "predicate": "delivered_state_and_evaluable_parity_probe",
        "healthy": expected_healthy,
        "delivered": True,
        "final_delivery_state": "delivered",
        "final_delivery_attempt": 0,
        "parity_probe_evaluable": True,
        "parity_clean": expected_clean,
        "board_parity_clean": expected_clean,
        "global_discovery_parity_clean": True,
        "board_stale_node_count": len(stale_items),
        "global_discovery_stale_digest_count": 0,
        "global_discovery_evaluation": "evaluated",
        "matching_stale_node_count": expected_matching_count,
        "failure_reasons": expected_reasons,
        "probe_error_class": None,
    }


@pytest.mark.asyncio
async def test_kg_operations_health_fails_closed_when_parity_is_not_evaluable(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    from okto_pulse.core.kg import stale_canonical_parity

    async def _unavailable(*_args, **_kwargs):
        raise RuntimeError("board graph unavailable")

    monkeypatch.setattr(
        stale_canonical_parity,
        "list_stale_canonical_parity",
        _unavailable,
    )
    register_takedown_telemetry_read_port(
        _RecordingTakedownTelemetryReader(_snapshot())
    )

    with caplog.at_level(
        "WARNING", logger="okto_pulse.core.application.kg_operations"
    ):
        payload = await CoreKnowledgeGraphOperations(
            object(), clock=lambda: NOW
        ).query_takedown_telemetry(
            board_id=BOARD_ID,
            delete_event_id=DELETE_EVENT_ID,
        )

    health = payload["e2e_health"]
    assert health["healthy"] is False
    assert health["delivered"] is True
    assert health["final_delivery_state"] == "delivered"
    assert health["final_delivery_attempt"] == 0
    assert health["parity_probe_evaluable"] is False
    assert health["parity_clean"] is None
    assert health["board_parity_clean"] is None
    assert health["global_discovery_parity_clean"] is None
    assert health["board_stale_node_count"] is None
    assert health["global_discovery_stale_digest_count"] is None
    assert health["global_discovery_evaluation"] is None
    assert health["matching_stale_node_count"] is None
    assert health["failure_reasons"] == ["parity_probe_not_evaluable"]
    assert health["probe_error_class"] == "RuntimeError"
    record = next(
        item
        for item in caplog.records
        if getattr(item, "event", None)
        == "kg.takedown.parity_probe_unavailable"
    )
    assert record.delete_event_id == DELETE_EVENT_ID
    assert record.delivery_key == DELIVERY_KEY


@pytest.mark.asyncio
async def test_kg_operations_health_requires_final_highest_attempt_to_be_delivered(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.kg import stale_canonical_parity

    async def _clean_probe(_context, *, board_id, limit, offset):
        assert (limit, offset) == (200, 0)
        return {
            "board_id": board_id,
            "items": [],
            "count": 0,
            "mutation_allowed": False,
            "global_discovery_evaluation": "evaluated",
            "global_discovery_stale_digest_count": 0,
        }

    monkeypatch.setattr(
        stale_canonical_parity,
        "list_stale_canonical_parity",
        _clean_probe,
    )
    divergent = TakedownTelemetrySnapshot(
        board_id="board-card9",
        delete_event_id=DELETE_EVENT_ID,
        delivery_key=DELIVERY_KEY,
        artifact_type="spec",
        artifact_id="spec-card9",
        generation=1,
        states=(
            _transition(TakedownState.INTENT_CREATED),
            _transition(
                TakedownState.DELIVERED,
                occurred_at=NOW + timedelta(seconds=20),
                attempt=0,
            ),
            _transition(
                TakedownState.OUTBOX_PERSISTED,
                occurred_at=NOW + timedelta(seconds=30),
                attempt=1,
            ),
            _transition(
                TakedownState.DELIVERY_DEBT,
                occurred_at=NOW + timedelta(seconds=40),
                attempt=1,
            ),
        ),
        aggregates=_aggregates(p95=20.0, oldest_debt=10.0),
    )
    register_takedown_telemetry_read_port(
        _RecordingTakedownTelemetryReader(divergent)
    )

    payload = await CoreKnowledgeGraphOperations(
        object(), clock=lambda: NOW
    ).query_takedown_telemetry(
        board_id=BOARD_ID,
        delete_event_id=DELETE_EVENT_ID,
    )

    health = payload["e2e_health"]
    assert health["healthy"] is False
    assert health["delivered"] is False
    assert health["final_delivery_state"] == "delivery_debt"
    assert health["final_delivery_attempt"] == 1
    assert health["parity_probe_evaluable"] is True
    assert health["parity_clean"] is True
    assert health["failure_reasons"] == ["delivered_state_not_observed"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    (
        "gd_evaluation",
        "digest_count",
        "expected_evaluable",
        "expected_clean",
        "expected_reason",
    ),
    (
        (
            "not_evaluated",
            0,
            False,
            None,
            "parity_probe_not_evaluable",
        ),
        (
            "evaluated",
            1,
            True,
            False,
            "global_discovery_parity_mismatch_detected",
        ),
    ),
    ids=("gd_not_evaluable", "digest_mismatch"),
)
async def test_kg_operations_health_requires_evaluable_clean_global_discovery(
    monkeypatch: pytest.MonkeyPatch,
    gd_evaluation: str,
    digest_count: int,
    expected_evaluable: bool,
    expected_clean: bool | None,
    expected_reason: str,
) -> None:
    from okto_pulse.core.kg import stale_canonical_parity

    async def _probe(_context, *, board_id, limit, offset):
        assert (limit, offset) == (200, 0)
        return {
            "board_id": board_id,
            "items": [],
            "count": 0,
            "mutation_allowed": False,
            "global_discovery_evaluation": gd_evaluation,
            "global_discovery_stale_digest_count": digest_count,
        }

    monkeypatch.setattr(
        stale_canonical_parity,
        "list_stale_canonical_parity",
        _probe,
    )
    register_takedown_telemetry_read_port(
        _RecordingTakedownTelemetryReader(_snapshot())
    )

    payload = await CoreKnowledgeGraphOperations(
        object(), clock=lambda: NOW
    ).query_takedown_telemetry(
        board_id=BOARD_ID,
        delete_event_id=DELETE_EVENT_ID,
    )

    health = payload["e2e_health"]
    assert health["healthy"] is False
    assert health["delivered"] is True
    assert health["board_parity_clean"] is True
    assert health["parity_probe_evaluable"] is expected_evaluable
    assert health["parity_clean"] is expected_clean
    assert health["global_discovery_parity_clean"] is expected_clean
    assert health["failure_reasons"] == [expected_reason]


def test_stale_parity_detector_does_not_swallow_per_node_type_read_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.kg import interfaces, stale_canonical_parity

    class _PartiallyUnreadableCypher:
        def __init__(self) -> None:
            self.calls = 0

        def execute_read_only(self, *_args, **_kwargs):
            self.calls += 1
            if self.calls == 2:
                raise RuntimeError("requirement label unreadable")
            return {"rows": []}

    cypher = _PartiallyUnreadableCypher()
    monkeypatch.setattr(stale_canonical_parity, "_source_index", lambda _board: {})
    monkeypatch.setattr(
        interfaces,
        "get_kg_registry",
        lambda: SimpleNamespace(cypher_executor=cypher),
    )

    with pytest.raises(RuntimeError, match="requirement label unreadable"):
        stale_canonical_parity.detect_board_graph_stale("board-card9")

    assert cypher.calls == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "selector",
    (
        {},
        {"delete_event_id": DELETE_EVENT_ID, "delivery_key": DELIVERY_KEY},
    ),
    ids=("missing", "ambiguous"),
)
async def test_kg_operations_query_takedown_telemetry_enforces_selector_xor(
    selector: dict[str, str],
) -> None:
    reader = _RecordingTakedownTelemetryReader(_snapshot())
    register_takedown_telemetry_read_port(reader)
    operations = CoreKnowledgeGraphOperations(object(), clock=lambda: NOW)

    with pytest.raises(
        ValueError,
        match="takedown_telemetry_identity_one_of_required",
    ):
        await operations.query_takedown_telemetry(
            board_id=BOARD_ID,
            **selector,
        )

    assert reader.calls == []


@pytest.mark.asyncio
async def test_kg_operations_query_takedown_telemetry_not_found_is_fail_closed(
) -> None:
    reader = _RecordingTakedownTelemetryReader(None)
    register_takedown_telemetry_read_port(reader)
    operations = CoreKnowledgeGraphOperations(object(), clock=lambda: NOW)

    payload = await operations.query_takedown_telemetry(
        board_id=BOARD_ID,
        delete_event_id=DELETE_EVENT_ID
    )

    assert payload == {
        "found": False,
        "error": "takedown_telemetry_not_found",
        "selector": {"delete_event_id": DELETE_EVENT_ID},
        "observed_at": NOW.isoformat(),
    }
    assert "states" not in payload
    assert "aggregates" not in payload
    json.dumps(payload)


@pytest.mark.asyncio
async def test_kg_operations_query_takedown_telemetry_rejects_selector_mismatch(
) -> None:
    reader = _RecordingTakedownTelemetryReader(_snapshot())
    register_takedown_telemetry_read_port(reader)
    operations = CoreKnowledgeGraphOperations(object(), clock=lambda: NOW)

    with pytest.raises(RuntimeError, match="takedown_telemetry_selector_mismatch"):
        await operations.query_takedown_telemetry(
            board_id=BOARD_ID,
            delivery_key="gd_parity:other-delivery"
        )
