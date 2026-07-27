from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone

import pytest

from okto_pulse.core.kg.health_state import HealthState, MetricStatus
from okto_pulse.core.kg.interfaces.graph_runtime_store import (
    GraphRuntimeObservationState,
    GraphRuntimeState,
)
from okto_pulse.core.kg.interfaces.storage_ref import StorageRef
from okto_pulse.core.kg.materialization_health import (
    BoardHealthCensus,
    CensusStatus,
    HealthProbeDeadline,
    MaterializationEvidenceRequest,
    MaterializationHealthBaseline,
    MaterializationHealthPolicy,
    MaterializationState,
)


_OBSERVED_AT = datetime(2026, 7, 16, 12, 0, tzinfo=timezone.utc)


def test_all_materialization_probes_share_one_monotonic_deadline() -> None:
    deadline = HealthProbeDeadline(deadline_at=12.5)
    request = MaterializationEvidenceRequest(
        board_id="board-1",
        generation="generation-1",
        deadline=deadline,
    )

    assert request.deadline is deadline
    assert request.deadline.remaining_seconds(now=10.0) == 2.5
    assert request.deadline.remaining_seconds(now=13.0) == 0.0
    assert request.deadline.expired(now=12.5) is True


def _runtime_state(
    state: GraphRuntimeObservationState,
    *,
    generation: str | None = "generation-1",
    reason_code: str | None = None,
) -> GraphRuntimeState:
    return GraphRuntimeState.from_observation(
        board_id="board-1",
        storage_ref=StorageRef("board:board-1", "test"),
        state=state,
        generation=generation,
        reason_code=reason_code,
        observed_at=_OBSERVED_AT,
        backend="test",
    )


def _census(
    *,
    generation: str = "generation-1",
    status: CensusStatus = CensusStatus.AVAILABLE,
    source_count: int | None = 0,
    queue_depth: int | None = 0,
    active_queue_count: int | None = 0,
    dead_letter_count: int | None = 0,
    global_outbox_dead_letter_count: int | None = 0,
    reason_code: str = "board_census_ok",
) -> BoardHealthCensus:
    return BoardHealthCensus(
        generation=generation,
        status=status,
        source_count=source_count,
        queue_depth=queue_depth,
        active_queue_count=active_queue_count,
        dead_letter_count=dead_letter_count,
        global_outbox_dead_letter_count=global_outbox_dead_letter_count,
        reason_code=reason_code,
        observed_at=_OBSERVED_AT,
    )


def _baseline(
    *,
    graph_state: HealthState = HealthState.AT_RISK,
    discovery_state: HealthState = HealthState.HEALTHY,
    overall_state: HealthState = HealthState.AT_RISK,
    metric_status: MetricStatus = MetricStatus.UNAVAILABLE,
) -> MaterializationHealthBaseline:
    return MaterializationHealthBaseline(
        graph_state=graph_state,
        discovery_state=discovery_state,
        overall_state=overall_state,
        metric_status=metric_status,
        classification_reasons=("metric.unavailable",),
    )


@pytest.mark.parametrize(
    ("state", "exists", "status"),
    [
        (GraphRuntimeObservationState.PRESENT_READABLE_CANDIDATE, True, "available"),
        (GraphRuntimeObservationState.CONFIRMED_ABSENT, False, "absent"),
        (
            GraphRuntimeObservationState.PRESENT_UNREADABLE_OR_ERROR,
            False,
            "unavailable",
        ),
        (GraphRuntimeObservationState.PROVIDER_UNAVAILABLE, False, "unavailable"),
    ],
)
def test_four_state_runtime_factory_preserves_binary_consumers(
    state: GraphRuntimeObservationState,
    exists: bool,
    status: str,
) -> None:
    observation = _runtime_state(state, reason_code=f"{state.value}_reason")

    assert observation.state is state
    assert observation.normalized_state is state
    assert observation.exists is exists
    assert observation.status == status
    assert observation.generation == "generation-1"
    assert observation.reason_code == f"{state.value}_reason"
    assert observation.observed_at == _OBSERVED_AT


def test_legacy_absence_is_not_promoted_to_confirmed_absence() -> None:
    legacy = GraphRuntimeState(
        board_id="board-1",
        storage_ref=StorageRef("board:board-1", "legacy"),
        exists=False,
        status="absent",
        unavailable_reason="graph_absent",
    )

    assert legacy.state is None
    assert (
        legacy.normalized_state
        is GraphRuntimeObservationState.PROVIDER_UNAVAILABLE
    )


@pytest.mark.parametrize(
    ("discovery_state", "discovery_health", "expected_overall"),
    [
        (
            GraphRuntimeObservationState.CONFIRMED_ABSENT,
            HealthState.HEALTHY,
            HealthState.HEALTHY,
        ),
        (
            GraphRuntimeObservationState.PRESENT_READABLE_CANDIDATE,
            HealthState.AT_RISK,
            HealthState.AT_RISK,
        ),
    ],
)
def test_confirmed_empty_is_known_healthy_but_discovery_stays_independent(
    discovery_state: GraphRuntimeObservationState,
    discovery_health: HealthState,
    expected_overall: HealthState,
) -> None:
    result = MaterializationHealthPolicy().evaluate(
        board_store=_runtime_state(
            GraphRuntimeObservationState.CONFIRMED_ABSENT,
            reason_code="board_graph_confirmed_absent",
        ),
        census=_census(),
        discovery_store=_runtime_state(
            discovery_state,
            reason_code=f"discovery_{discovery_state.value}",
        ),
        baseline=_baseline(
            discovery_state=discovery_health,
            overall_state=HealthState.AT_RISK,
        ),
    )

    assert result.materialization_state is MaterializationState.NOT_MATERIALIZED
    assert result.materialization_generation == "generation-1"
    assert result.graph_state is HealthState.HEALTHY
    assert result.discovery_state is discovery_health
    assert result.overall_state is expected_overall
    assert result.metric_status is MetricStatus.AVAILABLE
    assert result.classification_reason == "empty_board_not_materialized"
    assert result.probe_reason_codes == {
        "board_graph": "board_graph_confirmed_absent",
        "board_census": "board_census_ok",
        "global_discovery": f"discovery_{discovery_state.value}",
    }

    assert asdict(result.known_empty_metrics) == {
        "queue_depth": 0,
        "active_queue_count": 0,
        "dead_letter_count": 0,
        "global_outbox_dead_letter_count": 0,
        "total_nodes": 0,
        "default_score_count": 0,
        "default_score_ratio": 0.0,
        "avg_relevance": 0.0,
        "canonical_layer_count": 0,
        "working_layer_count": 0,
        "source_count": 0,
        "board_storage_total_bytes": 0,
        "oldest_pending_age_s": None,
        "oldest_dead_letter_age_s": None,
        "high_water_mark_pct": None,
        "last_decay_tick_at": None,
        "graph_schema_version": None,
    }


def test_confirmed_empty_does_not_hide_independent_discovery_failure() -> None:
    result = MaterializationHealthPolicy().evaluate(
        board_store=_runtime_state(
            GraphRuntimeObservationState.CONFIRMED_ABSENT,
            reason_code="board_graph_confirmed_absent",
        ),
        census=_census(),
        discovery_store=_runtime_state(
            GraphRuntimeObservationState.PROVIDER_UNAVAILABLE,
            reason_code="discovery_provider_unavailable",
        ),
        baseline=_baseline(
            discovery_state=HealthState.HEALTHY,
            overall_state=HealthState.AT_RISK,
        ),
    )

    assert result.materialization_state is MaterializationState.NOT_MATERIALIZED
    assert result.graph_state is HealthState.HEALTHY
    assert result.discovery_state is HealthState.AT_RISK
    assert result.overall_state is HealthState.AT_RISK
    assert result.metric_status is MetricStatus.UNAVAILABLE
    assert result.classification_reason == "empty_board_not_materialized"
    assert result.probe_reason_codes["global_discovery"] == (
        "discovery_provider_unavailable"
    )
    assert result.known_empty_metrics is not None


@pytest.mark.parametrize(
    ("board_store", "census", "reason"),
    [
        (
            _runtime_state(
                GraphRuntimeObservationState.PRESENT_UNREADABLE_OR_ERROR,
                reason_code="board_stat_timeout",
            ),
            _census(),
            "board_stat_timeout",
        ),
        (
            _runtime_state(
                GraphRuntimeObservationState.PROVIDER_UNAVAILABLE,
                reason_code="board_provider_unavailable",
            ),
            _census(),
            "board_provider_unavailable",
        ),
        (
            _runtime_state(
                GraphRuntimeObservationState.CONFIRMED_ABSENT,
                reason_code="board_graph_confirmed_absent",
            ),
            _census(
                status=CensusStatus.UNAVAILABLE,
                source_count=None,
                queue_depth=None,
                active_queue_count=None,
                dead_letter_count=None,
                global_outbox_dead_letter_count=None,
                reason_code="board_census_timeout",
            ),
            "board_census_timeout",
        ),
        (
            _runtime_state(
                GraphRuntimeObservationState.CONFIRMED_ABSENT,
                reason_code="board_graph_confirmed_absent",
            ),
            _census(source_count=1),
            "board_census_nonzero",
        ),
    ],
)
def test_incomplete_or_contradictory_board_evidence_fails_closed(
    board_store: GraphRuntimeState,
    census: BoardHealthCensus,
    reason: str,
) -> None:
    result = MaterializationHealthPolicy().evaluate(
        board_store=board_store,
        census=census,
        discovery_store=_runtime_state(
            GraphRuntimeObservationState.CONFIRMED_ABSENT,
            reason_code="discovery_confirmed_absent",
        ),
        baseline=_baseline(),
    )

    assert result.materialization_state is MaterializationState.UNKNOWN
    assert result.graph_state is HealthState.AT_RISK
    assert result.overall_state is HealthState.AT_RISK
    assert result.metric_status is MetricStatus.UNAVAILABLE
    assert result.classification_reason == reason
    assert result.known_empty_metrics is None


def test_mixed_generation_evidence_is_conservative() -> None:
    result = MaterializationHealthPolicy().evaluate(
        board_store=_runtime_state(
            GraphRuntimeObservationState.CONFIRMED_ABSENT,
            generation="generation-1",
            reason_code="board_graph_confirmed_absent",
        ),
        census=_census(generation="generation-2"),
        discovery_store=_runtime_state(
            GraphRuntimeObservationState.CONFIRMED_ABSENT,
            reason_code="discovery_confirmed_absent",
        ),
        baseline=_baseline(),
    )

    assert result.materialization_state is MaterializationState.UNKNOWN
    assert result.materialization_generation is None
    assert result.graph_state is HealthState.AT_RISK
    assert result.metric_status is MetricStatus.UNAVAILABLE
    assert result.classification_reason == "materialization_generation_changed"
    assert result.known_empty_metrics is None


def test_unbound_generations_report_the_actual_fail_closed_reason() -> None:
    result = MaterializationHealthPolicy().evaluate(
        board_store=_runtime_state(
            GraphRuntimeObservationState.CONFIRMED_ABSENT,
            generation=None,
            reason_code="board_graph_confirmed_absent",
        ),
        census=_census(generation=None),
        discovery_store=_runtime_state(
            GraphRuntimeObservationState.CONFIRMED_ABSENT,
            reason_code="discovery_confirmed_absent",
        ),
        baseline=_baseline(),
    )

    assert result.materialization_state is MaterializationState.UNKNOWN
    assert result.materialization_generation is None
    assert result.graph_state is HealthState.AT_RISK
    assert result.metric_status is MetricStatus.UNAVAILABLE
    assert result.classification_reason == "materialization_generation_unbound"
    assert result.known_empty_metrics is None


@pytest.mark.parametrize(
    "preserved_state",
    [HealthState.RECOVERY_NEEDED, HealthState.QUARANTINED],
)
def test_unreadable_store_preserves_stronger_existing_health_state(
    preserved_state: HealthState,
) -> None:
    result = MaterializationHealthPolicy().evaluate(
        board_store=_runtime_state(
            GraphRuntimeObservationState.PRESENT_UNREADABLE_OR_ERROR,
            reason_code="board_store_corrupt",
        ),
        census=_census(),
        discovery_store=_runtime_state(
            GraphRuntimeObservationState.CONFIRMED_ABSENT,
            reason_code="discovery_confirmed_absent",
        ),
        baseline=_baseline(
            graph_state=preserved_state,
            overall_state=preserved_state,
        ),
    )

    assert result.materialization_state is MaterializationState.UNKNOWN
    assert result.graph_state is preserved_state
    assert result.overall_state is preserved_state
    assert result.classification_reason == "board_store_corrupt"


def test_present_readable_board_is_materialized_without_reclassifying_health() -> None:
    baseline = _baseline(
        graph_state=HealthState.HEALTHY,
        discovery_state=HealthState.HEALTHY,
        overall_state=HealthState.HEALTHY,
        metric_status=MetricStatus.AVAILABLE,
    )

    result = MaterializationHealthPolicy().evaluate(
        board_store=_runtime_state(
            GraphRuntimeObservationState.PRESENT_READABLE_CANDIDATE,
            reason_code="board_graph_present",
        ),
        census=_census(source_count=2),
        discovery_store=_runtime_state(
            GraphRuntimeObservationState.PRESENT_READABLE_CANDIDATE,
            reason_code="discovery_present",
        ),
        baseline=baseline,
    )

    assert result.materialization_state is MaterializationState.MATERIALIZED
    assert result.materialization_generation == "generation-1"
    assert result.graph_state is HealthState.HEALTHY
    assert result.overall_state is HealthState.HEALTHY
    assert result.metric_status is MetricStatus.AVAILABLE
    assert result.classification_reason == "board_graph_present"
    assert result.known_empty_metrics is None
