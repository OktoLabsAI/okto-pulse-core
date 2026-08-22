from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from okto_pulse.core.application.processors.consolidation import (
    _observe_spec_dependency_projection_lag_after_ack,
)
from okto_pulse.core.events.handlers.consolidation_enqueuer import (
    ConsolidationEnqueuer,
)
from okto_pulse.core.events.types import (
    SpecDependencyAdded,
    SpecDependencyRemoved,
    SpecVersionBumped,
)
from okto_pulse.core.ports.consolidation import ConsolidationQueueRecord
from okto_pulse.core.ports.relational_effects import (
    ConsolidationQueueUpsert,
    register_relational_effects_port,
)
from okto_pulse.core.services.spec_dependency_observability import (
    METRIC_SPEC_DEPENDENCY_PROJECTION_LAG_SECONDS,
    get_spec_dependency_metric_samples,
    reset_spec_dependency_observability_for_tests,
)


class _CapturingRelationalEffects:
    def __init__(self) -> None:
        self.upserts: list[ConsolidationQueueUpsert] = []

    async def upsert_consolidation_queue_unless_tombstoned(
        self,
        _session: object,
        upsert: ConsolidationQueueUpsert,
    ) -> bool:
        self.upserts.append(upsert)
        return True

    async def count_active_consolidation_queue(
        self,
        _session: object,
        *,
        board_id: str,
    ) -> int:
        assert board_id == "board-1"
        return len(self.upserts)


def _queue_record(
    upsert: ConsolidationQueueUpsert,
    *,
    triggered_at: datetime,
) -> ConsolidationQueueRecord:
    return ConsolidationQueueRecord(
        id=f"queue-{upsert.artifact_id}",
        board_id=upsert.board_id,
        artifact_type=upsert.artifact_type,
        artifact_id=upsert.artifact_id,
        status="done",
        attempts=0,
        last_error=None,
        next_retry_at=None,
        claimed_at=None,
        claim_timeout_at=None,
        worker_id=None,
        claimed_by_session_id=None,
        triggered_at=triggered_at,
        priority=upsert.priority,
        payload=upsert.payload,
        triggered_by_event=upsert.triggered_by_event,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["added", "removed"])
async def test_dual_target_dependency_lag_is_emitted_only_after_owner_ack(
    operation: str,
) -> None:
    mutation_at = datetime(2026, 8, 12, tzinfo=timezone.utc)
    effects = _CapturingRelationalEffects()
    register_relational_effects_port(effects)
    event = (
        SpecDependencyAdded(
            event_id="dependency-mutation-1",
            board_id="board-1",
            occurred_at=mutation_at,
            spec_id="dependent-spec",
            dependency_id="dependency-1",
            target_spec_id="prerequisite-spec",
            projection_owner_spec_id="dependent-spec",
            source_version=2,
            source_status_on_create="draft",
            resolved_on_create=False,
        )
        if operation == "added"
        else SpecDependencyRemoved(
            event_id="dependency-mutation-1",
            board_id="board-1",
            occurred_at=mutation_at,
            spec_id="dependent-spec",
            dependency_id="dependency-1",
            target_spec_id="prerequisite-spec",
            projection_owner_spec_id="dependent-spec",
            source_version=2,
            removal_reason="no_longer_required",
        )
    )
    version_event = SpecVersionBumped(
        event_id="dependency-version-signal-1",
        board_id="board-1",
        occurred_at=mutation_at,
        spec_id="dependent-spec",
        old_version=1,
        new_version=2,
        changed_fields=["dependencies"],
    )

    enqueuer = ConsolidationEnqueuer()
    assert enqueuer._map_targets(version_event) == []
    await enqueuer.handle(version_event, object())
    assert effects.upserts == []
    await enqueuer.handle(event, object())

    assert [upsert.artifact_id for upsert in effects.upserts] == [
        "prerequisite-spec",
        "dependent-spec",
    ]
    by_role = {
        str(upsert.payload["target_role"]): _queue_record(
            upsert,
            triggered_at=mutation_at,
        )
        for upsert in effects.upserts
        if upsert.payload is not None
    }
    assert set(by_role) == {"endpoint_bootstrap", "projection_owner"}
    assert {
        str(upsert.payload["mutation_event_id"])
        for upsert in effects.upserts
        if upsert.payload is not None
    } == {event.event_id}
    assert event.payload_for_storage()["projection_owner_spec_id"] == (
        "dependent-spec"
    )

    reset_spec_dependency_observability_for_tests()
    projected_at = mutation_at + timedelta(seconds=3)
    assert (
        _observe_spec_dependency_projection_lag_after_ack(
            by_role["endpoint_bootstrap"],
            projected_at=projected_at,
        )
        is False
    )
    assert get_spec_dependency_metric_samples() == []

    assert (
        _observe_spec_dependency_projection_lag_after_ack(
            by_role["projection_owner"],
            projected_at=projected_at,
        )
        is True
    )
    samples = get_spec_dependency_metric_samples()
    assert len(samples) == 1
    assert samples[0] == {
        "metric_name": METRIC_SPEC_DEPENDENCY_PROJECTION_LAG_SECONDS,
        "value": 3.0,
        "labels": {
            "operation": operation,
            "outcome": "projected",
            "reason_code": "none",
        },
    }
    reset_spec_dependency_observability_for_tests()
