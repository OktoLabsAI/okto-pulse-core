"""BASE F2C/F3: live pipelines cannot resurrect archived Sprint materialization."""

from copy import deepcopy
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from okto_pulse.core.application.processors.consolidation import _process_queue_entry
from okto_pulse.core.application.processors.deterministic_kg import DeterministicWorker
from okto_pulse.core.events import types
from okto_pulse.core.events.handlers.cancellation_decay import SourceArchiveLifecycleHandler
from okto_pulse.core.events.handlers.consolidation_enqueuer import ConsolidationEnqueuer
from okto_pulse.core.events.registry import registered_handlers


@pytest.mark.parametrize("event_type,class_name", [
    ("sprint.created", "SprintCreated"),
    ("sprint.moved", "SprintMoved"),
    ("sprint.closed", "SprintClosed"),
])
def test_retired_events_have_no_live_dto_handler_or_projection(event_type, class_name):
    assert not hasattr(types, class_name)
    assert types.resolve_event_class(event_type) is None
    assert event_type not in types.EVENT_TYPES
    assert registered_handlers(event_type) == ()
    historical = SimpleNamespace(event_type=event_type, sprint_id="s", spec_id="spec")
    assert ConsolidationEnqueuer()._map_targets(historical) == []


def test_worker_rejects_sprint_instead_of_recreating_nodes():
    worker = DeterministicWorker()
    assert not hasattr(worker, "process_sprint")
    with pytest.raises(ValueError):
        worker.process_artifact("sprint", {"id": "s", "title": "Historical"})


def test_card_projection_is_independent_of_historical_sprint_metadata():
    card = {"id": "card-1234", "board_id": "b", "spec_id": "spec-1234",
            "title": "Task", "priority": "high", "card_type": "normal"}
    legacy = {**card, "sprint_id": "sprint-old"}
    before = deepcopy(legacy)
    worker = DeterministicWorker()
    result = worker.process_card(legacy)
    assert result == worker.process_card(card)
    assert legacy == before
    assert any(edge.to_candidate_id == "spec_spec-123_entity" for edge in result.edges)
    assert not any("sprint" in edge.to_candidate_id for edge in result.edges)


@pytest.mark.asyncio
@pytest.mark.parametrize("work_kind", ["consolidate", "stale_reconcile", "stale_sweep"])
async def test_legacy_queue_work_cannot_access_graph_or_acknowledge_success(work_kind):
    # Bare objects fail on any adapter access; no provider is required for refusal.
    entry = SimpleNamespace(artifact_type="sprint", work_kind=work_kind)
    with pytest.raises(ValueError, match="retired_sprint_work_requires_offline_cutover"):
        await _process_queue_entry(object(), entry)


@pytest.mark.asyncio
@pytest.mark.parametrize("archived", [True, False])
async def test_historical_archive_event_cannot_restore_or_mutate_sprint_graph(archived):
    payload = dict(board_id="b", artifact_type="sprint", artifact_id="s", archived=archived)
    with pytest.raises(ValidationError):
        types.ArtifactArchiveChanged(**payload)
    # Raw historical envelopes cannot become new live events. Defense in depth
    # still refuses an internal caller bypassing typed deserialization.
    event = SimpleNamespace(event_type="artifact.archive_changed", **payload)
    assert ConsolidationEnqueuer()._map_targets(event) == []
    with pytest.raises(ValueError, match="retired_sprint_work_requires_offline_cutover"):
        await SourceArchiveLifecycleHandler().handle(event, object())


def test_legacy_mixed_card_event_still_projects_card():
    event = types.CardCreated.model_validate({"board_id": "b", "card_id": "c",
        "spec_id": "spec", "sprint_id": "s"})
    assert ConsolidationEnqueuer()._map_targets(event) == [("card", "c")]
