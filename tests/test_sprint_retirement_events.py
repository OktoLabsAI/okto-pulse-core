"""BASE F2C/F3: retirement must not discard a Card fact with Sprint metadata."""

from copy import deepcopy

import pytest

from okto_pulse.core.domain.sprint_retirement_events import (
    classify_historical_sprint_event,
    classify_historical_sprint_execution,
    classify_historical_sprint_queue,
)
from okto_pulse.core.events.types import ArtifactArchiveChanged, CardCreated, SprintClosed, SprintCreated, SprintMoved


@pytest.mark.parametrize("event", [
    SprintCreated(board_id="b", sprint_id="s", spec_id="spec"),
    SprintMoved(board_id="b", sprint_id="s", from_status="active", to_status="closed"),
    SprintClosed(board_id="b", sprint_id="s"),
    ArtifactArchiveChanged(board_id="b", artifact_type="sprint", artifact_id="s", archived=True),
    ArtifactArchiveChanged(board_id="b", artifact_type="sprint", artifact_id="s", archived=False),
])
def test_real_stored_exclusive_contracts_are_eligible_only_for_archived_supersession(event):
    result = classify_historical_sprint_event(event.event_type, event.payload_for_storage())
    assert result.action == "supersede" and result.sprint_ids == ("s",)


def test_real_card_event_preserves_all_facts_and_input_bytes():
    event = CardCreated(board_id="b", card_id="card", spec_id="spec", sprint_id="s", card_type="bug", priority="high")
    payload = event.payload_for_storage()
    original = deepcopy(payload)
    result = classify_historical_sprint_event(event.event_type, payload)
    assert result.action == "preserve" and result.reason == "mixed_card_event"
    assert result.sprint_ids == ("s",)
    assert payload == original


@pytest.mark.parametrize(("event_type", "payload", "reason"), [
    ("sprint.future", {"sprint_id": "s"}, "unknown_sprint_event"),
    ("sprint.closed", {"sprint_id": "s", "card_id": "c"}, "exclusive_contract_drift"),
    ("sprint.moved", {"sprint_id": "s", "to_status": "closed"}, "exclusive_contract_drift"),
    ("sprint.created", {"sprint_id": "s", "spec_id": "sprint:other"}, "exclusive_reference_drift"),
    ("artifact.archive_changed", {"artifact_type": "sprint", "artifact_id": "s", "archived": 1}, "archive_contract_drift"),
    ("card.created", {"card_id": "c", "sprint_id": "s", "new_effect": True}, "unclassified_sprint_reference"),
    ("other.event", {"nested": [{"subject_type": "sprint", "subject_id": "s"}]}, "unclassified_sprint_reference"),
    ("other.event", {"source_ref": "sprint:s:qa:q"}, "unclassified_sprint_reference"),
])
def test_unknown_or_mixed_effects_require_investigation(event_type, payload, reason):
    result = classify_historical_sprint_event(event_type, payload)
    assert result.action == "review" and result.reason == reason


@pytest.mark.parametrize("payload", [None, [], {"sprint_id": 1}, {"sprint_id": " "}, {"artifact_type": "sprint"}])
def test_invalid_envelopes_and_references_do_not_become_no_work(payload):
    with pytest.raises(ValueError, match="sprint_retirement_event"):
        classify_historical_sprint_event("card.created", payload)


def test_unrelated_event_is_not_retired_for_a_prose_mention():
    result = classify_historical_sprint_event("card.conclusion_added", {"card_id": "c", "text": "Sprint was useful"})
    assert result.action == "preserve" and result.sprint_ids == ()


def test_reference_walk_is_bounded():
    with pytest.raises(ValueError, match="structure_limit"):
        classify_historical_sprint_event("unknown", {"items": [0] * 5001})
    payload = {}
    for _ in range(34):
        payload = {"nested": payload}
    with pytest.raises(ValueError, match="structure_limit"):
        classify_historical_sprint_event("unknown", payload)


@pytest.mark.parametrize(("handler", "status", "action"), [
    ("ConsolidationEnqueuer", "pending", "supersede"),
    ("ConsolidationEnqueuer", "failed", "supersede"),
    ("ConsolidationEnqueuer", "dlq", "supersede"),
    ("ConsolidationEnqueuer", "done", "preserve"),
    ("ConsolidationEnqueuer", "processing", "review"),
    ("NewHandler", "pending", "review"),
    ("ConsolidationEnqueuer", "new_status", "review"),
])
def test_execution_plan_keeps_history_and_refuses_unknown_effects(handler, status, action):
    disposition = classify_historical_sprint_event("sprint.closed", {"sprint_id": "s"})
    assert classify_historical_sprint_execution("sprint.closed", disposition, handler_name=handler, status=status)[0] == action


@pytest.mark.parametrize(("status", "payload", "kind", "action"), [
    ("pending", None, "consolidate", "supersede"),
    ("failed", {}, "stale_reconcile", "supersede"),
    ("done", None, "consolidate", "preserve"),
    ("claimed", None, "consolidate", "review"),
    ("pending", {"card_id": "c"}, "consolidate", "review"),
    ("pending", None, "stale_sweep", "review"),
])
def test_queue_exclusivity_is_closed(status, payload, kind, action):
    assert classify_historical_sprint_queue(artifact_type="sprint", artifact_id="s", work_kind=kind,
        status=status, payload=payload).action == action


@pytest.mark.parametrize("payload", [False, 0, [], ""])
def test_falsey_invalid_queue_payload_does_not_become_empty(payload):
    with pytest.raises(ValueError, match="queue_payload_invalid"):
        classify_historical_sprint_queue(artifact_type="sprint", artifact_id="s", work_kind="consolidate",
            status="pending", payload=payload)
