from __future__ import annotations

import pytest

from okto_pulse.core.kg.cognitive_closeout_gate import (
    CognitiveCloseoutGate,
    CognitiveCloseoutGateError,
    CognitiveCloseoutOutcome,
    CognitiveCloseoutReason,
    get_closeout_gate_counter_labels,
    get_closeout_gate_event_count,
    get_closeout_gate_samples,
    reset_closeout_gate_samples,
    resolve_cognitive_source_refs,
)
from okto_pulse.core.kg.rebuild_audit import (
    CognitiveConsolidationItem,
    CognitiveItemStatus,
)


def _item(source_ref: str, status: str, item_id: str | None = None) -> CognitiveConsolidationItem:
    return CognitiveConsolidationItem(
        item_id=item_id or f"item-{source_ref.replace(':', '-')}",
        board_id="board-1",
        kg_generation_id="kg-1",
        source_ref=source_ref,
        artifact_type=source_ref.split(":", 1)[0],
        status=status,
        recorded_at="2026-05-29T00:00:00Z",
    )


class FakeStore:
    def __init__(self, items: list[CognitiveConsolidationItem] | None = None):
        self.items = list(items or [])
        self.list_calls = 0

    def latest_generation(self, board_id: str) -> str | None:
        assert board_id == "board-1"
        return "kg-1"

    def list_items(
        self,
        board_id: str,
        kg_generation_id: str,
        *,
        status_filter: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[CognitiveConsolidationItem]:
        assert board_id == "board-1"
        assert kg_generation_id == "kg-1"
        self.list_calls += 1
        return list(self.items)


class RaisingStore:
    def latest_generation(self, board_id: str) -> str | None:
        raise RuntimeError("ledger offline")

    def list_items(self, *args, **kwargs):
        raise AssertionError("latest_generation should fail first")


def test_resolve_spec_source_refs_includes_active_formal_decisions_only():
    refs = resolve_cognitive_source_refs(
        entity_type="spec",
        entity={
            "id": "spec-1",
            "decisions": [
                {"id": "dec-active", "title": "Use store", "rationale": "Stable", "status": "active"},
                {"id": "dec-default", "title": "Default active", "rationale": "Traceable"},
                {"id": "dec-revoked", "title": "Old", "rationale": "No", "status": "revoked"},
                {"id": "dec-superseded", "title": "Older", "rationale": "No", "status": "superseded"},
                "prose-only decision",
            ],
        },
    )

    assert refs.source_refs == (
        "spec:spec-1",
        "decision:spec-1:dec-active",
        "decision:spec-1:dec-default",
    )
    assert "decision:spec-1:dec-revoked" in refs.ignored_sources
    assert "decision:spec-1:dec-superseded" in refs.ignored_sources
    assert "decision:spec-1:<non-object-4>" in refs.ignored_sources


@pytest.mark.parametrize(
    "status",
    [
        CognitiveItemStatus.PENDING.value,
        CognitiveItemStatus.IN_PROGRESS.value,
        CognitiveItemStatus.FAILED.value,
    ],
)
def test_gate_blocks_active_cognitive_statuses(status: str):
    reset_closeout_gate_samples()
    gate = CognitiveCloseoutGate(store=FakeStore([_item("task:card-1", status)]))

    result = gate.evaluate(
        board_id="board-1",
        entity_type="task",
        entity_id="card-1",
        target_status="done",
    )

    assert result.allowed is False
    assert result.reason == CognitiveCloseoutReason.COGNITIVE_CONSOLIDATION_PENDING.value
    assert result.outcome == CognitiveCloseoutOutcome.BLOCKED.value
    assert result.blocking_count == 1
    assert get_closeout_gate_samples()[-1] == {
        "board_id_hash": get_closeout_gate_samples()[-1]["board_id_hash"],
        "entity_id_hash": get_closeout_gate_samples()[-1]["entity_id_hash"],
        "entity_type": "task",
        "outcome": "blocked",
        "reason": "cognitive_consolidation_pending",
        "skip_enabled": "false",
        "blocking_count_bucket": "1",
    }


@pytest.mark.parametrize(
    "status",
    [CognitiveItemStatus.CONSOLIDATED.value, CognitiveItemStatus.SKIPPED.value],
)
def test_gate_allows_terminal_statuses(status: str):
    gate = CognitiveCloseoutGate(store=FakeStore([_item("refinement:ref-1", status)]))

    result = gate.evaluate(
        board_id="board-1",
        entity_type="refinement",
        entity_id="ref-1",
        target_status="done",
    )

    assert result.allowed is True
    assert result.reason == CognitiveCloseoutReason.NO_ACTIVE_COGNITIVE_ITEMS.value
    assert result.blocking_items == ()


def test_gate_fails_closed_when_ledger_unavailable():
    gate = CognitiveCloseoutGate(store=RaisingStore())

    result = gate.evaluate(
        board_id="board-1",
        entity_type="bug",
        entity_id="bug-1",
        target_status="done",
    )

    assert result.allowed is False
    assert result.reason == CognitiveCloseoutReason.COGNITIVE_STATUS_UNAVAILABLE.value
    assert result.outcome == CognitiveCloseoutOutcome.UNAVAILABLE.value


def test_board_skip_bypasses_active_items_without_hiding_blocking_context():
    store = FakeStore([_item("test:test-1", CognitiveItemStatus.PENDING.value)])
    gate = CognitiveCloseoutGate(store=store)

    result = gate.evaluate(
        board_id="board-1",
        entity_type="test",
        entity_id="test-1",
        target_status="done",
        board_skip_enabled=True,
    )

    assert result.allowed is True
    assert result.reason == CognitiveCloseoutReason.BOARD_SKIP_ENABLED.value
    assert result.outcome == CognitiveCloseoutOutcome.SKIPPED.value
    assert result.blocking_count == 1
    assert store.items[0].status == CognitiveItemStatus.PENDING.value


def test_spec_decision_child_source_ref_blocks_spec_closeout():
    reset_closeout_gate_samples()
    gate = CognitiveCloseoutGate(
        store=FakeStore([
            _item("decision:spec-1:dec-active", CognitiveItemStatus.PENDING.value)
        ])
    )

    result = gate.evaluate(
        board_id="board-1",
        entity_type="spec",
        entity_id="spec-1",
        entity={
            "id": "spec-1",
            "decisions": [
                {"id": "dec-active", "title": "Use gate", "rationale": "Traceable"}
            ],
        },
        target_status="done",
    )

    assert result.allowed is False
    assert result.has_decision_child_block is True
    assert result.blocking_items[0].source_ref == "decision:spec-1:dec-active"
    assert get_closeout_gate_event_count(
        entity_type="spec",
        outcome="blocked",
        reason="active_decision_child_pending",
    ) == 1


def test_skip_bypass_and_unavailable_metrics_are_distinguishable():
    reset_closeout_gate_samples()
    store = FakeStore([_item("task:task-1", CognitiveItemStatus.PENDING.value)])
    gate = CognitiveCloseoutGate(store=store)

    gate.evaluate(
        board_id="board-1",
        entity_type="task",
        entity_id="task-1",
        target_status="done",
        board_skip_enabled=True,
    )
    CognitiveCloseoutGate(store=RaisingStore()).evaluate(
        board_id="board-1",
        entity_type="bug",
        entity_id="bug-1",
        target_status="done",
    )

    assert get_closeout_gate_event_count(
        outcome="skipped",
        reason="board_skip_enabled",
        skip_enabled="true",
    ) == 1
    assert get_closeout_gate_event_count(
        outcome="unavailable",
        reason="cognitive_status_unavailable",
        skip_enabled="false",
    ) == 1


def test_metric_emission_failure_does_not_change_gate_decision(monkeypatch):
    class _BrokenSamples:
        def append(self, _item):
            raise RuntimeError("metric sink unavailable")

    monkeypatch.setattr(
        "okto_pulse.core.kg.cognitive_closeout_gate._closeout_samples",
        _BrokenSamples(),
    )
    gate = CognitiveCloseoutGate(store=FakeStore())

    result = gate.evaluate(
        board_id="board-1",
        entity_type="task",
        entity_id="task-1",
        target_status="done",
    )

    assert result.allowed is True
    assert result.reason == CognitiveCloseoutReason.NO_ACTIVE_COGNITIVE_ITEMS.value


def test_gate_rejects_non_done_target_status():
    gate = CognitiveCloseoutGate(store=FakeStore())

    with pytest.raises(CognitiveCloseoutGateError) as exc:
        gate.evaluate(
            board_id="board-1",
            entity_type="task",
            entity_id="card-1",
            target_status="validation",
        )

    assert exc.value.code == "invalid_target_status"


def test_closeout_metric_labels_are_bounded():
    assert get_closeout_gate_counter_labels() == (
        "board_id_hash",
        "entity_id_hash",
        "entity_type",
        "outcome",
        "reason",
        "skip_enabled",
        "blocking_count_bucket",
    )
