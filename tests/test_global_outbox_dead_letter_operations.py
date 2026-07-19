from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from okto_pulse.core.application.global_outbox_dead_letter import (
    GlobalOutboxDeadLetterError,
    GlobalOutboxDeadLetterOperations,
    global_outbox_dead_letter_metric_snapshot,
    reset_global_outbox_dead_letter_metrics_for_tests,
)
from okto_pulse.core.ports.global_outbox import (
    GlobalOutboxDeadLetterCursor,
    GlobalOutboxEventRecord,
)


NOW = datetime(2026, 7, 16, 17, 30, tzinfo=timezone.utc)


def _row(
    row_id: str,
    *,
    offset: int,
    retry_count: int = -1,
    processed: bool = False,
    last_error: str = "graph_unavailable: failed to open global graph",
    payload: dict | None = None,
) -> GlobalOutboxEventRecord:
    return GlobalOutboxEventRecord(
        id=row_id,
        event_id=f"event-{row_id}",
        board_id="board-dlq",
        session_id=f"session-{row_id}",
        payload=dict(payload or {"artifact_id": f"artifact-{row_id}"}),
        retry_count=retry_count,
        last_error=last_error,
        processed_at=NOW if processed else None,
        created_at=NOW + timedelta(seconds=offset),
    )


class MemoryGlobalOutboxStore:
    def __init__(self, rows: list[GlobalOutboxEventRecord]) -> None:
        self.rows = {row.id: row for row in rows}
        self.save_calls = 0

    async def list_terminal_events(
        self,
        context,
        *,
        limit: int,
        after: GlobalOutboxDeadLetterCursor | None = None,
    ) -> tuple[GlobalOutboxEventRecord, ...]:
        del context
        rows = sorted(self.rows.values(), key=lambda row: (row.created_at, row.id))
        rows = [
            row
            for row in rows
            if row.processed_at is None
            and (row.retry_count == -1 or row.retry_count >= 5)
            and (
                after is None or (row.created_at, row.id) > (after.created_at, after.id)
            )
        ]
        return tuple(rows[:limit])

    async def get_events_by_ids(
        self, context, *, ids: tuple[str, ...]
    ) -> tuple[GlobalOutboxEventRecord, ...]:
        del context
        return tuple(self.rows[row_id] for row_id in ids if row_id in self.rows)

    async def save_events(self, context, events) -> None:
        del context
        self.save_calls += 1
        for event in events:
            self.rows[event.id] = event

    async def requeue_terminal_events(self, context, events) -> None:
        await self.save_events(context, events)


@pytest.mark.asyncio
async def test_list_uses_opaque_stable_keyset_and_returns_only_terminal_rows():
    store = MemoryGlobalOutboxStore(
        [
            _row("terminal-a", offset=0),
            _row(
                "terminal-b",
                offset=0,
                retry_count=5,
                last_error="board_read_failed: source inventory unavailable",
            ),
            _row("terminal-c", offset=1, last_error="unclassified failure"),
            _row("active", offset=2, retry_count=0),
            _row("processed", offset=3, retry_count=5, processed=True),
        ]
    )
    operations = GlobalOutboxDeadLetterOperations(store=store, clock=lambda: NOW)

    first = await operations.list(context=object(), limit=2)
    second = await operations.list(
        context=object(), limit=2, cursor=first["next_cursor"]
    )

    assert [item["dead_letter_id"] for item in first["items"]] == [
        "terminal-a",
        "terminal-b",
    ]
    assert first["count"] == 2
    assert first["next_cursor"]
    assert [item["dead_letter_id"] for item in second["items"]] == ["terminal-c"]
    assert second["next_cursor"] is None
    assert {item["classification"] for item in first["items"]} == {
        "global_open_failure",
        "board_source_failure",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("selection", "expected_code"),
    [
        ([], "no_dlq_selected"),
        ([f"terminal-{index}" for index in range(101)], "selection_too_large"),
        (["terminal-a", "terminal-a"], "duplicate_id"),
        (["missing"], "selected_id_missing_or_wrong_class"),
        (["active"], "selected_id_missing_or_wrong_class"),
        (["terminal-a", "active"], "mixed_selection_ineligible"),
        (["terminal-a", "missing"], "mixed_selection_ineligible"),
    ],
)
async def test_invalid_reprocess_selection_is_typed_and_changes_zero_rows(
    selection: list[str], expected_code: str
):
    store = MemoryGlobalOutboxStore(
        [
            _row("terminal-a", offset=0),
            _row("active", offset=1, retry_count=0),
        ]
    )
    operations = GlobalOutboxDeadLetterOperations(store=store, clock=lambda: NOW)
    before = {
        row_id: (row.retry_count, row.last_error, dict(row.payload))
        for row_id, row in store.rows.items()
    }

    with pytest.raises(GlobalOutboxDeadLetterError) as raised:
        await operations.reprocess(
            context=object(),
            dead_letter_ids=selection,
            reason="operator_retry_after_graph_recovery",
        )

    assert raised.value.code == expected_code
    assert raised.value.mutated is False
    assert store.save_calls == 0
    assert {
        row_id: (row.retry_count, row.last_error, dict(row.payload))
        for row_id, row in store.rows.items()
    } == before


@pytest.mark.asyncio
async def test_reprocess_preserves_identity_and_is_idempotent_after_queue_and_apply():
    terminal = _row("terminal-a", offset=0)
    store = MemoryGlobalOutboxStore([terminal])
    operations = GlobalOutboxDeadLetterOperations(store=store, clock=lambda: NOW)

    first = await operations.reprocess(
        context=object(),
        dead_letter_ids=["terminal-a"],
        reason="operator_retry_after_graph_recovery",
    )
    second = await operations.reprocess(
        context=object(),
        dead_letter_ids=["terminal-a"],
        reason="operator_retry_after_graph_recovery",
    )
    terminal.processed_at = NOW + timedelta(seconds=5)
    third = await operations.reprocess(
        context=object(),
        dead_letter_ids=["terminal-a"],
        reason="operator_retry_after_graph_recovery",
    )

    assert first == {
        "selected_ids": ["terminal-a"],
        "requeued_ids": ["terminal-a"],
        "already_queued_ids": [],
        "already_applied_ids": [],
        "rejected_ids": [],
    }
    assert second["requeued_ids"] == []
    assert second["already_queued_ids"] == ["terminal-a"]
    assert third["already_applied_ids"] == ["terminal-a"]
    assert terminal.id == "terminal-a"
    assert terminal.event_id == "event-terminal-a"
    assert terminal.retry_count == 0
    assert terminal.last_error is None
    assert terminal.payload["_dlq_reprocess"]["reason"] == (
        "operator_retry_after_graph_recovery"
    )
    assert store.save_calls == 1


@pytest.mark.asyncio
async def test_verify_distinguishes_absent_terminal_queued_applied_and_superseded():
    terminal = _row("terminal", offset=0)
    queued = _row(
        "queued",
        offset=1,
        retry_count=0,
        last_error="",
        payload={"_dlq_reprocess": {"reason": "operator_retry"}},
    )
    applied = _row(
        "applied",
        offset=2,
        retry_count=0,
        processed=True,
        last_error="",
        payload={"_dlq_reprocess": {"reason": "operator_retry"}},
    )
    processing = _row(
        "processing",
        offset=3,
        retry_count=1,
        last_error="transient retry scheduled",
        payload={"_dlq_reprocess": {"reason": "operator_retry"}},
    )
    superseded = _row(
        "superseded",
        offset=4,
        payload={"superseded_by_dead_letter_id": "applied"},
    )
    store = MemoryGlobalOutboxStore([terminal, queued, processing, applied, superseded])
    operations = GlobalOutboxDeadLetterOperations(store=store, clock=lambda: NOW)

    result = await operations.verify(
        context=object(),
        dead_letter_ids=[
            "missing",
            "terminal",
            "queued",
            "processing",
            "applied",
            "superseded",
        ],
    )
    by_id = {item["dead_letter_id"]: item for item in result["items"]}

    assert by_id["missing"]["state"] == "absent"
    assert by_id["terminal"]["state"] == "still_dead_lettered"
    assert by_id["queued"]["state"] == "queued"
    assert by_id["processing"]["state"] == "processing"
    assert by_id["processing"]["reason_code"] == "reprocessed_event_retrying"
    assert by_id["applied"]["state"] == "applied"
    assert by_id["superseded"]["state"] == "superseded"
    assert by_id["superseded"]["authoritative_id"] == "applied"
    assert by_id["superseded"]["supersedence_chain"] == [
        "superseded",
        "applied",
    ]


@pytest.mark.asyncio
async def test_verify_reports_dangling_supersedence_without_false_authority():
    dangling = _row(
        "dangling",
        offset=0,
        payload={"superseded_by_dead_letter_id": "missing-successor"},
    )
    operations = GlobalOutboxDeadLetterOperations(
        store=MemoryGlobalOutboxStore([dangling]),
        clock=lambda: NOW,
    )

    result = await operations.verify(
        context=object(),
        dead_letter_ids=["dangling"],
    )

    assert result["items"] == [
        {
            "dead_letter_id": "dangling",
            "state": "superseded",
            "event_id": "event-dangling",
            "authoritative_id": None,
            "supersedence_chain": ["dangling", "missing-successor"],
            "reason_code": "supersedence_target_absent",
        }
    ]


@pytest.mark.asyncio
async def test_operation_metrics_keep_monotonic_counts_but_only_100_latencies():
    reset_global_outbox_dead_letter_metrics_for_tests()
    operations = GlobalOutboxDeadLetterOperations(
        store=MemoryGlobalOutboxStore([_row("terminal", offset=0)]),
        clock=lambda: NOW,
    )

    for _ in range(105):
        await operations.list(context=object(), limit=1)

    snapshot = global_outbox_dead_letter_metric_snapshot()
    assert snapshot["counts"]["list:success:global_open_failure"] == 105
    assert len(snapshot["latency_ms"]["list:success"]) == 100
    assert snapshot["retention_limit_per_operation_outcome"] == 100
    assert snapshot["retained_latency_samples"] == 100
