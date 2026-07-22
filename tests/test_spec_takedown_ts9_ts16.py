"""Focused executable proofs for governed-takedown scenarios TS11-TS16.

The file deliberately exercises the real Core worker boundaries.  Persistence
of the final TS13 receipt and the TS16 debt/redrive path live in the matching
Community test module.
"""

from __future__ import annotations

import logging
from contextlib import nullcontext
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

import pytest

from okto_pulse.core.application.processors import consolidation
from okto_pulse.core.kg import canonical_stale_reconciler as reconciler
from okto_pulse.core.kg.canonical_stale_reconciler import (
    ALL_NODE_TYPES,
    StaleReconcileResult,
)
from okto_pulse.core.ports.stale_sweep import (
    StaleSweepCandidate,
    StaleSweepRunAction,
    StaleSweepRunReceipt,
)
from test_card6_delivery_transfer_worker import (
    _DeliveryPort,
    _QueueStore,
    _entry,
    _processor,
    _registered_ports,
    _track_mark_failed,
)
from test_card8_stale_sweep import _sweep_entry


NOW = datetime(2026, 7, 21, 15, 0, tzinfo=timezone.utc)


class _SkipBlockingExecution:
    async def run(self, _operation: Any) -> None:
        """The graph lifecycle has its own tests; retain the worker boundary."""


@pytest.mark.asyncio
async def test_ts11_partial_and_final_runs_share_delete_event_but_not_attempt(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A partial run and its convergent retry remain distinct telemetry runs."""

    entry = _entry()
    entry.status = "claimed"
    entry.claim_token = "ts11-claim"
    completed_without_requirement = [
        node_type
        for node_type in ALL_NODE_TYPES
        if node_type != "Requirement"
    ]
    partial_counts = {node_type: 0 for node_type in ALL_NODE_TYPES}
    partial_counts["Requirement"] = 1
    final_counts = dict(partial_counts)
    final_counts["Requirement"] = 0
    runs = [
        StaleReconcileResult(
            board_id=entry.board_id,
            correlation_id=entry.delete_event_id or "missing",
            scanned=1,
            incomplete=True,
            failed_types=["Requirement"],
            scanned_by_type=partial_counts,
            completed_types=completed_without_requirement,
        ),
        StaleReconcileResult(
            board_id=entry.board_id,
            correlation_id=entry.delete_event_id or "missing",
            scanned=0,
            scanned_by_type=final_counts,
            completed_types=list(ALL_NODE_TYPES),
        ),
    ]

    async def _claim_is_current(*_args: object, **_kwargs: object) -> bool:
        return True

    async def _reconcile(*_args: object, **_kwargs: object) -> StaleReconcileResult:
        return runs.pop(0)

    monkeypatch.setattr(
        consolidation,
        "_queue_claim_is_current_and_unfenced",
        _claim_is_current,
    )
    monkeypatch.setattr(consolidation, "under_safe_write", lambda *_a, **_k: nullcontext())
    monkeypatch.setattr(reconciler, "reconcile_stale_canonical", _reconcile)
    caplog.set_level(logging.INFO, logger="okto_pulse.kg.consolidation_worker")

    entry.attempts = 4
    assert not await consolidation._process_stale_reconcile_entry(
        object(),
        entry,
        blocking_execution=_SkipBlockingExecution(),
    )
    entry.attempts = 5
    assert await consolidation._process_stale_reconcile_entry(
        object(),
        entry,
        blocking_execution=_SkipBlockingExecution(),
    )

    records = [
        record
        for record in caplog.records
        if getattr(record, "event", None) == "kg.stale_reconcile.run"
    ]
    assert len(records) == 2
    assert {record.delete_event_id for record in records} == {
        entry.delete_event_id
    }
    assert [record.queue_attempt for record in records] == [4, 5]
    assert [record.incomplete for record in records] == [True, False]
    assert [record.failed_types for record in records] == [
        ["Requirement"],
        [],
    ]
    assert records[0].completed_types == completed_without_requirement
    assert records[1].completed_types == list(ALL_NODE_TYPES)


def test_ts13_final_receipt_preserves_complete_per_type_scan_proof() -> None:
    """The boolean worker boundary must not collapse ontology scan evidence."""

    entry = _entry()
    entry.attempts = 3
    counts = {
        node_type: index
        for index, node_type in enumerate(ALL_NODE_TYPES)
    }
    result = StaleReconcileResult(
        board_id=entry.board_id,
        correlation_id=entry.delete_event_id or "missing",
        scanned=sum(counts.values()),
        scanned_by_type=counts,
        completed_types=list(ALL_NODE_TYPES),
    )

    details = consolidation._stale_reconcile_telemetry_details(result, entry)

    assert details["queue_attempt"] == 3
    assert details["scanned_by_type"] == counts
    assert details["completed_types"] == list(ALL_NODE_TYPES)
    assert set(details["scanned_by_type"]) == set(ALL_NODE_TYPES)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "source_refs",
    ([], ["malformed-without-type"]),
    ids=("empty", "malformed"),
)
async def test_ts12_invalid_durable_job_retries_without_ack_or_scope_expansion(
    monkeypatch: pytest.MonkeyPatch,
    source_refs: list[str],
) -> None:
    """Invalid durable refs fail before reconcile and remain retryable."""

    entry = _entry()
    assert entry.payload is not None
    entry.payload["source_refs"] = source_refs
    store = _QueueStore(entry)
    delivery = _DeliveryPort(store)
    processor = _processor(monkeypatch, store)
    failures = _track_mark_failed(monkeypatch, processor)
    reconcile_calls: list[object] = []

    async def _unexpected_reconcile(*_args: object, **_kwargs: object) -> None:
        reconcile_calls.append(object())
        raise AssertionError("invalid refs broadened into a reconcile sweep")

    monkeypatch.setattr(
        reconciler,
        "reconcile_stale_canonical",
        _unexpected_reconcile,
    )

    with _registered_ports(store, delivery):
        assert await processor.process_batch() == 0

    fresh = store.entries[entry.id]
    assert reconcile_calls == []
    assert store.ack_calls == []
    assert delivery.read_calls == []
    assert delivery.transfer_calls == []
    assert fresh.status == "pending"
    assert fresh.attempts == 1
    assert len(failures) == 1
    assert failures[0]["max_attempts"] == 3


@pytest.mark.asyncio
async def test_ts16_board_absent_preserves_checkpoint_and_emits_reschedule(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A deleted board is observable and cannot consume the catch-up cursor."""

    cursor = reconciler.encode_stale_sweep_cursor(
        StaleSweepCandidate("card", "checkpoint-7")
    )
    entry = _sweep_entry(
        payload={"cursor": cursor, "budget": 7, "attempt": 4},
    )

    class _Store:
        async def board_exists(self, _context: object, *, board_id: str) -> bool:
            assert board_id == entry.board_id
            return False

    class _SweepPort:
        request: object | None = None

        async def reschedule_stale_sweep(
            self,
            _context: object,
            request: Any,
        ) -> StaleSweepRunReceipt:
            self.request = request
            return StaleSweepRunReceipt(
                entry_id=request.entry_id,
                board_id=request.board_id,
                action=StaleSweepRunAction.RESCHEDULED,
                cursor=request.cursor,
                budget=request.budget,
                attempt=request.attempt,
                enqueued=0,
                has_more=True,
                reason=request.reason,
            )

    port = _SweepPort()
    monkeypatch.setattr(
        consolidation,
        "get_consolidation_persistence_port",
        lambda: _Store(),
    )
    monkeypatch.setattr(consolidation, "get_stale_sweep_port", lambda: port)
    monkeypatch.setattr(
        consolidation,
        "get_kg_registry",
        lambda: (_ for _ in ()).throw(
            AssertionError("board_absent must stop before graph access")
        ),
    )
    caplog.set_level(logging.INFO, logger="okto_pulse.kg.consolidation_worker")

    receipt = await consolidation._process_stale_sweep_entry(
        object(),
        entry,
        clock=SimpleNamespace(now=lambda: NOW),
    )

    assert isinstance(receipt, StaleSweepRunReceipt)
    assert receipt.action is StaleSweepRunAction.RESCHEDULED
    assert receipt.reason == "board_absent"
    assert port.request is not None
    assert port.request.cursor == cursor
    assert port.request.budget == 7
    assert port.request.attempt == 4
    assert port.request.retry_at > NOW
    records = [
        record
        for record in caplog.records
        if getattr(record, "event", None)
        == "kg.stale_sweep.rescheduled.staged"
    ]
    assert len(records) == 1
    assert records[0].reason == "board_absent"
    assert records[0].cursor == cursor
    assert records[0].attempt == 4
