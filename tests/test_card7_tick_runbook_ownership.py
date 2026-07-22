from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from okto_pulse.core.application.global_outbox_dead_letter import (
    GlobalOutboxDeadLetterError,
    GlobalOutboxDeadLetterOperations,
)
from okto_pulse.core.ports.delivery_ledger import (
    DeliveryAttemptEnvelope,
    DeliveryMaintenanceReceipt,
)
from okto_pulse.core.ports.global_outbox import GlobalOutboxEventRecord


NOW = datetime(2026, 7, 21, 12, 0, tzinfo=timezone.utc)


class _TickLedger:
    def __init__(self, order: list[str]) -> None:
        self.order = order
        self.calls: list[tuple[str, object, str, datetime, int]] = []

    async def reconcile_orphaned_attempts(
        self,
        context,
        *,
        board_id: str,
        now: datetime,
        limit: int,
    ) -> DeliveryMaintenanceReceipt:
        self.order.append("watchdog")
        self.calls.append(("watchdog", context, board_id, now, limit))
        return DeliveryMaintenanceReceipt(scanned=3, transitioned=2)

    async def redrive_delivery_debt(
        self,
        context,
        *,
        now: datetime,
        limit: int,
    ) -> DeliveryMaintenanceReceipt:
        self.order.append("redrive")
        self.calls.append(("redrive", context, None, now, limit))
        return DeliveryMaintenanceReceipt(scanned=4, emitted=1, concurrency_lost=1)


@pytest.mark.asyncio
async def test_tick_runs_watchdog_then_redrive_after_failed_graph_scope(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    from okto_pulse.core.events.handlers import kg_decay_tick

    order: list[str] = []
    ledger = _TickLedger(order)
    session = object()

    async def fail_graph(_fn, board_id, *_args, **_kwargs):
        assert board_id == "board-card7"
        order.append("graph")
        raise RuntimeError("graph unavailable")

    async def ignore_tick_run(*_args, **_kwargs) -> None:
        return None

    monkeypatch.setattr(kg_decay_tick.asyncio, "to_thread", fail_graph)
    monkeypatch.setattr(kg_decay_tick, "_persist_tick_run", ignore_tick_run)
    monkeypatch.setattr(
        kg_decay_tick,
        "get_delivery_ledger_port",
        lambda: ledger,
    )

    with caplog.at_level("INFO", logger=kg_decay_tick.__name__):
        summary = await kg_decay_tick._run_daily_tick(
            tick_id="tick-card7",
            session=session,
            board_id="board-card7",
            delivery_watchdog_limit=7,
            delivery_redrive_limit=11,
        )

    assert order == ["graph", "watchdog", "redrive"]
    assert summary["boards_failed"] == 1
    assert [(call[0], call[1], call[2], call[4]) for call in ledger.calls] == [
        ("watchdog", session, "board-card7", 7),
        ("redrive", session, None, 11),
    ]
    assert ledger.calls[0][3] == ledger.calls[1][3]
    assert ledger.calls[0][3].tzinfo is not None
    receipts = {
        getattr(record, "event", None): record for record in caplog.records
    }
    watchdog = receipts["kg.tick.delivery_watchdog.staged"]
    redrive = receipts["kg.tick.delivery_redrive.staged"]
    assert (watchdog.scanned, watchdog.transitioned, watchdog.limit) == (3, 2, 7)
    assert (redrive.scanned, redrive.emitted, redrive.limit) == (4, 1, 11)
    assert watchdog.transaction_state == "pending_caller_commit"
    assert redrive.commit_owner == "dispatcher"


@pytest.mark.asyncio
async def test_redrive_has_more_publishes_one_deterministic_bounded_continuation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core import events
    from okto_pulse.core.events.handlers import kg_decay_tick
    from okto_pulse.core.events.types import KGDeliveryRedriveTick

    class Ledger:
        async def redrive_delivery_debt(
            self,
            context,
            *,
            now: datetime,
            limit: int,
        ) -> DeliveryMaintenanceReceipt:
            assert context is session
            assert now == NOW
            assert limit == 2
            return DeliveryMaintenanceReceipt(
                scanned=2,
                transitioned=2,
                emitted=2,
                has_more=True,
                oldest_debt_age_seconds=3600,
                checkpoint_version=7,
                resume_board_id="board-resume",
            )

    published: list[tuple[object, object]] = []

    async def capture(event, session) -> None:
        published.append((event, session))

    session = object()
    monkeypatch.setattr(events, "publish", capture)

    receipt = await kg_decay_tick._run_delivery_redrive_pass(
        port=Ledger(),
        session=session,
        board_id="board-origin",
        now=NOW,
        redrive_limit=2,
    )

    assert receipt.has_more is True
    assert len(published) == 1
    continuation, published_session = published[0]
    assert isinstance(continuation, KGDeliveryRedriveTick)
    assert published_session is session
    assert continuation.board_id == "board-resume"
    assert continuation.checkpoint_version == 7
    assert continuation.run_id == continuation.event_id
    assert continuation.run_id == (
        kg_decay_tick._delivery_redrive_continuation_id(7)
    )
    assert continuation.actor_type == "system"


@pytest.mark.asyncio
async def test_dedicated_redrive_handler_never_repeats_graph_decay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.events.handlers import kg_decay_tick
    from okto_pulse.core.events.types import KGDeliveryRedriveTick

    calls: list[tuple[object, int]] = []

    class Ledger:
        async def redrive_delivery_debt(
            self,
            context,
            *,
            now: datetime,
            limit: int,
        ) -> DeliveryMaintenanceReceipt:
            assert now.tzinfo is not None
            calls.append((context, limit))
            return DeliveryMaintenanceReceipt(scanned=0)

    async def forbidden_daily_tick(**_kwargs):
        raise AssertionError("redrive continuation repeated graph decay")

    session = object()
    monkeypatch.setattr(kg_decay_tick, "_run_daily_tick", forbidden_daily_tick)
    monkeypatch.setattr(kg_decay_tick, "get_delivery_ledger_port", Ledger)
    event = KGDeliveryRedriveTick(
        board_id="board-card7",
        actor_type="system",
        run_id="redrive-run-1",
        scheduled_at=NOW.isoformat(),
        checkpoint_version=1,
    )

    await kg_decay_tick.KGDeliveryRedriveTickHandler().handle(event, session)

    assert calls == [(session, kg_decay_tick.KG_DELIVERY_REDRIVE_LIMIT)]


@pytest.mark.asyncio
async def test_dedicated_redrive_handler_fails_closed_without_ledger_port(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.events.handlers import kg_decay_tick
    from okto_pulse.core.events.types import KGDeliveryRedriveTick

    def missing_port():
        raise RuntimeError("delivery_ledger_port_not_configured")

    monkeypatch.setattr(kg_decay_tick, "get_delivery_ledger_port", missing_port)
    event = KGDeliveryRedriveTick(
        board_id="board-card7",
        actor_type="system",
        run_id="redrive-run-missing-port",
        scheduled_at=NOW.isoformat(),
        checkpoint_version=3,
    )

    with pytest.raises(
        kg_decay_tick.DeliveryMaintenanceFailed,
        match="delivery_redrive_port_unavailable",
    ):
        await kg_decay_tick.KGDeliveryRedriveTickHandler().handle(
            event,
            object(),
        )


@pytest.mark.asyncio
async def test_tick_skips_delivery_maintenance_only_when_port_is_not_registered(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.events.handlers import kg_decay_tick

    async def graph_ok(_fn, _board_id, *_args, **_kwargs):
        return (0, 0)

    async def ignore_tick_run(*_args, **_kwargs) -> None:
        return None

    def missing_port():
        raise RuntimeError("delivery_ledger_port_not_configured")

    monkeypatch.setattr(kg_decay_tick.asyncio, "to_thread", graph_ok)
    monkeypatch.setattr(kg_decay_tick, "_persist_tick_run", ignore_tick_run)
    monkeypatch.setattr(kg_decay_tick, "get_delivery_ledger_port", missing_port)

    summary = await kg_decay_tick._run_daily_tick(
        tick_id="tick-legacy-runtime",
        session=object(),
        board_id="board-legacy",
    )

    assert summary["boards_processed"] == 1
    assert summary["boards_failed"] == 0


@pytest.mark.asyncio
async def test_delivery_maintenance_failure_escapes_handler_for_uow_rollback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.events.handlers import kg_decay_tick
    from okto_pulse.core.events.types import KGDailyTick

    persist_calls = 0

    async def fail_maintenance_tick(**_kwargs):
        raise kg_decay_tick.DeliveryMaintenanceFailed(
            "delivery_maintenance_failed:board-card7"
        )

    async def forbidden_persist(*_args, **_kwargs) -> None:
        nonlocal persist_calls
        persist_calls += 1

    monkeypatch.setattr(kg_decay_tick, "_run_daily_tick", fail_maintenance_tick)
    monkeypatch.setattr(kg_decay_tick, "_persist_tick_run", forbidden_persist)
    event = KGDailyTick(
        tick_id="tick-card7-rollback",
        scheduled_at=NOW.isoformat(),
        board_id="board-card7",
        actor_id=None,
        actor_type="system",
    )

    with pytest.raises(kg_decay_tick.DeliveryMaintenanceFailed):
        await kg_decay_tick.KGDailyTickHandler().handle(event, object())

    assert persist_calls == 0


def _outbox_row(
    row_id: str,
    *,
    event_id: str | None = None,
    payload: dict[str, object] | None = None,
    retry_count: int = -1,
) -> GlobalOutboxEventRecord:
    return GlobalOutboxEventRecord(
        id=row_id,
        event_id=event_id or f"legacy:{row_id}",
        board_id="board-card7",
        session_id=None,
        payload=dict(payload or {}),
        retry_count=retry_count,
        last_error="graph_unavailable: bad WAL",
        processed_at=None,
        created_at=NOW + timedelta(seconds=len(row_id)),
    )


def _governed_row(row_id: str = "governed") -> GlobalOutboxEventRecord:
    envelope = DeliveryAttemptEnvelope(
        board_id="board-card7",
        artifact_type="story",
        artifact_id="story-card7",
        generation=2,
        delete_event_id="delete-card7",
        attempt=1,
    )
    row = _outbox_row(
        row_id,
        event_id=envelope.attempt_event_key,
        payload=dict(envelope.payload),
    )
    row.session_id = envelope.outbox_session_id
    return row


class _RunbookStore:
    def __init__(self, rows: list[GlobalOutboxEventRecord]) -> None:
        self.rows = {row.id: row for row in rows}
        self.requeued: list[GlobalOutboxEventRecord] = []

    async def get_events_by_ids(
        self,
        _context,
        *,
        ids: tuple[str, ...],
    ) -> tuple[GlobalOutboxEventRecord, ...]:
        return tuple(self.rows[row_id] for row_id in ids if row_id in self.rows)

    async def requeue_terminal_events(self, _context, events) -> None:
        self.requeued.extend(events)


@pytest.mark.asyncio
async def test_manual_dlq_reprocess_rejects_mixed_governed_selection_without_mutation(
) -> None:
    governed = _governed_row()
    legacy = _outbox_row("legacy")
    store = _RunbookStore([governed, legacy])
    before = {
        row.id: (row.retry_count, row.last_error, dict(row.payload))
        for row in (governed, legacy)
    }

    with pytest.raises(GlobalOutboxDeadLetterError) as raised:
        await GlobalOutboxDeadLetterOperations(store=store).reprocess(
            context=object(),
            dead_letter_ids=[legacy.id, governed.id],
            reason="operator requested replay",
        )

    assert raised.value.code == "governed_delivery_attempt_tick_owned"
    assert raised.value.mutated is False
    assert raised.value.detail == {
        "rejected_count": 1,
        "owner": "kg.tick.daily",
    }
    assert store.requeued == []
    assert {
        row.id: (row.retry_count, row.last_error, dict(row.payload))
        for row in (governed, legacy)
    } == before


class _RecoveryStore:
    def __init__(self, rows: list[GlobalOutboxEventRecord]) -> None:
        self.rows = rows
        self.saved: list[GlobalOutboxEventRecord] = []
        self.list_calls = 0

    async def list_dead_letters(self, _context, **_kwargs):
        self.list_calls += 1
        return tuple(self.rows) if self.list_calls == 1 else ()

    async def save_events(self, _context, events) -> None:
        self.saved.extend(events)


@pytest.mark.asyncio
async def test_global_recovery_defers_governed_attempt_and_requeues_eligible_legacy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.application.kg_operations import CoreKnowledgeGraphOperations
    from okto_pulse.core.kg import canonical_demotion_global_sync as sync_module
    from okto_pulse.core.ports import global_outbox as outbox_module

    governed = _governed_row("governed-recovery")
    legacy = _outbox_row("legacy-recovery")
    store = _RecoveryStore([governed, legacy])
    enqueued: list[str] = []

    async def enqueue(_context, *, board_id, **_kwargs):
        enqueued.append(board_id)
        return {"enqueued": True}

    monkeypatch.setattr(outbox_module, "get_global_outbox_store", lambda: store)
    monkeypatch.setattr(sync_module, "enqueue_digest_layer_reconciliation", enqueue)

    result = await CoreKnowledgeGraphOperations(
        object()
    ).recover_global_discovery_delivery(
        run_id="recovery-card7",
        board_ids=["board-card7"],
        dead_letter_limit=50,
    )

    assert result["dead_letters_requeued"] == 1
    assert result["governed_delivery_attempts_deferred"] == 1
    assert [row.id for row in store.saved] == [legacy.id]
    assert legacy.retry_count == 0 and legacy.last_error is None
    assert governed.retry_count == -1
    assert governed.last_error == "graph_unavailable: bad WAL"
    assert enqueued == ["board-card7"]
