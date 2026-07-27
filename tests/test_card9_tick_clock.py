"""Card 9 deterministic clock coverage for takedown delivery maintenance."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from okto_pulse.core.events.handlers import kg_decay_tick
from okto_pulse.core.events.types import KGDailyTick, KGDeliveryRedriveTick
from okto_pulse.core.ports.delivery_ledger import DeliveryMaintenanceReceipt
from okto_pulse.core.ports.takedown_telemetry import (
    TakedownAggregates,
    TakedownSloEvaluation,
)


BOUNDARY = datetime(2026, 7, 21, 18, 0, tzinfo=timezone.utc)


class _FixedClock:
    def __init__(self, now: datetime = BOUNDARY) -> None:
        self.value = now
        self.calls = 0

    def now(self) -> datetime:
        self.calls += 1
        return self.value


class _Ledger:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object, datetime, int]] = []

    async def reconcile_orphaned_attempts(
        self,
        context: object,
        *,
        board_id: str,
        now: datetime,
        limit: int,
    ) -> DeliveryMaintenanceReceipt:
        self.calls.append((f"watchdog:{board_id}", context, now, limit))
        return DeliveryMaintenanceReceipt(scanned=limit)

    async def redrive_delivery_debt(
        self,
        context: object,
        *,
        now: datetime,
        limit: int,
    ) -> DeliveryMaintenanceReceipt:
        self.calls.append(("redrive", context, now, limit))
        return DeliveryMaintenanceReceipt(scanned=limit)


class _Monitor:
    def __init__(self, aggregates: TakedownAggregates) -> None:
        self.aggregates = aggregates
        self.calls: list[tuple[object, str, datetime, str]] = []

    async def evaluate_takedown_slo(
        self,
        context: object,
        *,
        board_id: str,
        now: datetime,
        transaction_state: str,
    ) -> TakedownSloEvaluation:
        self.calls.append((context, board_id, now, transaction_state))
        return TakedownSloEvaluation(
            board_id=board_id,
            observed_at=now,
            transaction_state=transaction_state,
            aggregates=self.aggregates,
        )


def _aggregates(*, samples: int = 1, p95: float = 120.0) -> TakedownAggregates:
    return TakedownAggregates(
        delivery_debt_backlog=0,
        oldest_debt_age_seconds=None,
        circuit_breaker_state="closed",
        circuit_breaker_reason="global_outbox_terminal_backlog_absent",
        p95_seconds_1h=p95,
        p95_sample_count=samples,
    )


@pytest.mark.asyncio
async def test_delivery_maintenance_uses_one_controlled_boundary_and_limits(
) -> None:
    clock = _FixedClock()
    ledger = _Ledger()
    session = object()

    await kg_decay_tick._run_delivery_maintenance(
        port=ledger,
        session=session,
        board_id="board-card9",
        watchdog_limit=7,
        redrive_limit=11,
        clock=clock,
    )

    assert clock.calls == 1
    assert ledger.calls == [
        ("watchdog:board-card9", session, BOUNDARY, 7),
        ("redrive", session, BOUNDARY, 11),
    ]


@pytest.mark.asyncio
async def test_daily_tick_propagates_controlled_clock_and_exact_cutoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _FixedClock()
    ledger = _Ledger()
    monitor = _Monitor(_aggregates())
    captured: dict[str, object] = {}

    async def process_board(_operation, board_id, cutoff, *, batch_size):
        captured["board"] = board_id
        captured["cutoff"] = cutoff
        captured["batch_size"] = batch_size
        return (0, 0)

    async def persist_tick_run(_session, **values) -> None:
        captured["persisted"] = values

    monkeypatch.setattr(kg_decay_tick.asyncio, "to_thread", process_board)
    monkeypatch.setattr(kg_decay_tick, "_persist_tick_run", persist_tick_run)
    monkeypatch.setattr(
        kg_decay_tick,
        "_optional_delivery_ledger_port",
        lambda: ledger,
    )
    monkeypatch.setattr(
        kg_decay_tick,
        "_optional_stale_sweep_port",
        lambda: None,
    )
    monkeypatch.setattr(
        kg_decay_tick,
        "_optional_takedown_telemetry_port",
        lambda: monitor,
    )

    summary = await kg_decay_tick._run_daily_tick(
        tick_id="tick-card9-clock",
        session="session-card9",
        board_id="board-card9",
        batch_size=13,
        staleness_days=7,
        delivery_watchdog_limit=17,
        delivery_redrive_limit=19,
        clock=clock,
    )

    assert captured["board"] == "board-card9"
    assert captured["batch_size"] == 13
    assert captured["cutoff"] == (
        BOUNDARY - timedelta(days=7)
    ).isoformat()
    assert ledger.calls == [
        ("watchdog:board-card9", "session-card9", BOUNDARY, 17),
        ("redrive", "session-card9", BOUNDARY, 19),
    ]
    assert monitor.calls == [
        (
            "session-card9",
            "board-card9",
            BOUNDARY,
            "pending_caller_commit",
        )
    ]
    persisted = captured["persisted"]
    assert isinstance(persisted, dict)
    assert persisted["started_at"] == BOUNDARY
    assert persisted["completed_at"] == BOUNDARY
    assert persisted["duration_ms"] == 0.0
    assert summary["duration_ms"] == 0.0


@pytest.mark.asyncio
async def test_registered_handlers_keep_no_arg_construction_and_accept_clock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    daily_default = kg_decay_tick.KGDailyTickHandler()
    redrive_default = kg_decay_tick.KGDeliveryRedriveTickHandler()
    assert daily_default._clock is None
    assert redrive_default._clock is None

    clock = _FixedClock()
    observed: dict[str, object] = {}

    async def run_daily_tick(**kwargs) -> dict[str, object]:
        observed.update(kwargs)
        return {}

    monkeypatch.setattr(kg_decay_tick, "_run_daily_tick", run_daily_tick)
    event = KGDailyTick(
        tick_id="tick-card9-handler",
        scheduled_at=BOUNDARY.isoformat(),
        board_id="board-card9",
        actor_id=None,
        actor_type="system",
    )

    await kg_decay_tick.KGDailyTickHandler(clock=clock).handle(event, object())

    assert observed["clock"] is clock
    assert clock.calls == 1


@pytest.mark.asyncio
async def test_redrive_handler_uses_injected_clock_at_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _FixedClock()
    ledger = _Ledger()
    monitor = _Monitor(_aggregates())
    monkeypatch.setattr(
        kg_decay_tick,
        "_optional_delivery_ledger_port",
        lambda: ledger,
    )
    monkeypatch.setattr(
        kg_decay_tick,
        "_optional_takedown_telemetry_port",
        lambda: monitor,
    )
    event = KGDeliveryRedriveTick(
        board_id="board-card9",
        actor_type="system",
        run_id="redrive-card9-clock",
        scheduled_at=BOUNDARY.isoformat(),
        checkpoint_version=1,
    )

    await kg_decay_tick.KGDeliveryRedriveTickHandler(clock=clock).handle(
        event,
        "session-card9",
    )

    assert ledger.calls == [
        (
            "redrive",
            "session-card9",
            BOUNDARY,
            kg_decay_tick.KG_DELIVERY_REDRIVE_LIMIT,
        )
    ]
    assert monitor.calls == [
        (
            "session-card9",
            "board-card9",
            BOUNDARY,
            "pending_caller_commit",
        )
    ]
    assert clock.calls == 1


@pytest.mark.asyncio
async def test_tick_publication_uses_injected_clock_when_timestamp_is_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core import events

    clock = _FixedClock()
    published: list[KGDailyTick] = []

    async def capture(event, session) -> None:
        assert session == "session-card9"
        published.append(event)

    monkeypatch.setattr(events, "publish", capture)

    await kg_decay_tick.publish_tick_events(
        "session-card9",
        board_id="board-card9",
        clock=clock,
    )

    assert len(published) == 1
    assert published[0].scheduled_at == BOUNDARY.isoformat()
    assert clock.calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("aggregates", "expected_status", "expected_event"),
    (
        (
            _aggregates(samples=1, p95=120.0),
            "within_slo",
            "kg.takedown.slo_evaluated",
        ),
        (
            _aggregates(samples=0, p95=0.0),
            "insufficient_data",
            "kg.takedown.slo_evaluation_insufficient_data",
        ),
    ),
    ids=("within_slo", "empty_window_is_not_health"),
)
async def test_tick_slo_evaluation_distinguishes_empty_window_from_health(
    aggregates: TakedownAggregates,
    expected_status: str,
    expected_event: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monitor = _Monitor(aggregates)

    with caplog.at_level("INFO", logger=kg_decay_tick.__name__):
        evaluation = await kg_decay_tick._evaluate_takedown_slo(
            port=monitor,
            session="session-card9",
            board_id="board-card9",
            now=BOUNDARY,
            transaction_state="pending_caller_commit",
            correlation_id="tick-card9-monitor",
            correlation_kind="kg.tick.daily",
        )

    assert evaluation is not None
    assert evaluation.status.value == expected_status
    record = next(
        item
        for item in caplog.records
        if getattr(item, "event", None) == expected_event
    )
    assert record.monitor_status == "evaluated"
    assert record.observed_at == BOUNDARY.isoformat()
    assert record.correlation_id == "tick-card9-monitor"
    assert record.correlation_kind == "kg.tick.daily"


@pytest.mark.asyncio
async def test_tick_slo_monitor_failure_is_structured_and_non_fatal(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class _BrokenMonitor:
        async def evaluate_takedown_slo(self, *_args, **_kwargs):
            raise RuntimeError("aggregate probe unavailable")

    with caplog.at_level("ERROR", logger=kg_decay_tick.__name__):
        result = await kg_decay_tick._evaluate_takedown_slo(
            port=_BrokenMonitor(),  # type: ignore[arg-type]
            session="session-card9",
            board_id="board-card9",
            now=BOUNDARY,
            transaction_state="pending_caller_commit",
            correlation_id="redrive-card9-monitor",
            correlation_kind="kg.tick.delivery_redrive",
        )

    assert result is None
    record = next(
        item
        for item in caplog.records
        if getattr(item, "event", None) == "kg.takedown.slo_evaluation_failed"
    )
    assert record.monitor_status == "failed"
    assert record.error_class == "RuntimeError"
    assert record.transaction_state == "pending_caller_commit"
    assert record.correlation_id == "redrive-card9-monitor"
    assert record.correlation_kind == "kg.tick.delivery_redrive"


@pytest.mark.asyncio
async def test_tick_slo_monitor_rejects_mismatched_evaluation_identity(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class _MismatchedMonitor:
        async def evaluate_takedown_slo(
            self,
            _context,
            *,
            board_id,
            now,
            transaction_state,
        ):
            del board_id, now, transaction_state
            return TakedownSloEvaluation(
                board_id="other-board",
                observed_at=BOUNDARY + timedelta(seconds=1),
                transaction_state="committed",
                aggregates=_aggregates(),
            )

    with caplog.at_level("ERROR", logger=kg_decay_tick.__name__):
        result = await kg_decay_tick._evaluate_takedown_slo(
            port=_MismatchedMonitor(),  # type: ignore[arg-type]
            session="session-card9",
            board_id="board-card9",
            now=BOUNDARY,
            transaction_state="pending_caller_commit",
            correlation_id="tick-card9-mismatch",
            correlation_kind="kg.tick.daily",
        )

    assert result is None
    record = next(
        item
        for item in caplog.records
        if getattr(item, "event", None) == "kg.takedown.slo_evaluation_failed"
    )
    assert record.error == "takedown_slo_evaluation_identity_mismatch"
    assert record.board_id == "board-card9"
    assert record.observed_at == BOUNDARY.isoformat()
    assert record.correlation_id == "tick-card9-mismatch"
