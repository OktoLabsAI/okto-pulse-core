from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime, timedelta, timezone
from threading import Event
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from okto_pulse.core.application import domain_event_delivery
from okto_pulse.core.application import kg_tick
from okto_pulse.core.application.domain_event_delivery import (
    DomainEventDeliveryProcessor,
)
from okto_pulse.core.events import types as event_types
from okto_pulse.core.events.handlers import kg_decay_tick
from okto_pulse.core.kg import schema_contract, write_barrier
from okto_pulse.core.kg.interfaces import registry as registry_module
from okto_pulse.core.kg.interfaces.graph_lifecycle import GraphLifecycleStepResult
from okto_pulse.core.kg.single_writer_lock import KGSingleWriterLock
from okto_pulse.core.ports.domain_event_delivery import (
    DomainEventExecution,
    DomainEventFailure,
    StoredDomainEvent,
)


pytestmark = pytest.mark.asyncio


class _GuardPort:
    def __init__(
        self,
        *,
        active: bool = False,
        read_error: BaseException | None = None,
    ) -> None:
        self.active = active
        self.read_error = read_error
        self.list_calls = 0

    async def is_global_recovery_active(self, _session: object) -> bool:
        if self.read_error is not None:
            raise self.read_error
        return self.active

    async def fence_kg_tick_publication(self, session: object) -> bool:
        return await self.is_global_recovery_active(session)

    async def list_board_ids(self, _session: object) -> list[str]:
        self.list_calls += 1
        return ["board-terminal-resume"]


class _ResetScope:
    def __init__(self, transaction: "_ResetTransaction", board_id: str) -> None:
        self._transaction = transaction
        self._board_id = board_id

    async def __aenter__(self) -> "_ResetScope":
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    def execute(self, statement: str) -> None:
        self._transaction.statements.append((self._board_id, statement))
        failure = self._transaction.failures.pop(
            (self._board_id, "statement"),
            None,
        )
        if failure is not None:
            raise failure


class _ResetTransaction:
    def __init__(
        self,
        failures: dict[tuple[str, str], Exception] | None = None,
    ) -> None:
        self.failures = dict(failures or {})
        self.opens: list[str] = []
        self.statements: list[tuple[str, str]] = []

    async def begin(self, board_id: str) -> _ResetScope:
        self.opens.append(board_id)
        failure = self.failures.pop((board_id, "open"), None)
        if failure is not None:
            raise failure
        return _ResetScope(self, board_id)


class _ResetGraphLifecycle:
    def __init__(self) -> None:
        self.steps: list[tuple[str, str, str]] = []

    def apply_step(
        self,
        board_id: str,
        graph_type: str,
        step: str,
    ) -> GraphLifecycleStepResult:
        self.steps.append((board_id, graph_type, step))
        return GraphLifecycleStepResult(ok=True)


class _TickDeliveryStore:
    def __init__(self, *, event_type: str, force_full_rebuild: bool) -> None:
        self.now = datetime(2026, 7, 23, 18, 0, tzinfo=timezone.utc)
        self.status = "pending"
        self.attempts = 0
        self.next_attempt_at: datetime | None = None
        self.processed_at: datetime | None = None
        self.failure: DomainEventFailure | None = None
        self.invocations = 0
        self.event = StoredDomainEvent(
            event_id="event-full-rebuild",
            event_type=event_type,
            board_id="board-force",
            actor_id="agent-test",
            actor_type="agent",
            occurred_at=self.now,
            payload={
                "tick_id": "tick-delivery",
                "scheduled_at": self.now.isoformat(),
                "force_full_rebuild": force_full_rebuild,
            },
        )

    async def recover_orphans(self) -> int:
        return 0

    async def claim_ready(
        self,
        *,
        limit: int,
        now: datetime,
    ) -> list[tuple[str, str]]:
        assert limit == 50
        if (
            self.status == "pending"
            and (self.next_attempt_at is None or self.next_attempt_at <= now)
        ):
            return [("execution-full-rebuild", self.event.event_id)]
        return []

    async def begin_attempt(
        self,
        execution_id: str,
    ) -> DomainEventExecution | None:
        if execution_id != "execution-full-rebuild" or self.status != "pending":
            return None
        self.status = "processing"
        self.attempts += 1
        return DomainEventExecution(
            execution_id,
            self.event.event_id,
            "KGDailyTickHandler",
            self.attempts,
        )

    async def load_event(self, event_id: str) -> StoredDomainEvent | None:
        return self.event if event_id == self.event.event_id else None

    async def invoke_handler(
        self,
        execution_id: str,
        handler: type,
        event: object,
        *,
        processed_at: datetime,
    ) -> None:
        assert execution_id == "execution-full-rebuild"
        self.invocations += 1
        await handler().handle(event, object())
        self.status = "done"
        self.failure = None
        self.next_attempt_at = None
        self.processed_at = processed_at

    async def mark_event_missing(
        self,
        execution_id: str,
        *,
        processed_at: datetime,
    ) -> None:
        del execution_id
        self.status = "dlq"
        self.processed_at = processed_at

    async def mark_failed(
        self,
        execution_id: str,
        failure: DomainEventFailure,
    ) -> None:
        assert execution_id == "execution-full-rebuild"
        self.failure = failure
        self.status = "dlq" if failure.terminal else "pending"
        self.next_attempt_at = failure.next_attempt_at
        self.processed_at = failure.processed_at


def _install_reset_graph(
    monkeypatch: pytest.MonkeyPatch,
    transaction: _ResetTransaction,
) -> _ResetGraphLifecycle:
    lifecycle = _ResetGraphLifecycle()
    monkeypatch.setattr(
        registry_module,
        "get_kg_registry",
        lambda: SimpleNamespace(
            graph_transaction=transaction,
            graph_lifecycle=lifecycle,
        ),
    )
    monkeypatch.setattr(
        schema_contract,
        "VECTOR_INDEX_TYPES",
        ("Decision", "Learning"),
    )
    return lifecycle


async def test_active_recovery_defers_before_event_publication(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    port = _GuardPort(active=True)
    published: list[object] = []

    async def capture(event: object, session: object) -> None:
        del session
        published.append(event)

    monkeypatch.setattr(kg_tick, "get_relational_effects_port", lambda: port)
    monkeypatch.setattr(kg_decay_tick, "get_relational_effects_port", lambda: port)
    monkeypatch.setattr("okto_pulse.core.events.publish", capture)

    with pytest.raises(kg_tick.KGTickAdmissionDeferred) as deferred:
        await kg_decay_tick.publish_tick_events(object())

    assert deferred.value.reason_code == "global_recovery_active"
    assert deferred.value.retryable is True
    assert port.list_calls == 0
    assert published == []


async def test_recovery_guard_read_error_is_observable_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    port = _GuardPort(read_error=RuntimeError("relational read unavailable"))
    published: list[object] = []

    async def capture(event: object, session: object) -> None:
        del session
        published.append(event)

    monkeypatch.setattr(kg_tick, "get_relational_effects_port", lambda: port)
    monkeypatch.setattr(kg_decay_tick, "get_relational_effects_port", lambda: port)
    monkeypatch.setattr("okto_pulse.core.events.publish", capture)

    with (
        caplog.at_level(logging.WARNING, logger="okto_pulse.application.kg_tick"),
        pytest.raises(kg_tick.KGTickAdmissionDeferred) as deferred,
    ):
        await kg_decay_tick.publish_tick_events(object())

    assert deferred.value.reason_code == "recovery_guard_unavailable"
    assert port.list_calls == 0
    assert published == []
    record = next(
        item for item in caplog.records if item.message.startswith("kg.tick.deferred")
    )
    assert record.event == "kg.tick.deferred"
    assert record.reason == "recovery_guard_unavailable"


async def test_terminal_recovery_resumes_and_carries_rebuild_intent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    port = _GuardPort(active=False)
    published: list[event_types.KGFullRebuildTick] = []

    async def capture(event: object, session: object) -> None:
        del session
        assert isinstance(event, event_types.KGFullRebuildTick)
        published.append(event)

    monkeypatch.setattr(kg_tick, "get_relational_effects_port", lambda: port)
    monkeypatch.setattr(kg_decay_tick, "get_relational_effects_port", lambda: port)
    monkeypatch.setattr("okto_pulse.core.events.publish", capture)

    await kg_decay_tick.publish_tick_events(
        object(),
        force_full_rebuild=True,
    )

    assert port.list_calls == 1
    assert len(published) == 1
    assert published[0].board_id == "board-terminal-resume"
    assert published[0].event_type == "kg.tick.full_rebuild"
    assert published[0].force_full_rebuild is True


async def test_manual_full_rebuild_never_resets_graph_before_durable_event(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    published: list[dict[str, object]] = []

    async def forbidden_reset(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("graph reset must run only in the durable handler")

    async def capture_publish(
        session: object,
        *,
        board_id: str | None = None,
        **kwargs: object,
    ) -> list[str]:
        published.append({"session": session, "board_id": board_id, **kwargs})
        return ["durable-tick"]

    monkeypatch.setattr(kg_tick, "reset_last_recomputed_at", forbidden_reset)
    monkeypatch.setattr(kg_decay_tick, "publish_tick_events", capture_publish)
    session = object()

    await kg_tick.dispatch_manual_tick(
        tick_id="manual-force",
        board_id="board-force",
        force_full_rebuild=True,
        relational_context=session,
    )

    assert published == [
        {
            "session": session,
            "board_id": "board-force",
            "actor_id": "manual-trigger",
            "actor_type": "user",
            "scheduled_at": published[0]["scheduled_at"],
            "force_full_rebuild": True,
            "tick_id": "manual-force",
        }
    ]


async def test_durable_handler_resets_only_after_admission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    order: list[str] = []
    session = object()

    async def admit(context: object, *, trigger: str) -> None:
        assert context is session
        assert trigger == "daily_handler"
        order.append("admitted")

    async def reset(
        board_id: str | None,
        *,
        relational_context: object | None = None,
        mutation_ref: str | None = None,
    ) -> None:
        assert board_id == "board-force"
        assert relational_context is session
        assert mutation_ref == "tick-force"
        order.append("reset")

    async def run(**kwargs: object) -> dict[str, object]:
        assert kwargs["session"] is session
        order.append("tick")
        return {}

    monkeypatch.setattr(kg_tick, "require_kg_tick_admission", admit)
    monkeypatch.setattr(kg_tick, "reset_last_recomputed_at", reset)
    monkeypatch.setattr(kg_decay_tick, "_run_daily_tick", run)

    await kg_decay_tick.KGDailyTickHandler().handle(
        event_types.KGFullRebuildTick(
            board_id="board-force",
            tick_id="tick-force",
            scheduled_at="2026-07-23T12:00:00+00:00",
        ),
        session,
    )

    assert order == ["admitted", "reset", "tick"]


@pytest.mark.parametrize(
    ("failure_site", "error_type", "detail"),
    (
        ("open", MemoryError, "bad allocation"),
        ("open", RuntimeError, "database is locked"),
        ("statement", MemoryError, "bad allocation"),
        ("statement", RuntimeError, "database is locked"),
    ),
    ids=(
        "open-memory-pressure",
        "open-lock-contention",
        "statement-memory-pressure",
        "statement-lock-contention",
    ),
)
async def test_full_rebuild_reset_fails_closed_and_retry_is_idempotent(
    monkeypatch: pytest.MonkeyPatch,
    failure_site: str,
    error_type: type[Exception],
    detail: str,
) -> None:
    transaction = _ResetTransaction(
        {("board-force", failure_site): error_type(detail)}
    )
    _install_reset_graph(monkeypatch, transaction)

    with pytest.raises(kg_tick.KGTickFullRebuildResetFailed) as failed:
        await kg_tick.reset_last_recomputed_at("board-force")

    assert failed.value.code == "kg_tick_full_rebuild_reset_failed"
    assert failed.value.retryable is True
    assert len(failed.value.failures) == 1
    failure = failed.value.failures[0]
    assert failure.board_id == "board-force"
    assert failure.phase == (
        "graph_open" if failure_site == "open" else "statement"
    )
    assert failure.node_type == (
        None if failure_site == "open" else "Decision"
    )
    assert failure.error_type == error_type.__name__
    assert failure.detail == detail

    # The operation is a repeatable SET-to-NULL. A retry reopens the board and
    # safely completes every node type after the transient failure clears.
    await kg_tick.reset_last_recomputed_at("board-force")
    assert transaction.opens == ["board-force", "board-force"]
    assert transaction.statements[-2:] == [
        (
            "board-force",
            "MATCH (n:Decision) SET n.last_recomputed_at = NULL",
        ),
        (
            "board-force",
            "MATCH (n:Learning) SET n.last_recomputed_at = NULL",
        ),
    ]


async def test_full_rebuild_reset_aggregates_board_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transaction = _ResetTransaction(
        {
            ("board-open", "open"): MemoryError("bad allocation"),
            ("board-statement", "statement"): RuntimeError("database is locked"),
        }
    )
    _install_reset_graph(monkeypatch, transaction)

    class _BoardInventory:
        async def list_all_board_ids(
            self,
            _context: object,
            *,
            limit: int,
        ) -> list[str]:
            assert limit == 10_000
            return ["board-open", "board-statement"]

    monkeypatch.setattr(
        kg_tick,
        "get_kg_operational_read_model_port",
        lambda: _BoardInventory(),
    )

    with pytest.raises(kg_tick.KGTickFullRebuildResetFailed) as failed:
        await kg_tick.reset_last_recomputed_at(
            None,
            relational_context=object(),
        )

    assert [(item.board_id, item.phase) for item in failed.value.failures] == [
        ("board-open", "graph_open"),
        ("board-statement", "statement"),
    ]


async def test_reset_failure_aborts_tick_without_persisting_false_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = object()
    reset_failure = kg_tick.KGTickFullRebuildResetFailed(
        (
            kg_tick.KGTickFullRebuildResetFailure(
                board_id="board-force",
                phase="graph_open",
                node_type=None,
                error_type="MemoryError",
                detail="bad allocation",
            ),
        )
    )
    calls: list[str] = []

    async def admit(_context: object, *, trigger: str) -> None:
        assert trigger == "daily_handler"

    async def fail_reset(*_args: object, **_kwargs: object) -> None:
        calls.append("reset")
        raise reset_failure

    async def forbidden_tick(**_kwargs: object) -> dict[str, object]:
        calls.append("tick")
        raise AssertionError("tick must not run after an incomplete reset")

    async def forbidden_run_persist(*_args: object, **_kwargs: object) -> None:
        calls.append("persist")
        raise AssertionError("failed reset must not create a successful/error run")

    monkeypatch.setattr(kg_tick, "require_kg_tick_admission", admit)
    monkeypatch.setattr(kg_tick, "reset_last_recomputed_at", fail_reset)
    monkeypatch.setattr(kg_decay_tick, "_run_daily_tick", forbidden_tick)
    monkeypatch.setattr(kg_decay_tick, "_persist_tick_run", forbidden_run_persist)

    with pytest.raises(kg_tick.KGTickFullRebuildResetFailed) as failed:
        await kg_decay_tick.KGDailyTickHandler().handle(
            event_types.KGFullRebuildTick(
                board_id="board-force",
                tick_id="tick-force-failed",
                scheduled_at="2026-07-23T12:00:00+00:00",
            ),
            session,
        )

    assert failed.value is reset_failure
    assert calls == ["reset"]


async def test_old_daily_tick_payload_defaults_to_no_rebuild() -> None:
    event = event_types.KGDailyTick.model_validate(
        {
            "board_id": "board-old-payload",
            "tick_id": "tick-old-payload",
            "scheduled_at": "2026-07-23T12:00:00+00:00",
        }
    )

    assert event.force_full_rebuild is False


async def test_daily_tick_rejects_forced_rebuild_intent() -> None:
    with pytest.raises(ValidationError):
        event_types.KGDailyTick.model_validate(
            {
                "board_id": "board-invalid-downgrade",
                "tick_id": "tick-invalid-downgrade",
                "scheduled_at": "2026-07-23T12:00:00+00:00",
                "force_full_rebuild": True,
            }
        )


async def test_full_rebuild_cancel_drains_acquire_and_exact_release(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transaction = _ResetTransaction()
    _install_reset_graph(monkeypatch, transaction)
    acquire_started = Event()
    allow_acquire = Event()
    release_started = Event()
    allow_release = Event()
    release_calls: list[tuple[str, str]] = []

    class _CancellationAdversarialWriter:
        def acquire(
            self,
            *,
            board_id: str,
            operation: str,
            owner_id: str,
        ) -> object:
            assert operation == kg_tick.KG_TICK_FULL_REBUILD_OPERATION
            assert owner_id.startswith("kg-tick-full-rebuild:")
            acquire_started.set()
            assert allow_acquire.wait(timeout=2)
            return SimpleNamespace(
                acquired=True,
                owner_token="cancel-safe-owner",
                current_owner=owner_id,
            )

        def is_owner(self, board_id: str, owner_token: str) -> bool:
            return (
                board_id == "board-cancel-safe"
                and owner_token == "cancel-safe-owner"
            )

        def release(self, *, board_id: str, owner_token: str) -> bool:
            release_started.set()
            assert allow_release.wait(timeout=2)
            release_calls.append((board_id, owner_token))
            return True

    monkeypatch.setattr(
        "okto_pulse.core.kg.single_writer_lock.KGSingleWriterLock",
        _CancellationAdversarialWriter,
    )
    task = asyncio.create_task(
        kg_tick.reset_last_recomputed_at(
            "board-cancel-safe",
            mutation_ref="tick-cancel-safe",
        )
    )
    try:
        assert await asyncio.to_thread(acquire_started.wait, 2)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()

        allow_acquire.set()
        assert await asyncio.to_thread(release_started.wait, 2)
        await asyncio.sleep(0)
        assert not task.done()

        allow_release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        allow_acquire.set()
        allow_release.set()
        if not task.done():
            task.cancel()

    assert release_calls == [
        ("board-cancel-safe", "cancel-safe-owner")
    ]
    assert transaction.opens == ["board-cancel-safe"]


async def test_global_full_rebuild_fanout_is_deterministic_and_one_timestamp(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FanoutPort(_GuardPort):
        async def list_board_ids(self, _session: object) -> list[str]:
            self.list_calls += 1
            return ["board-z", "board-a", "board-z"]

    port = _FanoutPort()
    published: list[event_types.KGFullRebuildTick] = []

    async def capture(event: object, session: object) -> None:
        del session
        assert isinstance(event, event_types.KGFullRebuildTick)
        published.append(event)

    monkeypatch.setattr(kg_tick, "get_relational_effects_port", lambda: port)
    monkeypatch.setattr(kg_decay_tick, "get_relational_effects_port", lambda: port)
    monkeypatch.setattr("okto_pulse.core.events.publish", capture)
    scheduled_at = "2026-07-23T18:30:00+00:00"
    root_tick_id = "manual-root-tick"

    tick_ids = await kg_decay_tick.publish_tick_events(
        object(),
        board_id=None,
        tick_id=root_tick_id,
        scheduled_at=scheduled_at,
        force_full_rebuild=True,
    )

    expected = [
        str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"urn:okto-pulse:kg-tick:{root_tick_id}:board:{board_id}",
            )
        )
        for board_id in ("board-a", "board-z")
    ]
    assert tick_ids == expected
    assert [event.tick_id for event in published] == expected
    assert [event.board_id for event in published] == ["board-a", "board-z"]
    assert {event.scheduled_at for event in published} == {scheduled_at}
    assert {
        event.event_type for event in published
    } == {event_types.KGFullRebuildTick.event_type}


async def test_force_full_handler_maps_star_scope_to_global_inventory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reset_calls: list[tuple[str | None, object, str | None]] = []
    session = object()

    async def admit(_context: object, *, trigger: str) -> None:
        assert trigger == "daily_handler"

    async def reset(
        board_id: str | None,
        *,
        relational_context: object | None = None,
        mutation_ref: str | None = None,
    ) -> None:
        reset_calls.append((board_id, relational_context, mutation_ref))

    async def run(**_kwargs: object) -> dict[str, object]:
        return {}

    monkeypatch.setattr(kg_tick, "require_kg_tick_admission", admit)
    monkeypatch.setattr(kg_tick, "reset_last_recomputed_at", reset)
    monkeypatch.setattr(kg_decay_tick, "_run_daily_tick", run)

    await kg_decay_tick.KGDailyTickHandler().handle(
        event_types.KGFullRebuildTick(
            board_id="*",
            tick_id="tick-global-force",
            scheduled_at="2026-07-23T18:30:00+00:00",
        ),
        session,
    )

    assert reset_calls == [(None, session, "tick-global-force")]


async def test_full_rebuild_handler_obeys_strict_safe_write_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transaction = _ResetTransaction()
    lifecycle = _install_reset_graph(monkeypatch, transaction)
    session = object()
    runs: list[str] = []

    async def admit(_context: object, *, trigger: str) -> None:
        assert trigger == "daily_handler"

    async def run(**kwargs: object) -> dict[str, object]:
        runs.append(str(kwargs["tick_id"]))
        return {}

    monkeypatch.setattr(kg_tick, "require_kg_tick_admission", admit)
    monkeypatch.setattr(kg_decay_tick, "_run_daily_tick", run)
    previous_mode = write_barrier.get_barrier_mode()
    write_barrier.set_barrier_mode(write_barrier.BarrierMode.STRICT)
    try:
        await kg_decay_tick.KGDailyTickHandler().handle(
            event_types.KGFullRebuildTick(
                board_id="board-force",
                tick_id="tick-strict-force",
                scheduled_at="2026-07-23T18:30:00+00:00",
            ),
            session,
        )
    finally:
        write_barrier.set_barrier_mode(previous_mode)

    assert transaction.opens == ["board-force"]
    assert transaction.statements == [
        (
            "board-force",
            "MATCH (n:Decision) SET n.last_recomputed_at = NULL",
        ),
        (
            "board-force",
            "MATCH (n:Learning) SET n.last_recomputed_at = NULL",
        ),
    ]
    assert lifecycle.steps == [
        ("board-force", "board_graph", "checkpoint"),
        ("board-force", "board_graph", "flush"),
        ("board-force", "board_graph", "fsync"),
    ]
    assert runs == ["tick-strict-force"]


async def test_full_rebuild_writer_contention_is_typed_retryable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transaction = _ResetTransaction()
    _install_reset_graph(monkeypatch, transaction)
    existing_writer = KGSingleWriterLock()
    acquisition = existing_writer.acquire(
        board_id="board-force",
        operation="other-graph-mutation",
        owner_id="other-process",
    )
    assert acquisition.acquired is True
    assert acquisition.owner_token is not None
    try:
        with pytest.raises(kg_tick.KGTickFullRebuildResetFailed) as failed:
            await kg_tick.reset_last_recomputed_at(
                "board-force",
                mutation_ref="tick-contended",
            )
    finally:
        existing_writer.release(
            board_id="board-force",
            owner_token=acquisition.owner_token,
        )

    assert failed.value.retryable is True
    assert [(item.phase, item.error_type) for item in failed.value.failures] == [
        ("writer_acquire", "SingleWriterLockError"),
    ]
    assert transaction.opens == []


async def test_typed_reset_failure_retries_then_acks_and_cleans_backoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _TickDeliveryStore(
        event_type=event_types.KGFullRebuildTick.event_type,
        force_full_rebuild=True,
    )
    attempts = 0
    tick_runs: list[str] = []

    async def admit(_context: object, *, trigger: str) -> None:
        assert trigger == "daily_handler"

    async def reset(*_args: object, **_kwargs: object) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise kg_tick.KGTickFullRebuildResetFailed(
                (
                    kg_tick.KGTickFullRebuildResetFailure(
                        board_id="board-force",
                        phase="writer_acquire",
                        node_type=None,
                        error_type="SingleWriterLockError",
                        detail="writer busy",
                    ),
                )
            )

    async def run(**kwargs: object) -> dict[str, object]:
        tick_runs.append(str(kwargs["tick_id"]))
        return {}

    monkeypatch.setattr(kg_tick, "require_kg_tick_admission", admit)
    monkeypatch.setattr(kg_tick, "reset_last_recomputed_at", reset)
    monkeypatch.setattr(kg_decay_tick, "_run_daily_tick", run)
    processor = DomainEventDeliveryProcessor(
        store,
        handler_resolver=lambda _handler_name, _event_type: (
            kg_decay_tick.KGDailyTickHandler
        ),
        clock=lambda: store.now,
    )

    assert await processor.process_batch() == 1
    assert store.status == "pending"
    assert store.attempts == 1
    assert store.failure is not None
    assert store.failure.terminal is False
    assert store.failure.processed_at is None
    assert store.failure.next_attempt_at == store.now + timedelta(seconds=2)
    assert tick_runs == []

    store.now = store.failure.next_attempt_at
    assert await processor.process_batch() == 1
    assert store.status == "done"
    assert store.attempts == 2
    assert store.processed_at == store.now
    assert store.next_attempt_at is None
    assert store.failure is None
    assert tick_runs == ["tick-delivery"]


async def test_old_consumer_retries_unknown_full_rebuild_until_new_consumer_adopts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _TickDeliveryStore(
        event_type=event_types.KGFullRebuildTick.event_type,
        force_full_rebuild=True,
    )
    tick_runs: list[str] = []

    async def admit(_context: object, *, trigger: str) -> None:
        assert trigger == "daily_handler"

    async def reset(*_args: object, **_kwargs: object) -> None:
        return None

    async def run(**kwargs: object) -> dict[str, object]:
        tick_runs.append(str(kwargs["tick_id"]))
        return {}

    monkeypatch.setattr(kg_tick, "require_kg_tick_admission", admit)
    monkeypatch.setattr(kg_tick, "reset_last_recomputed_at", reset)
    monkeypatch.setattr(kg_decay_tick, "_run_daily_tick", run)
    monkeypatch.setattr(
        domain_event_delivery,
        "resolve_event_class",
        lambda _event_type: None,
    )
    processor = DomainEventDeliveryProcessor(
        store,
        handler_resolver=lambda _handler_name, _event_type: (
            kg_decay_tick.KGDailyTickHandler
        ),
        clock=lambda: store.now,
    )

    assert await processor.process_batch() == 1
    assert store.status == "pending"
    assert store.attempts == 1
    assert store.invocations == 0
    assert store.failure is not None
    assert store.failure.terminal is False
    assert store.processed_at is None
    assert tick_runs == []

    store.now = store.failure.next_attempt_at
    monkeypatch.setattr(
        domain_event_delivery,
        "resolve_event_class",
        event_types.resolve_event_class,
    )
    assert await processor.process_batch() == 1
    assert store.status == "done"
    assert store.attempts == 2
    assert store.invocations == 1
    assert store.failure is None
    assert store.next_attempt_at is None
    assert tick_runs == ["tick-delivery"]
