"""EventDispatcher — asyncio worker that drains handler executions.

Polls domain_event_handler_executions for status='pending' rows whose
next_attempt_at has elapsed, reconstructs the DomainEvent subclass from
the paired domain_events row, and invokes the handler. Each execution is
processed in its own session/commit so handler A failing does NOT affect
handler B (handler isolation).

Retries use exponential backoff capped at 5 minutes. After MAX_ATTEMPTS
unsuccessful attempts the row is moved to status='dlq' for manual replay.
Startup recovery resets any orphaned 'processing' rows to 'pending' —
covers the case where the process crashed mid-dispatch.

Startup order (enforced by core/app.py lifespan):
    init_db → register handlers → dispatcher.start → consolidation_worker.start
Shutdown is reverse.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import async_sessionmaker

from okto_pulse.core.events import bus as _bus_module
from okto_pulse.core.events.types import DomainEvent, resolve_event_class
from okto_pulse.core.models.db import DomainEventHandlerExecution, DomainEventRow

logger = logging.getLogger(__name__)


MAX_ATTEMPTS = 5
BACKOFF_BASE = 2
BACKOFF_CAP_SECONDS = 300
DRAIN_BATCH_SIZE = 50
POLL_INTERVAL_SECONDS = 5.0


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _event_from_row(row: DomainEventRow) -> DomainEvent:
    """Reconstruct the DomainEvent subclass instance from a DB row."""
    cls = resolve_event_class(row.event_type)
    if cls is None:
        raise ValueError(f"Unknown event_type: {row.event_type}")
    payload = row.payload_json if isinstance(row.payload_json, dict) else {}
    return cls(
        event_id=row.id,
        board_id=row.board_id,
        actor_id=row.actor_id,
        actor_type=row.actor_type,
        occurred_at=row.occurred_at,
        **payload,
    )


class EventDispatcher:
    """Background asyncio worker that drains pending handler executions.

    Owns its own asyncio.Event so the loop binding is always correct.
    Publishers reach it via core.events.dispatcher.get_dispatcher() so the
    bus module doesn't need to hold a loop-bound singleton at import time.
    """

    def __init__(self, session_factory: async_sessionmaker) -> None:
        self._session_factory = session_factory
        self._task: asyncio.Task | None = None
        self._running = False
        self._wake_event: asyncio.Event | None = None

    def notify(self) -> None:
        """Signal the wake Event so the drain loop resumes immediately."""
        if self._wake_event is not None:
            try:
                self._wake_event.set()
            except RuntimeError:
                # Loop differs (cross-loop publisher). Poll fallback covers it.
                pass

    async def start(self) -> None:
        """Reset orphans and launch the drain loop as an asyncio task."""
        # Create the Event now so it binds to the current running loop.
        self._wake_event = asyncio.Event()

        # Recovery: any 'processing' row from a previous crashed run is
        # orphaned. Mark it pending so the next drain picks it up.
        async with self._session_factory() as session:
            await session.execute(
                update(DomainEventHandlerExecution)
                .where(DomainEventHandlerExecution.status == "processing")
                .values(status="pending", next_attempt_at=None)
            )
            await session.commit()

        self._running = True
        self._task = asyncio.create_task(self._loop(), name="event_dispatcher")
        # Register self so the bus can signal us.
        set_dispatcher(self)
        logger.info("EventDispatcher started")

    async def stop(self, timeout: float = 5.0) -> None:
        """Cancel the drain loop and wait for it to finish."""
        self._running = False
        # Unblock the poll fallback so cancellation is immediate.
        self.notify()

        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await asyncio.wait_for(self._task, timeout=timeout)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass

        # De-register so stale publishers don't keep signaling a dead task.
        if get_dispatcher() is self:
            set_dispatcher(None)
        self._wake_event = None
        logger.info("EventDispatcher stopped")

    async def _loop(self) -> None:
        while self._running:
            try:
                await self._drain_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Dispatcher drain error — continuing")

            wake = self._wake_event
            if wake is None:
                return
            try:
                await asyncio.wait_for(wake.wait(), timeout=POLL_INTERVAL_SECONDS)
                wake.clear()
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                raise

    async def _drain_once(self) -> None:
        """Fetch up to DRAIN_BATCH_SIZE ready executions and process each."""
        now = _utcnow()
        async with self._session_factory() as session:
            result = await session.execute(
                select(
                    DomainEventHandlerExecution.id,
                    DomainEventHandlerExecution.event_id,
                )
                .join(
                    DomainEventRow,
                    DomainEventRow.id == DomainEventHandlerExecution.event_id,
                )
                .where(DomainEventHandlerExecution.status == "pending")
                .where(
                    (DomainEventHandlerExecution.next_attempt_at.is_(None))
                    | (DomainEventHandlerExecution.next_attempt_at <= now)
                )
                .order_by(
                    DomainEventRow.occurred_at.asc(),
                    DomainEventRow.id.asc(),
                )
                .limit(DRAIN_BATCH_SIZE)
            )
            pairs = result.all()

        for execution_id, event_id in pairs:
            if not self._running:
                break
            await self._process_one(execution_id, event_id)

    async def _process_one(self, execution_id: str, event_id: str) -> None:
        """Process a single execution with per-handler transaction isolation."""
        async with self._session_factory() as session:
            execution = await session.get(DomainEventHandlerExecution, execution_id)
            if execution is None or execution.status != "pending":
                return  # Raced by another drain (or transitioned already).

            # Claim: mark processing and bump attempts.
            execution.status = "processing"
            execution.attempts = (execution.attempts or 0) + 1
            await session.commit()

            event_row = await session.get(DomainEventRow, event_id)
            if event_row is None:
                # Foreign key CASCADE should prevent this; treat defensively.
                execution.status = "dlq"
                execution.last_error = "event row missing"
                execution.processed_at = _utcnow()
                await session.commit()
                return

            try:
                event = _event_from_row(event_row)
                handler_cls = self._resolve_handler(
                    execution.handler_name, event.event_type
                )
                handler = handler_cls()
                await handler.handle(event, session)
                execution.status = "done"
                execution.processed_at = _utcnow()
                await session.commit()
            except Exception as exc:  # noqa: BLE001 — handler isolation
                # Rollback any partial handler writes before mutating the
                # execution row (distinct transaction-level rollback, then
                # re-read the row to update status cleanly).
                await session.rollback()
                fresh = await session.get(DomainEventHandlerExecution, execution_id)
                if fresh is None:
                    return
                fresh.last_error = str(exc)[:500]
                if fresh.attempts >= MAX_ATTEMPTS:
                    fresh.status = "dlq"
                    fresh.processed_at = _utcnow()
                    logger.error(
                        "Handler %s → DLQ after %d attempts (event_id=%s): %s",
                        fresh.handler_name, fresh.attempts, event_id, exc,
                    )
                else:
                    delay = min(BACKOFF_BASE ** fresh.attempts, BACKOFF_CAP_SECONDS)
                    fresh.status = "pending"
                    fresh.next_attempt_at = _utcnow() + timedelta(seconds=delay)
                    logger.warning(
                        "Handler %s failed attempt %d; retry in %ds (event_id=%s): %s",
                        fresh.handler_name, fresh.attempts, delay, event_id, exc,
                    )
                await session.commit()

    def _resolve_handler(self, handler_name: str, event_type: str) -> type:
        handlers = _bus_module._registry.get(event_type, [])
        for h in handlers:
            if h.__name__ == handler_name:
                return h
        raise RuntimeError(
            f"Handler {handler_name} not registered for {event_type}"
        )


# Singleton wiring — actual instance created in core/app.py lifespan once
# the session factory is available. This module-level reference is here
# only as a convenience; do not set it directly outside of app startup.
_dispatcher: EventDispatcher | None = None


def get_dispatcher() -> EventDispatcher | None:
    return _dispatcher


def set_dispatcher(dispatcher: EventDispatcher | None) -> None:
    global _dispatcher
    _dispatcher = dispatcher


__all__ = [
    "EventDispatcher",
    "MAX_ATTEMPTS",
    "BACKOFF_BASE",
    "BACKOFF_CAP_SECONDS",
    "DRAIN_BATCH_SIZE",
    "POLL_INTERVAL_SECONDS",
    "get_dispatcher",
    "set_dispatcher",
]
