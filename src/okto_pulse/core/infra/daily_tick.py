"""Daily KG tick runtime helpers.

Kept out of ``core.app`` so edition lifespans can schedule the same observable
tick without importing private app helpers.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import Any

logger = logging.getLogger(__name__)


def tick_next_run_from_last(
    last_completed_at: datetime | None,
    interval_minutes: int,
    now: datetime,
) -> datetime:
    """Return the next tick run honoring persisted history."""

    floor = now + timedelta(seconds=120)
    if last_completed_at is None:
        return floor
    if last_completed_at.tzinfo is None:
        last_completed_at = last_completed_at.replace(tzinfo=timezone.utc)
    due = last_completed_at + timedelta(minutes=interval_minutes)
    return max(due, floor)


async def compute_tick_catch_up_next_run(
    interval_minutes: int,
    *,
    session_factory_provider: Callable[[], Any] | None = None,
    now_provider: Callable[[], datetime] | None = None,
) -> datetime | None:
    """Read the last persisted tick and return the scheduler next_run_time."""

    from sqlalchemy import Column, DateTime, MetaData, Table, select

    from okto_pulse.core.infra.database import get_session_factory

    kg_tick_runs = Table(
        "kg_tick_runs",
        MetaData(),
        Column("completed_at", DateTime(timezone=True)),
    )

    factory = (
        session_factory_provider()
        if session_factory_provider is not None
        else get_session_factory()
    )
    async with factory() as session:
        last = (
            await session.execute(
                select(kg_tick_runs.c.completed_at)
                .where(kg_tick_runs.c.completed_at.is_not(None))
                .order_by(kg_tick_runs.c.completed_at.desc())
                .limit(1)
            )
        ).scalars().first()
    now = now_provider() if now_provider is not None else datetime.now(timezone.utc)
    return tick_next_run_from_last(last, interval_minutes, now)


async def emit_daily_tick(
    *,
    session_factory_provider: Callable[[], Any] | None = None,
) -> None:
    """Emit KGDailyTick events when this replica owns the in-process lock."""

    from okto_pulse.core.infra.database import get_session_factory
    from okto_pulse.core.kg.workers.advisory_lock import get_async_lock

    lock = get_async_lock("kg_daily_tick", "global")
    if lock.locked():
        logger.info(
            "kg.tick.skipped reason=non_leader",
            extra={"event": "kg.tick.skipped", "reason": "non_leader"},
        )
        return
    async with lock:
        try:
            factory = (
                session_factory_provider()
                if session_factory_provider is not None
                else get_session_factory()
            )
        except AssertionError:
            logger.warning(
                "kg.tick.no_session_factory",
                extra={"event": "kg.tick.no_session_factory"},
            )
            return
        scheduled_at = datetime.now(timezone.utc).isoformat()
        try:
            from okto_pulse.core.events.handlers.kg_decay_tick import (
                publish_tick_events,
            )

            async with factory() as session:
                tick_ids = await publish_tick_events(
                    session, scheduled_at=scheduled_at,
                )
                await session.commit()
            logger.info(
                "kg.tick.emitted",
                extra={
                    "event": "kg.tick.emitted",
                    "tick_count": len(tick_ids),
                    "scheduled_at": scheduled_at,
                },
            )
        except Exception as exc:
            logger.error(
                "kg.tick.emit_failed err=%s",
                exc,
                extra={"event": "kg.tick.emit_failed", "error": str(exc)},
            )


__all__ = [
    "compute_tick_catch_up_next_run",
    "emit_daily_tick",
    "tick_next_run_from_last",
]
