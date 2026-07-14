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
    uow_factory_provider: Callable[[], Any] | None = None,
    now_provider: Callable[[], datetime] | None = None,
) -> datetime | None:
    """Read the last persisted tick and return the scheduler next_run_time."""

    from okto_pulse.core.runtime_registry import resolve_unit_of_work_factory

    factory = (
        uow_factory_provider()
        if uow_factory_provider is not None
        else resolve_unit_of_work_factory()
    )
    realm_scope = factory.resolve_realm_scope()
    async with factory(realm_scope=realm_scope) as uow:
        last = await uow.services.read_latest_kg_tick_completed_at()
    now = now_provider() if now_provider is not None else datetime.now(timezone.utc)
    return tick_next_run_from_last(last, interval_minutes, now)


async def emit_daily_tick(
    *,
    uow_factory_provider: Callable[[], Any] | None = None,
) -> None:
    """Emit KGDailyTick events when this replica owns the lease."""

    from okto_pulse.core.runtime_registry import resolve_unit_of_work_factory
    from okto_pulse.core.ports.coordination import (
        CoordinationProviderMissing,
        get_lease_provider,
    )

    try:
        lease_provider = get_lease_provider()
    except CoordinationProviderMissing:
        logger.warning(
            "kg.tick.no_lease_provider",
            extra={"event": "kg.tick.no_lease_provider"},
        )
        return

    lease = await lease_provider.try_acquire("kg_daily_tick", ttl_seconds=300)
    if lease is None:
        logger.info(
            "kg.tick.skipped reason=non_leader",
            extra={"event": "kg.tick.skipped", "reason": "non_leader"},
        )
        return

    try:
        try:
            factory = (
                uow_factory_provider()
                if uow_factory_provider is not None
                else resolve_unit_of_work_factory()
            )
        except RuntimeError:
            logger.warning(
                "kg.tick.no_uow_factory",
                extra={"event": "kg.tick.no_uow_factory"},
            )
            return
        scheduled_at = datetime.now(timezone.utc).isoformat()
        try:
            realm_scope = factory.resolve_realm_scope()
            async with factory(realm_scope=realm_scope) as uow:
                tick_ids = await uow.services.publish_kg_tick_events(
                    scheduled_at=scheduled_at
                )
                await uow.commit()
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
    finally:
        await lease_provider.release(lease)


__all__ = [
    "compute_tick_catch_up_next_run",
    "emit_daily_tick",
    "tick_next_run_from_last",
]
