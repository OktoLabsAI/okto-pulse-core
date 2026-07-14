"""Public application startup operations for edition composition roots."""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import datetime
from typing import Any


def tick_next_run_from_last(
    last_completed_at: datetime | None,
    interval_minutes: int,
    now: datetime,
) -> datetime:
    from okto_pulse.core.infra.daily_tick import tick_next_run_from_last as operation

    return operation(last_completed_at, interval_minutes, now)


async def compute_tick_catch_up_next_run(
    interval_minutes: int,
    *,
    uow_factory_provider: Callable[[], Any] | None = None,
    now_provider: Callable[[], datetime] | None = None,
) -> datetime | None:
    from okto_pulse.core.infra.daily_tick import compute_tick_catch_up_next_run as operation

    return await operation(
        interval_minutes,
        uow_factory_provider=uow_factory_provider,
        now_provider=now_provider,
    )


async def emit_daily_tick(
    *, uow_factory_provider: Callable[[], Any] | None = None
) -> None:
    from okto_pulse.core.infra.daily_tick import emit_daily_tick as operation

    await operation(uow_factory_provider=uow_factory_provider)


async def apply_persisted_runtime_settings() -> dict[str, int]:
    from okto_pulse.core.services.settings_service import (
        apply_persisted_settings_to_core_settings,
    )

    return await apply_persisted_settings_to_core_settings()


async def backfill_qa_answered_at(relational_context: Any) -> dict[str, int]:
    from okto_pulse.core.services.application_startup import backfill_qa_answered_at

    return await backfill_qa_answered_at(relational_context)


async def run_startup_schema_sweep(
    *, uow_factory: Any | None = None, logger: logging.Logger
) -> None:
    from okto_pulse.core.infra.startup_schema_sweep import run_startup_schema_sweep

    await run_startup_schema_sweep(uow_factory=uow_factory, logger=logger)


__all__ = [
    "apply_persisted_runtime_settings",
    "backfill_qa_answered_at",
    "compute_tick_catch_up_next_run",
    "emit_daily_tick",
    "run_startup_schema_sweep",
    "tick_next_run_from_last",
]
