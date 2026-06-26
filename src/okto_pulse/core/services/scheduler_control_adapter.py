"""Concrete SchedulerControl adapter (spec #15, card 03829e48).

Bridges the ``SchedulerControl`` port to the existing process-global APScheduler
singleton (``kg.scheduler_singleton._scheduler``). The singleton access is MOVED
here, not introduced: ``settings_service`` no longer reaches the scheduler
directly — it goes through this port. ``_scheduler`` therefore stays in the
register-before-remove ledger (AntiSingletonGate) until a composition root owns
it; this card does not move that ownership.

Keeps the canonical job id ``kg_daily_tick`` and the existing soft-fail /
audit semantics: a missing scheduler is a ``skipped`` outcome, never a raise.
"""

from __future__ import annotations

from datetime import timezone
from typing import Any, Mapping

from okto_pulse.core.ports.scheduler import (
    KG_DAILY_TICK_JOB_ID,
    SchedulerResult,
)


class SingletonSchedulerControl:
    """``SchedulerControl`` backed by the process-global APScheduler singleton.

    Structurally implements the ``SchedulerControl`` Protocol. The concrete
    ``apscheduler`` / ``scheduler_singleton`` imports are deferred to call time
    so importing this module stays side-effect free (import_side_effect gate).
    """

    def is_available(self) -> bool:
        from okto_pulse.core.kg.scheduler_singleton import get_scheduler

        return get_scheduler() is not None

    async def reschedule_job(
        self, job_id: str, trigger: Mapping[str, Any]
    ) -> SchedulerResult:
        from apscheduler.triggers.interval import IntervalTrigger

        from okto_pulse.core.kg.scheduler_singleton import get_scheduler

        scheduler = get_scheduler()
        if scheduler is None:
            return SchedulerResult(
                job_id=job_id,
                scheduled=False,
                message="no_scheduler",
                audit_status="skipped",
            )
        minutes = int(trigger["minutes"])
        job = scheduler.reschedule_job(
            job_id,
            trigger=IntervalTrigger(minutes=minutes, timezone=timezone.utc),
        )
        return SchedulerResult(
            job_id=job_id,
            scheduled=True,
            next_run_time=getattr(job, "next_run_time", None),
            audit_status="rescheduled",
        )

    async def shutdown(self, wait: bool = False) -> None:
        from okto_pulse.core.kg.scheduler_singleton import get_scheduler

        scheduler = get_scheduler()
        if scheduler is not None:
            scheduler.shutdown(wait=wait)


#: Canonical job id re-exported for callers wiring the tick reschedule.
__all__ = ["SingletonSchedulerControl", "KG_DAILY_TICK_JOB_ID"]
