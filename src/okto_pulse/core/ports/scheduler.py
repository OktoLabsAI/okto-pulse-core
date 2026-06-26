"""SchedulerControl runtime port (spec #15, tr_816873bf / api_69859f5e).

Decouples runtime settings from the concrete APScheduler singleton
(``kg.scheduler_singleton._scheduler``). The canonical KG tick job id is
``kg_daily_tick`` (NOT ``kg_tick``). Pure: Protocol + frozen DTO, no concrete
scheduler import.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, Mapping, Protocol, runtime_checkable

#: Canonical scheduler job id for the KG daily tick (api_69859f5e notes).
KG_DAILY_TICK_JOB_ID = "kg_daily_tick"

SchedulerAuditStatus = Literal["rescheduled", "skipped", "failed"]


@dataclass(frozen=True)
class SchedulerResult:
    """Outcome of a scheduler operation, preserving existing audit semantics."""

    job_id: str
    scheduled: bool
    next_run_time: datetime | None = None
    message: str | None = None
    audit_status: SchedulerAuditStatus = "rescheduled"


@runtime_checkable
class SchedulerControl(Protocol):
    """Port over the runtime scheduler — no global singleton dependency."""

    def is_available(self) -> bool:
        """True when a concrete scheduler is wired for runtime effects."""
        ...

    async def reschedule_job(
        self, job_id: str, trigger: Mapping[str, Any]
    ) -> SchedulerResult:
        """Reschedule ``job_id`` with ``trigger``; keep rescheduled/skipped auditable."""
        ...

    async def shutdown(self, wait: bool = False) -> None:
        """Stop the scheduler."""
        ...
