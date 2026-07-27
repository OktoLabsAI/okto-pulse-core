"""SchedulerControl runtime port (spec #15, tr_816873bf / api_69859f5e).

Decouples runtime settings and job registration from concrete scheduler
frameworks. The canonical KG tick job id is ``kg_daily_tick`` (NOT
``kg_tick``). Pure: Protocol + frozen DTOs, no concrete scheduler import.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, Mapping, Protocol, runtime_checkable

#: Canonical scheduler job id for the KG daily tick (api_69859f5e notes).
KG_DAILY_TICK_JOB_ID = "kg_daily_tick"

SchedulerAuditStatus = Literal["rescheduled", "skipped", "failed"]
SchedulerJobHandler = Callable[[], Any]


@dataclass(frozen=True)
class JobSpec:
    """Core-owned scheduler job policy independent from adapter mechanics."""

    job_id: str
    interval_minutes: int
    max_instances: int = 1
    coalesce: bool = True
    replace_existing: bool = True
    next_run_time: datetime | None = None
    handler_ref: str | None = None


@dataclass(frozen=True)
class SchedulerResult:
    """Outcome of a scheduler operation, preserving existing audit semantics."""

    job_id: str
    scheduled: bool
    next_run_time: datetime | None = None
    message: str | None = None
    audit_status: SchedulerAuditStatus = "rescheduled"


@dataclass(frozen=True)
class SchedulerJobSnapshot:
    """Read-only scheduler job state exposed by edition adapters."""

    job_id: str
    exists: bool
    next_run_time: datetime | None = None
    message: str | None = None


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

    async def register_job(
        self,
        job_spec: JobSpec,
        handler: SchedulerJobHandler,
    ) -> SchedulerResult:
        """Register a scheduler job described by a core-owned ``JobSpec``."""
        ...

    async def get_job_snapshot(self, job_id: str) -> SchedulerJobSnapshot:
        """Return read-only status for a registered job, without concrete runtime."""
        ...

    async def shutdown(self, wait: bool = False) -> None:
        """Stop the scheduler."""
        ...
