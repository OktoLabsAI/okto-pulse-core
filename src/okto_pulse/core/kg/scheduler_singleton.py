"""Module-level reference to the KG decay tick scheduler.

Spec 54399628 (Wave 2 NC f9732afc) — needed by settings_service to
hot-reload the IntervalTrigger when an operator changes
`kg_decay_tick_interval_minutes` via PUT /api/v1/settings/runtime.

The scheduler is set once at app boot (`core/app.py` lifespan and
`community/main.py` startup) and read by services that need to
reschedule jobs without a server restart.

Falls back to ``None`` when called before boot or in test contexts that
skip lifespan — services must handle this gracefully.
"""

from __future__ import annotations

from typing import Any

_scheduler: Any = None


def set_scheduler(scheduler: Any) -> None:
    """Register the boot-time AsyncIOScheduler instance for hot-reload."""
    global _scheduler
    _scheduler = scheduler


def get_scheduler() -> Any:
    """Return the registered scheduler or None if unavailable."""
    return _scheduler


def clear_scheduler_for_tests() -> None:
    """Reset the singleton between pytest runs."""
    global _scheduler
    _scheduler = None
