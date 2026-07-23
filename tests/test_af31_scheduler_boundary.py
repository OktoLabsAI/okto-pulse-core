"""AF31-S1R scheduler boundary tests."""

from __future__ import annotations

import ast
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import pytest

from okto_pulse.core.ports.scheduler import (
    JobSpec,
    SchedulerJobSnapshot,
    SchedulerResult,
)


CORE_ROOT = Path(__file__).parents[1] / "src" / "okto_pulse" / "core"


class _RecordingSchedulerControl:
    def __init__(self) -> None:
        self.registered: list[tuple[JobSpec, object]] = []

    def is_available(self) -> bool:
        return True

    async def register_job(self, job_spec: JobSpec, handler) -> SchedulerResult:
        self.registered.append((job_spec, handler))
        return SchedulerResult(job_id=job_spec.job_id, scheduled=True)

    async def reschedule_job(
        self, job_id: str, trigger: Mapping[str, Any]
    ) -> SchedulerResult:
        return SchedulerResult(job_id=job_id, scheduled=True)

    async def get_job_snapshot(self, job_id: str) -> SchedulerJobSnapshot:
        return SchedulerJobSnapshot(job_id=job_id, exists=True)

    async def shutdown(self, wait: bool = False) -> None:
        return None


def _runtime_core_files() -> list[Path]:
    files: list[Path] = []
    for path in CORE_ROOT.rglob("*.py"):
        rel = path.relative_to(CORE_ROOT).as_posix()
        if rel.startswith("application/boundary/"):
            continue
        if rel.startswith("mcp/resources/"):
            continue
        files.append(path)
    return files


@pytest.mark.asyncio
async def test_core_registers_kg_daily_tick_through_jobspec(monkeypatch) -> None:
    from okto_pulse.community import app as app_mod
    from okto_pulse.core.infra.config import (
        CoreSettings,
        configure_settings,
        get_settings,
    )

    previous = get_settings()
    next_run = datetime(2026, 7, 4, 10, 0, tzinfo=timezone.utc)
    control = _RecordingSchedulerControl()
    monkeypatch.delenv("KG_DAILY_TICK_DISABLED", raising=False)
    monkeypatch.setattr(
        app_mod,
        "compute_tick_catch_up_next_run",
        lambda interval_minutes: _awaitable(next_run),
    )

    try:
        configure_settings(CoreSettings(kg_decay_tick_interval_minutes=37))
        result = await app_mod.register_kg_daily_tick_job(control)
    finally:
        configure_settings(previous)

    assert result is not None
    assert result.scheduled is True
    assert len(control.registered) == 1
    job_spec, handler = control.registered[0]
    assert job_spec.job_id == "kg_daily_tick"
    assert job_spec.interval_minutes == 37
    assert job_spec.max_instances == 1
    assert job_spec.coalesce is True
    assert job_spec.replace_existing is True
    assert job_spec.next_run_time == next_run
    assert (
        job_spec.handler_ref
        == "okto_pulse.core.application.startup.emit_daily_tick"
    )
    assert handler is app_mod.emit_daily_tick


async def _awaitable(value):
    return value


def test_core_runtime_has_no_concrete_apscheduler_dependency() -> None:
    violations: list[str] = []
    forbidden_calls = {"AsyncIOScheduler", "IntervalTrigger"}
    for path in _runtime_core_files():
        tree = ast.parse(path.read_text(encoding="utf-8"))
        rel = path.relative_to(CORE_ROOT).as_posix()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name == "apscheduler" or alias.name.startswith(
                        "apscheduler."
                    ):
                        violations.append(f"{rel}: import {alias.name}")
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ""
                if module == "apscheduler" or module.startswith("apscheduler."):
                    violations.append(f"{rel}: from {module} import ...")
            elif isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                if node.func.id in forbidden_calls:
                    violations.append(f"{rel}: calls {node.func.id}")

    assert violations == []
