"""R-P2-06B — scheduler singleton extraction.

The core common ``settings_service`` no longer constructs an implicit
``SingletonSchedulerControl`` fallback. The runtime tick effect flows through the
``SchedulerControl`` the composition injects (``RuntimeComposition.scheduler_control``,
resolved by the API from ``app.state.runtime_composition``). A ``None`` port is an
EXPLICIT skip — the core never reaches the process-global scheduler singleton.

Covers spec R-P2-06B (FR fr_73989fdf, TR tr_e32c2e90, AC ac_dfa42019,
scenario ts_93a7ed9c):
  - import/conformance: settings_service does not import the adapter nor the
    scheduler singleton;
  - service: a fake SchedulerControl receives the reschedule; absence (None) is
    an explicit skip WITHOUT instantiating the singleton; an unavailable port
    skips too;
  - app: create_app preserves the composition on ``app.state.runtime_composition``.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

from okto_pulse.core.ports.scheduler import (
    KG_DAILY_TICK_JOB_ID,
    JobSpec,
    SchedulerJobSnapshot,
    SchedulerResult,
)
from okto_pulse.core.services.settings_service import apply_tick_runtime_effects

_SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
_SETTINGS_SERVICE = _SRC_ROOT / "okto_pulse" / "core" / "services" / "settings_service.py"


class _FakeScheduler:
    """Explicit test SchedulerControl — records the reschedule, no singleton."""

    def __init__(self, *, available: bool = True) -> None:
        self._available = available
        self.calls: list[tuple[str, dict]] = []

    def is_available(self) -> bool:
        return self._available

    async def reschedule_job(self, job_id: str, trigger) -> SchedulerResult:
        self.calls.append((job_id, dict(trigger)))
        return SchedulerResult(job_id=job_id, scheduled=True, audit_status="rescheduled")

    async def register_job(self, job_spec: JobSpec, handler) -> SchedulerResult:
        return SchedulerResult(
            job_id=job_spec.job_id,
            scheduled=True,
            audit_status="rescheduled",
        )

    async def get_job_snapshot(self, job_id: str) -> SchedulerJobSnapshot:
        return SchedulerJobSnapshot(job_id=job_id, exists=True)

    async def shutdown(self, wait: bool = False) -> None:  # pragma: no cover - unused
        ...


# --- import / conformance audit ----------------------------------------------
def test_settings_service_does_not_reach_singleton_or_adapter():
    src = _SETTINGS_SERVICE.read_text(encoding="utf-8")
    # No IMPORT of the concrete adapter module and no CONSTRUCTION of the
    # singleton bridge (a doc-comment may still NAME it to explain the removal).
    assert "scheduler_control_adapter" not in src
    assert "SingletonSchedulerControl(" not in src
    # And never the process-global scheduler singleton directly (conformance).
    assert "kg.scheduler_singleton" not in src


# --- service: port receives the reschedule -----------------------------------
def test_injected_port_receives_reschedule():
    fake = _FakeScheduler(available=True)
    results = asyncio.run(
        apply_tick_runtime_effects({"kg_decay_tick_interval_minutes": 45}, fake)
    )
    assert fake.calls == [(KG_DAILY_TICK_JOB_ID, {"minutes": 45})]
    assert len(results) == 1
    assert results[0].status == "applied"


# --- service: absent port -> explicit skip, NO singleton ---------------------
def test_none_port_skips_without_singleton():
    results = asyncio.run(
        apply_tick_runtime_effects({"kg_decay_tick_interval_minutes": 45}, None)
    )
    assert len(results) == 1
    assert results[0].status == "skipped"
    assert results[0].job_id == KG_DAILY_TICK_JOB_ID


def test_unavailable_port_skips_without_calling_reschedule():
    fake = _FakeScheduler(available=False)
    results = asyncio.run(
        apply_tick_runtime_effects({"kg_decay_tick_interval_minutes": 45}, fake)
    )
    assert results[0].status == "skipped"
    assert fake.calls == []


def test_no_tick_change_is_a_noop():
    fake = _FakeScheduler(available=True)
    results = asyncio.run(
        apply_tick_runtime_effects({"kg_event_queue_max_size": 100}, fake)
    )
    assert results == []
    assert fake.calls == []


# --- app: composition preserved on app.state ---------------------------------
def test_create_app_preserves_composition_on_app_state(monkeypatch):
    from okto_pulse.community import app as app_mod
    from okto_pulse.core.composition import RuntimeComposition
    from okto_pulse.core.infra.config import (
        CoreSettings,
        configure_settings,
        get_settings,
    )

    # This test only asserts composition preservation on app.state. The core
    # app factory no longer opens a concrete database; it only verifies that an
    # edition-owned relational runtime was configured before default lifespan.
    runtime_checks: list[str] = []
    monkeypatch.setattr(
        app_mod,
        "is_database_runtime_configured",
        lambda: runtime_checks.append("is_database_runtime_configured") or True,
    )

    class _Auth:
        async def get_current_user(self, *a, **k):  # pragma: no cover - unused
            return None

    class _Storage:
        async def save(self, *a, **k):  # pragma: no cover - unused
            return ""

        async def load(self, *a, **k):  # pragma: no cover - unused
            return b""

        async def delete(self, *a, **k):  # pragma: no cover - unused
            return False

    composition = RuntimeComposition(
        settings_provider=object(),
        auth_provider=object(),
        storage_provider=object(),
        event_bus=object(),
        uow_factory=object(),
        scheduler_control=_FakeScheduler(),
    )
    original_settings = get_settings()
    try:
        app = app_mod.create_app(
            settings=CoreSettings(),
            auth_provider=_Auth(),
            storage_provider=_Storage(),
            composition=composition,
        )
    finally:
        configure_settings(original_settings)
    assert runtime_checks == ["is_database_runtime_configured"]
    assert app.state.runtime_composition is composition
    assert app.state.runtime_composition.scheduler_control is composition.scheduler_control
