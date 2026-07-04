"""R-P2-06C — general settings-effects contract.

06B made the single runtime effect (the KG decay-tick reschedule) flow through a
composition-injected ``SchedulerControl``. 06C consolidates the GENERAL contract:
a settings change triggers a runtime effect ONLY through an injected port, the
core never constructs a concrete effect provider, and the effect->port mapping is
an EXECUTABLE inventory (``SETTINGS_RUNTIME_EFFECT_PORTS``) backing the conformance
gate. GRAPH_DB_KEYS go through persistence + ``KGConfigChangeGuard`` and trigger NO
local runtime effect — even when a SchedulerControl is available.

Covers spec R-P2-06C (FR fr_89a5da6c, TR tr_0f6cfd46, AC ac_c99958f1).
"""

from __future__ import annotations

import asyncio

from okto_pulse.core.application.boundary.conformance_suite import (
    settings_split_conformance,
)
from okto_pulse.core.composition import ALL_PROVIDER_KEYS
from okto_pulse.core.ports.scheduler import (
    JobSpec,
    SchedulerJobSnapshot,
    SchedulerResult,
)
from okto_pulse.core.services.settings_service import (
    GRAPH_DB_KEYS,
    RUNTIME_KEYS,
    SETTINGS_RUNTIME_EFFECT_PORTS,
    apply_tick_runtime_effects,
    get_runtime_settings,
    put_runtime_settings,
)


class _FakeScheduler:
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


# --- conformance gate enforces the general contract --------------------------
def test_conformance_gate_enforces_general_settings_effect_contract():
    report = settings_split_conformance()
    assert report.status in ("baseline", "passed"), report.evidence
    checks = report.evidence["checks"]
    # R-P2-06C additions: no implicit concrete effect provider in the core.
    assert checks["no_implicit_singleton_construction"] is True
    assert checks["no_effect_adapter_import"] is True
    assert checks["has_effect_port_inventory"] is True
    # R-P2-06B invariant preserved.
    assert checks["no_direct_singleton_import"] is True


# --- the inventory is the executable canonical source ------------------------
def test_effect_port_inventory_is_canonical_and_executable():
    # The single mapped effect today (06C condition 1).
    assert SETTINGS_RUNTIME_EFFECT_PORTS == {
        "kg_decay_tick_interval_minutes": "scheduler_control"
    }
    # Every mapped setting is a real persisted runtime key.
    for setting in SETTINGS_RUNTIME_EFFECT_PORTS:
        assert setting in RUNTIME_KEYS
    # Every effect port is a real composition provider key.
    for provider_key in SETTINGS_RUNTIME_EFFECT_PORTS.values():
        assert provider_key in ALL_PROVIDER_KEYS
    # Absence-of-unintended-effect: GRAPH_DB_KEYS trigger NO runtime effect.
    assert set(GRAPH_DB_KEYS).isdisjoint(SETTINGS_RUNTIME_EFFECT_PORTS)


# --- GRAPH_DB_KEYS trigger no effect even with a fake scheduler (unit) --------
def test_graph_db_key_only_change_triggers_no_effect_with_fake_available():
    fake = _FakeScheduler(available=True)
    results = asyncio.run(
        apply_tick_runtime_effects({"kg_kuzu_max_db_size_gb": 8}, fake)
    )
    assert results == []
    assert fake.calls == []  # the scheduler was NEVER reached


# --- GRAPH_DB_KEYS persist through the guard with no effect (integration) -----
def test_graph_db_key_change_persists_via_guard_without_runtime_effect():
    async def _run():
        from okto_pulse.core.infra.config import GRAPH_DB_MAX_SIZE_GB_VALUES
        from okto_pulse.core.infra.database import get_session_factory
        from okto_pulse.core.services.settings_service import AppSetting

        factory = get_session_factory()
        async with factory() as db:
            current = await get_runtime_settings(db)
            current_val = int(current["kg_kuzu_max_db_size_gb"])
            # A guard-allowed change clamped inside the schema: the next valid
            # power-of-two grow when possible, else a same-value no-op (already at
            # the max). Both exercise the guard + persistence path with no effect.
            higher = [v for v in GRAPH_DB_MAX_SIZE_GB_VALUES if v > current_val]
            target = higher[0] if higher else current_val
            fake = _FakeScheduler(available=True)
            # GRAPH_DB_KEYS go through KGConfigChangeGuard + persistence; they are
            # restart-required (NOT hot-applied), so the live view is unchanged.
            await put_runtime_settings(
                db,
                {"kg_kuzu_max_db_size_gb": target},
                migration_plan_ref="MP-06C-TEST",
                restart_policy="scheduled",
                scheduler_control=fake,
            )
            row = await db.get(AppSetting, "kg_kuzu_max_db_size_gb")
            persisted = int(row.value) if row is not None else None
        return target, fake, persisted

    target, fake, persisted = asyncio.run(_run())
    # Persisted through the guard to the app_settings table...
    assert persisted == target
    # ...and the SchedulerControl was NEVER called: a GRAPH_DB_KEY change triggers
    # NO runtime effect, even with the port available (06C condition 3).
    assert fake.calls == []
