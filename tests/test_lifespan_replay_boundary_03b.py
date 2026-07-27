"""Spec #03 (card b4f07ad3) — CommunityLifespanReplay + CompositionBoundaryGate.

Locks the golden lifecycle replay (15 required events, closed 3-exclusion
allowlist, unexpected_lifecycle_delta / required_lifecycle_event_missing) and the
deterministic composition boundary classification (baseline / blocking /
xfail_advisory + register-before-remove).
"""

from __future__ import annotations

import inspect

import pytest

from okto_pulse.community.app import create_app
from okto_pulse.core.application.boundary import (
    DEFAULT_ONLY_EXCLUSIONS,
    REQUIRED_LIFECYCLE_EVENTS,
    CommunityLifespanReplay,
    CommunityLifespanReplayInput,
    CompositionBoundaryGate,
    CompositionBoundaryGateInput,
    LifecycleFallbackGate,
    NamedLifecycleHook,
    RuntimeWorkerBoundaryGate,
)
from okto_pulse.core.composition import RuntimeComposition, RuntimeProviderMissing
from okto_pulse.core.ports.runtime_workers import (
    RuntimeWorkerRegistry,
    RuntimeWorkerSpec,
    WorkerDrainIncomplete,
)

_STARTUP_EVENTS = [
    "init_db", "seed_community_defaults", "backfill_qa_answered_at",
    "_afg_backfill_task", "_preload_embedding_model", "event_dispatcher",
    "consolidation_worker", "cleanup_worker", "outbox_worker", "settings_service",
    "scheduler", "mcp_runtime", "frontend_served", "_metrics_beacon_loop",
]


def _golden_hooks(extra=()):
    hooks = [NamedLifecycleHook(startup_event=e) for e in _STARTUP_EVENTS]
    hooks.append(NamedLifecycleHook(shutdown_event="close_db"))
    hooks.extend(extra)
    return tuple(hooks)


def _composition(hooks, **overrides) -> RuntimeComposition:
    base = dict(
        settings_provider=1, auth_provider=1, storage_provider=1,
        event_bus=1, uow_factory=1, lifecycle_hooks=hooks,
    )
    base.update(overrides)
    return RuntimeComposition(**base)


def test_closed_baseline_constants() -> None:
    assert len(REQUIRED_LIFECYCLE_EVENTS) == 15
    assert set(DEFAULT_ONLY_EXCLUSIONS) == {
        "kg_migration_sweep", "shutdown_kg_events_hub", "close_all_connections"
    }


@pytest.mark.asyncio
async def test_replay_golden_observes_all_required_events() -> None:
    comp = _composition(_golden_hooks(), scheduler_control=1)
    report = await CommunityLifespanReplay().run(
        CommunityLifespanReplayInput(composition=comp, include_frontend=True)
    )
    assert report.status == "passed", report.as_dict()
    observed = set(report.startup_events) | set(report.shutdown_events)
    assert set(REQUIRED_LIFECYCLE_EVENTS) <= observed
    assert report.workers == ["cleanup_worker", "consolidation_worker", "event_dispatcher", "outbox_worker"]
    assert report.scheduler == "running"
    assert report.mcp_runtime == "registered"
    assert report.frontend == "served"
    assert report.unexpected_deltas == []
    assert report.order_mismatches == []


@pytest.mark.asyncio
async def test_replay_missing_required_event_is_flagged() -> None:
    hooks = tuple(NamedLifecycleHook(startup_event=e) for e in _STARTUP_EVENTS)  # no close_db
    report = await CommunityLifespanReplay().run(
        CommunityLifespanReplayInput(composition=_composition(hooks))
    )
    assert report.status == "required_lifecycle_event_missing"
    assert any(d.event == "close_db" and d.kind == "missing" for d in report.unexpected_deltas)


@pytest.mark.asyncio
async def test_replay_accepted_exclusion_is_not_unexpected_but_rogue_is() -> None:
    extra = (
        NamedLifecycleHook(startup_event="kg_migration_sweep"),  # accepted exclusion
        NamedLifecycleHook(startup_event="rogue_event"),  # genuine delta
    )
    report = await CommunityLifespanReplay().run(
        CommunityLifespanReplayInput(composition=_composition(_golden_hooks(extra)))
    )
    assert report.status == "unexpected_lifecycle_delta"
    assert "kg_migration_sweep" in report.default_only_exclusions
    assert [d.event for d in report.unexpected_deltas if d.kind == "unexpected"] == ["rogue_event"]


@pytest.mark.asyncio
async def test_replay_scheduler_event_order_mismatch_is_flagged() -> None:
    events = list(_STARTUP_EVENTS)
    settings_index = events.index("settings_service")
    scheduler_index = events.index("scheduler")
    events[settings_index], events[scheduler_index] = (
        events[scheduler_index],
        events[settings_index],
    )
    hooks = [NamedLifecycleHook(startup_event=e) for e in events]
    hooks.append(NamedLifecycleHook(shutdown_event="close_db"))

    report = await CommunityLifespanReplay().run(
        CommunityLifespanReplayInput(composition=_composition(tuple(hooks)))
    )

    assert report.status == "lifecycle_event_order_mismatch"
    mismatch_events = {m.event for m in report.order_mismatches}
    assert {"settings_service", "scheduler"} <= mismatch_events
    assert report.unexpected_deltas == []


@pytest.mark.asyncio
async def test_replay_strict_runtime_missing_provider_raises() -> None:
    comp = _composition(_golden_hooks(), event_bus=None)
    with pytest.raises(RuntimeProviderMissing):
        await CommunityLifespanReplay().run(
            CommunityLifespanReplayInput(composition=comp, strict_runtime=True)
        )


@pytest.mark.asyncio
async def test_replay_strict_runtime_empty_hooks_are_missing_provider() -> None:
    comp = _composition(())
    with pytest.raises(RuntimeProviderMissing) as exc:
        await CommunityLifespanReplay().run(
            CommunityLifespanReplayInput(composition=comp, strict_runtime=True)
        )
    assert exc.value.missing == ["lifecycle_hooks"]


@pytest.mark.asyncio
async def test_replay_reports_community_replay_failed_on_hook_error() -> None:
    class _Boom:
        startup_event = "init_db"
        shutdown_event = None

        async def on_startup(self) -> None:
            raise RuntimeError("boom")

        async def on_shutdown(self) -> None:
            return None

    comp = _composition((_Boom(),))
    report = await CommunityLifespanReplay().run(
        CommunityLifespanReplayInput(composition=comp)
    )
    assert report.status == "community_replay_failed"
    assert report.error and "boom" in report.error


# --------------------------------------------------------------------------- #
# CompositionBoundaryGate
# --------------------------------------------------------------------------- #
def _write_app_layer_file(tmp_path, relpath: str, body: str):
    path = tmp_path / "okto_pulse" / "core" / relpath
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")
    return tmp_path


def test_boundary_gate_real_tree_has_no_new_blocking() -> None:
    report = CompositionBoundaryGate().run(CompositionBoundaryGateInput(mode="bootstrap"))
    findings = report.evidence["findings"]
    assert [f for f in findings if f["status"] == "blocking"] == []
    # KG symbols deferred, owned-provider wiring catalogued as baseline
    assert any(f["status"] == "xfail_advisory" and f["provider_key"] == "kg_registry" for f in findings)


def test_boundary_gate_new_concrete_wiring_blocks(tmp_path) -> None:
    root = _write_app_layer_file(
        tmp_path, "services/rogue.py",
        "def wire():\n    return SqliteOutboxEventBus(session_factory=None)\n",
    )
    report = CompositionBoundaryGate().run(
        CompositionBoundaryGateInput(source_root=root, baseline_snapshot=frozenset())
    )
    assert report.status == "blocking"
    blocking = [f for f in report.evidence["findings"] if f["status"] == "blocking"]
    assert any(
        f["provider_key"] == "event_bus" and f["trigger"] == "new_concrete_wiring"
        for f in blocking
    )


def test_boundary_gate_existing_debt_is_baseline(tmp_path) -> None:
    root = _write_app_layer_file(
        tmp_path, "services/known.py",
        "def wire():\n    configure_settings(None)\n",
    )
    snapshot = frozenset({"okto_pulse/core/services/known.py::configure_settings"})
    report = CompositionBoundaryGate().run(
        CompositionBoundaryGateInput(source_root=root, baseline_snapshot=snapshot)
    )
    findings = report.evidence["findings"]
    assert findings and all(f["status"] == "baseline" for f in findings)
    assert report.status == "baseline"


def test_boundary_gate_kg_symbol_is_xfail_advisory(tmp_path) -> None:
    root = _write_app_layer_file(
        tmp_path, "services/kg.py",
        "def wire():\n    return get_kg_registry()\n",
    )
    report = CompositionBoundaryGate().run(
        CompositionBoundaryGateInput(source_root=root, baseline_snapshot=frozenset())
    )
    findings = report.evidence["findings"]
    assert findings and all(f["status"] == "xfail_advisory" for f in findings)
    assert all(f["adapter_target"] == "deferred_to_05" for f in findings)


def test_boundary_gate_provider_catalog_missing_blocks() -> None:
    report = CompositionBoundaryGate().run(CompositionBoundaryGateInput(owned_provider_keys=()))
    assert report.status == "blocking"
    assert report.evidence["error"] == "provider_catalog_missing"


def test_boundary_gate_register_before_remove(tmp_path) -> None:
    # removing a fallback without a registered Community provider -> blocking
    missing = CompositionBoundaryGate().run(
        CompositionBoundaryGateInput(
            source_root=tmp_path, baseline_snapshot=frozenset(),
            removed_fallbacks=("event_bus",),
        )
    )
    assert missing.status == "blocking"
    assert any(
        f["trigger"] == "fallback_removal_without_provider" and f["provider_key"] == "event_bus"
        for f in missing.evidence["findings"]
    )
    # with the Community provider registered -> no register-before-remove finding
    ok = CompositionBoundaryGate().run(
        CompositionBoundaryGateInput(
            source_root=tmp_path, baseline_snapshot=frozenset(),
            removed_fallbacks=("event_bus",),
            registered_community_providers=("event_bus",),
        )
    )
    assert not any(
        f["trigger"] == "fallback_removal_without_provider" for f in ok.evidence["findings"]
    )


def test_boundary_gate_blocking_mode_without_baseline_does_not_mask_new_wiring(tmp_path) -> None:
    # codex rework (val_2b30c779): in mode='blocking' with no baseline_snapshot the
    # gate must NOT self-baseline the current tree (that masks the new wiring).
    root = _write_app_layer_file(
        tmp_path, "services/rogue.py",
        "def wire():\n    return SqliteOutboxEventBus(session_factory=None)\n",
    )
    report = CompositionBoundaryGate().run(
        CompositionBoundaryGateInput(source_root=root, mode="blocking")  # no baseline_snapshot
    )
    assert report.status == "blocking", report.evidence
    findings = report.evidence["findings"]
    assert any(
        f["provider_key"] == "event_bus"
        and f["trigger"] == "new_concrete_wiring"
        and f["status"] == "blocking"
        for f in findings
    )
    assert not any(f["status"] == "baseline" for f in findings)


def test_boundary_gate_blocking_mode_real_tree_uses_versioned_catalogue() -> None:
    from okto_pulse.core.application.boundary import DEFAULT_COMPOSITION_BASELINE

    # blocking mode + no explicit baseline -> the reviewed versioned catalogue,
    # never a self-scan. Catalogued debt = baseline, KG = xfail, no NEW blocking.
    report = CompositionBoundaryGate().run(CompositionBoundaryGateInput(mode="blocking"))
    findings = report.evidence["findings"]
    assert [f for f in findings if f["status"] == "blocking"] == []
    baseline_keys = {
        f"{f['file']}::{f['symbol']}" for f in findings if f["status"] == "baseline"
    }
    assert baseline_keys == set()
    assert DEFAULT_COMPOSITION_BASELINE == frozenset()


# --------------------------------------------------------------------------- #
# R08B LifecycleFallbackGate
# --------------------------------------------------------------------------- #
def test_lifecycle_fallback_gate_real_tree_passes() -> None:
    report = LifecycleFallbackGate().run()
    assert report.status == "passed", report.as_dict()
    assert report.evidence["offenders"] == []


def test_lifecycle_fallback_gate_blocks_community_concrete_in_core_app(tmp_path) -> None:
    core = tmp_path / "okto_pulse" / "core"
    core.mkdir(parents=True)
    (core / "app.py").write_text(
        "from okto_pulse.community.adapters.data import CommunityOutboxEventBus\n\n"
        "def fallback(session_factory):\n"
        "    return CommunityOutboxEventBus(session_factory)\n",
        encoding="utf-8",
    )
    (core / "composition.py").write_text("", encoding="utf-8")

    report = LifecycleFallbackGate().run(source_root=tmp_path)

    assert report.status == "blocking"
    offenders = report.evidence["offenders"]
    assert any(
        item["file"] == "okto_pulse/core/app.py"
        and item["symbol"] == "CommunityOutboxEventBus"
        for item in offenders
    )


def test_lifecycle_fallback_gate_blocks_alias_import_evasion(tmp_path) -> None:
    core = tmp_path / "okto_pulse" / "core"
    core.mkdir(parents=True)
    (core / "app.py").write_text(
        "from okto_pulse.community.adapters.data import CommunityOutboxEventBus as Foo\n\n"
        "def fallback(session_factory):\n"
        "    return Foo(session_factory)\n",
        encoding="utf-8",
    )
    (core / "composition.py").write_text("", encoding="utf-8")

    report = LifecycleFallbackGate().run(source_root=tmp_path)

    assert report.status == "blocking"
    offenders = report.evidence["offenders"]
    assert any(
        item["kind"] == "from_import"
        and item["symbol"] == "CommunityOutboxEventBus"
        for item in offenders
    )


# --------------------------------------------------------------------------- #
# R08C RuntimeWorkerRegistry + RuntimeWorkerBoundaryGate
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_runtime_worker_registry_start_stop_is_idempotent() -> None:
    events: list[str] = []

    class _Worker:
        def __init__(self, family: str) -> None:
            self.family = family

        async def stop(self) -> None:
            events.append(f"stop:{self.family}")

    async def _start(family: str) -> _Worker:
        events.append(f"start:{family}")
        return _Worker(family)

    registry = RuntimeWorkerRegistry(
        (
            RuntimeWorkerSpec(
                family="event_dispatcher",
                start=lambda: _start("event_dispatcher"),
                stop=lambda worker: worker.stop(),
            ),
            RuntimeWorkerSpec(
                family="consolidation_worker",
                start=lambda: _start("consolidation_worker"),
                stop=lambda worker: worker.stop(),
            ),
        )
    )

    assert await registry.start_all() == (
        "event_dispatcher",
        "consolidation_worker",
    )
    assert await registry.start_all() == (
        "event_dispatcher",
        "consolidation_worker",
    )
    assert registry.start_count("event_dispatcher") == 1
    assert registry.start_count("consolidation_worker") == 1
    failures = await registry.stop_all()

    assert failures == ()
    assert events == [
        "start:event_dispatcher",
        "start:consolidation_worker",
        "stop:consolidation_worker",
        "stop:event_dispatcher",
    ]
    assert registry.active_families == ()


@pytest.mark.asyncio
async def test_runtime_worker_registry_stop_priority_preserves_dispatcher_first() -> None:
    events: list[str] = []

    class _Worker:
        def __init__(self, family: str) -> None:
            self.family = family

    async def _start(family: str) -> _Worker:
        events.append(f"start:{family}")
        return _Worker(family)

    async def _stop(worker: _Worker) -> None:
        events.append(f"stop:{worker.family}")

    registry = RuntimeWorkerRegistry(
        (
            RuntimeWorkerSpec(
                family="event_dispatcher",
                start=lambda: _start("event_dispatcher"),
                stop=_stop,
                stop_priority=300,
            ),
            RuntimeWorkerSpec(
                family="cleanup_worker",
                start=lambda: _start("cleanup_worker"),
                stop=_stop,
                stop_priority=100,
            ),
            RuntimeWorkerSpec(
                family="consolidation_worker",
                start=lambda: _start("consolidation_worker"),
                stop=_stop,
            ),
            RuntimeWorkerSpec(
                family="outbox_worker",
                start=lambda: _start("outbox_worker"),
                stop=_stop,
                stop_priority=200,
            ),
        )
    )

    await registry.start_all()
    failures = await registry.stop_all()

    assert failures == ()
    assert events == [
        "start:event_dispatcher",
        "start:cleanup_worker",
        "start:consolidation_worker",
        "start:outbox_worker",
        "stop:event_dispatcher",
        "stop:outbox_worker",
        "stop:cleanup_worker",
        "stop:consolidation_worker",
    ]


@pytest.mark.asyncio
async def test_runtime_worker_registry_retains_handle_on_native_drain_timeout() -> None:
    handle = object()

    async def _stop(_handle: object) -> None:
        raise WorkerDrainIncomplete(
            family="consolidation_worker",
            phase="test_drain",
            pending_tasks=1,
            pending_operations=1,
            timeout_seconds=0.01,
        )

    registry = RuntimeWorkerRegistry(
        (
            RuntimeWorkerSpec(
                family="consolidation_worker",
                start=lambda: handle,
                stop=_stop,
            ),
        )
    )
    await registry.start_all()

    failures = await registry.stop_all()

    assert len(failures) == 1
    assert failures[0].resource_close_unsafe is True
    assert registry.get_handle("consolidation_worker") is handle
    assert registry.active_families == ("consolidation_worker",)


def test_runtime_worker_boundary_gate_real_core_tree_passes() -> None:
    report = RuntimeWorkerBoundaryGate().run()
    assert report.status == "passed", report.as_dict()
    assert (
        "okto_pulse/core/application/processors/global_outbox.py"
        in report.evidence["scanned_files"]
    )
    assert report.evidence["offenders"] == []


def test_core_app_does_not_build_default_runtime_workers() -> None:
    source = inspect.getsource(create_app)
    assert "build_default_runtime_worker_registry" not in source
    assert "runtime_worker_registry is not None" in source


def test_runtime_worker_boundary_gate_blocks_core_direct_worker_start(tmp_path) -> None:
    core = tmp_path / "okto_pulse" / "core"
    core.mkdir(parents=True)
    (core / "app.py").write_text(
        "from okto_pulse.core.kg.workers.cleanup import get_cleanup_worker\n\n"
        "async def boot():\n"
        "    worker = get_cleanup_worker()\n"
        "    await worker.start()\n",
        encoding="utf-8",
    )

    report = RuntimeWorkerBoundaryGate().run(source_root=tmp_path)

    assert report.status == "blocking"
    offenders = report.evidence["offenders"]
    assert any(
        item["kind"] == "direct_worker_import"
        and item["symbol"] == "get_cleanup_worker"
        for item in offenders
    )


def test_runtime_worker_boundary_gate_blocks_worker_module_alias(tmp_path) -> None:
    core = tmp_path / "okto_pulse" / "core"
    core.mkdir(parents=True)
    (core / "app.py").write_text(
        "import okto_pulse.core.events.dispatcher as dispatcher\n\n"
        "async def boot(session_factory):\n"
        "    worker = dispatcher.EventDispatcher(session_factory)\n"
        "    await worker.start()\n",
        encoding="utf-8",
    )

    report = RuntimeWorkerBoundaryGate().run(source_root=tmp_path)

    assert report.status == "blocking"
    offenders = report.evidence["offenders"]
    assert any(
        item["kind"] == "direct_worker_module_import"
        and item["symbol"] == "okto_pulse.core.events.dispatcher"
        for item in offenders
    )
    assert any(
        item["kind"] == "direct_worker_attribute_reference"
        and item["symbol"] == "EventDispatcher"
        for item in offenders
    )


def test_runtime_worker_boundary_gate_blocks_private_tick_import(tmp_path) -> None:
    community = tmp_path / "okto_pulse" / "community"
    community.mkdir(parents=True)
    (community / "main.py").write_text(
        "from okto_pulse.community.app import _emit_daily_tick\n",
        encoding="utf-8",
    )

    report = RuntimeWorkerBoundaryGate().run(community_source_root=tmp_path)

    assert report.status == "blocking"
    offenders = report.evidence["offenders"]
    assert any(
        item["kind"] == "private_tick_import"
        and item["symbol"] == "_emit_daily_tick"
        for item in offenders
    )


def test_runtime_worker_boundary_gate_blocks_private_tick_module_alias(
    tmp_path,
) -> None:
    community = tmp_path / "okto_pulse" / "community"
    community.mkdir(parents=True)
    (community / "main.py").write_text(
        "import okto_pulse.community.app as core_app\n\n"
        "async def tick():\n"
        "    await core_app._emit_daily_tick()\n",
        encoding="utf-8",
    )

    report = RuntimeWorkerBoundaryGate().run(community_source_root=tmp_path)

    assert report.status == "blocking"
    offenders = report.evidence["offenders"]
    assert any(
        item["kind"] == "private_tick_module_import"
        and item["symbol"] == "okto_pulse.community.app"
        for item in offenders
    )
    assert any(
        item["kind"] == "private_tick_attribute_reference"
        and item["symbol"] == "_emit_daily_tick"
        for item in offenders
    )
