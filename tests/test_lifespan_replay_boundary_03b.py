"""Spec #03 (card b4f07ad3) — CommunityLifespanReplay + CompositionBoundaryGate.

Locks the golden lifecycle replay (15 required events, closed 3-exclusion
allowlist, unexpected_lifecycle_delta / required_lifecycle_event_missing) and the
deterministic composition boundary classification (baseline / blocking /
xfail_advisory + register-before-remove).
"""

from __future__ import annotations

import pytest

from okto_pulse.core.application.boundary import (
    DEFAULT_ONLY_EXCLUSIONS,
    REQUIRED_LIFECYCLE_EVENTS,
    CommunityLifespanReplay,
    CommunityLifespanReplayInput,
    CompositionBoundaryGate,
    CompositionBoundaryGateInput,
    NamedLifecycleHook,
)
from okto_pulse.core.composition import RuntimeComposition, RuntimeProviderMissing

_STARTUP_EVENTS = [
    "init_db", "seed_community_defaults", "backfill_qa_answered_at",
    "_afg_backfill_task", "_preload_embedding_model", "event_dispatcher",
    "consolidation_worker", "cleanup_worker", "outbox_worker", "settings_service",
    "scheduler", "mcp_session_factory", "frontend_served", "_metrics_beacon_loop",
]


def _golden_hooks(extra=()):
    hooks = [NamedLifecycleHook(startup_event=e) for e in _STARTUP_EVENTS]
    hooks.append(NamedLifecycleHook(shutdown_event="close_db"))
    hooks.extend(extra)
    return tuple(hooks)


def _composition(hooks, **overrides) -> RuntimeComposition:
    base = dict(
        settings_provider=1, auth_provider=1, storage_provider=1,
        session_factory=1, event_bus=1, lifecycle_hooks=hooks,
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
    comp = _composition(_golden_hooks(), scheduler_control=1, mcp_session_factory=1)
    report = await CommunityLifespanReplay().run(
        CommunityLifespanReplayInput(composition=comp, include_frontend=True)
    )
    assert report.status == "passed", report.as_dict()
    observed = set(report.startup_events) | set(report.shutdown_events)
    assert set(REQUIRED_LIFECYCLE_EVENTS) <= observed
    assert report.workers == ["cleanup_worker", "consolidation_worker", "event_dispatcher", "outbox_worker"]
    assert report.scheduler == "running"
    assert report.mcp_session_factory == "registered"
    assert report.frontend == "served"
    assert report.unexpected_deltas == []


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
async def test_replay_strict_runtime_missing_provider_raises() -> None:
    comp = _composition(_golden_hooks(), event_bus=None)
    with pytest.raises(RuntimeProviderMissing):
        await CommunityLifespanReplay().run(
            CommunityLifespanReplayInput(composition=comp, strict_runtime=True)
        )


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
    assert baseline_keys, "the known catalogued debt must classify as baseline"
    assert baseline_keys <= DEFAULT_COMPOSITION_BASELINE
