"""Spec #15 card 03829e48 — conformance suite, AntiSingletonGate, port
conformance and the runtime settings effect split.

Covers ts_51bb35b4 (persist without scheduler), ts_78f653b4 (apply_runtime_effects
via SchedulerControl) and the AC line ac_a4d41673/ac_c9b328fb/ac_b74a281f/
ac_d5c5864f/ac_1e9c0475/ac_c43799ff.
"""

from __future__ import annotations

import logging

import pytest

from okto_pulse.core.application.boundary.conformance_suite import (
    AXES,
    ConformanceSuite,
    scheduler_signal_conformance,
    settings_split_conformance,
)
from okto_pulse.core.application.boundary.port_conformance import (
    PortConformanceGate,
    _protocol_members,
)
from okto_pulse.core.application.boundary.scheduler_control_symbol_gate import (
    SchedulerControlSymbolGate,
)
from okto_pulse.core.application.boundary.singleton_gate import (
    SINGLETON_LEDGER,
    AntiSingletonGate,
    AntiSingletonGateInput,
)
from okto_pulse.core.infra.config import configure_settings, get_settings
from okto_pulse.core.ports.scheduler import (
    KG_DAILY_TICK_JOB_ID,
    JobSpec,
    SchedulerJobSnapshot,
    SchedulerResult,
)
from okto_pulse.core.services.settings_service import (
    _apply_live_tick_settings,
    apply_tick_runtime_effects,
)


# --------------------------------------------------------------------------- #
# Fakes
# --------------------------------------------------------------------------- #
class _FakeSchedulerControl:
    """In-memory SchedulerControl for the settings-effect tests."""

    def __init__(self, *, available: bool = True, raises: Exception | None = None) -> None:
        self._available = available
        self._raises = raises
        self.calls: list[tuple[str, dict]] = []

    def is_available(self) -> bool:
        return self._available

    async def reschedule_job(self, job_id: str, trigger) -> SchedulerResult:
        self.calls.append((job_id, dict(trigger)))
        if self._raises is not None:
            raise self._raises
        return SchedulerResult(job_id=job_id, scheduled=True, audit_status="rescheduled")

    async def register_job(self, job_spec: JobSpec, handler) -> SchedulerResult:
        return SchedulerResult(
            job_id=job_spec.job_id,
            scheduled=True,
            audit_status="rescheduled",
        )

    async def get_job_snapshot(self, job_id: str) -> SchedulerJobSnapshot:
        return SchedulerJobSnapshot(job_id=job_id, exists=True)

    async def shutdown(self, wait: bool = False) -> None:  # pragma: no cover - trivial
        return None


# --------------------------------------------------------------------------- #
# Settings effect split — ts_51bb35b4 / ts_78f653b4
# --------------------------------------------------------------------------- #
def test_persist_live_tick_settings_no_scheduler_access() -> None:
    # ts_51bb35b4 / ac_a4d41673: persistence runs with NO scheduler wired and
    # only updates live CoreSettings. _apply_live_tick_settings takes no scheduler
    # argument and never imports the singleton — structurally scheduler-free.
    original = get_settings()
    try:
        _apply_live_tick_settings({"kg_decay_tick_interval_minutes": 4242})
        assert get_settings().kg_decay_tick_interval_minutes == 4242
    finally:
        configure_settings(original)


@pytest.mark.asyncio
async def test_apply_runtime_effects_reschedules_via_port() -> None:
    # ts_78f653b4 / ac_c9b328fb: with a SchedulerControl available, a tick change
    # calls reschedule_job(kg_daily_tick) and the success stays auditable.
    control = _FakeSchedulerControl(available=True)
    results = await apply_tick_runtime_effects(
        {"kg_decay_tick_interval_minutes": 15}, control
    )
    assert control.calls == [(KG_DAILY_TICK_JOB_ID, {"minutes": 15})]
    assert len(results) == 1
    assert results[0].status == "applied"
    assert results[0].job_id == KG_DAILY_TICK_JOB_ID
    assert results[0].audit_status == "rescheduled"


@pytest.mark.asyncio
async def test_apply_runtime_effects_skips_when_no_scheduler() -> None:
    # ac_a4d41673 path: no scheduler wired -> skipped, never an access/raise.
    control = _FakeSchedulerControl(available=False)
    results = await apply_tick_runtime_effects(
        {"kg_decay_tick_interval_minutes": 30}, control
    )
    assert control.calls == []
    assert results[0].status == "skipped"
    assert results[0].audit_status == "skipped"


@pytest.mark.asyncio
async def test_apply_runtime_effects_failure_emits_sanitized_signal() -> None:
    # ac_b74a281f / fr_70c29790: a reschedule failure emits kg.tick.reschedule_failed
    # with error_class and a sanitized message — never the secret, never a raise.
    boom = RuntimeError("scheduler down token=SUPER-SECRET password=hunter2")
    control = _FakeSchedulerControl(available=True, raises=boom)
    results = await apply_tick_runtime_effects(
        {"kg_decay_tick_interval_minutes": 10}, control, actor_id="op-9"
    )
    res = results[0]
    assert res.status == "failed"
    assert res.signal == "kg.tick.reschedule_failed"
    assert res.error_class == "RuntimeError"
    assert "SUPER-SECRET" not in (res.sanitized_message or "")
    assert "hunter2" not in (res.sanitized_message or "")


@pytest.mark.asyncio
async def test_apply_runtime_effects_noop_without_interval() -> None:
    control = _FakeSchedulerControl(available=True)
    results = await apply_tick_runtime_effects({"kg_decay_tick_staleness_days": 3}, control)
    assert results == []
    assert control.calls == []


# --------------------------------------------------------------------------- #
# AntiSingletonGate — ac_d5c5864f / ac_1e9c0475
# --------------------------------------------------------------------------- #
def test_anti_singleton_clean_tree_is_baseline() -> None:
    report = AntiSingletonGate().run()
    assert report.status == "baseline"
    assert report.evidence["new_singletons"] == []
    assert set(report.evidence["ledger"]) == set(SINGLETON_LEDGER)


def test_anti_singleton_ledger_carries_register_before_remove_metadata() -> None:
    # ac_1e9c0475: each existing singleton has owner, target adapter and a
    # retirement criterion.
    for name, meta in SINGLETON_LEDGER.items():
        assert meta["owner"], name
        assert meta["target_provider"], name
        assert meta["expected_adapter"], name
        assert meta["retirement_criterion"], name
    required_legacy_singletons = {
        "_mcp_session_factory",
        "_permission_cache",
    }
    assert required_legacy_singletons <= set(SINGLETON_LEDGER)


def test_anti_singleton_new_global_singleton_blocks(tmp_path) -> None:
    # ac_d5c5864f: a NEW module-global singleton blocks with file, symbol and
    # remediation_hint.
    mod = tmp_path / "okto_pulse" / "core" / "newmod.py"
    mod.parent.mkdir(parents=True)
    mod.write_text(
        "_new_singleton = None\n\n\ndef set_it(x):\n    global _new_singleton\n"
        "    _new_singleton = x\n",
        encoding="utf-8",
    )
    report = AntiSingletonGate().run(AntiSingletonGateInput(source_root=tmp_path))
    assert report.status == "blocking"
    assert report.evidence["error"] == "new_singleton"
    new = report.evidence["new_singletons"]
    assert any(s["name"] == "_new_singleton" and "newmod.py" in s["file"] for s in new)
    assert report.remediation_hint


def test_anti_singleton_module_constants_are_not_flagged(tmp_path) -> None:
    # lookup tables / __all__ / sample buffers are NOT singletons.
    mod = tmp_path / "okto_pulse" / "core" / "constmod.py"
    mod.parent.mkdir(parents=True)
    mod.write_text(
        "__all__ = ['x']\n_RANK = {'a': 1}\n_BUF = []\n_PAIRS = ('a', 'b')\n",
        encoding="utf-8",
    )
    report = AntiSingletonGate().run(AntiSingletonGateInput(source_root=tmp_path))
    assert report.status == "baseline"
    assert report.evidence["new_singletons"] == []


def test_anti_singleton_provider_bridge_cache_state_blocks(tmp_path) -> None:
    mod = (
        tmp_path
        / "okto_pulse"
        / "core"
        / "kg"
        / "rogue"
        / "llm_provider_bridges.py"
    )
    mod.parent.mkdir(parents=True)
    mod.write_text(
        "_bridge_cache = {}\n"
        "_bridge_lock = object()\n\n"
        "def memoize(key, value):\n"
        "    _bridge_cache[key] = value\n"
        "    return _bridge_cache[key]\n",
        encoding="utf-8",
    )

    report = AntiSingletonGate().run(AntiSingletonGateInput(source_root=tmp_path))

    assert report.status == "blocking"
    assert report.evidence["error"] == "new_singleton"
    new = {(s["name"], s["kind"]) for s in report.evidence["new_singletons"]}
    assert ("_bridge_cache", "provider_bridge_global_state") in new
    assert ("_bridge_lock", "provider_bridge_global_state") in new


def test_anti_singleton_provider_bridge_namespace_constant_is_not_flagged(tmp_path) -> None:
    mod = (
        tmp_path
        / "okto_pulse"
        / "core"
        / "kg"
        / "clean"
        / "llm_provider_bridges.py"
    )
    mod.parent.mkdir(parents=True)
    mod.write_text(
        "_BRIDGE_CACHE_NAMESPACE = 'kg.clean'\n"
        "def make():\n"
        "    return _BRIDGE_CACHE_NAMESPACE\n",
        encoding="utf-8",
    )

    report = AntiSingletonGate().run(AntiSingletonGateInput(source_root=tmp_path))

    assert report.status == "baseline"
    assert report.evidence["new_singletons"] == []


# --------------------------------------------------------------------------- #
# PortConformanceGate — ac_c43799ff axis support
# --------------------------------------------------------------------------- #
def test_port_conformance_passes_for_four_protocols() -> None:
    report = PortConformanceGate().run()
    assert report.status == "passed"
    assert set(report.evidence["ports_checked"]) == {
        "SchedulerControl",
        "RuntimeSettingsPort",
        "RuntimeControl",
        "RuntimeEventBusPort",
    }
    assert report.evidence["adapters_checked"] == []
    assert report.evidence["adapter_conformance_owner"] == "edition"
    assert report.evidence["findings"] == []


def test_port_conformance_detects_nonconformant_class() -> None:
    # the structural check has teeth: a class missing methods is not the port.
    from okto_pulse.core.ports.scheduler import SchedulerControl as SC

    class _Broken:
        def is_available(self) -> bool:
            return True

    assert not isinstance(_Broken(), SC)
    assert _protocol_members(SC) == {
        "get_job_snapshot",
        "is_available",
        "register_job",
        "reschedule_job",
        "shutdown",
    }


# --------------------------------------------------------------------------- #
# SchedulerControlSymbolGate — R08 core concrete removal
# --------------------------------------------------------------------------- #
def test_scheduler_control_symbol_gate_clean_tree_passes() -> None:
    report = SchedulerControlSymbolGate().run()
    assert report.status == "passed"
    assert report.evidence["offenders"] == []


def test_scheduler_control_symbol_gate_blocks_core_class_or_import(tmp_path) -> None:
    mod = tmp_path / "okto_pulse" / "core" / "services" / "bad_scheduler.py"
    mod.parent.mkdir(parents=True)
    mod.write_text(
        "from somewhere import SingletonSchedulerControl\n\n"
        "class SingletonSchedulerControl:\n"
        "    pass\n",
        encoding="utf-8",
    )
    report = SchedulerControlSymbolGate().run(source_root=tmp_path)
    assert report.status == "blocking"
    kinds = {item["kind"] for item in report.evidence["offenders"]}
    assert {"from_import", "class_def"} <= kinds


# --------------------------------------------------------------------------- #
# ConformanceSuite — fr_13fe67d0
# --------------------------------------------------------------------------- #
def test_conformance_suite_reports_per_axis() -> None:
    report = ConformanceSuite().run()
    assert set(report["axes"]) == set(AXES)
    allowed = {"passed", "baseline", "xfail_advisory", "blocking", "reject"}
    for axis, r in report["axes"].items():
        assert r["status"] in allowed, axis
        assert r["owner"], axis
    assert report["axes"]["singleton"]["status"] == "baseline"
    assert report["axes"]["port_conformance"]["status"] == "passed"
    assert report["axes"]["scheduler_control_symbol"]["status"] == "passed"
    assert report["axes"]["lifecycle_fallback"]["status"] == "passed"
    assert report["axes"]["runtime_worker_boundary"]["status"] == "passed"
    assert report["axes"]["runtime_settings_effect_split"]["status"] == "baseline"
    assert report["axes"]["scheduler_signal"]["status"] == "baseline"
    # the two integration axes are deferred, with a runnable promotion path
    assert "community_smoke" in report["advisory_axes"]
    assert "mcp_replay" in report["advisory_axes"]
    assert report["blocking_axes"] == []


def test_conformance_settings_split_blocks_on_monolith(tmp_path) -> None:
    svc = tmp_path / "okto_pulse" / "core" / "services" / "settings_service.py"
    svc.parent.mkdir(parents=True)
    svc.write_text(
        "def _maybe_reschedule_tick(values):\n"
        "    from okto_pulse.core.kg.scheduler_singleton import get_scheduler\n"
        "    get_scheduler()\n",
        encoding="utf-8",
    )
    report = settings_split_conformance(tmp_path)
    assert report.status == "blocking"
    assert "monolith_removed" in report.evidence["failed"]
    assert "no_direct_singleton_import" in report.evidence["failed"]


def test_conformance_scheduler_signal_is_secret_free() -> None:
    report = scheduler_signal_conformance()
    assert report.status == "baseline"
    assert report.evidence["error"] == "signal_contract_ok"


# --------------------------------------------------------------------------- #
# Rework regressions (codex validation of 03829e48)
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_apply_runtime_effects_failure_log_text_is_sanitized(caplog) -> None:
    # blocker 1: the textual failure log must use the sanitized message, never the
    # raw exception (which can carry a secret/token).
    boom = RuntimeError("scheduler down token=SUPER-SECRET password=hunter2")
    control = _FakeSchedulerControl(available=True, raises=boom)
    with caplog.at_level(logging.WARNING, logger="okto_pulse.services.settings"):
        await apply_tick_runtime_effects({"kg_decay_tick_interval_minutes": 10}, control)
    blob = " ".join(r.getMessage() for r in caplog.records)
    assert "kg.tick.reschedule_failed" in blob
    assert "SUPER-SECRET" not in blob
    assert "hunter2" not in blob
    assert "[REDACTED]" in blob


def test_conformance_suite_singleton_axis_honours_source_root(tmp_path) -> None:
    # blocker 2: ConformanceSuite must propagate source_root to AntiSingletonGate.
    mod = tmp_path / "okto_pulse" / "core" / "x.py"
    mod.parent.mkdir(parents=True)
    mod.write_text(
        "_x = None\n\n\ndef set_x(v):\n    global _x\n    _x = v\n", encoding="utf-8"
    )
    report = ConformanceSuite().run(source_root=tmp_path)
    assert report["axes"]["singleton"]["status"] == "blocking"
    assert "singleton" in report["blocking_axes"]


@pytest.mark.asyncio
async def test_apply_runtime_effects_respects_port_skipped_result() -> None:
    # blocker 3: a port that reports the job was NOT scheduled (skipped) must yield
    # a skipped effect, not applied/rescheduled.
    class _SkippingControl(_FakeSchedulerControl):
        async def reschedule_job(self, job_id, trigger):
            self.calls.append((job_id, dict(trigger)))
            return SchedulerResult(job_id=job_id, scheduled=False, audit_status="skipped")

    control = _SkippingControl(available=True)
    results = await apply_tick_runtime_effects(
        {"kg_decay_tick_interval_minutes": 12}, control
    )
    assert control.calls == [(KG_DAILY_TICK_JOB_ID, {"minutes": 12})]
    assert results[0].status == "skipped"
    assert results[0].audit_status == "skipped"
