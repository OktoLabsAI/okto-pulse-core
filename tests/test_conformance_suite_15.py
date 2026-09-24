"""Runtime boundary gates and surviving scheduler contracts after F4 tuning retirement."""

from __future__ import annotations


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


# --------------------------------------------------------------------------- #
# Fakes
# --------------------------------------------------------------------------- #


# --------------------------------------------------------------------------- #
# Settings effect split — ts_51bb35b4 / ts_78f653b4
# --------------------------------------------------------------------------- #


# --------------------------------------------------------------------------- #
# AntiSingletonGate — ac_d5c5864f / ac_1e9c0475
# --------------------------------------------------------------------------- #
def test_anti_singleton_clean_tree_passes() -> None:
    report = AntiSingletonGate().run()
    assert report.status == "passed"
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
    assert SINGLETON_LEDGER == {}


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
    assert report.status == "passed"
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

    assert report.status == "passed"
    assert report.evidence["new_singletons"] == []


# --------------------------------------------------------------------------- #
# PortConformanceGate — ac_c43799ff axis support
# --------------------------------------------------------------------------- #
def test_port_conformance_passes_for_four_protocols() -> None:
    report = PortConformanceGate().run()
    assert report.status == "passed"
    assert set(report.evidence["ports_checked"]) == {
        "SchedulerControl",
        "RuntimeSettingsStartupPort",
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
    assert report["axes"]["singleton"]["status"] == "passed"
    assert report["axes"]["port_conformance"]["status"] == "passed"
    assert report["axes"]["scheduler_control_symbol"]["status"] == "passed"
    assert report["axes"]["lifecycle_fallback"]["status"] == "passed"
    assert report["axes"]["runtime_worker_boundary"]["status"] == "passed"
    assert report["axes"]["af30_3c_relational_residue"]["status"] == "passed"
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
