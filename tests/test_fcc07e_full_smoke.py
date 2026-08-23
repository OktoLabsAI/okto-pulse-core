"""FCC-07E-IMP4 — Final Clean Core FULL-MODE smoke integration tests.

Covers the closed E-IMP4 scope: wiring the two #12 smoke gates into the FCC-07E
``full`` mode via INJECTABLE adapters + temporary isolation. The teeth are real
and deterministic — NO real venv/pip/subprocess is ever run:

* FAKE adapter returning ``passed``  -> incorporated as a ``success`` runner row.
* FAKE adapter returning ``blocking`` -> incorporated as a ``blocked`` runner row.
* PREREQUISITES ABSENT (injected unavailable check) -> every gate is ``skipped``
  with an honest reason and is NEVER even run, so it can never become a fake
  ``success`` (the key anti-requirement).
* Temporary isolation: the sandbox is really created, the adapter writes an
  artifact INTO it, and after the run the sandbox (and its artifact) is gone while
  a user file OUTSIDE the sandbox is untouched.
* Determinism, the ``success|blocked|skipped`` enum, no Community import, the real
  adapter confining the real gate's venv to the sandbox, and the runner-shell
  incorporation (reused verbatim in ``full`` mode).
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

import okto_pulse.core.application.boundary.final_clean_core_full_smoke as _mod
from okto_pulse.core.application.boundary.community_smoke import CommandResult
from okto_pulse.core.application.boundary.final_clean_core_full_smoke import (
    CoreSmokeInstallAdapter,
    FullModePrerequisites,
    PrerequisiteCheck,
    SmokeIsolation,
    detect_full_prerequisites,
    run_final_clean_core_full_mode,
    run_full_mode_smoke,
)
from okto_pulse.core.application.boundary.final_clean_core_runner import (
    RunnerGateResult,
)
from okto_pulse.core.application.boundary.report import GateReport
from okto_pulse.core.application.boundary.community_smoke import SmokeInstallInput

MODULE_PY = Path(_mod.__file__).resolve()

_AVAILABLE = PrerequisiteCheck(available=True, reason="ok")


class _FakeAdapter:
    """Deterministic injected adapter — returns a fixed GateReport status and
    records whether it was actually run (so the skip path can prove non-execution)."""

    def __init__(self, gate_id: str, status: str, *, spec_id: str = "FCC-07E"):
        self.gate_id = gate_id
        self.spec_id = spec_id
        self.status = status
        self.calls = 0

    def run(self, isolation: SmokeIsolation) -> GateReport:
        self.calls += 1
        return GateReport(
            gate_id=self.gate_id,
            subject="fake smoke",
            status=self.status,  # type: ignore[arg-type]
            remediation_hint=None if self.status == "passed" else "fix the smoke",
        )


def _passing_gate(gate_id: str, *, status: str = "success") -> RunnerGateResult:
    return RunnerGateResult(
        gate_id=gate_id,
        spec_id="FCC-07A",
        status=status,  # type: ignore[arg-type]
        owner="okto-pulse-core/boundary",
    )


# --------------------------------------------------------------------------- #
# FAKE success / failure -> incorporated runner rows.
# --------------------------------------------------------------------------- #
def test_fake_success_maps_to_runner_success_and_command_result():
    adapter = _FakeAdapter("core_smoke_install", "passed")
    result = run_full_mode_smoke(adapters=[adapter], prerequisites=_AVAILABLE)

    assert adapter.calls == 1  # the gate really ran inside the sandbox
    assert len(result.gate_results) == 1
    row = result.gate_results[0]
    assert row.gate_id == "core_smoke_install"
    assert row.status == "success"
    # a supplementary command row mirrors the gate that ran, exit_code 0.
    assert len(result.command_results) == 1
    assert result.command_results[0].exit_code == 0
    assert result.ran is True


def test_fake_failure_maps_to_blocked_and_failed_command():
    adapter = _FakeAdapter("community_rebuild_reinstall_smoke", "blocking")
    result = run_full_mode_smoke(adapters=[adapter], prerequisites=_AVAILABLE)

    row = result.gate_results[0]
    assert row.status == "blocked"
    assert row.remediation == "fix the smoke"
    assert result.command_results[0].exit_code == 1


# --------------------------------------------------------------------------- #
# KEY anti-requirement: prerequisites absent -> skipped-honest, NEVER success.
# --------------------------------------------------------------------------- #
def test_prereqs_absent_emits_skipped_honest_never_fake_success():
    reason = "wheel artifact not built: dist/okto_pulse_core-0.3.3-py3-none-any.whl"
    adapters = [
        _FakeAdapter("core_smoke_install", "passed"),
        _FakeAdapter("community_rebuild_reinstall_smoke", "passed"),
    ]
    result = run_full_mode_smoke(
        adapters=adapters,
        prerequisites=PrerequisiteCheck(available=False, reason=reason),
    )

    # No sandbox was created and no command rows were emitted.
    assert result.isolation_root is None
    assert result.command_results == ()
    assert result.ran is False
    # The gates were NEVER run -> a fake success is impossible.
    assert all(a.calls == 0 for a in adapters)
    for row in result.gate_results:
        assert row.status == "skipped"
        assert row.status != "success"
        assert row.remediation is not None
        assert "full prerequisites unavailable" in row.remediation
        assert reason in row.remediation


def test_advisory_report_status_maps_to_skipped_not_success():
    # A baseline/xfail_advisory waiver is NOT a clean success and must not be
    # reported as one — it collapses to skipped.
    result = run_full_mode_smoke(
        adapters=[_FakeAdapter("g", "xfail_advisory")], prerequisites=_AVAILABLE
    )
    row = result.gate_results[0]
    assert row.status == "skipped"
    assert row.status != "success"
    # a skipped gate emits no command row.
    assert result.command_results == ()


# --------------------------------------------------------------------------- #
# Temporary isolation: created, cleaned, and user files untouched.
# --------------------------------------------------------------------------- #
def test_temp_isolation_is_created_cleaned_and_user_files_untouched(tmp_path):
    temp_root = tmp_path / "sandbox_root"
    temp_root.mkdir()
    user_dir = tmp_path / "user_files"
    user_dir.mkdir()
    user_file = user_dir / "important.txt"
    user_file.write_text("DO NOT DELETE", encoding="utf-8")

    captured: dict[str, Path] = {}

    class _WritingAdapter:
        gate_id = "core_smoke_install"
        spec_id = "FCC-07E"

        def run(self, isolation: SmokeIsolation) -> GateReport:
            # the sandbox is real and writable; simulate venv/install artifacts.
            assert isolation.root.exists()
            marker = isolation.venv_dir() / "pyvenv.cfg"
            marker.parent.mkdir(parents=True, exist_ok=True)
            marker.write_text("home = /isolated", encoding="utf-8")
            captured["marker"] = marker
            assert marker.exists()
            return GateReport(gate_id=self.gate_id, subject="x", status="passed")

    result = run_full_mode_smoke(
        adapters=[_WritingAdapter()],
        prerequisites=_AVAILABLE,
        temp_root=temp_root,
    )

    assert result.gate_results[0].status == "success"
    # the sandbox that WAS used is gone (cleaned), and so is the artifact in it.
    assert result.isolation_root is not None
    assert not Path(result.isolation_root).exists()
    assert not captured["marker"].exists()
    # the sandbox lived strictly UNDER temp_root (never near the user files).
    assert Path(result.isolation_root).parent == temp_root
    # cleanup removed ONLY the sandbox: temp_root survives...
    assert temp_root.exists()
    # ...and NOTHING outside the sandbox was touched — the user file is intact.
    assert user_file.exists()
    assert user_file.read_text(encoding="utf-8") == "DO NOT DELETE"


def test_isolation_is_cleaned_even_when_adapter_raises(tmp_path):
    class _BoomAdapter:
        gate_id = "core_smoke_install"
        spec_id = "FCC-07E"

        def run(self, isolation: SmokeIsolation) -> GateReport:
            (isolation.root / "scratch").write_text("x", encoding="utf-8")
            raise RuntimeError("smoke blew up")

    with pytest.raises(RuntimeError, match="smoke blew up"):
        run_full_mode_smoke(
            adapters=[_BoomAdapter()], prerequisites=_AVAILABLE, temp_root=tmp_path
        )
    # the TemporaryDirectory cleaned the sandbox despite the exception; only the
    # uniquely-named sandbox was under tmp_path and it is gone.
    assert list(tmp_path.iterdir()) == []


# --------------------------------------------------------------------------- #
# Determinism + status enum.
# --------------------------------------------------------------------------- #
def test_deterministic_regardless_of_adapter_order():
    def build(gate_ids_and_status):
        adapters = [_FakeAdapter(gid, st) for gid, st in gate_ids_and_status]
        out = run_full_mode_smoke(adapters=adapters, prerequisites=_AVAILABLE).as_dict()
        # isolation_root is a unique temp name by construction; drop it so the
        # comparison is over the deterministic gate/command rows.
        out.pop("isolation_root")
        return out

    spec = [("z_gate", "passed"), ("a_gate", "blocking"), ("m_gate", "passed")]
    assert build(spec) == build(list(reversed(spec)))
    # rows are sorted by the shell's ordering keys.
    ordered = build(spec)
    gate_ids = [g["gate_id"] for g in ordered["gate_results"]]
    assert gate_ids == sorted(gate_ids)


def test_status_enum_only_success_blocked_skipped():
    ran = run_full_mode_smoke(
        adapters=[_FakeAdapter("g1", "passed"), _FakeAdapter("g2", "blocking")],
        prerequisites=_AVAILABLE,
    )
    skipped = run_full_mode_smoke(
        adapters=[_FakeAdapter("g3", "passed")],
        prerequisites=PrerequisiteCheck(available=False, reason="x"),
    )
    statuses = {g.status for g in ran.gate_results} | {
        g.status for g in skipped.gate_results
    }
    assert statuses <= {"success", "blocked", "skipped"}
    assert "passed" not in statuses and "blocking" not in statuses


def test_prerequisites_accepts_callable():
    result = run_full_mode_smoke(
        adapters=[_FakeAdapter("g", "passed")],
        prerequisites=lambda: PrerequisiteCheck(available=True, reason="late"),
    )
    assert result.gate_results[0].status == "success"


def test_invalid_prerequisites_is_rejected():
    with pytest.raises(TypeError):
        run_full_mode_smoke(adapters=[], prerequisites="nope")  # type: ignore[arg-type]


# --------------------------------------------------------------------------- #
# Real prerequisite detection (deterministic; never builds anything).
# --------------------------------------------------------------------------- #
def test_detect_prereqs_missing_wheel_is_unavailable(tmp_path):
    check = detect_full_prerequisites(
        FullModePrerequisites(required_wheels=(tmp_path / "nope.whl",)),
        temp_root=tmp_path,
    )
    assert check.available is False
    assert "wheel artifact not built" in check.reason


def test_detect_prereqs_no_wheel_declared_is_unavailable(tmp_path):
    check = detect_full_prerequisites(FullModePrerequisites(), temp_root=tmp_path)
    assert check.available is False
    assert "no wheel artifact declared" in check.reason


def test_detect_prereqs_available_when_wheel_present(tmp_path):
    wheel = tmp_path / "okto_pulse_core-0.3.3-py3-none-any.whl"
    wheel.write_bytes(b"PK\x03\x04")  # a present (dummy) wheel artifact
    check = detect_full_prerequisites(
        FullModePrerequisites(required_wheels=(wheel,)),
        temp_root=tmp_path,
    )
    assert check.available is True
    assert "present" in check.reason


# --------------------------------------------------------------------------- #
# Real adapter wiring: the REAL CoreSmokeInstallGate runs with a deterministic
# fake CommandRunner (#12 pattern) — no real venv/pip — and its isolated venv is
# confined to the sandbox under temp_root.
# --------------------------------------------------------------------------- #
def test_real_core_adapter_confines_install_to_sandbox(tmp_path):
    calls: list[str] = []

    class _Runner:
        def run(self, command: str) -> CommandResult:
            calls.append(command)
            return CommandResult(command=command, returncode=0, ok=True, output_tail="")

    adapter = CoreSmokeInstallAdapter(
        gate_input=SmokeInstallInput(
            wheel_path=Path("dist/core.whl"),
            expected_imports=("okto_pulse.core.application.boundary",),
        ),
        runner=_Runner(),
    )
    result = run_full_mode_smoke(
        adapters=[adapter], prerequisites=_AVAILABLE, temp_root=tmp_path
    )

    assert result.gate_results[0].status == "success"
    assert calls, "the real gate must have constructed its commands"
    # the venv was created UNDER tmp_path's sandbox (isolation), never the host env.
    assert any("venv" in c and str(tmp_path) in c for c in calls)
    assert not any("community" in c.lower() for c in calls)
    # the sandbox is cleaned afterwards.
    assert not Path(result.isolation_root).exists()


# --------------------------------------------------------------------------- #
# Incorporation into the reused E-IMP1 shell (full mode).
# --------------------------------------------------------------------------- #
def test_incorporated_into_runner_full_mode_blocks_on_smoke_failure():
    smoke = run_full_mode_smoke(
        adapters=[_FakeAdapter("community_rebuild_reinstall_smoke", "blocking")],
        prerequisites=_AVAILABLE,
    )
    report = run_final_clean_core_full_mode(
        smoke=smoke, gate_results=[_passing_gate("FCC07A")]
    )
    assert report.mode == "full"
    assert report.overall_status == "blocked"
    assert report.exit_code != 0
    assert any(
        g.gate_id == "community_rebuild_reinstall_smoke" and g.status == "blocked"
        for g in report.gates
    )


def test_incorporated_into_runner_full_mode_success():
    smoke = run_full_mode_smoke(
        adapters=[_FakeAdapter("core_smoke_install", "passed")],
        prerequisites=_AVAILABLE,
    )
    report = run_final_clean_core_full_mode(
        smoke=smoke, gate_results=[_passing_gate("FCC07A")]
    )
    assert report.overall_status == "success"
    assert report.exit_code == 0
    assert report.mode == "full"


def test_incorporated_skipped_smoke_does_not_block_runner():
    smoke = run_full_mode_smoke(
        adapters=[_FakeAdapter("core_smoke_install", "passed")],
        prerequisites=PrerequisiteCheck(available=False, reason="wheel not built"),
    )
    report = run_final_clean_core_full_mode(
        smoke=smoke, gate_results=[_passing_gate("FCC07A")]
    )
    assert report.summary.skipped == 1
    assert report.summary.blocking == 0
    assert report.overall_status == "success"
    assert report.exit_code == 0


# --------------------------------------------------------------------------- #
# Boundary hygiene + public surface.
# --------------------------------------------------------------------------- #
def test_module_does_not_import_community():
    tree = ast.parse(MODULE_PY.read_text(encoding="utf-8"))
    imported: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.append(node.module)
    assert not any(name.startswith("okto_pulse.community") for name in imported), imported


def test_exported_from_boundary_package():
    import okto_pulse.core.application.boundary as boundary

    assert "run_full_mode_smoke" in boundary.__all__
    assert "run_final_clean_core_full_mode" in boundary.__all__
    assert "SmokeGateAdapter" in boundary.__all__
    assert boundary.run_full_mode_smoke is run_full_mode_smoke
