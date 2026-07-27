"""FCC-07E-IMP3 — Final Clean Core FOCUSED-SUITE execution + failure-map tests.

Covers the closed E-IMP3 scope: registering and running the focused pytest
suites of the FCC-07E runner through an INJECTABLE
:class:`~okto_pulse.core.application.boundary.community_smoke.CommandRunner`
(the #12 pattern) and mapping each result into the FCC-07E-IMP1 contract.

The teeth are real and deterministic — NO real pytest is ever spawned (a fake
CommandRunner returns deterministic exit codes):

* a fake returning exit 0 -> every suite is a ``success`` runner row;
* a fake returning exit != 0 for one suite -> that suite is ``blocked`` with a
  SPECIFIC remediation that names the exact command, the exact failing test files
  AND the owning gate (the KEY FR6/AC6 anti-requirement — never a generic
  "pytest failed");
* command / exit_code / test_files / owner are mapped correctly;
* the run is deterministic regardless of suite order;
* the default set covers the five required groups (adapter readiness /
  dependency-conformance / fake-registry-guard / community-wiring /
  composition-smoke), and every default suite names test files that REALLY exist;
* the enum is ``success | blocked | skipped`` (never passed/blocking) and no
  ``okto_pulse.community`` import is needed.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

import okto_pulse.core.application.boundary.final_clean_core_focused_suites as _mod
from okto_pulse.core.application.boundary.community_smoke import CommandResult
from okto_pulse.core.application.boundary.final_clean_core_focused_suites import (
    DEFAULT_FOCUSED_SUITES,
    REQUIRED_SUITE_GATE_IDS,
    FocusedSuite,
    run_final_clean_core_quick,
    run_focused_suites,
)

MODULE_PY = Path(_mod.__file__).resolve()
#: repo root (…/okto_labs_pulse_core), two parents above this test's tests/ dir.
REPO_ROOT = Path(__file__).resolve().parents[1]


class _FakeRunner:
    """Deterministic injected CommandRunner — returns a fixed exit code per command
    (or a default) and records the exact commands it was asked to run, so a test
    can prove NO real pytest is spawned and the command strings are correct."""

    def __init__(self, *, default_exit: int = 0, by_substring: dict[str, int] | None = None):
        self.default_exit = default_exit
        self.by_substring = by_substring or {}
        self.commands: list[str] = []

    def run(self, command: str) -> CommandResult:
        self.commands.append(command)
        code = self.default_exit
        for needle, exit_code in self.by_substring.items():
            if needle in command:
                code = exit_code
                break
        return CommandResult(
            command=command, returncode=code, ok=code == 0, output_tail=""
        )


_SUITE_A = FocusedSuite(
    gate_id="FCC07B",
    owner="okto-pulse-core/boundary",
    spec_id="FCC-07B",
    test_files=("tests/test_fcc07b_readiness_aggregator.py",),
)
_SUITE_B = FocusedSuite(
    gate_id="FCC07D",
    owner="okto-pulse-core/runtime",
    spec_id="FCC-07D",
    test_files=(
        "tests/test_fcc07d_test_provider_policy.py",
        "tests/test_fcc07d_runtime_guard.py",
    ),
)


# --------------------------------------------------------------------------- #
# All suites pass (fake exit 0) -> every suite is a success row.
# --------------------------------------------------------------------------- #
def test_all_suites_pass_when_runner_returns_zero():
    runner = _FakeRunner(default_exit=0)
    result = run_focused_suites(suites=[_SUITE_A, _SUITE_B], command_runner=runner)

    # No real pytest was spawned — only the fake runner saw the commands.
    assert len(runner.commands) == 2
    assert all("uv run pytest" in c for c in runner.commands)

    assert {g.status for g in result.gate_results} == {"success"}
    assert all(c.exit_code == 0 for c in result.command_results)
    assert result.all_passed is True
    # no success row carries a (failure) remediation.
    assert all(g.remediation is None for g in result.gate_results)


# --------------------------------------------------------------------------- #
# KEY anti-requirement (FR6/AC6): a failed suite is blocked with a SPECIFIC
# remediation naming the command + the test files + the gate — never generic.
# --------------------------------------------------------------------------- #
def test_failed_suite_is_blocked_with_specific_remediation_not_generic():
    # only the FCC07D suite's command fails (exit 1); the FCC07B one passes.
    runner = _FakeRunner(by_substring={"test_fcc07d_runtime_guard.py": 1})
    result = run_focused_suites(suites=[_SUITE_A, _SUITE_B], command_runner=runner)

    by_gate = {g.gate_id: g for g in result.gate_results}
    passing = by_gate["FCC07B"]
    failing = by_gate["FCC07D"]

    assert passing.status == "success"
    assert failing.status == "blocked"

    remediation = failing.remediation
    assert remediation is not None
    # NOT a generic message.
    assert remediation != "pytest failed"
    assert "pytest failed" not in remediation.lower()

    # the SPECIFIC command is present...
    expected_command = _SUITE_B.command()
    assert expected_command in remediation
    # ...and EVERY failing test file is named...
    for test_file in _SUITE_B.test_files:
        assert test_file in remediation
    # ...and the owning gate (id AND owner) is named...
    assert _SUITE_B.gate_id in remediation
    assert _SUITE_B.owner in remediation
    # ...and the exit code is surfaced.
    assert "exit_code=1" in remediation

    # the gate row itself also carries the command + test_file mapping.
    assert failing.command == expected_command
    assert all(tf in (failing.test_file or "") for tf in _SUITE_B.test_files)


def test_failed_command_result_maps_command_exit_test_files_owner():
    runner = _FakeRunner(by_substring={"test_fcc07d": 2})
    result = run_focused_suites(suites=[_SUITE_A, _SUITE_B], command_runner=runner)

    by_command_gate = {c.owner: c for c in result.command_results}
    failed = next(c for c in result.command_results if c.failed)

    assert failed.command == _SUITE_B.command()
    assert failed.exit_code == 2
    assert failed.test_files == _SUITE_B.test_files
    assert failed.owner == _SUITE_B.owner
    # the passing suite's command row is mapped too.
    assert by_command_gate["okto-pulse-core/boundary"].exit_code == 0


# --------------------------------------------------------------------------- #
# Determinism — output is independent of suite iteration order.
# --------------------------------------------------------------------------- #
def test_deterministic_regardless_of_suite_order():
    runner1 = _FakeRunner(by_substring={"test_fcc07d_runtime_guard.py": 1})
    runner2 = _FakeRunner(by_substring={"test_fcc07d_runtime_guard.py": 1})
    forward = run_focused_suites(suites=[_SUITE_A, _SUITE_B], command_runner=runner1)
    reverse = run_focused_suites(suites=[_SUITE_B, _SUITE_A], command_runner=runner2)
    assert forward.as_dict() == reverse.as_dict()
    # rows are sorted by the shell's ordering keys (gate_id first).
    gate_ids = [g.gate_id for g in forward.gate_results]
    assert gate_ids == sorted(gate_ids)


# --------------------------------------------------------------------------- #
# Status enum is success|blocked|skipped (never passed/blocking).
# --------------------------------------------------------------------------- #
def test_status_enum_only_success_blocked_skipped():
    runner = _FakeRunner(by_substring={"test_fcc07d": 1})
    result = run_focused_suites(suites=[_SUITE_A, _SUITE_B], command_runner=runner)
    statuses = {g.status for g in result.gate_results}
    assert statuses <= {"success", "blocked", "skipped"}
    assert "passed" not in statuses and "blocking" not in statuses


# --------------------------------------------------------------------------- #
# The default set covers the five required groups, with REAL test files.
# --------------------------------------------------------------------------- #
def test_default_set_covers_the_five_required_groups():
    gate_ids = {s.gate_id for s in DEFAULT_FOCUSED_SUITES}
    assert gate_ids == set(REQUIRED_SUITE_GATE_IDS)
    assert len(REQUIRED_SUITE_GATE_IDS) == 5


def test_every_default_suite_names_test_files_that_really_exist():
    for suite in DEFAULT_FOCUSED_SUITES:
        assert suite.test_files, suite.gate_id
        for test_file in suite.test_files:
            assert (REPO_ROOT / test_file).is_file(), f"{suite.gate_id}: {test_file}"
            # the derived command focuses on exactly those real files.
            assert test_file in suite.command()
        assert suite.command().startswith("uv run pytest")
        assert suite.command().endswith("-p no:cacheprovider -q")


def test_default_run_uses_injected_fake_no_real_pytest():
    # the WHOLE default set runs through the fake — proving the unit path never
    # spawns a real pytest and every default suite is wired to the runner.
    runner = _FakeRunner(default_exit=0)
    result = run_focused_suites(command_runner=runner)
    assert len(runner.commands) == len(DEFAULT_FOCUSED_SUITES)
    assert {g.gate_id for g in result.gate_results} == set(REQUIRED_SUITE_GATE_IDS)
    assert result.all_passed is True


# --------------------------------------------------------------------------- #
# Validation — a focused suite must declare at least one real test file.
# --------------------------------------------------------------------------- #
def test_empty_suite_is_rejected():
    with pytest.raises(ValueError):
        FocusedSuite(gate_id="x", owner="o", test_files=())


def test_command_override_is_honoured():
    suite = FocusedSuite(
        gate_id="custom",
        owner="o",
        test_files=("tests/test_fcc07b_readiness_aggregator.py",),
        command_override="uv run pytest -k readiness -q",
    )
    runner = _FakeRunner(default_exit=0)
    result = run_focused_suites(suites=[suite], command_runner=runner)
    assert runner.commands == ["uv run pytest -k readiness -q"]
    assert result.command_results[0].command == "uv run pytest -k readiness -q"


# --------------------------------------------------------------------------- #
# Integration — the focused suites feed the reused E-IMP1 shell (quick mode).
# --------------------------------------------------------------------------- #
def test_run_final_clean_core_quick_success():
    runner = _FakeRunner(default_exit=0)
    report = run_final_clean_core_quick(
        suites=[_SUITE_A, _SUITE_B], command_runner=runner
    )
    assert report.mode == "quick"
    assert report.overall_status == "success"
    assert report.exit_code == 0
    # the focused gate rows landed in the shell report.
    assert {g.gate_id for g in report.gates} >= {"FCC07B", "FCC07D"}


def test_run_final_clean_core_quick_blocks_on_failed_suite():
    runner = _FakeRunner(by_substring={"test_fcc07d_runtime_guard.py": 1})
    report = run_final_clean_core_quick(
        suites=[_SUITE_A, _SUITE_B], command_runner=runner
    )
    assert report.mode == "quick"
    assert report.overall_status == "blocked"
    assert report.exit_code != 0
    # both the blocked gate AND the failed command are present (defence in depth).
    assert any(g.gate_id == "FCC07D" and g.status == "blocked" for g in report.gates)
    assert any(c.failed for c in report.commands)
    blocked = next(g for g in report.gates if g.gate_id == "FCC07D")
    assert blocked.remediation and _SUITE_B.command() in blocked.remediation


def test_quick_run_can_concatenate_injected_gate_and_command_rows():
    from okto_pulse.core.application.boundary.final_clean_core_runner import (
        RunnerCommandResult,
        RunnerGateResult,
    )

    runner = _FakeRunner(default_exit=0)
    report = run_final_clean_core_quick(
        suites=[_SUITE_A],
        command_runner=runner,
        gate_results=[
            RunnerGateResult(gate_id="FCC07A", spec_id="FCC-07A", status="success")
        ],
        command_results=[RunnerCommandResult(command="extra", exit_code=0)],
    )
    assert {g.gate_id for g in report.gates} >= {"FCC07A", "FCC07B"}
    assert any(c.command == "extra" for c in report.commands)
    assert report.overall_status == "success"


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

    for name in (
        "FocusedSuite",
        "DEFAULT_FOCUSED_SUITES",
        "run_focused_suites",
        "run_final_clean_core_quick",
        "REQUIRED_SUITE_GATE_IDS",
    ):
        assert name in boundary.__all__, name
    assert boundary.run_focused_suites is run_focused_suites
