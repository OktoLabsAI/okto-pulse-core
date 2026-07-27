"""FCC-07E final runner — automated test_scenarios (contract api_170877a6).

This module automates four FCC-07E "final clean core" runner test_scenarios as
REAL, deterministic pytest tests. Each test is named for its ``ts_id`` and is
driven entirely through injection/fakes — NO venv, NO pip, NO real pytest, and
NO ``okto_pulse.community`` import is ever performed. The runner status enum is
the contract's ``success | blocked | skipped`` (never ``passed``/``blocking``),
and the report ``ordering`` is the list of the five sort-key NAMES while the
``gates[]`` array is what is sorted by those keys.

Scenarios:

* ``ts_d9124192`` (integration, AC1+AC2) — quick-mode clean success vs an
  FCC-07B evidence blocker, driven through the real quick-mode entry point
  (focused suites + the orchestrator's public report->row mappers).
* ``ts_3f432bdf`` (negative, AC3+AC4+AC6) — the runner surfaces the FCC-07C
  dependency finding, the FCC-07D provider finding AND the exact failing focused
  pytest command/file, never a generic "pytest failed".
* ``ts_aef4d8bb`` (integration, AC8) — exit codes + deterministic schema:
  shuffled input yields a byte-identical ordered report; JSON round-trips and
  markdown is stable.
* ``ts_4299abdf`` (integration, AC5+AC7) — full mode against FAKE injectable
  smoke gate adapters: both results + temp isolation metadata + reproduction
  refs + failure remediation, with the sandbox cleaned and user files untouched.

(The 5th scenario ``ts_96be1c1d`` is MANUAL and is intentionally NOT automated
here.)
"""

from __future__ import annotations

import json
import random
from pathlib import Path

from okto_pulse.core.application.boundary.adapter_readiness_inventory import (
    AdapterEvidence,
    AdapterInventoryEntry,
)
from okto_pulse.core.application.boundary.community_smoke import (
    CommandResult,
    CommunityRebuildReinstallSmokeGate,
    CoreSmokeInstallGate,
)
from okto_pulse.core.application.boundary.final_clean_core_focused_suites import (
    FocusedSuite,
    run_final_clean_core_quick,
)
from okto_pulse.core.application.boundary.final_clean_core_full_smoke import (
    PrerequisiteCheck,
    SmokeIsolation,
    run_final_clean_core_full_mode,
    run_full_mode_smoke,
)
from okto_pulse.core.application.boundary.final_clean_core_orchestrator import (
    GATE_FCC07B,
    GATE_FCC07C,
    GATE_FCC07D,
    map_packaging_ownership,
    map_provider_guard,
    map_readiness,
)
from okto_pulse.core.application.boundary.final_clean_core_runner import (
    ORDERING_KEYS,
    RunnerGateResult,
    render_final_clean_core_report,
    run_final_clean_core,
)
from okto_pulse.core.application.boundary.packaging_ownership_gate import (
    PackagingOwnershipReport,
    PackagingOwnershipRow,
)
from okto_pulse.core.application.boundary.readiness_aggregator import (
    ReadinessAggregateReport,
    aggregate_adapter_readiness,
)
from okto_pulse.core.application.boundary.report import GateReport
from okto_pulse.core.application.boundary.runtime_composition_guard import (
    RuntimeCompositionGuardReport,
)
from okto_pulse.core.application.boundary.testing_provider_policy import ProviderVerdict


# =========================================================================== #
# Shared deterministic fakes (mirroring the per-IMP test fixtures).
# =========================================================================== #
class _FakeRunner:
    """Deterministic injected ``CommandRunner`` (the #12 pattern) — returns a
    fixed exit code per command substring (or a default) and records the exact
    commands it was asked to run, so a test proves NO real pytest is spawned."""

    def __init__(
        self, *, default_exit: int = 0, by_substring: dict[str, int] | None = None
    ) -> None:
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
        return CommandResult(command=command, returncode=code, ok=code == 0, output_tail="")


class _FakeSmokeAdapter:
    """Deterministic injected full-mode smoke gate adapter — returns a fixed
    ``GateReport`` status and records whether it actually ran (so the test proves
    the gate ran inside the sandbox and never touched a real venv/user file)."""

    def __init__(self, gate_id: str, status: str, *, spec_id: str = "FCC-07E") -> None:
        self.gate_id = gate_id
        self.spec_id = spec_id
        self.status = status
        self.calls = 0

    def run(self, isolation: SmokeIsolation) -> GateReport:
        self.calls += 1
        # Honest: the sandbox handed to us is real and writable, but we confine
        # every (simulated) write to it — never a user file.
        assert isolation.root.exists()
        return GateReport(
            gate_id=self.gate_id,
            subject="fake full-mode smoke",
            status=self.status,  # type: ignore[arg-type]
            owner="okto-pulse-core/architecture",
            remediation_hint=None if self.status == "passed" else "rebuild the wheel and rerun the smoke",
        )


def _fcc07b_inventory_entry(adapter_key: str) -> AdapterInventoryEntry:
    """A single synthetic adapter inventory entry so the REAL readiness evaluator
    produces exactly one row (ready/blocked) for the FCC-07B fixture."""
    return AdapterInventoryEntry(
        adapter_key=adapter_key,
        owner="okto-pulse-core/boundary",
        current_module="okto_pulse/core/kg/providers/embedded/demo_provider.py",
        port_ref="DemoProvider",
        wave="R05-DEMO",
        predecessor_refs=("#06_kg_ports",),
        target_destination="community/adapters (demo provider)",
        packages=("stdlib",),
        oracles_required=("demo_register_before_remove",),
        removal_criterion=(
            "Community registers a DemoProvider via composition with the "
            "register-before-remove oracle; remove the core default only after "
            "the oracle passes."
        ),
        status="blocked",
    )


def _make_ownership_row(
    *, symbol: str, adapter_key: str, surface: str, remediation: str
) -> PackagingOwnershipRow:
    return PackagingOwnershipRow(
        surface=surface,
        symbol=symbol,
        classification="community_owned",
        scope="runtime",
        action="block",
        severity="blocking",
        adapter_key=adapter_key,
        owning_fcc_or_wave="R05-KG",
        diagnostic_code="import_boundary_violation",
        remediation=remediation,
    )


def _make_ownership_report(
    *, ok: bool, blocking: tuple[PackagingOwnershipRow, ...] = ()
) -> PackagingOwnershipReport:
    return PackagingOwnershipReport(
        ok=ok,
        status="passed" if ok else "blocking",
        rows=blocking,
        blocking=blocking,
        temporary_exceptions=(),
        scoped_out=(),
        ledger_integrity_ok=True,
        matrix_ok=ok,
        surfaces_audited=("manifest", "lock", "source", "wheel"),
        adapter_keys_missing=(),
    )


def _make_violation(
    *, provider_key: str, module: str, composition_path: str, remediation: str
) -> ProviderVerdict:
    return ProviderVerdict(
        module=module,
        classification="violation",
        reason="test-only provider in production",
        provider_key=provider_key,
        object_type="MemoryGraphStore",
        remediation=remediation,
        composition_path=composition_path,
    )


def _make_guard_report(
    *, violations: tuple[ProviderVerdict, ...] = ()
) -> RuntimeCompositionGuardReport:
    return RuntimeCompositionGuardReport(
        context="production",
        verdicts=violations,
        violations=violations,
        production_allowed=0,
        test_only_allowed=0,
    )


# Two focused suites pointed at REAL repo test files; with a fake runner they are
# never actually executed. Their gate_ids are deliberately distinct from the
# FCC07B/C/D gate ids so the injected A/B/C/D rows are unambiguous.
_FOCUSED_PASS_A = FocusedSuite(
    gate_id="focused_quick_a",
    owner="okto-pulse-core/boundary",
    spec_id="FCC-07E",
    test_files=("tests/test_fcc07e_runner_shell.py",),
)
_FOCUSED_PASS_B = FocusedSuite(
    gate_id="focused_quick_b",
    owner="okto-pulse-core/runtime",
    spec_id="FCC-07E",
    test_files=("tests/test_fcc07e_focused_suites.py",),
)


def _b_rows(report: ReadinessAggregateReport, evidence: dict[str, AdapterEvidence]):
    return map_readiness(
        report,
        spec_id="FCC-07B",
        owner="okto-pulse-core/boundary",
        evidence_by_key=evidence,
    )


def _c_rows_clean():
    return map_packaging_ownership(
        _make_ownership_report(ok=True),
        spec_id="FCC-07C",
        owner="okto-pulse-core/architecture",
    )


def _d_rows_clean():
    return map_provider_guard(
        _make_guard_report(),
        spec_id="FCC-07D",
        owner="okto-pulse-core/runtime",
    )


# =========================================================================== #
# ts_d9124192 (integration, AC1+AC2) — quick clean success vs FCC-07B blocker.
#
# Entry point: ``run_final_clean_core_quick`` — the genuine quick-mode runner
# (focused suites through a fake CommandRunner) fed with the B/C/D gate rows
# produced by the orchestrator's REAL public mappers (``map_readiness`` /
# ``map_packaging_ownership`` / ``map_provider_guard``). This exercises the
# report->RunnerGateResult mapping (preferred approach) AND lists the focused
# suites, so both "gates" and "suites" appear in one quick-mode report. The
# FCC-07B blocked row is built from the REAL readiness evaluator over a synthetic
# inventory entry, so its failed-evidence is computed, not fabricated.
# =========================================================================== #
def test_ts_d9124192_quick_clean_success_and_fcc07b_evidence_blocker():
    adapter_key = "fcc07b_demo_adapter"
    inventory = (_fcc07b_inventory_entry(adapter_key),)

    # --- GIVEN setup #1: FCC-07B/C/D all pass (B adapter fully evidenced). ---
    ready_evidence = {
        adapter_key: AdapterEvidence(
            port_closed=True,
            community_registered=True,
            oracle_passed=True,
            import_audit_passed=True,
            dependency_audit_passed=True,
            register_before_remove_passed=True,
        )
    }
    ready_report = aggregate_adapter_readiness(ready_evidence, inventory=inventory)
    assert ready_report.row_for(adapter_key).status == "ready"

    clean_runner = _FakeRunner(default_exit=0)
    clean = run_final_clean_core_quick(
        suites=[_FOCUSED_PASS_A, _FOCUSED_PASS_B],
        command_runner=clean_runner,
        gate_results=[
            *_b_rows(ready_report, ready_evidence),
            *_c_rows_clean(),
            *_d_rows_clean(),
        ],
    )

    # THEN clean -> success, exit 0, gates AND suites listed.
    assert clean.mode == "quick"
    assert clean.overall_status == "success"
    assert clean.exit_code == 0
    assert clean.summary.blocking == 0
    gate_ids = {g.gate_id for g in clean.gates}
    assert {GATE_FCC07B, GATE_FCC07C, GATE_FCC07D} <= gate_ids  # gates listed
    assert {"focused_quick_a", "focused_quick_b"} <= gate_ids  # suites listed
    # the focused suites ran (through the fake runner) and produced command rows.
    assert len(clean_runner.commands) == 2
    assert all("uv run pytest" in c for c in clean_runner.commands)
    assert len(clean.commands) == 2
    # the FCC-07B row is a clean success carrying the merged evidence.
    b_clean = next(g for g in clean.gates if g.gate_id == GATE_FCC07B)
    assert b_clean.status == "success"
    assert b_clean.adapter_key == adapter_key

    # --- GIVEN setup #2: same, but FCC-07B is evidence-BLOCKED. ---
    blocked_evidence = {
        adapter_key: AdapterEvidence(
            port_closed=True,
            community_registered=True,
            oracle_passed=True,
            import_audit_passed=True,
            dependency_audit_passed=True,
            register_before_remove_passed=False,  # <- the one failed evidence
        )
    }
    blocked_report = aggregate_adapter_readiness(blocked_evidence, inventory=inventory)
    source_row = blocked_report.row_for(adapter_key)
    # the REAL evaluator computed the failed-evidence (not hand-set).
    assert source_row.status == "blocked"
    assert source_row.failed_evidence == ("register_before_remove_passed",)

    blocked = run_final_clean_core_quick(
        suites=[_FOCUSED_PASS_A, _FOCUSED_PASS_B],
        command_runner=_FakeRunner(default_exit=0),  # suites still pass
        gate_results=[
            *_b_rows(blocked_report, blocked_evidence),
            *_c_rows_clean(),
            *_d_rows_clean(),
        ],
    )

    # THEN blocked -> non-zero exit, and the report surfaces (from FCC-07B):
    # adapter_key + failed evidence + owning gate + remediation.
    assert blocked.exit_code != 0
    assert blocked.overall_status == "blocked"
    b_blocked = next(g for g in blocked.gates if g.gate_id == GATE_FCC07B)
    assert b_blocked.status == "blocked"
    assert b_blocked.adapter_key == adapter_key  # adapter_key surfaced
    assert b_blocked.gate_id == GATE_FCC07B  # owning gate surfaced
    # the failed evidence is surfaced on the row's evidence_fields (False).
    assert b_blocked.evidence_fields.as_map()["register_before_remove_passed"] is False
    # remediation (the removal criterion) is surfaced, not generic.
    assert b_blocked.remediation is not None
    assert "register-before-remove" in b_blocked.remediation
    # the only blocker is FCC-07B (the focused suites passed).
    assert blocked.summary.blocking == 1


# =========================================================================== #
# ts_3f432bdf (negative, AC3+AC4+AC6) — the runner surfaces the FCC-07C
# dependency finding, the FCC-07D provider finding AND the exact failing focused
# pytest command/file, never a generic "pytest failed".
#
# Entry point: ``run_final_clean_core_quick`` fed with the C/D blocked rows from
# the orchestrator's real mappers and a fake CommandRunner that fails exactly one
# focused suite (the focused-suite failure map is exercised verbatim).
# =========================================================================== #
def test_ts_3f432bdf_runner_surfaces_dependency_and_provider_and_pytest_failures():
    # GIVEN an FCC-07C dependency-ownership blocking finding (family + surface +
    # remediation are all carried by the injected ownership row).
    c_remediation = (
        "move 'kuzu' out of the productive core (surface=source); declare it as a "
        "Community-owned dependency and register the adapter before removal."
    )
    c_rows = map_packaging_ownership(
        _make_ownership_report(
            ok=False,
            blocking=(
                _make_ownership_row(
                    symbol="kuzu",
                    adapter_key="kuzu_graph_store",
                    surface="source",
                    remediation=c_remediation,
                ),
            ),
        ),
        spec_id="FCC-07C",
        owner="okto-pulse-core/architecture",
    )

    # GIVEN an FCC-07D test-only provider used in production (provider_key +
    # module + composition_path all carried via the verdict -> runner row).
    d_module = "okto_pulse.core.kg.providers.testing.memory_graph_store"
    d_composition_path = "RuntimeComposition.providers.graph_store"
    d_rows = map_provider_guard(
        _make_guard_report(
            violations=(
                _make_violation(
                    provider_key="graph_store",
                    module=d_module,
                    composition_path=d_composition_path,
                    remediation=(
                        f"register a runtime adapter for module {d_module} instead "
                        "of the test-only provider"
                    ),
                ),
            )
        ),
        spec_id="FCC-07D",
        owner="okto-pulse-core/runtime",
    )

    # GIVEN one focused pytest command that FAILS (exit 1) — the other passes.
    failing_file = "tests/test_fcc07e_full_smoke.py"
    passing_suite = FocusedSuite(
        gate_id="focused_pass",
        owner="okto-pulse-core/boundary",
        spec_id="FCC-07E",
        test_files=("tests/test_fcc07e_runner_shell.py",),
    )
    failing_suite = FocusedSuite(
        gate_id="focused_fail",
        owner="okto-pulse-core/runtime",
        spec_id="FCC-07E",
        test_files=(failing_file,),
    )
    runner = _FakeRunner(by_substring={failing_file: 1})

    # WHEN the final runner runs quick mode.
    report = run_final_clean_core_quick(
        suites=[passing_suite, failing_suite],
        command_runner=runner,
        gate_results=[*c_rows, *d_rows],
    )

    assert report.mode == "quick"
    assert report.overall_status == "blocked"
    assert report.exit_code != 0

    # THEN the FCC-07C finding: dependency family + surface + remediation.
    c_row = next(g for g in report.gates if g.gate_id == GATE_FCC07C)
    assert c_row.status == "blocked"
    assert c_row.dependency_family == "kuzu"  # family
    assert c_row.remediation is not None
    assert "surface=source" in c_row.remediation  # surface
    assert "kuzu" in c_row.remediation  # remediation references the family

    # THEN the FCC-07D finding: provider_key + module + composition_path.
    d_row = next(g for g in report.gates if g.gate_id == GATE_FCC07D)
    assert d_row.status == "blocked"
    assert d_row.provider_key == "graph_store"  # provider_key
    assert d_row.remediation is not None
    assert d_module in d_row.remediation  # module
    assert f"composition_path={d_composition_path}" in d_row.remediation  # composition_path

    # THEN the EXACT failing focused command/file is surfaced — NOT generic.
    failed_commands = [c for c in report.commands if c.failed]
    assert len(failed_commands) == 1
    failed_command = failed_commands[0]
    assert failed_command.command == failing_suite.command()
    assert failed_command.exit_code == 1
    assert failing_file in failed_command.test_files
    # the focused-suite gate row carries the SPECIFIC remediation (command + file
    # + owning gate), never a generic "pytest failed".
    fail_gate = next(g for g in report.gates if g.gate_id == "focused_fail")
    assert fail_gate.status == "blocked"
    assert fail_gate.remediation is not None
    assert "pytest failed" not in fail_gate.remediation.lower()
    assert failing_suite.command() in fail_gate.remediation
    assert failing_file in fail_gate.remediation
    assert "focused_fail" in fail_gate.remediation


# =========================================================================== #
# ts_aef4d8bb (integration, AC8) — exit codes + deterministic report schema.
#
# Entry point: ``run_final_clean_core`` (the shell) + ``render_*`` — the
# determinism / exit-code / schema / render core. A clean set exits 0; a blocking
# set exits non-zero; a shuffled input (adapter/dependency/provider) yields a
# byte-identical ordered report; JSON round-trips and markdown is stable.
# =========================================================================== #
def _identity_gate(
    gate_id: str,
    *,
    spec_id: str,
    status: str = "success",
    adapter_key: str | None = None,
    dependency_family: str | None = None,
    provider_key: str | None = None,
) -> RunnerGateResult:
    return RunnerGateResult(
        gate_id=gate_id,
        spec_id=spec_id,
        status=status,  # type: ignore[arg-type]
        owner="okto-pulse-core/boundary",
        adapter_key=adapter_key,
        dependency_family=dependency_family,
        provider_key=provider_key,
    )


def test_ts_aef4d8bb_exit_codes_and_report_schema_deterministic():
    # A result set spanning all three identity axes (adapter/dependency/provider).
    clean_gates = [
        _identity_gate("FCC07A", spec_id="FCC-07A"),
        _identity_gate("FCC07B", spec_id="FCC-07B", adapter_key="z_adapter"),
        _identity_gate("FCC07B", spec_id="FCC-07B", adapter_key="a_adapter"),
        _identity_gate("FCC07C", spec_id="FCC-07C", dependency_family="requests"),
        _identity_gate("FCC07C", spec_id="FCC-07C", dependency_family="kuzu"),
        _identity_gate("FCC07D", spec_id="FCC-07D", provider_key="event_bus"),
        _identity_gate("FCC07D", spec_id="FCC-07D", provider_key="graph_store"),
    ]
    # The blocking set is the clean set with one provider gate flipped to blocked.
    blocking_gates = list(clean_gates)
    blocking_gates[-1] = _identity_gate(
        "FCC07D", spec_id="FCC-07D", status="blocked", provider_key="graph_store"
    )

    clean_baseline = run_final_clean_core(mode="quick", gate_results=clean_gates)
    blocking_baseline = run_final_clean_core(mode="quick", gate_results=blocking_gates)

    # THEN clean -> exit 0 / success; blocking -> non-zero / blocked.
    assert clean_baseline.exit_code == 0
    assert clean_baseline.overall_status == "success"
    assert blocking_baseline.exit_code != 0
    assert blocking_baseline.overall_status == "blocked"

    # ordering is the list of the five sort-key NAMES; gates[] is sorted by them.
    assert clean_baseline.as_dict()["ordering"] == list(ORDERING_KEYS)
    assert list(ORDERING_KEYS) == [
        "gate_id",
        "spec_id",
        "adapter_key",
        "dependency_family",
        "provider_key",
    ]
    realised = [
        (g.gate_id, g.spec_id, g.adapter_key, g.dependency_family, g.provider_key)
        for g in clean_baseline.gates
    ]
    assert realised == sorted(realised)  # gates[] really is ordered by the keys

    # THEN a SHUFFLED input yields a byte-identical report (determinism, TR3/AC8).
    clean_payload = clean_baseline.as_dict()
    blocking_payload = blocking_baseline.as_dict()
    for seed in range(8):
        cg = list(clean_gates)
        bg = list(blocking_gates)
        random.Random(seed).shuffle(cg)
        random.Random(seed + 100).shuffle(bg)
        cg_again = run_final_clean_core(mode="quick", gate_results=cg)
        bg_again = run_final_clean_core(mode="quick", gate_results=bg)
        assert cg_again.as_dict() == clean_payload
        assert bg_again.as_dict() == blocking_payload
        # byte-identical JSON serialisation, not merely equal dicts.
        assert json.dumps(cg_again.as_dict(), sort_keys=True) == json.dumps(
            clean_payload, sort_keys=True
        )

    # THEN render: JSON round-trips to as_dict; markdown is stable across renders.
    for report in (clean_baseline, blocking_baseline):
        rendered_json = render_final_clean_core_report(report, fmt="json")
        assert json.loads(rendered_json) == report.as_dict()
        md_once = render_final_clean_core_report(report, fmt="markdown")
        md_twice = render_final_clean_core_report(report, fmt="markdown")
        assert md_once == md_twice  # stable
    # the blocking markdown surfaces the BLOCKED overall status.
    assert "BLOCKED" in render_final_clean_core_report(
        blocking_baseline, fmt="markdown"
    )


# =========================================================================== #
# ts_4299abdf (integration, AC5+AC7) — full mode via injectable gate adapters.
#
# Entry point: ``run_full_mode_smoke`` + ``run_final_clean_core_full_mode``.
# The two #12 smoke gates (CoreSmokeInstallGate / CommunityRebuildReinstallSmoke
# Gate) are represented by FAKE injectable adapters that return a deterministic
# GateReport WITHOUT touching a real venv/user file. The report includes both
# full-mode results + temp isolation metadata (isolation_root) + reproduction
# refs (command rows) + the failure remediation, with no chat-only prerequisite,
# and the temp sandbox is cleaned without touching user files.
# =========================================================================== #
def test_ts_4299abdf_full_mode_via_injectable_gate_adapters(tmp_path):
    temp_root = tmp_path / "sandbox_root"
    temp_root.mkdir()
    # A user file OUTSIDE the sandbox that cleanup must never touch.
    user_file = tmp_path / "user_keepme.txt"
    user_file.write_text("DO NOT DELETE", encoding="utf-8")

    core_adapter = _FakeSmokeAdapter(CoreSmokeInstallGate.gate_id, "passed")
    community_adapter = _FakeSmokeAdapter(
        CommunityRebuildReinstallSmokeGate.gate_id, "blocking"
    )

    # WHEN the runner runs full mode against the fake adapters. Prerequisites are
    # a real injected PrerequisiteCheck (NOT a chat-only gate).
    smoke = run_full_mode_smoke(
        adapters=[core_adapter, community_adapter],
        prerequisites=PrerequisiteCheck(
            available=True,
            reason="full prerequisites present (wheel + venv tooling + writable temp root)",
        ),
        temp_root=temp_root,
    )

    # THEN both adapters actually ran inside the sandbox (no fake success/skip).
    assert core_adapter.calls == 1
    assert community_adapter.calls == 1
    assert smoke.ran is True

    # THEN both full-mode results are present, one success + one blocked.
    by_gate = {g.gate_id: g for g in smoke.gate_results}
    assert set(by_gate) == {
        CoreSmokeInstallGate.gate_id,
        CommunityRebuildReinstallSmokeGate.gate_id,
    }
    assert by_gate[CoreSmokeInstallGate.gate_id].status == "success"
    blocked_row = by_gate[CommunityRebuildReinstallSmokeGate.gate_id]
    assert blocked_row.status == "blocked"
    # THEN the failure remediation is surfaced on the blocked row.
    assert blocked_row.remediation == "rebuild the wheel and rerun the smoke"

    # THEN temp isolation metadata (isolation_root) is present, lived strictly
    # under temp_root, and has been CLEANED after the run.
    assert smoke.isolation_root is not None
    isolation_root = Path(smoke.isolation_root)
    assert isolation_root.parent == temp_root
    assert not isolation_root.exists()  # cleaned
    # cleanup removed ONLY the sandbox: temp_root and the user file survive.
    assert temp_root.exists()
    assert user_file.exists()
    assert user_file.read_text(encoding="utf-8") == "DO NOT DELETE"

    # THEN reproduction refs: a command row per gate that ran (success exit 0,
    # blocked exit 1), referencing the full-mode isolated smoke.
    assert len(smoke.command_results) == 2
    cmd_by_owner_exit = {(c.exit_code) for c in smoke.command_results}
    assert cmd_by_owner_exit == {0, 1}
    assert all("full-mode isolated smoke" in c.command for c in smoke.command_results)

    # WHEN the smoke result is incorporated into the reused shell (full mode).
    report = run_final_clean_core_full_mode(
        smoke=smoke,
        gate_results=[
            RunnerGateResult(
                gate_id="FCC07A",
                spec_id="FCC-07A",
                status="success",
                owner="okto-pulse-core/boundary",
            )
        ],
    )

    # THEN the final report is full mode, blocked (one blocking smoke gate), and
    # carries BOTH full-mode gate rows.
    assert report.mode == "full"
    assert report.overall_status == "blocked"
    assert report.exit_code != 0
    final_gate_ids = {g.gate_id for g in report.gates}
    assert {
        CoreSmokeInstallGate.gate_id,
        CommunityRebuildReinstallSmokeGate.gate_id,
    } <= final_gate_ids
    # the failure remediation is preserved in the final runner report.
    final_blocked = next(
        g for g in report.gates if g.gate_id == CommunityRebuildReinstallSmokeGate.gate_id
    )
    assert final_blocked.status == "blocked"
    assert final_blocked.remediation == "rebuild the wheel and rerun the smoke"
