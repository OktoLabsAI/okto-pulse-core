"""FCC-07E-IMP1 — Final Clean Core runner SHELL tests (contract api_170877a6).

Covers the closed E-IMP1 scope: the locally-callable runner shell that accepts
INJECTED gate + command results, orders them deterministically by the five
ordering keys (TR3), emits the exact ``api_170877a6`` schema (FR5), and computes
the fail-closed exit code (TR2/AC8).

* AC8 — all gates passed + no command failure -> exit 0 / ``success``; one
  blocking gate -> non-zero / ``blocked``; one command with a non-zero exit_code
  -> non-zero / ``blocked``; the schema is deterministic and ordered.
* FR5 — every top-level / gate-row / command-row / summary field is present and
  ``evidence_fields`` carries exactly the six REQUIRED_EVIDENCE keys.
* TR3 — a shuffled input yields a byte-identical, ordered report.
* FR1/FR2 — ``mode`` is recorded as ``quick`` / ``full``.

Teeth are real synthetic injected results; no Pulse server is started and no
Community code is imported.
"""

from __future__ import annotations

import ast
import json
import random
from pathlib import Path

import pytest

import okto_pulse.core.application.boundary.final_clean_core_runner as _runner_mod
from okto_pulse.core.application.boundary.adapter_readiness_inventory import (
    REQUIRED_EVIDENCE,
    AdapterEvidence,
)
from okto_pulse.core.application.boundary.final_clean_core_runner import (
    ORDERING_KEYS,
    RunnerCommandResult,
    RunnerGateResult,
    RunnerReport,
    render_final_clean_core_report,
    run_final_clean_core,
)

RUNNER_PY = Path(_runner_mod.__file__).resolve()


def _passing_gate(
    gate_id: str,
    *,
    spec_id: str = "FCC-07A",
    adapter_key: str | None = None,
    dependency_family: str | None = None,
    provider_key: str | None = None,
    status: str = "success",
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


# ---------------------------------------------------------------------------
# AC8 — exit code semantics.
# ---------------------------------------------------------------------------
def test_all_gates_passed_no_command_failure_exits_zero_success():
    report = run_final_clean_core(
        mode="quick",
        gate_results=[
            _passing_gate("fcc07a_conformance_matrix"),
            _passing_gate("fcc07b_readiness", spec_id="FCC-07B", adapter_key="kuzu_graph_store"),
        ],
        command_results=[
            RunnerCommandResult(command="pytest tests/test_fcc07a.py", exit_code=0),
        ],
    )

    assert report.exit_code == 0
    assert report.overall_status == "success"
    assert report.success is True
    assert report.summary.blocking == 0
    assert report.summary.passed == 2


def test_one_blocking_gate_exits_nonzero_blocked():
    report = run_final_clean_core(
        mode="quick",
        gate_results=[
            _passing_gate("fcc07a_conformance_matrix"),
            _passing_gate("fcc07d_provider_guard", spec_id="FCC-07D", status="blocked"),
        ],
    )

    assert report.exit_code != 0
    assert report.overall_status == "blocked"
    assert report.summary.blocking == 1
    assert report.summary.passed == 1


def test_one_failed_command_exits_nonzero_blocked():
    report = run_final_clean_core(
        mode="quick",
        gate_results=[_passing_gate("fcc07a_conformance_matrix")],
        command_results=[
            RunnerCommandResult(command="pytest tests/test_ok.py", exit_code=0),
            RunnerCommandResult(
                command="pytest tests/test_fail.py",
                exit_code=1,
                test_files=("tests/test_fail.py",),
                owner="okto-pulse-core/boundary",
            ),
        ],
    )

    # No blocking gate, but a failed focused command must still block (TR2).
    assert report.summary.blocking == 0
    assert report.exit_code != 0
    assert report.overall_status == "blocked"


def test_skipped_gate_does_not_block_exit_zero():
    report = run_final_clean_core(
        mode="quick",
        gate_results=[
            _passing_gate("fcc07a_conformance_matrix"),
            _passing_gate("fcc07e_full_smoke", spec_id="FCC-07E", status="skipped"),
        ],
    )

    assert report.summary.skipped == 1
    assert report.summary.blocking == 0
    assert report.exit_code == 0
    assert report.overall_status == "success"


def test_required_gate_missing_is_fail_closed_blocking():
    report = run_final_clean_core(
        mode="full",
        gate_results=[_passing_gate("fcc07a_conformance_matrix")],
        required_gate_ids=["fcc07a_conformance_matrix", "fcc07c_packaging"],
    )

    # The present required gate passes; the absent one is synthesised as blocking.
    assert report.exit_code != 0
    assert report.overall_status == "blocked"
    synthetic = next(g for g in report.gates if g.gate_id == "fcc07c_packaging")
    assert synthetic.status == "blocked"
    assert synthetic.remediation and "did not run" in synthetic.remediation
    assert report.summary.blocking == 1


# ---------------------------------------------------------------------------
# FR5 — exact api_170877a6 schema.
# ---------------------------------------------------------------------------
def test_schema_matches_api_170877a6():
    report = run_final_clean_core(
        mode="full",
        gate_results=[
            RunnerGateResult(
                gate_id="fcc07b_readiness",
                spec_id="FCC-07B",
                status="success",
                owner="okto-pulse-core/kg",
                adapter_key="kuzu_graph_store",
                dependency_family=None,
                provider_key=None,
                evidence_fields=AdapterEvidence(
                    port_closed=True,
                    community_registered=True,
                    oracle_passed=True,
                    import_audit_passed=True,
                    dependency_audit_passed=True,
                    register_before_remove_passed=True,
                ),
                command="pytest tests/test_fcc07b.py",
                test_file="tests/test_fcc07b.py",
                remediation="register-before-remove + dimension-parity oracle",
            ),
        ],
        command_results=[
            RunnerCommandResult(
                command="pytest tests/test_fcc07b.py",
                exit_code=0,
                test_files=("tests/test_fcc07b.py",),
                owner="okto-pulse-core/kg",
            ),
        ],
    )

    payload = report.as_dict()

    assert set(payload) == {
        "overall_status",
        "mode",
        "exit_code",
        "ordering",
        "gates",
        "commands",
        "summary",
    }
    assert payload["overall_status"] == "success"
    assert payload["mode"] == "full"
    assert isinstance(payload["exit_code"], int)

    gate_row = payload["gates"][0]
    assert set(gate_row) == {
        "gate_id",
        "spec_id",
        "status",
        "owner",
        "adapter_key",
        "dependency_family",
        "provider_key",
        "evidence_fields",
        "command",
        "test_file",
        "remediation",
    }
    # evidence_fields carries EXACTLY the six REQUIRED_EVIDENCE keys.
    assert set(gate_row["evidence_fields"]) == set(REQUIRED_EVIDENCE)
    assert tuple(gate_row["evidence_fields"]) == REQUIRED_EVIDENCE
    assert all(value is True for value in gate_row["evidence_fields"].values())

    command_row = payload["commands"][0]
    assert set(command_row) == {"command", "exit_code", "test_files", "owner"}
    assert command_row["test_files"] == ["tests/test_fcc07b.py"]

    assert set(payload["summary"]) == {"blocking", "passed", "skipped"}

    # ordering is the fixed list of the five sort-key NAMES (api_170877a6);
    # the realised gate order lives in the gates[] array.
    assert payload["ordering"] == [
        "gate_id",
        "spec_id",
        "adapter_key",
        "dependency_family",
        "provider_key",
    ]


def test_default_evidence_fields_are_all_none_six_keys():
    report = run_final_clean_core(
        mode="quick",
        gate_results=[_passing_gate("fcc07c_packaging", spec_id="FCC-07C")],
    )
    evidence = report.as_dict()["gates"][0]["evidence_fields"]
    assert tuple(evidence) == REQUIRED_EVIDENCE
    assert all(value is None for value in evidence.values())


# ---------------------------------------------------------------------------
# TR3 — deterministic ordering by the five keys.
# ---------------------------------------------------------------------------
def test_deterministic_ordering_by_five_keys():
    gates = [
        _passing_gate("g_b", spec_id="S1", adapter_key="z_adapter"),
        _passing_gate("g_a", spec_id="S2", provider_key="p1"),
        _passing_gate("g_a", spec_id="S1", adapter_key="a_adapter"),
        _passing_gate("g_a", spec_id="S1", adapter_key="a_adapter", dependency_family="numpy"),
        _passing_gate("g_c", spec_id="S0"),
    ]
    shuffled = list(gates)
    random.Random(1234).shuffle(shuffled)

    report = run_final_clean_core(mode="quick", gate_results=shuffled)

    realised = [(g.gate_id, g.spec_id, g.adapter_key, g.dependency_family, g.provider_key)
                for g in report.gates]
    assert realised == [
        ("g_a", "S1", "a_adapter", None, None),
        ("g_a", "S1", "a_adapter", "numpy", None),
        ("g_a", "S2", None, None, "p1"),
        ("g_b", "S1", "z_adapter", None, None),
        ("g_c", "S0", None, None, None),
    ]
    # `ordering` names the five sort keys (constant); the realised order is
    # proven by the gates[] array above.
    assert report.as_dict()["ordering"] == list(ORDERING_KEYS)
    assert list(ORDERING_KEYS) == [
        "gate_id",
        "spec_id",
        "adapter_key",
        "dependency_family",
        "provider_key",
    ]


def test_shuffled_input_yields_byte_identical_report():
    gates = [
        _passing_gate("fcc07a", spec_id="A", adapter_key="a1"),
        _passing_gate("fcc07b", spec_id="B", status="blocked"),
        _passing_gate("fcc07c", spec_id="C", dependency_family="requests"),
        _passing_gate("fcc07d", spec_id="D", provider_key="event_bus"),
    ]
    commands = [
        RunnerCommandResult(command="cmd-b", exit_code=0),
        RunnerCommandResult(command="cmd-a", exit_code=0),
    ]

    baseline = run_final_clean_core(
        mode="full", gate_results=gates, command_results=commands
    ).as_dict()

    for seed in range(8):
        g = list(gates)
        c = list(commands)
        random.Random(seed).shuffle(g)
        random.Random(seed + 100).shuffle(c)
        again = run_final_clean_core(
            mode="full", gate_results=g, command_results=c
        ).as_dict()
        assert again == baseline


# ---------------------------------------------------------------------------
# FR1/FR2 — mode recorded; summary counts; callables; JSON.
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("mode", ["quick", "full"])
def test_mode_is_recorded(mode):
    report = run_final_clean_core(
        mode=mode, gate_results=[_passing_gate("fcc07a")]
    )
    assert report.mode == mode
    assert report.as_dict()["mode"] == mode


def test_invalid_mode_is_rejected():
    with pytest.raises(ValueError):
        run_final_clean_core(mode="deep", gate_results=[])  # type: ignore[arg-type]


def test_invalid_gate_status_is_rejected():
    bad = RunnerGateResult(gate_id="x", spec_id="S", status="weird")  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        run_final_clean_core(mode="quick", gate_results=[bad])


def test_summary_counts_are_correct():
    report = run_final_clean_core(
        mode="full",
        gate_results=[
            _passing_gate("g1", status="success"),
            _passing_gate("g2", status="success"),
            _passing_gate("g3", status="blocked"),
            _passing_gate("g4", status="skipped"),
        ],
    )
    assert report.summary.as_dict() == {"blocking": 1, "passed": 2, "skipped": 1}


def test_gate_results_accept_callables():
    report = run_final_clean_core(
        mode="quick",
        gate_results=[lambda: _passing_gate("fcc07a"), _passing_gate("fcc07b", spec_id="B")],
        command_results=[lambda: RunnerCommandResult(command="pytest -q", exit_code=0)],
    )
    assert report.exit_code == 0
    assert {g.gate_id for g in report.gates} == {"fcc07a", "fcc07b"}
    assert report.commands[0].command == "pytest -q"


def test_report_is_json_serialisable():
    report = run_final_clean_core(
        mode="full",
        gate_results=[
            _passing_gate("fcc07a", adapter_key="kuzu_graph_store"),
            _passing_gate("fcc07d", spec_id="D", status="blocked", provider_key="event_bus"),
        ],
        command_results=[RunnerCommandResult(command="pytest -q", exit_code=2)],
    )
    encoded = json.dumps(report.as_dict(), sort_keys=True)
    decoded = json.loads(encoded)
    assert decoded == report.as_dict()
    # Round-trips and is identical across two runs (determinism).
    assert encoded == json.dumps(
        run_final_clean_core(
            mode="full",
            gate_results=[
                _passing_gate("fcc07a", adapter_key="kuzu_graph_store"),
                _passing_gate("fcc07d", spec_id="D", status="blocked", provider_key="event_bus"),
            ],
            command_results=[RunnerCommandResult(command="pytest -q", exit_code=2)],
        ).as_dict(),
        sort_keys=True,
    )


def test_empty_run_is_success():
    report = run_final_clean_core(mode="quick", gate_results=[])
    assert report.exit_code == 0
    assert report.overall_status == "success"
    assert report.summary.as_dict() == {"blocking": 0, "passed": 0, "skipped": 0}
    assert report.gates == ()
    assert report.commands == ()


# ---------------------------------------------------------------------------
# Boundary hygiene — the shell imports no Community code.
# ---------------------------------------------------------------------------
def test_runner_module_does_not_import_community():
    tree = ast.parse(RUNNER_PY.read_text(encoding="utf-8"))
    imported: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.append(node.module)
    assert not any(name.startswith("okto_pulse.community") for name in imported), imported


def test_runner_report_dataclasses_are_frozen():
    report = run_final_clean_core(mode="quick", gate_results=[_passing_gate("g")])
    assert isinstance(report, RunnerReport)
    with pytest.raises(Exception):
        report.exit_code = 99  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Contract enum — gates[].status is success|blocked|skipped (api_170877a6),
# NOT passed/blocking; the summary keeps the contract's count names.
# ---------------------------------------------------------------------------
def test_gate_status_uses_contract_enum_success_blocked_skipped():
    report = run_final_clean_core(
        mode="full",
        gate_results=[
            _passing_gate("g_ok", spec_id="A"),  # -> success
            _passing_gate("g_bad", spec_id="B", status="blocked"),
            _passing_gate("g_skip", spec_id="C", status="skipped"),
        ],
    )
    statuses = {row["status"] for row in report.as_dict()["gates"]}
    assert statuses == {"success", "blocked", "skipped"}
    assert "passed" not in statuses and "blocking" not in statuses
    # summary keeps the contract's count names (blocking/passed/skipped).
    assert report.as_dict()["summary"] == {"blocking": 1, "passed": 1, "skipped": 1}
    assert report.overall_status == "blocked"
    assert report.exit_code != 0


# ---------------------------------------------------------------------------
# FR5 — the report renders a STABLE json AND markdown surface.
# ---------------------------------------------------------------------------
def test_render_report_json_and_markdown_are_stable():
    report = run_final_clean_core(
        mode="quick",
        gate_results=[
            _passing_gate("fcc07b", spec_id="B", adapter_key="kuzu_graph_store")
        ],
        command_results=[
            RunnerCommandResult(command="pytest x", exit_code=1, owner="FCC07A")
        ],
    )
    # json render round-trips to the exact as_dict payload.
    assert json.loads(render_final_clean_core_report(report, fmt="json")) == report.as_dict()
    # markdown render is deterministic and surfaces status + the failing command.
    md = render_final_clean_core_report(report, fmt="markdown")
    assert md == render_final_clean_core_report(report, fmt="markdown")  # stable
    assert "Final clean-core runner" in md
    assert "kuzu_graph_store" in md
    assert "Failing commands" in md and "pytest x" in md
    # an unknown format fails closed.
    with pytest.raises(ValueError):
        render_final_clean_core_report(report, fmt="yaml")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Public surface — the runner is the local entrypoint, exported from the
# boundary package (callable without a Pulse server).
# ---------------------------------------------------------------------------
def test_runner_is_exported_from_boundary_package():
    import okto_pulse.core.application.boundary as boundary

    assert "run_final_clean_core" in boundary.__all__
    assert "render_final_clean_core_report" in boundary.__all__
    assert boundary.run_final_clean_core is run_final_clean_core
    # callable locally with injected results, no server started.
    report = boundary.run_final_clean_core(
        mode="quick", gate_results=[_passing_gate("g")]
    )
    assert report.overall_status == "success"
