"""FCC-07E-IMP5 — the documented recipes actually run (anti-fabrication).

These tests execute the EXACT public recipes documented in
``docs/fcc07e_final_clean_core_runner.md`` so the doc can never drift from the
real API or claim an output it cannot produce. They assert the recipes run and
the report renders as attachable release evidence — NOT a specific pass/fail
(the live core's real conformance state is what it is).
"""

from __future__ import annotations

import json
from pathlib import Path

from okto_pulse.core.application.boundary import (
    DEFAULT_FOCUSED_SUITES,
    CommunityRebuildReinstallSmokeAdapter,
    CommunitySmokeInput,
    CoreSmokeInstallAdapter,
    FullModePrerequisites,
    RunnerReport,
    SmokeInstallInput,
    detect_full_prerequisites,
    orchestrate_final_clean_core,
    render_final_clean_core_report,
    run_final_clean_core_full_mode,
    run_full_mode_smoke,
)

DOC = Path(__file__).resolve().parents[1] / "docs" / "fcc07e_final_clean_core_runner.md"


def test_doc_exists_and_names_the_runner_modules():
    text = DOC.read_text(encoding="utf-8")
    for symbol in (
        "orchestrate_final_clean_core",
        "run_full_mode_smoke",
        "render_final_clean_core_report",
        "detect_full_prerequisites",
    ):
        assert symbol in text, symbol


def test_quick_orchestration_recipe_runs_and_renders():
    # docs §2a — the exact documented quick recipe (incl. the D fail-close).
    report = orchestrate_final_clean_core(
        mode="quick",
        use_real_builders=True,
        adapter_evidence={},
        required_gate_ids=["FCC07D"],
    )
    assert isinstance(report, RunnerReport)
    assert report.mode == "quick"
    assert report.overall_status in {"success", "blocked"}
    assert isinstance(report.exit_code, int)
    # success IFF exit_code 0 (the contract invariant the doc states).
    assert (report.overall_status == "success") == (report.exit_code == 0)
    # release-evidence renders (docs §6): json round-trips, markdown has the header.
    assert json.loads(render_final_clean_core_report(report, fmt="json")) == report.as_dict()
    assert "Final clean-core runner" in render_final_clean_core_report(report, fmt="markdown")


def test_quick_recipe_maps_abc_but_not_d_without_a_provider_source():
    # docs §2a "FCC-07D note": without an explicit provider source, the
    # use_real_builders quick path maps A/B/C only — D is NOT auto-mapped.
    report = orchestrate_final_clean_core(
        mode="quick",
        use_real_builders=True,
        adapter_evidence={},
    )
    gate_ids = {gate.gate_id for gate in report.gates}
    assert "FCC07A" in gate_ids
    assert "FCC07B" in gate_ids
    assert "FCC07C" in gate_ids
    assert "FCC07D" not in gate_ids  # no live composition -> not mapped


def test_required_gate_ids_fail_closes_an_absent_fcc07d():
    # docs §2a "FCC-07D note": listing D in required_gate_ids fail-closes when no
    # provider source is given — a synthesised blocked row, never silently green.
    report = orchestrate_final_clean_core(
        mode="quick",
        use_real_builders=True,
        adapter_evidence={},
        required_gate_ids=["FCC07D"],
    )
    d_rows = [gate for gate in report.gates if gate.gate_id == "FCC07D"]
    assert len(d_rows) == 1
    assert d_rows[0].status == "blocked"
    assert "did not run" in (d_rows[0].remediation or "")
    assert report.overall_status == "blocked"
    assert report.exit_code != 0


def test_default_focused_suites_match_the_documented_five_groups():
    # docs §2b lists exactly these five gate ids.
    documented = {
        "FCC07B",
        "FCC07A_C",
        "FCC07D",
        "community_wiring",
        "composition_smoke",
    }
    assert {suite.gate_id for suite in DEFAULT_FOCUSED_SUITES} == documented


def test_full_mode_recipe_skips_honestly_when_wheel_absent():
    # docs §4 — the exact documented full recipe, with no wheel built.
    wheel = Path("dist/okto_pulse_core-0.0.0-py3-none-any.whl")  # does not exist
    prereqs = FullModePrerequisites(required_wheels=(wheel,))

    check = detect_full_prerequisites(prereqs)
    assert check.available is False
    assert "wheel" in check.reason.lower()

    smoke = run_full_mode_smoke(
        adapters=[
            CoreSmokeInstallAdapter(
                gate_input=SmokeInstallInput(
                    wheel_path=wheel, expected_imports=("okto_pulse",)
                ),
            ),
            CommunityRebuildReinstallSmokeAdapter(
                gate_input=CommunitySmokeInput(oracle=None),
            ),
        ],
        prerequisites=lambda: detect_full_prerequisites(prereqs),
    )
    # honest skip, never a fake success; the gates were not run.
    assert smoke.isolation_root is None
    assert {row.status for row in smoke.gate_results} == {"skipped"}
    assert all(row.status != "success" for row in smoke.gate_results)

    report = run_final_clean_core_full_mode(smoke=smoke)
    assert report.mode == "full"
    # a skip does not fake success, but it also does not block on its own.
    assert json.loads(render_final_clean_core_report(report, fmt="json")) == report.as_dict()
