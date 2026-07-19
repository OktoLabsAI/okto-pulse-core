"""SPEC 9e0bf979 / card 93ec6f57 — re-executable validation evidence contract.

Unit regressions over the single-source evidence gate
(``services/test_scenario_lifecycle``):

* evidence_class taxonomy is fail-closed (invalid value never normalized);
* per-class minimum fields (fr_52f084b4): expected_output_snapshot required for
  every class except the direct automated_test_pointer; run_log /
  non_replayable_justified additionally require non_replayable_justification
  (fr_0937529f);
* write vs read composition: a NEW gated write without evidence_class accepts
  ONLY the direct test pointer, while run-log-like must be replayable-grade;
  already-persisted legacy evidence stays valid on READ (ac_8212cdbb);
* cheap/existing enforcement (fr_958f0c9c / br_078725cc): an explicit run_log /
  non_replayable_justified fails when replay_should_exist=True OR a cheap/
  existing replay signal (test_file_path / replay_command / mcp_replay_manifest)
  is present;
* legacy evidence is inferable/upgradable for display without data loss.

Reproduce:
  .venv/Scripts/python -m pytest -p no:logging -q tests/test_evidence_class_contract.py
"""

from __future__ import annotations

import pytest

from okto_pulse.core.services.test_scenario_lifecycle import (
    EVIDENCE_CLASSES,
    InvalidEvidenceClassError,
    infer_evidence_class,
    is_valid_evidence_class,
    replay_is_cheap_or_existing,
    replayable_evidence_required,
    validate_evidence_class,
    validate_test_scenario_evidence,
)


def _ok(status, evidence, **kw):
    ok, missing = validate_test_scenario_evidence(status, evidence, **kw)
    return ok, missing


# ---------------------------------------------------------------------------
# evidence_class taxonomy is fail-closed
# ---------------------------------------------------------------------------


def test_evidence_class_taxonomy():
    assert EVIDENCE_CLASSES == (
        "automated_test_pointer",
        "replay_command",
        "mcp_replay_manifest",
        "manual_checklist",
        "run_log",
        "non_replayable_justified",
    )
    assert is_valid_evidence_class("run_log") is True
    assert is_valid_evidence_class("totally_made_up") is False
    assert validate_evidence_class("replay_command") == "replay_command"
    with pytest.raises(InvalidEvidenceClassError):
        validate_evidence_class("totally_made_up")


def test_invalid_evidence_class_fails_closed_in_gate():
    ok, missing = _ok("passed", {"evidence_class": "bogus", "last_run_at": "t"})
    assert ok is False
    assert any("evidence_class must be one of" in m for m in missing)


# ---------------------------------------------------------------------------
# per-class minimum fields (gated status)
# ---------------------------------------------------------------------------


def test_automated_test_pointer_valid():  # ts_f89dcf2c
    ev = {
        "evidence_class": "automated_test_pointer",
        "test_file_path": "tests/test_x.py",
        "test_function": "test_y",
    }
    assert _ok("automated", ev) == (True, [])
    # missing the pointer fields → rejected
    bad = {"evidence_class": "automated_test_pointer", "test_file_path": "tests/x.py"}
    ok, missing = _ok("automated", bad)
    assert ok is False and "test_function" in missing


def test_replay_command_accepted_but_legacy_manifest_is_unverified():  # ts_bf3a5210
    cmd = {
        "evidence_class": "replay_command",
        "replay_command": "pytest tests/test_x.py::test_y",
        "expected_output_snapshot": "1 passed",
    }
    assert _ok("passed", cmd) == (True, [])
    manifest = {
        "evidence_class": "mcp_replay_manifest",
        "mcp_replay_manifest": "manifests/replay_x.json",
        "expected_output_snapshot": "node materialized",
    }
    ok, missing = _ok("passed", manifest)
    assert ok is False
    assert "evidence_v2.legacy_mcp_replay_manifest_unverified" in missing
    # replay_command without expected_output_snapshot → rejected
    ok, missing = _ok(
        "passed", {"evidence_class": "replay_command", "replay_command": "pytest x"}
    )
    assert ok is False and "expected_output_snapshot" in missing


def test_manual_checklist_requires_ref_and_expected_output():
    ok, missing = _ok(
        "passed",
        {"evidence_class": "manual_checklist", "manual_checklist_ref": "checklists/x.md"},
    )
    assert ok is False and "expected_output_snapshot" in missing
    good = {
        "evidence_class": "manual_checklist",
        "manual_checklist_ref": "checklists/x.md",
        "expected_output_snapshot": "all 5 steps observed green",
    }
    assert _ok("passed", good) == (True, [])


def test_run_log_requires_justification_and_snapshot():  # ts_00a95149
    # run-log without justification + snapshot → rejected
    bare = {
        "evidence_class": "run_log",
        "last_run_at": "2026-06-19T00:00:00",
        "output_snippet": "1 passed",
    }
    ok, missing = _ok("passed", bare)
    assert ok is False
    assert "non_replayable_justification" in missing
    assert "expected_output_snapshot" in missing
    # with both → accepted structurally (validator still judges credibility)
    full = {
        **bare,
        "non_replayable_justification": "dogfood MCP flow, no deterministic harness yet",
        "expected_output_snapshot": "spec moved to done; KG node materialized",
    }
    assert _ok("passed", full) == (True, [])


def test_non_replayable_justified_requires_justification_and_snapshot():
    ok, missing = _ok(
        "passed",
        {"evidence_class": "non_replayable_justified", "expected_output_snapshot": "x"},
    )
    assert ok is False and "non_replayable_justification" in missing


# ---------------------------------------------------------------------------
# cheap/existing enforcement (fr_958f0c9c / br_078725cc) — codex required
# ---------------------------------------------------------------------------


def test_run_log_rejected_when_replay_should_exist_true():
    ev = {
        "evidence_class": "run_log",
        "last_run_at": "t",
        "output_snippet": "ok",
        "non_replayable_justification": "claimed",
        "expected_output_snapshot": "ok",
        "replay_should_exist": True,
    }
    ok, missing = _ok("passed", ev)
    assert ok is False
    assert any("replayable_evidence_required" in m for m in missing)


def test_run_log_rejected_when_cheap_replay_signal_present():
    # a replay_command present means the replayable artifact already exists →
    # the run_log class is the wrong choice.
    ev = {
        "evidence_class": "run_log",
        "last_run_at": "t",
        "output_snippet": "ok",
        "non_replayable_justification": "claimed",
        "expected_output_snapshot": "ok",
        "replay_command": "pytest tests/test_x.py",
    }
    ok, missing = _ok("passed", ev)
    assert ok is False
    assert any("replayable_evidence_required" in m for m in missing)


def test_non_replayable_justified_rejected_when_test_pointer_present():
    ev = {
        "evidence_class": "non_replayable_justified",
        "non_replayable_justification": "claimed",
        "expected_output_snapshot": "ok",
        "test_file_path": "tests/test_x.py",  # cheap/existing signal
    }
    ok, _missing = _ok("passed", ev)
    assert ok is False


def test_run_log_passes_without_any_cheap_signal():
    ev = {
        "evidence_class": "run_log",
        "last_run_at": "t",
        "output_snippet": "ok",
        "non_replayable_justification": "genuine dogfood, no deterministic harness",
        "expected_output_snapshot": "spec done + KG node",
    }
    # replay_should_exist omitted, no cheap signal → structurally accepted.
    assert _ok("passed", ev) == (True, [])


def test_replayable_helpers():
    assert replay_is_cheap_or_existing({"test_file_path": "x"}) is True
    assert replay_is_cheap_or_existing({"replay_command": "x"}) is True
    assert replay_is_cheap_or_existing({"mcp_replay_manifest": "x"}) is True
    assert replay_is_cheap_or_existing({"output_snippet": "x"}) is False
    assert replayable_evidence_required({"replay_should_exist": True}) is True
    assert replayable_evidence_required({"replay_command": "x"}) is True
    assert replayable_evidence_required({"output_snippet": "x"}) is False


# ---------------------------------------------------------------------------
# write vs read composition + backward compatibility (ac_8212cdbb)
# ---------------------------------------------------------------------------


def test_write_unclassed_pointer_grandfathered_but_runlog_rejected():
    pointer = {"test_file_path": "tests/test_x.py", "test_function": "test_y"}
    assert _ok("passed", pointer, for_write=True) == (True, [])
    # run-log-like on a NEW write without a class → rejected, with a hint to
    # declare a replayable class.
    runlog = {"last_run_at": "t", "output_snippet": "1 passed"}
    ok, missing = _ok("passed", runlog, for_write=True)
    assert ok is False
    assert "expected_output_snapshot" in missing
    assert "non_replayable_justification" in missing
    assert any("evidence_class" in m for m in missing)


def test_read_legacy_evidence_stays_valid():
    # legacy run-log-shaped passed evidence (no evidence_class) remains valid on
    # READ so previously persisted scenarios are not retroactively rejected.
    legacy = {"last_run_at": "t", "output_snippet": "1 passed"}
    assert _ok("passed", legacy) == (True, [])  # for_write=False (default)
    legacy_pointer = {"test_file_path": "tests/x.py", "test_function": "test_y"}
    assert _ok("automated", legacy_pointer) == (True, [])


def test_non_gated_status_never_requires_evidence():
    assert _ok("draft", None) == (True, [])
    assert _ok("ready", {"evidence_class": "bogus"}) == (True, [])  # not gated


def test_infer_evidence_class_for_display_and_upgrade():
    assert (
        infer_evidence_class({"test_file_path": "x", "test_function": "y"})
        == "automated_test_pointer"
    )
    assert infer_evidence_class({"replay_command": "x"}) == "replay_command"
    assert (
        infer_evidence_class({"last_run_at": "t", "output_snippet": "o"}) == "run_log"
    )
    # explicit class wins; empty/None → None
    assert infer_evidence_class({"evidence_class": "manual_checklist"}) == "manual_checklist"
    assert infer_evidence_class(None) is None
    assert infer_evidence_class({}) is None
