"""R1.2 — R1 tool-description budget as a profile of the shared R5 scanner.

Covers spec ``MCP Tools/List Compact Description Budget`` scenarios owned by
R1.2:

- ``ts_90e81d5e`` — R1 budget uses the SHARED ``MCPPayloadBudgetScanner`` with
  ``budget_profile=tool_descriptions`` and ``unit=chars`` (no separate scanner).
- ``ts_3936f3bc`` — the scanner fails on oversized docs with an actionable,
  char-based (token-free) violation shape.

Plus the R1.2 acceptance: live gate passes after R1.1, names/schema stability,
safety discoverability floor, canonical ``mcp_budget_violation_total`` telemetry,
and NO parallel ``MCPToolDescriptionBudgetScanner``.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

from okto_pulse.core.mcp import server as mcp_server
from okto_pulse.core.mcp.payload_budget import (
    METRIC_BUDGET_VIOLATION,
    TOOL_DESCRIPTIONS_BUDGET_PROFILE,
    MCPPayloadBudgetScanner,
    MCPProjectionTelemetrySink,
    snapshot_instructions,
    snapshot_tool_descriptions,
)

MCP_DIR = Path(__file__).parent.parent / "src" / "okto_pulse" / "core" / "mcp"


def _tools() -> dict:
    return asyncio.new_event_loop().run_until_complete(mcp_server.mcp.get_tools())


# ---------------------------------------------------------------------------
# ts_90e81d5e — shared scanner, tool_descriptions profile, live PASS after R1.1
# ---------------------------------------------------------------------------


def test_r1_uses_shared_scanner_tool_descriptions_profile_and_live_passes():
    profile = TOOL_DESCRIPTIONS_BUDGET_PROFILE
    # Formalised in the shared profile contract (not just a test):
    assert profile.name == "tool_descriptions"
    assert profile.per_tool_description_chars == 900
    assert profile.aggregate_tool_description_chars == 70000
    # Owner-authorised renegotiation (R1.1 card): instructions budget 8000 -> 10000.
    assert profile.always_loaded_instruction_chars == 10000

    scanner = MCPPayloadBudgetScanner(profile)
    report = scanner.scan(
        tool_descriptions=snapshot_tool_descriptions(mcp_server.mcp),
        instructions=snapshot_instructions(mcp_server),
    )
    # After R1.1 the live MCP surface PASSES the gate.
    assert report.passed is True, report.to_dict()["violations"]
    assert report.unit == "chars"
    assert report.budget_profile == "tool_descriptions"
    assert report.measurements["aggregate_tool_description_chars"] <= 70000
    assert report.measurements["tool_count"] >= 200


# ---------------------------------------------------------------------------
# ts_3936f3bc — over-budget fixture fails with actionable, token-free shape
# ---------------------------------------------------------------------------


def test_over_budget_fixture_fails_with_actionable_char_based_shape():
    scanner = MCPPayloadBudgetScanner(TOOL_DESCRIPTIONS_BUDGET_PROFILE)
    report = scanner.scan(
        tool_descriptions={"okto_pulse_big": "x" * 1500, "okto_pulse_ok": "fine"},
        instructions={"big_instructions.md": "y" * 11000, "small.md": "ok"},
    )
    assert report.passed is False
    assert report.unit == "chars"

    paths = {v.path for v in report.violations}
    assert "tool:okto_pulse_big" in paths
    assert "instruction:big_instructions.md" in paths
    assert "tool:okto_pulse_ok" not in paths

    tool_v = next(v for v in report.violations if v.path == "tool:okto_pulse_big")
    d = tool_v.to_dict()
    for key in ("path", "actual_chars", "budget_chars", "reason", "budget_profile", "unit"):
        assert key in d
    assert d["budget_profile"] == "tool_descriptions"
    assert d["budget_chars"] == 900
    assert d["unit"] == "chars"

    blob = json.dumps(report.to_dict()).lower()
    assert "actual_tokens" not in blob
    assert "max_tokens" not in blob
    assert "token" not in blob


def test_instruction_over_renegotiated_budget_fails():
    # 10001 chars > the formalised 10000 instruction budget → violation.
    scanner = MCPPayloadBudgetScanner(TOOL_DESCRIPTIONS_BUDGET_PROFILE)
    report = scanner.scan(instructions={"agent_instructions.md": "z" * 10001})
    assert report.passed is False
    v = next(iter(report.violations))
    assert v.reason == "always_loaded_instruction_over_budget"
    assert v.budget_chars == 10000


# ---------------------------------------------------------------------------
# dec_71488495 — shared scanner only; no parallel R1 scanner
# ---------------------------------------------------------------------------


def test_no_parallel_tool_description_scanner_exists():
    for path in MCP_DIR.glob("*.py"):
        src = path.read_text(encoding="utf-8")
        assert "MCPToolDescriptionBudgetScanner" not in src, (
            f"parallel scanner referenced in {path.name}"
        )
    # the shared R5 scanner is the one and only budget scanner.
    from okto_pulse.core.mcp import payload_budget

    assert hasattr(payload_budget, "MCPPayloadBudgetScanner")
    assert not hasattr(payload_budget, "MCPToolDescriptionBudgetScanner")


# ---------------------------------------------------------------------------
# tr_eb27b7b0 — callable names + schema parameter keys stable after compaction
# ---------------------------------------------------------------------------

BASELINE_SCHEMA = {
    "okto_pulse_create_card": {"board_id", "title", "spec_id", "status", "card_type"},
    "okto_pulse_submit_task_validation": {"board_id", "card_id"},
}


def test_callable_names_and_schema_keys_stable():
    tools = _tools()
    # Surface size is additive after the R1 baseline. Keep the current reviewed
    # surface pinned so accidental tool drops/duplicates remain visible.
    # 2026-07-12 (auditoria MCP): re-pinned 259→265 — the pin had rotted while
    # 6 tools landed (chain node_type era +0; kg_provenance_drift, export et
    # al. +6). Set-level drift is now ALSO guarded by
    # test_mcp_tools_catalog_drift.py, which names the exact delta.
    # 2026-07-27: reviewed SK-A surface is 292 tools. The five additive Quality
    # assessment commands expose record/current/receipt/history/findings.
    assert len(tools) == 292
    for name, expected_keys in BASELINE_SCHEMA.items():
        assert name in tools
        props = set(tools[name].parameters.get("properties", {}))
        assert expected_keys.issubset(props), (name, expected_keys - props)
    # required flags preserved for a representative tool
    create_required = set(tools["okto_pulse_create_card"].parameters.get("required", []))
    assert {"board_id", "title"}.issubset(create_required)


# ---------------------------------------------------------------------------
# tr_315511f7 — safety floor: required topics still discoverable
# ---------------------------------------------------------------------------


def test_safety_topics_remain_discoverable():
    surface = mcp_server._load_instructions()
    for _uri, path, _desc in mcp_server._RESOURCE_REGISTRY:
        surface += "\n" + mcp_server._load_resource_file(path)

    instr = mcp_server._load_instructions()
    # KG health stop-rule, transitions, resource gate, card pre-flight stay inline.
    assert "stop-rule" in instr.lower() or "recovery_needed" in instr
    assert "reference/transitions" in instr or "Card Status Transitions" in instr
    assert "Resource Gate" in instr
    assert "Card execution pre-flight" in instr
    assert "Resource Fetching Protocol" in instr
    # destructive ops discoverable in instructions or a mandatory resource.
    assert "Destructive Operations" in instr or "destructive_ops" in surface


# ---------------------------------------------------------------------------
# or_c6fc02f1 / or_7d01d8de / or_ddc31b64 — canonical mcp_budget_violation_total
# ---------------------------------------------------------------------------


def test_budget_violation_emits_canonical_metric_no_tokens():
    scanner = MCPPayloadBudgetScanner(TOOL_DESCRIPTIONS_BUDGET_PROFILE)
    report = scanner.scan(tool_descriptions={"okto_pulse_big": "x" * 1500})
    assert report.passed is False

    sink = MCPProjectionTelemetrySink()
    for violation in report.violations:
        assert sink.record_budget_violation(violation).accepted is True
    assert sink.metrics[METRIC_BUDGET_VIOLATION] == len(report.violations)
    # safe labels only — no token fields, no payload bodies
    for event in sink.events:
        assert "actual_tokens" not in event and "max_tokens" not in event
        assert event["unit"] == "chars"
