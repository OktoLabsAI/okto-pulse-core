"""R1.1 — compact MCP tool descriptions + lazy documentation resources.

Covers spec ``MCP Tools/List Compact Description Budget`` scenarios owned by
R1.1:

- ``ts_65f65743`` — registered tool names remain stable after compaction.
- ``ts_8efb26f9`` — a compacted tool exposes one stable lazy doc reference.
- ``ts_9b5ff08a`` — compacted tool invocation preserves schema validation.
- ``ts_2c4e144c`` — safety guidance remains discoverable after compaction.
- ``ts_b0b631be`` — no silent enum/schema narrowing (snapshot stays stable).
- ``ts_84db6226`` — the tool-registration contract test uses compact-doc
  expectations.

BUDGET NOTE: tool descriptions hit the reviewed live-surface budget (aggregate ≤ 70000,
per-tool ≤ 900). ``agent_instructions.md`` is kept richer than the spec's 8000
target by an explicit, owner-authorised decision (see the R1.1 card comment) —
all operational/safety DIRECTIVES stay inline; only deep REFERENCE moved to lazy
resources. The renegotiated always-loaded budget is 10000 chars.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from okto_pulse.core.mcp import server as mcp_server
from okto_pulse.core.mcp.payload_budget import (
    BudgetProfile,
    MCPPayloadBudgetScanner,
    snapshot_instructions,
    snapshot_tool_descriptions,
)

# Renegotiated always-loaded instruction budget (owner-authorised, R1.1 card).
R1_INSTRUCTION_BUDGET = 10000
PER_TOOL_BUDGET = 900
AGGREGATE_TOOL_BUDGET = 70000

CONTRACT_TEST = (
    Path(__file__).parent / "test_mcp_tool_registration_contract.py"
)


def _tools() -> dict:
    return asyncio.new_event_loop().run_until_complete(mcp_server.mcp.get_tools())


# ---------------------------------------------------------------------------
# ts_65f65743 — tool names stable after compaction
# ---------------------------------------------------------------------------

# A representative operational baseline that must never lose/rename a tool.
BASELINE_TOOLS = {
    "okto_pulse_create_card",
    "okto_pulse_get_task_context",
    "okto_pulse_submit_task_validation",
    "okto_pulse_submit_spec_validation",
    "okto_pulse_add_architecture_design",
    "okto_pulse_get_traceability_report",
    "okto_pulse_kg_query_cypher",
    "okto_pulse_kg_health",
    "okto_pulse_list_by_board",
    "okto_pulse_update_test_scenario_status",
}


def test_tool_names_stable_after_compaction():
    names = set(_tools().keys())
    # No tool dropped or renamed.
    assert BASELINE_TOOLS.issubset(names)
    # Compaction only touched docstrings; every tool keeps the okto_pulse_ prefix.
    # Surface size is additive after the R1 baseline. Keep the current reviewed
    # surface pinned so accidental tool drops/duplicates remain visible.
    # 2026-07-12 (auditoria MCP): re-pinned 259→265 (pin apodrecido enquanto 6
    # tools entraram); o delta exato agora é nomeado por
    # test_mcp_tools_catalog_drift.py.
    # 2026-07-27: reviewed SK-A surface is 292 tools. The five additive Quality
    # assessment commands expose record/current/receipt/history/findings.
    assert len(names) == 292
    assert all(n.startswith("okto_pulse_") for n in names)


# ---------------------------------------------------------------------------
# ts_8efb26f9 — compacted tool exposes one stable lazy doc reference
# ---------------------------------------------------------------------------


def test_compacted_tool_exposes_one_stable_lazy_doc_uri():
    tools = _tools()
    registered = {entry[0] for entry in mcp_server._RESOURCE_REGISTRY}

    tool = "okto_pulse_add_architecture_design"
    uri = mcp_server.tool_docs_uri(tool)
    # exactly one canonical URI, and it is registered + resolvable
    assert uri == "okto-pulse://reference/tool-docs/architecture"
    assert uri in registered
    docs = mcp_server._load_resource_file("reference/tool-docs/architecture.md")
    assert docs  # resources/read resolves to non-empty content
    assert tool in docs  # the compacted tool's full docs live here

    # the compact tools/list description is within budget and dropped long content
    desc = tools[tool].description
    assert len(desc) <= PER_TOOL_BUDGET
    assert "Contextual validation examples:" not in desc  # long block moved out


# ---------------------------------------------------------------------------
# ts_9b5ff08a — schema validation preserved (no silent schema change)
# ---------------------------------------------------------------------------


def test_compacted_tool_preserves_parameter_schema():
    tools = _tools()
    params = tools["okto_pulse_create_card"].parameters
    props = params.get("properties", {})
    # Parameter names + required flags unchanged by docstring compaction.
    for key in ("board_id", "title", "spec_id", "status", "card_type"):
        assert key in props
    required = set(params.get("required", []))
    assert {"board_id", "title"}.issubset(required)


# ---------------------------------------------------------------------------
# ts_b0b631be — no silent enum/schema narrowing
# ---------------------------------------------------------------------------


def test_no_silent_enum_narrowing():
    # We did NOT migrate any prose enum to Literal — schemas stay byte-stable
    # (drift now guarded by test_mcp_tools_catalog_drift.py; the schema
    # snapshot pilot was retired in the 2026-07-12 MCP surface audit).
    # Spot-check that a field documenting an enum in prose is still a plain
    # string, not narrowed.
    tools = _tools()
    props = tools["okto_pulse_create_card"].parameters.get("properties", {})
    assert props["card_type"].get("type") == "string"
    assert "enum" not in props["card_type"]
    assert props["status"].get("type") == "string"


# ---------------------------------------------------------------------------
# ts_2c4e144c — safety guidance remains discoverable
# ---------------------------------------------------------------------------


def test_safety_guidance_discoverable_after_compaction():
    instr = mcp_server._load_instructions()

    # Every required safety/pre-flight topic is present in the always-loaded
    # instructions (as a directive or a mandatory resource reference).
    assert "kg-health" in instr or "KG health" in instr  # KG health stop-rule
    assert "recovery_needed" in instr or "stop-rule" in instr.lower()
    assert "Destructive Operations" in instr or "destructive_ops" in instr
    assert "Resource Gate" in instr
    assert "reference/transitions" in instr or "Card Status Transitions" in instr
    assert "Card execution pre-flight" in instr
    assert "Resource Fetching Protocol" in instr  # MANDATORY directive restored

    # Owner-authorised budget: rich always-loaded instructions, <= 10000 chars
    # (see R1.1 card decision). Directives stay inline; deep reference is lazy.
    assert len(instr) <= R1_INSTRUCTION_BUDGET


# ---------------------------------------------------------------------------
# ts_84db6226 — registration contract test updated to compact-doc expectations
# ---------------------------------------------------------------------------


def test_registration_contract_uses_compact_doc_expectations():
    src = CONTRACT_TEST.read_text(encoding="utf-8")
    # The updated contract test asserts lazy tool-docs references, not long
    # docstring substrings.
    assert "reference/tool-docs/" in src
    assert "_load_resource_file" in src


# ---------------------------------------------------------------------------
# R1.1 budget — strict tool-description budgets are met on the LIVE surface
# ---------------------------------------------------------------------------


def test_live_tool_description_budget_passes_after_compaction():
    profile = BudgetProfile(
        name="r1_tool_descriptions",
        per_tool_description_chars=PER_TOOL_BUDGET,
        aggregate_tool_description_chars=AGGREGATE_TOOL_BUDGET,
        always_loaded_instruction_chars=R1_INSTRUCTION_BUDGET,
    )
    scanner = MCPPayloadBudgetScanner(profile)
    report = scanner.scan(
        tool_descriptions=snapshot_tool_descriptions(mcp_server.mcp),
        instructions=snapshot_instructions(mcp_server),
    )
    # Unlike R5.2's pre-trim surface, R1.1 makes the LIVE surface pass the gate.
    assert report.passed is True, report.to_dict()["violations"]
    assert report.unit == "chars"
    assert report.measurements["aggregate_tool_description_chars"] <= AGGREGATE_TOOL_BUDGET


@pytest.mark.parametrize(
    "family",
    ["architecture", "card", "spec", "kg", "ideation", "test-scenario"],
)
def test_tool_docs_resources_registered_and_nonempty(family):
    uri = f"okto-pulse://reference/tool-docs/{family}"
    registered = {entry[0] for entry in mcp_server._RESOURCE_REGISTRY}
    assert uri in registered
    content = mcp_server._load_resource_file(f"reference/tool-docs/{family}.md")
    assert content.startswith("---")  # frontmatter
    expected_version = "2.0" if family == "test-scenario" else "1.0"
    assert f'version: "{expected_version}"' in content


def test_all_tool_doc_uris_are_registered_and_resolvable():
    """Codex val_cb28dd19: EVERY registered tool — not just a sample — must
    expose a stable doc URI that is registered AND resolves to a non-empty
    resource containing that tool's docs (api_fd7c5878)."""
    tools = _tools()
    registry = {entry[0]: entry[1] for entry in mcp_server._RESOURCE_REGISTRY}
    broken: list[tuple[str, str]] = []
    for tool_name in tools:
        uri = mcp_server.tool_docs_uri(tool_name)
        rel = registry.get(uri)
        if rel is None:
            broken.append((tool_name, f"unregistered:{uri}"))
            continue
        content = mcp_server._load_resource_file(rel)
        if not content.strip():
            broken.append((tool_name, f"empty:{uri}"))
        elif tool_name not in content:
            broken.append((tool_name, f"missing-tool-doc:{uri}"))
    assert broken == [], f"{len(broken)} tools with broken doc URIs: {broken[:12]}"
