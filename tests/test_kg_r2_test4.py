"""R2-TEST4 (card 92130925) — no agent-facing manual demotion (AC5).

Scenario ts_07935ff5: the stale-canonical parity surface is a READ-ONLY
diagnostic. No agent-facing MCP tool may demote/clear stale state or mask debt;
the only internal demotion path is the R2 reconciler (driven by a maturity/status
event or sweep), never an operator/agent mutation tool.

Proven two ways: (1) the real MCP drilldown tool declares ``mutation_allowed=false``
and leaves the canonical node untouched, and (2) the registered MCP tool surface
contains NO stale-demotion / canonical-demotion mutation tool.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile

import pytest
from mcp_runtime_testing import register_mcp_test_runtime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
os.environ.setdefault("KG_BASE_DIR", tempfile.mkdtemp(prefix="okto_kg_r2t4_"))

from r2_scenario_helpers import (
    count_canonical,
    new_board,
    seed_done_spec_canonical,
    set_spec_status,
)

from okto_pulse.core.mcp import server as mcp_server

STALE_TOOL = "okto_pulse_kg_stale_canonical_parity_list"


@pytest.fixture(autouse=True)
def _tmp_rebuild_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("OKTO_PULSE_REBUILD_BASE_DIR", str(tmp_path))
    return tmp_path


async def _make_stale_spec(db_factory, board_id):
    spec_id, _ref = await seed_done_spec_canonical(db_factory, board_id)
    await set_spec_status(db_factory, spec_id, "draft")
    return spec_id


# ===========================================================================
# ts_07935ff5 — the MCP diagnostic is read-only (mutation_allowed=false, no demote)
# ===========================================================================


@pytest.mark.asyncio
async def test_stale_parity_mcp_tool_is_read_only(db_factory, monkeypatch):
    board_id = await new_board(db_factory, "r2t4")
    await _make_stale_spec(db_factory, board_id)
    canonical_before = await count_canonical(board_id, "Requirement")
    assert canonical_before >= 1

    async def _fake_ctx(board_id: str):
        return type(
            "Ctx",
            (),
            {
                "agent_id": "r2t4-agent",
                "agent_name": "r2t4-agent",
                "permissions": ["board:read"],
            },
        )()

    register_mcp_test_runtime(db_factory)
    monkeypatch.setattr(mcp_server, "_get_agent_ctx", _fake_ctx)

    tool = await mcp_server.mcp.get_tool(STALE_TOOL)
    raw = await tool.fn(board_id=board_id, limit=50, offset=0)
    payload = json.loads(raw)

    assert payload["mutation_allowed"] is False
    assert payload["count"] >= 1
    assert payload["health_issue_code"] == "stale_canonical_parity"
    # TEETH: a diagnostic that secretly demoted would drop the canonical count.
    assert await count_canonical(board_id, "Requirement") == canonical_before


# ===========================================================================
# No agent-facing MCP tool can demote/clear stale or mask debt
# ===========================================================================


@pytest.mark.asyncio
async def test_no_agent_facing_mcp_tool_demotes_or_clears_stale():
    names = set((await mcp_server.mcp.get_tools()).keys())

    # The read-only drilldown IS exposed ...
    assert STALE_TOOL in names

    # ... but NO mutation tool over stale/canonical-demotion is exposed.
    mutation_tokens = (
        "demote", "reconcile_stale", "clear_stale", "stale_demotion",
        "canonical_demotion", "demotion_sync", "mutate_stale",
    )
    offenders = sorted(n for n in names if any(t in n for t in mutation_tokens))
    assert offenders == [], f"unexpected stale-mutation MCP tools: {offenders}"

    # The internal mutating primitives are NOT registered as agent-facing tools.
    for forbidden in (
        "okto_pulse_kg_reconcile_stale_canonical",
        "okto_pulse_kg_demote_stale_canonical",
        "okto_pulse_kg_canonical_demotion_global_sync",
        "okto_pulse_kg_clear_stale_canonical",
    ):
        assert forbidden not in names
