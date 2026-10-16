"""F4: no public stale-parity report or manual demotion capability."""
import pytest
from okto_pulse.core.mcp import server as mcp_server

STALE_TOOL = "okto_pulse_kg_stale_canonical_parity_list"


@pytest.mark.asyncio
async def test_no_agent_facing_mcp_tool_demotes_or_clears_stale():
    names = set((await mcp_server.mcp.get_tools()).keys())

    # F4 also removes the detailed read-only repair report.
    assert STALE_TOOL not in names

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
