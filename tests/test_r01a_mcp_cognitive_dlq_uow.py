"""The former cognitive DLQ transport was a technical maintenance dump (F4)."""
import importlib.util
import pytest
from okto_pulse.core.mcp import server
from okto_pulse.core.services import dead_letter_inspector_service

@pytest.mark.asyncio
async def test_technical_dlq_dump_removed_while_semantic_work_remains():
    tools = await server.mcp.get_tools()
    assert "okto_pulse_kg_list_cognitive_dlq" not in tools
    assert not hasattr(server, "okto_pulse_kg_list_cognitive_dlq")
    assert importlib.util.find_spec("okto_pulse.core.application.use_cases.list_cognitive_dlq") is None
    assert not hasattr(dead_letter_inspector_service, "list_cognitive_dlq_rows")
    assert {"okto_pulse_kg_list_cognitive_pending_items", "okto_pulse_kg_evaluate_cognitive_readiness", "okto_pulse_kg_begin_consolidation", "okto_pulse_kg_commit_consolidation"} <= tools.keys()
