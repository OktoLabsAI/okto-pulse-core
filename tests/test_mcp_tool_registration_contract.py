from __future__ import annotations

import pytest

from okto_pulse.core.mcp import server as mcp_server


@pytest.mark.asyncio
async def test_operational_mcp_tools_are_registered_and_described_currently():
    tools = await mcp_server.mcp.get_tools()

    required = {
        "okto_pulse_get_traceability_report",
        "okto_pulse_kg_dead_letter_list",
        "okto_pulse_kg_dead_letter_reprocess",
        "okto_pulse_create_card",
        "okto_pulse_submit_task_validation",
    }
    assert required.issubset(tools.keys())

    create_card_desc = tools["okto_pulse_create_card"].description
    assert '"test"' in create_card_desc
    assert "spec is approved" in create_card_desc
    assert "must be in 'done' status" not in create_card_desc
    assert 'Card type - "normal" (default), "test", or "bug"' in create_card_desc

    validation_desc = tools["okto_pulse_submit_task_validation"].description
    assert "failed remains in" in validation_desc
    assert "failed \u2192 not_started" not in validation_desc

    dlq_list_desc = tools["okto_pulse_kg_dead_letter_list"].description
    assert "okto_pulse_kg_dead_letter_reprocess" in dlq_list_desc
    assert "READ-only no MVP" not in dlq_list_desc
    assert "deferred v2" not in dlq_list_desc

    reprocess_desc = tools["okto_pulse_kg_dead_letter_reprocess"].description
    assert "requeue dead-lettered KG" in reprocess_desc

    traceability_desc = tools["okto_pulse_get_traceability_report"].description
    assert "traceability report" in traceability_desc
    assert "ideation" in traceability_desc
    assert "spec" in traceability_desc
    assert "card/test/bug" in traceability_desc
