from __future__ import annotations

import pytest

from okto_pulse.core.mcp import server as mcp_server


@pytest.mark.asyncio
async def test_operational_mcp_tools_are_registered_and_described_currently():
    tools = await mcp_server.mcp.get_tools()

    required = {
        "okto_pulse_get_traceability_report",
        "okto_pulse_kg_canonical_debt_list",
        "okto_pulse_kg_dead_letter_list",
        "okto_pulse_kg_dead_letter_reprocess",
        "okto_pulse_kg_connectivity_dlq_diagnose",
        "okto_pulse_kg_connectivity_dlq_reprocess",
        "okto_pulse_kg_connectivity_dlq_verify",
        "okto_pulse_kg_health_readiness",
        "okto_pulse_create_card",
        "okto_pulse_submit_task_validation",
        "okto_pulse_list_by_board",
        "okto_pulse_create_topic",
        "okto_pulse_update_topic",
        "okto_pulse_archive_topic",
        "okto_pulse_restore_topic",
        "okto_pulse_delete_topic",
        "okto_pulse_merge_topics",
        "okto_pulse_get_resource_gate_summary",
        "okto_pulse_mark_resource_not_applicable",
        "okto_pulse_clear_resource_not_applicable",
        "okto_pulse_create_story",
        "okto_pulse_update_story",
        "okto_pulse_move_story",
        "okto_pulse_archive_story",
        "okto_pulse_restore_story",
        "okto_pulse_link_story_to_ideation",
        "okto_pulse_convert_stories_to_ideation",
    }
    assert required.issubset(tools.keys())

    # --- R1.1: tools/list descriptions are now COMPACT summaries; the long-form
    # detail (args/returns/enum prose/examples) moved to lazy tool-docs resources.
    # Every required tool keeps its name + a non-empty description within budget,
    # and the moved detail must be discoverable in its tool-docs resource (i.e.
    # compacted, NOT deleted).
    for name in required:
        desc = tools[name].description or ""
        assert desc, f"{name} lost its description entirely"
        assert len(desc) <= 900, f"{name} description {len(desc)} > 900-char budget"

    load = mcp_server._load_resource_file

    # create_card: compact summary keeps the core; the card-type enum prose +
    # "spec is approved" moved to the card tool-docs resource.
    create_card_desc = tools["okto_pulse_create_card"].description
    assert "Create a new card" in create_card_desc
    assert "spec" in create_card_desc.lower()
    assert 'Card type - "normal" (default), "test", or "bug"' not in create_card_desc
    card_docs = load("reference/tool-docs/card.md")
    assert "okto_pulse_create_card" in card_docs
    assert 'Card type - "normal" (default), "test", or "bug"' in card_docs
    assert "spec is approved" in card_docs

    # submit_task_validation: compact summary names the three dimensions; the
    # full outcome prose ("failed remains in \u2026") moved to tool-docs.
    validation_desc = tools["okto_pulse_submit_task_validation"].description
    assert "confidence" in validation_desc
    assert "completeness" in validation_desc
    assert "drift" in validation_desc
    assert "failed remains in" in load("reference/tool-docs/misc.md")

    # canonical-debt list: health drill-down tool must be discoverable.
    debt_list_desc = tools["okto_pulse_kg_canonical_debt_list"].description
    assert "canonical-debt" in debt_list_desc.lower()
    assert "okto_pulse_kg_canonical_debt_list" in load("reference/tool-docs/kg.md")

    # dead-letter list: compact summary; the reprocess cross-reference moved to
    # the kg tool-docs resource.
    dlq_list_desc = tools["okto_pulse_kg_dead_letter_list"].description
    assert "dead-letter" in dlq_list_desc.lower()
    assert "okto_pulse_kg_dead_letter_reprocess" in load("reference/tool-docs/kg.md")

    # reprocess: its identity line survives in the compact summary.
    reprocess_desc = tools["okto_pulse_kg_dead_letter_reprocess"].description
    assert "requeue dead-lettered KG" in reprocess_desc

    # RKG-04: the connectivity-guard class tools are registered, documented and the
    # reprocess is fail-closed (never a broad reprocess of unanalysed DLQs).
    kg_docs = load("reference/tool-docs/kg.md")
    for tool_name in (
        "okto_pulse_kg_connectivity_dlq_diagnose",
        "okto_pulse_kg_connectivity_dlq_reprocess",
        "okto_pulse_kg_connectivity_dlq_verify",
    ):
        assert tool_name in kg_docs, f"{tool_name} missing from kg tool-docs"
    conn_reprocess_desc = tools["okto_pulse_kg_connectivity_dlq_reprocess"].description
    assert "fail-closed" in conn_reprocess_desc.lower()
    assert "selected_dlq_out_of_class" in kg_docs

    # RKG-05: the canonical non-maskable health/readiness tool is registered +
    # documented and keeps blocking vs would_block_done discoverable.
    assert "okto_pulse_kg_health_readiness" in kg_docs
    hr_desc = tools["okto_pulse_kg_health_readiness"].description
    assert "would_block_done" in hr_desc
    assert "non_maskable" in kg_docs.lower()

    # mockup family: "story" discoverability is preserved either in the compact
    # description or in the mockup tool-docs resource.
    mockup_docs = load("reference/tool-docs/mockup.md")
    for tool_name in (
        "okto_pulse_add_screen_mockup",
        "okto_pulse_update_screen_mockup",
        "okto_pulse_annotate_mockup",
        "okto_pulse_list_screen_mockups",
        "okto_pulse_delete_screen_mockup",
    ):
        in_desc = "story" in tools[tool_name].description.lower()
        in_docs = tool_name in mockup_docs and "story" in mockup_docs.lower()
        assert in_desc or in_docs, f"{tool_name}: story discoverability lost"
    list_mockups = tools["okto_pulse_list_screen_mockups"]
    assert "include_content" in list_mockups.parameters["properties"]
    assert "include_content" in mockup_docs
    assert "html_content_sha256" in mockup_docs

    # traceability summary keeps its full SDLC-chain identity inline.
    traceability_desc = tools["okto_pulse_get_traceability_report"].description
    assert "traceability report" in traceability_desc
    assert "ideation" in traceability_desc
    assert "spec" in traceability_desc
    assert "card/test/bug" in traceability_desc
