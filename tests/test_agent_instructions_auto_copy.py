"""TC-4 (TS4) — agent_instructions.md must include the mandatory auto-copy
bullet for mockups+KEs at the started→in_progress transition.

This is a simple grep-style guard. If someone reverts or rewrites the
Pre-Flight section without keeping the snapshot rule, this test fails.
"""

from __future__ import annotations

from pathlib import Path

import pytest


_INSTR = Path(__file__).resolve().parent.parent / "src" / "okto_pulse" / "core" / "mcp" / "agent_instructions.md"


@pytest.fixture(scope="module")
def text() -> str:
    assert _INSTR.exists(), f"agent_instructions.md not found at {_INSTR}"
    return _INSTR.read_text(encoding="utf-8")


def test_pre_flight_mentions_copy_mockups_to_card(text):
    assert "okto_pulse_copy_mockups_to_card" in text


def test_pre_flight_mentions_copy_knowledge_to_card(text):
    assert "okto_pulse_copy_knowledge_to_card" in text


def test_pre_flight_mentions_copy_architecture_to_card(text):
    assert "okto_pulse_copy_architecture_to_card" in text


def test_pre_flight_marks_copy_steps_as_mandatory(text):
    # The new bullet says "are mandatory before `started → in_progress`"
    assert "mandatory before `started" in text or "Steps 6, 7, and 8 are mandatory" in text


def test_card_kb_lifecycle_section_documents_5_tools(text):
    for name in (
        "okto_pulse_add_card_knowledge",
        "okto_pulse_list_card_knowledge",
        "okto_pulse_get_card_knowledge",
        "okto_pulse_update_card_knowledge",
        "okto_pulse_delete_card_knowledge",
    ):
        assert name in text, f"agent_instructions must reference {name}"


def test_resource_gate_section_documents_mcp_tools_and_warning(text):
    for name in (
        "okto_pulse_get_resource_gate_summary",
        "okto_pulse_mark_resource_not_applicable",
        "okto_pulse_clear_resource_not_applicable",
    ):
        assert name in text, f"agent_instructions must reference {name}"
    assert "Architecture, Mockup, and Knowledge Base" in text
    assert "justification` is mandatory" in text
    assert "partial or incorrect solutions" in text


def test_resource_gate_documents_entity_matrix_and_kb_scope(text):
    assert "Resource Gate `entity_type`" in text
    for entity_type in ("`ideation`", "`refinement`", "`spec`", "`card`"):
        assert entity_type in text
    assert "entity_type=card" in text
    assert "Tasks, tests, and bugs always use" in text
    assert "Knowledge Base can be attached from ideation through task" in text
    assert "Stories stay outside the Resource Gate until conversion to Ideation" in text
    assert "Knowledge Base only starts at spec/card" not in text


def test_resource_gate_na_playbook_is_reversible_and_auditable(text):
    assert "N/A playbook" in text
    assert "`justification` is mandatory" in text
    assert "partial or incorrect solutions" in text
    assert "okto_pulse_clear_resource_not_applicable" in text
    assert "reversible" in text


def test_stories_topics_section_documents_full_mcp_tool_index(text):
    for name in (
        "okto_pulse_create_topic",
        "okto_pulse_list_topics",
        "okto_pulse_update_topic",
        "okto_pulse_archive_topic",
        "okto_pulse_restore_topic",
        "okto_pulse_delete_topic",
        "okto_pulse_merge_topics",
        "okto_pulse_create_story",
        "okto_pulse_list_stories",
        "okto_pulse_update_story",
        "okto_pulse_move_story",
        "okto_pulse_archive_story",
        "okto_pulse_restore_story",
        "okto_pulse_link_story_to_ideation",
        "okto_pulse_convert_stories_to_ideation",
    ):
        assert name in text, f"agent_instructions must reference {name}"


def test_stories_topics_preflight_and_link_semantics_are_documented(text):
    assert "Story/Topic operation pre-flight" in text
    assert "status" in text
    assert "archived" in text
    assert "ideation_links" in text
    assert "A Story can link to at most one Ideation" in text
    assert "linking the same Story to a different Ideation is rejected" in text
    assert "Multiple Stories may link to the same Ideation" in text
    assert "mark_converted` argument is compatibility-only" in text
    assert "always marks the Story as `converted`" in text


def test_common_errors_cover_resource_gate_and_story_topic_recoveries(text):
    for error in (
        "resource_gate_missing_resources",
        "invalid_entity_type",
        "topic_not_empty",
        "Only ready Stories can be converted",
        "Story can only be linked to editable Ideations",
    ):
        assert error in text
    for recovery in (
        "copy_",
        "entity_type=card",
        "okto_pulse_merge_topics",
        "okto_pulse_move_story",
        "okto_pulse_archive_story",
        "ready",
        "editable",
    ):
        assert recovery in text


def test_rest_mirror_documented(text):
    assert "/api/v1/cards/{card_id}/knowledge" in text
    assert ".../download" in text or "/download" in text


def test_cognitive_kg_closeout_section_is_mandatory_for_specs_and_bugs(text):
    assert "Cognitive KG Closeout — mandatory for specs and bugs" in text
    assert "Before declaring a spec or bug complete" in text
    assert "Close a spec or bug" in text


def test_cognitive_kg_closeout_distinguishes_operational_health(text):
    assert "Operational KG health is not enough" in text
    assert "queue_depth=0" in text
    assert "dead_letter_count=0" in text
    assert "does not prove there are no learnings" in text


def test_cognitive_kg_closeout_documents_required_mcp_flow(text):
    for name in (
        "okto_pulse_get_spec_context",
        "okto_pulse_get_task_context",
        "okto_pulse_get_task_conclusions",
        "okto_pulse_kg_begin_consolidation",
        "okto_pulse_kg_add_node_candidate",
        "okto_pulse_kg_add_edge_candidate",
        "okto_pulse_kg_propose_reconciliation",
        "okto_pulse_kg_commit_consolidation",
        "okto_pulse_kg_abort_consolidation",
    ):
        assert name in text, f"agent_instructions must reference {name}"


def test_cognitive_kg_closeout_forbids_deterministic_fabrication(text):
    assert "Forbidden manual deterministic artifacts" in text
    assert "do not fabricate Task, Bug, Spec" in text
    for edge in ("implements", "tests", "belongs_to", "mentions", "violates", "derives_from"):
        assert edge in text


def test_cognitive_kg_closeout_final_response_reports_result(text):
    assert "Final response requirement for spec/bug work" in text
    assert "committed: session_id" in text
    assert "nothing_changed" in text
    assert "not_applicable" in text
