"""TC-4 (TS4) — agent_instructions.md must include the mandatory auto-copy
bullet for mockups+KEs at the started→in_progress transition.

This is a simple grep-style guard. If someone reverts or rewrites the
Pre-Flight section without keeping the snapshot rule, this test fails.
"""

from __future__ import annotations

from pathlib import Path

import pytest


_INSTR = Path(__file__).resolve().parent.parent / "src" / "okto_pulse" / "core" / "mcp" / "agent_instructions.md"
_RESOURCES = Path(__file__).resolve().parent.parent / "src" / "okto_pulse" / "core" / "mcp" / "resources"


@pytest.fixture(scope="module")
def text() -> str:
    assert _INSTR.exists(), f"agent_instructions.md not found at {_INSTR}"
    parts = [_INSTR.read_text(encoding="utf-8")]
    for resource in sorted(_RESOURCES.rglob("*.md")):
        parts.append(resource.read_text(encoding="utf-8"))
    return "\n\n".join(parts)


def test_pre_flight_mentions_copy_mockups_to_card(text):
    assert "okto_pulse_copy_mockups_to_card" in text


def test_pre_flight_mentions_copy_knowledge_to_card(text):
    assert "okto_pulse_copy_knowledge_to_card" in text


def test_pre_flight_mentions_copy_architecture_to_card(text):
    assert "okto_pulse_copy_architecture_to_card" in text


def test_pre_flight_marks_copy_steps_as_mandatory(text):
    assert "Mandatory before moving the card to `in_progress`" in text
    assert "MANDATORY — Copy artifacts into every card" in text


def test_current_copy_semantics_are_legacy_all_and_v2_is_not_active(text):
    assert "legacy_all" in text
    assert "Selective propagation v2" in text
    assert "not available until delivery B" in text or "unavailable until delivery B" in text


def test_knowledge_governance_resource_is_linked_and_authoritative(text):
    assert "okto-pulse://reference/knowledge-governance" in text
    assert "advisory, untrusted reference material" in text
    assert "A KB never replaces those artifacts" in text
    assert "Stable Reference Test" in text


def test_card_kb_lifecycle_section_documents_5_tools(text):
    for name in (
        "okto_pulse_add_card_knowledge",
        "okto_pulse_list_knowledge",
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
    assert "justification" in text
    assert "resource_gate_missing_resources" in text


def test_resource_gate_documents_entity_matrix_and_kb_scope(text):
    assert "Resource Gate `entity_type`" in text
    for entity_type in ("`ideation`", "`refinement`", "`spec`", "`card`"):
        assert entity_type in text
    assert "entity_type=card" in text
    assert "Tasks, tests, and bugs must use" in text
    assert "Knowledge Base" in text
    assert "Stories are lightweight" in text
    assert "Knowledge Base only starts at spec/card" not in text


def test_resource_gate_na_playbook_is_reversible_and_auditable(text):
    assert "marking N/A with `justification`" in text
    assert "justification" in text
    assert "resource_gate_missing_resources" in text
    assert "okto_pulse_clear_resource_not_applicable" in text
    assert "reversible" in text


def test_stories_topics_section_documents_full_mcp_tool_index(text):
    for name in (
        "okto_pulse_list_by_board",
        "okto_pulse_create_topic",
        "okto_pulse_update_topic",
        "okto_pulse_archive_topic",
        "okto_pulse_restore_topic",
        "okto_pulse_delete_topic",
        "okto_pulse_merge_topics",
        "okto_pulse_create_story",
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
    assert "okto_pulse_add_card_knowledge" in text
    assert "okto_pulse_get_card_knowledge" in text
    assert "okto_pulse_delete_card_knowledge" in text


def test_cognitive_kg_closeout_section_is_mandatory_for_specs_and_bugs(text):
    assert "Cognitive KG Closeout — Mandatory for Specs and Bugs" in text
    assert "Mandatory closeout sequence" in text
    assert "Final response requirement" in text


def test_cognitive_kg_closeout_distinguishes_operational_health(text):
    assert "KG Health" in text
    assert "queue_depth" in text
    assert "dead_letter_count" in text
    assert "When to consult" in text


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
    assert "Layer 1 deterministic worker" in text
    assert "Manual cognitive edges only" in text
    for edge in ("implements", "tests", "belongs_to", "mentions", "violates", "derives_from"):
        assert edge in text


def test_cognitive_kg_closeout_final_response_reports_result(text):
    assert "Final response requirement" in text
    assert "committed: session_id" in text
    assert "nothing_changed" in text
    assert "not_applicable" in text
