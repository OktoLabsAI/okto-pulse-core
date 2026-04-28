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


def test_pre_flight_marks_copy_steps_as_mandatory(text):
    # The new bullet says "are mandatory before `started → in_progress`"
    assert "mandatory before `started" in text or "Steps 6 and 7 are mandatory" in text


def test_card_kb_lifecycle_section_documents_5_tools(text):
    for name in (
        "okto_pulse_add_card_knowledge",
        "okto_pulse_list_card_knowledge",
        "okto_pulse_get_card_knowledge",
        "okto_pulse_update_card_knowledge",
        "okto_pulse_delete_card_knowledge",
    ):
        assert name in text, f"agent_instructions must reference {name}"


def test_rest_mirror_documented(text):
    assert "/api/v1/cards/{card_id}/knowledge" in text
    assert ".../download" in text or "/download" in text
