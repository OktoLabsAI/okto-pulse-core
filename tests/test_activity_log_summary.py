"""Ideação MCP-token-optimization Story 3 — activity log summary tests."""

from __future__ import annotations

from okto_pulse.core.mcp.server import _activity_log_summary


def test_summary_for_arch_create_carries_trigger_and_counts():
    details = {
        "trigger": "spec_architecture_created",
        "results": {
            "architecture": {"copied_count": 2, "ignored_count": 0, "removed_count": 0},
            "knowledge_base": {"copied_count": 0, "ignored_count": 0, "removed_count": 0},
            "mockup": {"copied_count": 0, "ignored_count": 0, "removed_count": 0},
        },
    }
    out = _activity_log_summary("spec_resources_auto_propagated", details)
    assert "trigger=spec_architecture_created" in out
    assert "arch.copied=2" in out
    assert "kb.copied=0" in out
    assert "mockup.copied=0" in out


def test_summary_includes_ignored_when_nonzero():
    details = {
        "trigger": "spec_mockups_changed",
        "results": {"mockup": {"copied_count": 0, "ignored_count": 1, "removed_count": 0}},
    }
    out = _activity_log_summary("spec_resources_auto_propagated", details)
    assert "mockup.ignored=1" in out


def test_summary_includes_removed_when_nonzero():
    details = {
        "trigger": "spec_knowledge_deleted",
        "results": {"knowledge_base": {"copied_count": 0, "ignored_count": 0, "removed_count": 1}},
    }
    out = _activity_log_summary("spec_resources_auto_propagated", details)
    assert "kb.removed=1" in out


def test_summary_falls_back_to_trigger_only_for_unknown_action():
    details = {"trigger": "card_created"}
    out = _activity_log_summary("card_created", details)
    assert out == "trigger=card_created"


def test_summary_empty_for_non_dict_details():
    assert _activity_log_summary("anything", None) == ""
    assert _activity_log_summary("anything", "raw string") == ""


def test_summary_for_action_without_trigger():
    assert _activity_log_summary("some_action", {}) == ""
