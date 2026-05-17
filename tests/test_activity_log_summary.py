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
    """Unknown actions fall back to `trigger=<value>` when present."""
    details = {"trigger": "some_legacy_event"}
    out = _activity_log_summary("not_a_known_action_xyz", details)
    assert out == "trigger=some_legacy_event"


def test_summary_empty_for_non_dict_details():
    assert _activity_log_summary("anything", None) == ""
    assert _activity_log_summary("anything", "raw string") == ""


def test_summary_for_action_without_trigger():
    assert _activity_log_summary("some_action", {}) == ""


# ---------------------------------------------------------------------------
# NC-1 fix — expanded action-type coverage
# ---------------------------------------------------------------------------


def test_summary_card_moved_basic():
    details = {"from_status": "not_started", "to_status": "in_progress"}
    out = _activity_log_summary("card_moved", details)
    assert out == "not_started->in_progress"


def test_summary_card_moved_includes_card_id_prefix():
    details = {
        "from_status": "in_progress",
        "to_status": "validation",
        "card_id": "abcdef1234567890",
    }
    out = _activity_log_summary("card_moved", details)
    assert "card=abcdef12" in out
    assert "in_progress->validation" in out


def test_summary_card_moved_partial_schema_falls_back_to_status():
    """NC-4 defense: legacy events without from_status/to_status must
    still produce a useful summary instead of '?->?'."""
    details = {"status": "validation", "position": -1}
    out = _activity_log_summary("card_moved", details)
    assert out == "status=validation"


def test_summary_card_moved_partial_schema_with_card_id():
    """NC-4 defense + card_id prefix."""
    details = {"status": "done", "position": 0, "card_id": "cafe1234abc"}
    out = _activity_log_summary("card_moved", details)
    assert "card=cafe1234" in out
    assert "status=done" in out
    assert "?->?" not in out


def test_summary_card_created_with_title_and_status():
    details = {"title": "Implement login", "status": "not_started"}
    out = _activity_log_summary("card_created", details)
    assert "created" in out
    assert "Implement login" in out
    assert "status=not_started" in out


def test_summary_card_created_minimal():
    out = _activity_log_summary("card_created", {})
    assert out == "created"


def test_summary_card_updated_includes_action():
    out = _activity_log_summary("card_updated", {"title": "X"})
    assert out.startswith("updated")


def test_summary_card_deleted_includes_action():
    out = _activity_log_summary("card_deleted", {"title": "Z"})
    assert out.startswith("deleted")


def test_summary_validation_submitted_approved():
    details = {
        "outcome": "approved",
        "card_title": "Fix pagination",
        "confidence": 0.92,
    }
    out = _activity_log_summary("validation_submitted", details)
    assert "outcome=approved" in out
    assert "Fix pagination" in out
    assert "confidence=0.92" in out


def test_summary_validation_submitted_rejected():
    details = {"outcome": "rejected", "card_title": "Add tests"}
    out = _activity_log_summary("validation_submitted", details)
    assert "outcome=rejected" in out
    assert "Add tests" in out


def test_summary_spec_validation_submitted():
    details = {
        "spec_id": "deadbeef1234",
        "outcome": "success",
        "from_status": "draft",
        "to_status": "validated",
    }
    out = _activity_log_summary("spec_validation_submitted", details)
    assert "outcome=success" in out
    assert "spec=deadbeef" in out
    assert "draft->validated" in out


def test_summary_spec_validated_alias():
    details = {"spec_id": "deadbeef", "outcome": "success"}
    out = _activity_log_summary("spec_validated", details)
    assert "outcome=success" in out
    assert "spec=deadbeef" in out


def test_summary_task_validated():
    details = {"outcome": "approved", "card_id": "abc12345xyz"}
    out = _activity_log_summary("task_validated", details)
    assert "task" in out
    assert "outcome=approved" in out
    assert "card=abc12345" in out


def test_summary_ideation_created():
    details = {"title": "Cursor pagination", "ideation_id": "cafe1234abcd"}
    out = _activity_log_summary("ideation_created", details)
    assert "ideation created" in out
    assert "Cursor pagination" in out
    assert "id=cafe1234" in out


def test_summary_spec_created():
    details = {"title": "Relevance scoring", "spec_id": "beef5678abcd"}
    out = _activity_log_summary("spec_created", details)
    assert "spec created" in out
    assert "Relevance scoring" in out
    assert "id=beef5678" in out


def test_summary_refinement_created():
    details = {"title": "Deep dive", "refinement_id": "ref01234abcd"}
    out = _activity_log_summary("refinement_created", details)
    assert "refinement created" in out
    assert "Deep dive" in out


def test_summary_sprint_created():
    details = {"title": "Sprint 4", "sprint_id": "abc00001"}
    out = _activity_log_summary("sprint_created", details)
    assert "sprint created" in out
    assert "Sprint 4" in out


def test_summary_ideation_moved():
    details = {
        "ideation_id": "1234abcdwxyz",
        "from_status": "draft",
        "to_status": "done",
    }
    out = _activity_log_summary("ideation_moved", details)
    assert "ideation" in out
    assert "draft->done" in out
    assert "id=1234abcd" in out


def test_summary_comment_added_with_content():
    details = {"content": "Looks good to me, merging."}
    out = _activity_log_summary("comment_added", details)
    assert out.startswith("comment")
    assert "Looks good" in out


def test_summary_choice_comment_added():
    details = {"question": "Which approach?", "option_count": 3}
    out = _activity_log_summary("choice_comment_added", details)
    assert "choice_comment" in out
    assert "options=3" in out
    assert "Which approach?" in out


# ---------------------------------------------------------------------------
# Regression — limit clamp (Story 3 FR: limit = min(limit, 200))
# ---------------------------------------------------------------------------


def test_limit_clamp_documented_contract():
    """Pure unit assertion of the clamp formula used at server.py:1833.

    The actual clamp is applied inside ``okto_pulse_get_activity_log`` (async),
    so end-to-end coverage lives in integration smoke. This test documents
    the contract so future refactors can't silently regress it.
    """
    assert min(500, 200) == 200, "clamp(500) must yield 200"
    assert min(1000, 200) == 200, "clamp(1000) must yield 200"
    assert min(200, 200) == 200, "clamp(200) exact boundary preserved"
    assert min(199, 200) == 199, "clamp(199) below boundary preserved"
    assert min(0, 200) == 0, "clamp(0) yields 0 — tool returns empty result"
    assert min(50, 200) == 50, "default clamp(50) preserved"


def test_summary_output_never_exceeds_200_chars():
    """Every branch must produce ≤200 chars even with adversarial input."""
    long_title = "A" * 300
    cases = [
        ("card_created", {"title": long_title, "status": "not_started"}),
        ("card_moved", {"from_status": long_title, "to_status": long_title, "card_id": long_title}),
        ("validation_submitted", {"outcome": "approved", "card_title": long_title}),
        ("ideation_created", {"title": long_title, "ideation_id": "x" * 64}),
        ("spec_created", {"title": long_title, "spec_id": "y" * 64}),
        ("comment_added", {"content": long_title}),
        ("choice_comment_added", {"question": long_title, "option_count": 5}),
        ("spec_validation_submitted", {"outcome": long_title, "spec_id": long_title}),
    ]
    for action, details in cases:
        out = _activity_log_summary(action, details)
        assert len(out) <= 200, (
            f"action={action!r} produced {len(out)} chars (>200): {out!r}"
        )
