"""Unit tests for built-in preset flag definitions.

Covers Ideação 1 — sprint.move.active_to_review órfão fix: the Validator
preset must own both sprint.move.active_to_review and
sprint.interact_in.active, while Spec/Executor/QA remain blocked so the
sprint-close ceremony isolates to Validator (+ Full Control).
"""

from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from okto_pulse.core.infra.permissions import (  # noqa: E402
    _get_nested,
    get_builtin_presets,
)


@pytest.fixture(scope="module")
def presets_by_name() -> dict:
    return {p["name"]: p for p in get_builtin_presets()}


# ---------------------------------------------------------------------------
# Validator — new flags present
# ---------------------------------------------------------------------------


def test_validator_has_sprint_active_to_review(presets_by_name):
    flags = presets_by_name["Validator"]["flags"]
    assert _get_nested(flags, "sprint.move.active_to_review") is True


def test_validator_has_sprint_interact_in_active(presets_by_name):
    flags = presets_by_name["Validator"]["flags"]
    assert _get_nested(flags, "sprint.interact_in.active") is True


def test_validator_retains_review_to_closed(presets_by_name):
    """Regression: the existing flag must not be dropped by the edit."""
    flags = presets_by_name["Validator"]["flags"]
    assert _get_nested(flags, "sprint.move.review_to_closed") is True


# ---------------------------------------------------------------------------
# Isolation — Executor/QA must NOT gain the flag
# (Spec already had sprint.move.active_to_review before the fix — it owns
#  sprint planning end-to-end. Validator now joins Spec in being able to
#  promote active→review, so the flag is no longer Spec-exclusive. The
#  fix closes the Validator gap, not a Spec/Validator overlap.)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("preset_name", ["Executor", "QA"])
def test_executor_qa_lack_active_to_review(presets_by_name, preset_name):
    flags = presets_by_name[preset_name]["flags"]
    assert _get_nested(flags, "sprint.move.active_to_review") is False, (
        f"{preset_name} has no sprint management duties — "
        f"sprint.move.active_to_review must remain False."
    )


@pytest.mark.parametrize("preset_name", ["Executor", "QA"])
def test_executor_qa_lack_review_to_closed(presets_by_name, preset_name):
    """Executor and QA do not close sprints."""
    flags = presets_by_name[preset_name]["flags"]
    assert _get_nested(flags, "sprint.move.review_to_closed") is False


def test_spec_preset_preserves_draft_to_active(presets_by_name):
    """Spec still owns sprint opening (draft→active) — not touched by this fix."""
    flags = presets_by_name["Spec"]["flags"]
    assert _get_nested(flags, "sprint.move.draft_to_active") is True


def test_validator_does_not_own_sprint_draft_to_active(presets_by_name):
    """Validator does NOT own sprint opening — only Spec (and Full Control)."""
    flags = presets_by_name["Validator"]["flags"]
    assert _get_nested(flags, "sprint.move.draft_to_active") is False


def test_validator_does_not_own_sprint_creation(presets_by_name):
    """Validator does not create sprints — planning remains with Spec."""
    flags = presets_by_name["Validator"]["flags"]
    assert _get_nested(flags, "sprint.entity.create") is False


# ---------------------------------------------------------------------------
# Full Control — regression: still holds the flag
# ---------------------------------------------------------------------------


def test_full_control_retains_sprint_active_to_review(presets_by_name):
    flags = presets_by_name["Full Control"]["flags"]
    assert _get_nested(flags, "sprint.move.active_to_review") is True


def test_full_control_retains_sprint_interact_in_active(presets_by_name):
    flags = presets_by_name["Full Control"]["flags"]
    assert _get_nested(flags, "sprint.interact_in.active") is True


# ---------------------------------------------------------------------------
# PermissionSet — end-to-end check via resolve_permissions
# ---------------------------------------------------------------------------


def test_resolve_permissions_validator_end_to_end(presets_by_name):
    """resolve_permissions yields a PermissionSet where Validator passes
    sprint.move.active_to_review and Executor is denied."""
    from okto_pulse.core.infra.permissions import resolve_permissions

    validator = resolve_permissions(None, presets_by_name["Validator"]["flags"], None)
    executor = resolve_permissions(None, presets_by_name["Executor"]["flags"], None)

    assert validator.has("sprint.move.active_to_review") is True
    assert validator.has("sprint.interact_in.active") is True
    assert executor.has("sprint.move.active_to_review") is False
    # Executor DOES have sprint.interact_in.active to read/Q&A in the
    # active sprint (see preset definition) — that's fine, it's a read
    # permission essentially. Only the move is blocked.
    assert executor.has("sprint.move.review_to_closed") is False


def test_resolve_permissions_check_with_state_validator_in_active(presets_by_name):
    """Validator must pass check_with_state for sprint move in active state."""
    from okto_pulse.core.infra.permissions import resolve_permissions

    validator = resolve_permissions(None, presets_by_name["Validator"]["flags"], None)
    err = validator.check_with_state(
        "sprint.move.active_to_review", entity="sprint", status="active"
    )
    assert err is None, f"Expected pass; got: {err}"


def test_resolve_permissions_check_with_state_executor_blocked_in_active(
    presets_by_name,
):
    """Executor must fail check_with_state on active_to_review — it
    lacks the move flag (though it has interact_in.active)."""
    from okto_pulse.core.infra.permissions import resolve_permissions

    executor = resolve_permissions(None, presets_by_name["Executor"]["flags"], None)
    err = executor.check_with_state(
        "sprint.move.active_to_review", entity="sprint", status="active"
    )
    assert err is not None
    assert "sprint.move.active_to_review" in err


# ---------------------------------------------------------------------------
# Ideação 6 — Rebalance KG flags across operational presets
# ---------------------------------------------------------------------------


KG_QUERY_FLAGS = [
    "kg.query.decision_history",
    "kg.query.related_context",
    "kg.query.supersedence_chain",
    "kg.query.contradictions",
    "kg.query.similar_decisions",
    "kg.query.constraint_explain",
    "kg.query.alternatives",
    "kg.query.learning_from_bugs",
    "kg.query.global",
]

KG_POWER_FLAGS = ["kg.power.natural", "kg.power.schema_info", "kg.power.cypher"]

KG_SESSION_FLAGS = [
    "kg.session.begin",
    "kg.session.add_node",
    "kg.session.add_edge",
    "kg.session.get_similar",
    "kg.session.propose",
    "kg.session.commit",
    "kg.session.abort",
]

KG_ADMIN_FLAGS = [
    "kg.admin.settings_read",
    "kg.admin.settings_write",
    "kg.admin.historical_consolidation",
    "kg.admin.wipe_board",
]


def test_kg_flags_spec(presets_by_name):
    """Spec owns the content — full power, full session, full admin."""
    flags = presets_by_name["Spec"]["flags"]
    for flag in KG_POWER_FLAGS:
        assert _get_nested(flags, flag) is True, f"Spec missing {flag}"
    for flag in KG_SESSION_FLAGS:
        assert _get_nested(flags, flag) is True, f"Spec missing {flag}"
    assert _get_nested(flags, "kg.admin.settings_read") is True
    assert _get_nested(flags, "kg.admin.settings_write") is True
    assert _get_nested(flags, "kg.admin.historical_consolidation") is True


def test_kg_flags_validator(presets_by_name):
    """Validator: full power + session, admin read-only."""
    flags = presets_by_name["Validator"]["flags"]
    for flag in KG_POWER_FLAGS:
        assert _get_nested(flags, flag) is True, f"Validator missing {flag}"
    for flag in KG_SESSION_FLAGS:
        assert _get_nested(flags, flag) is True, f"Validator missing {flag}"
    assert _get_nested(flags, "kg.admin.settings_read") is True
    assert _get_nested(flags, "kg.admin.settings_write") is False
    assert _get_nested(flags, "kg.admin.historical_consolidation") is False


def test_kg_flags_qa(presets_by_name):
    """QA: propose-only session (no commit/abort), natural+schema_info, no cypher."""
    flags = presets_by_name["QA"]["flags"]
    assert _get_nested(flags, "kg.power.natural") is True
    assert _get_nested(flags, "kg.power.schema_info") is True
    assert _get_nested(flags, "kg.power.cypher") is False
    # Propose path active
    for flag in [
        "kg.session.begin",
        "kg.session.add_node",
        "kg.session.add_edge",
        "kg.session.get_similar",
        "kg.session.propose",
    ]:
        assert _get_nested(flags, flag) is True, f"QA missing {flag}"
    # Commit/abort blocked — Spec/Validator commit on review
    assert _get_nested(flags, "kg.session.commit") is False
    assert _get_nested(flags, "kg.session.abort") is False


def test_kg_flags_executor(presets_by_name):
    """Executor: query + light power only. Zero session, no cypher."""
    flags = presets_by_name["Executor"]["flags"]
    assert _get_nested(flags, "kg.power.natural") is True
    assert _get_nested(flags, "kg.power.schema_info") is True
    assert _get_nested(flags, "kg.power.cypher") is False
    for flag in KG_SESSION_FLAGS:
        assert _get_nested(flags, flag) is False, f"Executor must not have {flag}"
    assert _get_nested(flags, "kg.admin.settings_read") is True
    assert _get_nested(flags, "kg.admin.settings_write") is False


@pytest.mark.parametrize("preset_name", ["Spec", "Executor", "QA", "Validator"])
def test_kg_query_primary_in_all_operational_presets(presets_by_name, preset_name):
    """All 4 operational presets keep the 9 primary kg.query flags."""
    flags = presets_by_name[preset_name]["flags"]
    for flag in KG_QUERY_FLAGS:
        assert _get_nested(flags, flag) is True, (
            f"{preset_name} must keep kg.query primary active: {flag}"
        )


def test_kg_flags_full_control_regression(presets_by_name):
    """Full Control regression — all 23 KG flags active."""
    flags = presets_by_name["Full Control"]["flags"]
    all_kg = KG_QUERY_FLAGS + KG_POWER_FLAGS + KG_SESSION_FLAGS + KG_ADMIN_FLAGS
    for flag in all_kg:
        assert _get_nested(flags, flag) is True, f"Full Control lost {flag}"


def test_kg_matrix_covered_by_test_suite():
    """Meta — guarantee the suite has the 5 required KG matrix functions."""
    import tests.test_presets as suite

    required = {
        "test_kg_flags_spec",
        "test_kg_flags_executor",
        "test_kg_flags_qa",
        "test_kg_flags_validator",
        "test_kg_flags_full_control_regression",
    }
    actual = {name for name in dir(suite) if name.startswith("test_kg_flags_")}
    missing = required - actual
    assert not missing, f"Missing KG matrix tests: {missing}"


# ---------------------------------------------------------------------------
# Backward compat — agents with permissions=null
# ---------------------------------------------------------------------------


def test_legacy_agents_retain_access_via_has_permission():
    """Agents with permissions=None continue to pass check_permission for any flag."""
    from okto_pulse.core.infra.permissions import check_permission, has_permission

    # Simulates an agent pre-granular-flag system (permissions column NULL).
    assert has_permission(None, "kg.power.cypher") is True
    assert has_permission(None, "kg.admin.settings_write") is True
    assert check_permission(None, "kg.session.commit") is None
    assert check_permission(None, "sprint.move.active_to_review") is None


# ---------------------------------------------------------------------------
# Ideação 2 — Ampliar card.entity.create para Executor e QA
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "preset_name,expected",
    [
        ("Executor", True),
        ("QA", True),
        ("Spec", True),
        ("Full Control", True),
        ("Validator", False),
    ],
)
def test_card_entity_create_by_preset(presets_by_name, preset_name, expected):
    """Executor, QA, Spec, Full Control can open cards (bug for Executor/QA,
    any type for Spec/Full Control by convention). Validator cannot — it
    only acts on validation status via submit_task_validation."""
    flags = presets_by_name[preset_name]["flags"]
    actual = _get_nested(flags, "card.entity.create")
    assert actual is expected, (
        f"{preset_name} expected card.entity.create={expected}, got {actual}"
    )


def test_executor_lacks_create_test(presets_by_name):
    """Executor does not own test scenarios — create_test stays QA/Spec-only."""
    flags = presets_by_name["Executor"]["flags"]
    assert _get_nested(flags, "card.entity.create_test") is False


def test_qa_has_both_create_and_create_test(presets_by_name):
    """QA owns test card lifecycle (create_test) AND can now open bugs (create)."""
    flags = presets_by_name["QA"]["flags"]
    assert _get_nested(flags, "card.entity.create") is True
    assert _get_nested(flags, "card.entity.create_test") is True


def test_spec_retains_both_create_flags(presets_by_name):
    """Spec regression: continues owning all card-creation capabilities."""
    flags = presets_by_name["Spec"]["flags"]
    assert _get_nested(flags, "card.entity.create") is True
    assert _get_nested(flags, "card.entity.create_test") is True


# ---------------------------------------------------------------------------
# Ideação 4 — Preset Reporter
# ---------------------------------------------------------------------------


def test_reporter_preset_exists(presets_by_name):
    """Reporter must appear in get_builtin_presets()."""
    assert "Reporter" in presets_by_name
    reporter = presets_by_name["Reporter"]
    assert "Observador" in reporter["description"] or "observador" in reporter["description"].lower()


def test_builtin_presets_includes_reporter():
    """Reporter is present in the built-in list (count is managed by the
    Sprint Manager test which currently pins to 7)."""
    names = {p["name"] for p in get_builtin_presets()}
    assert "Reporter" in names
    # All original presets still present.
    assert {"Full Control", "Executor", "Validator", "QA", "Spec"}.issubset(names)


def test_reporter_write_flags(presets_by_name):
    """Reporter allowed writes: card.entity.create (bug by convention),
    *.qa.ask, card.comments.*, attachments.upload."""
    flags = presets_by_name["Reporter"]["flags"]
    # Allowed writes
    assert _get_nested(flags, "card.entity.create") is True
    assert _get_nested(flags, "ideation.qa.ask") is True
    assert _get_nested(flags, "refinement.qa.ask") is True
    assert _get_nested(flags, "spec.qa.ask") is True
    assert _get_nested(flags, "sprint.qa.ask") is True
    assert _get_nested(flags, "card.qa.ask") is True
    assert _get_nested(flags, "card.comments.create") is True
    assert _get_nested(flags, "card.comments.respond_choice") is True
    assert _get_nested(flags, "card.attachments.upload") is True


def test_reporter_forbidden_flags(presets_by_name):
    """Reporter MUST NOT have: gate submissions, entity edit/delete (except
    card.entity.create), moves, qa.answer, session KG, cypher."""
    flags = presets_by_name["Reporter"]["flags"]
    # Gates
    assert _get_nested(flags, "spec.evaluations.submit") is False
    assert _get_nested(flags, "spec.validation.submit") is False
    assert _get_nested(flags, "sprint.evaluations.submit") is False
    assert _get_nested(flags, "card.validation.submit") is False
    # Entity writes
    assert _get_nested(flags, "spec.entity.create") is False
    assert _get_nested(flags, "spec.entity.edit_fields") is False
    assert _get_nested(flags, "card.entity.edit_fields") is False
    assert _get_nested(flags, "ideation.entity.create") is False
    assert _get_nested(flags, "refinement.entity.create") is False
    # Moves
    assert _get_nested(flags, "spec.move.draft_to_review") is False
    assert _get_nested(flags, "card.move.in_progress_to_done") is False
    assert _get_nested(flags, "sprint.move.draft_to_active") is False
    # Q&A answer (observer asks, doesn't answer)
    assert _get_nested(flags, "ideation.qa.answer") is False
    assert _get_nested(flags, "spec.qa.answer") is False
    assert _get_nested(flags, "card.qa.answer") is False
    # KG session + cypher
    assert _get_nested(flags, "kg.session.begin") is False
    assert _get_nested(flags, "kg.session.commit") is False
    assert _get_nested(flags, "kg.power.cypher") is False


def test_reporter_kg_matrix(presets_by_name):
    """Reporter KG baseline: queries + natural + schema_info + admin.read."""
    flags = presets_by_name["Reporter"]["flags"]
    for flag in KG_QUERY_FLAGS:
        assert _get_nested(flags, flag) is True, f"Reporter missing {flag}"
    assert _get_nested(flags, "kg.power.natural") is True
    assert _get_nested(flags, "kg.power.schema_info") is True
    assert _get_nested(flags, "kg.power.cypher") is False
    for flag in KG_SESSION_FLAGS:
        assert _get_nested(flags, flag) is False, f"Reporter must not have {flag}"
    assert _get_nested(flags, "kg.admin.settings_read") is True
    assert _get_nested(flags, "kg.admin.settings_write") is False
    assert _get_nested(flags, "kg.admin.historical_consolidation") is False


def test_reporter_interact_in_coverage(presets_by_name):
    """Reporter sees everything — interact_in.* True on main states."""
    flags = presets_by_name["Reporter"]["flags"]
    for state in ("draft", "evaluating", "refined"):
        assert _get_nested(flags, f"ideation.interact_in.{state}") is True
    for state in ("draft", "in_progress", "review", "approved"):
        assert _get_nested(flags, f"refinement.interact_in.{state}") is True
    for state in ("draft", "review", "approved", "validated", "in_progress", "done"):
        assert _get_nested(flags, f"spec.interact_in.{state}") is True
    for state in ("draft", "active", "review", "closed"):
        assert _get_nested(flags, f"sprint.interact_in.{state}") is True
    assert _get_nested(flags, "card.interact_in.not_started") is True


def test_reporter_addition_does_not_break_other_presets(presets_by_name):
    """Regression — adding Reporter left the other 5 presets unchanged
    on their critical flags."""
    # Validator keeps gate ownership
    assert _get_nested(presets_by_name["Validator"]["flags"], "spec.validation.submit") is True
    assert _get_nested(presets_by_name["Validator"]["flags"], "card.validation.submit") is True
    # Executor still owns card lifecycle
    assert _get_nested(presets_by_name["Executor"]["flags"], "card.entity.edit_fields") is True
    assert _get_nested(presets_by_name["Executor"]["flags"], "card.move.in_progress_to_validation") is True
    # QA still owns test scenarios
    assert _get_nested(presets_by_name["QA"]["flags"], "spec.tests.create") is True
    # Spec still owns ideation/refinement/spec derivation
    assert _get_nested(presets_by_name["Spec"]["flags"], "spec.cards_derive") is True
    assert _get_nested(presets_by_name["Spec"]["flags"], "ideation.specs_derive") is True


# ---------------------------------------------------------------------------
# Ideação 8 — generate_role_summary
# ---------------------------------------------------------------------------


def test_role_summary_legacy_null():
    """Agent with permissions=None (legacy Full Control) gets an explicit
    summary that flags the legacy source of its access."""
    from okto_pulse.core.infra.permissions import generate_role_summary

    summary = generate_role_summary(None)
    assert summary.startswith("Role: Full Control (legacy)")
    assert "unrestricted" in summary
    assert "\n" not in summary  # single line


def test_role_summary_full_control(presets_by_name):
    from okto_pulse.core.infra.permissions import generate_role_summary

    summary = generate_role_summary(presets_by_name["Full Control"]["flags"])
    assert summary.startswith("Role: Full Control")
    # Full Control should own the key gate capabilities
    assert "submit spec validations" in summary
    assert "submit task validations" in summary
    assert "commit KG consolidation" in summary
    # And include KG capabilities
    assert "cypher" in summary
    assert "consolidate" in summary


def test_role_summary_executor(presets_by_name):
    from okto_pulse.core.infra.permissions import generate_role_summary

    summary = generate_role_summary(presets_by_name["Executor"]["flags"])
    assert summary.startswith("Role: Executor")
    assert "create cards" in summary
    assert "submit gates" in summary  # in "Cannot:"
    # KG: query + natural (no cypher, no consolidate)
    assert "natural" in summary
    assert "cypher" not in summary
    assert "consolidate" not in summary


def test_role_summary_validator(presets_by_name):
    from okto_pulse.core.infra.permissions import generate_role_summary

    summary = generate_role_summary(presets_by_name["Validator"]["flags"])
    assert summary.startswith("Role: Validator")
    assert "submit spec validations" in summary
    assert "submit task validations" in summary
    assert "commit KG consolidation" in summary
    assert "cypher" in summary


def test_role_summary_qa(presets_by_name):
    from okto_pulse.core.infra.permissions import generate_role_summary

    summary = generate_role_summary(presets_by_name["QA"]["flags"])
    assert summary.startswith("Role: QA")
    assert "create test cards" in summary
    # QA proposes but does not commit — consolidate capability absent
    assert "consolidate" not in summary
    assert "cypher" not in summary


def test_role_summary_reporter(presets_by_name):
    from okto_pulse.core.infra.permissions import generate_role_summary

    summary = generate_role_summary(presets_by_name["Reporter"]["flags"])
    assert summary.startswith("Role: Reporter")
    assert "create cards" in summary
    assert "submit gates" in summary  # in Cannot
    assert "natural" in summary
    assert "cypher" not in summary
    assert "consolidate" not in summary


def test_role_summary_spec(presets_by_name):
    from okto_pulse.core.infra.permissions import generate_role_summary

    summary = generate_role_summary(presets_by_name["Spec"]["flags"])
    assert summary.startswith("Role: Spec")
    assert "create specs" in summary
    assert "edit KG settings" in summary
    assert "commit KG consolidation" in summary


def test_role_summary_empty_flags():
    """Empty/custom flags still produce a valid single-line string."""
    from okto_pulse.core.infra.permissions import generate_role_summary

    summary = generate_role_summary({})
    # No preset match → "Custom"
    assert summary.startswith("Role: Custom")
    assert "\n" not in summary


def test_role_summary_unknown_type():
    """Non-list, non-dict permissions return a safe fallback."""
    from okto_pulse.core.infra.permissions import generate_role_summary

    summary = generate_role_summary("invalid")
    assert summary == "Role: unknown"


# ---------------------------------------------------------------------------
# Ideação 5 — Preset Sprint Manager
# ---------------------------------------------------------------------------


def test_sprint_manager_preset_exists(presets_by_name):
    assert "Sprint Manager" in presets_by_name


def test_builtin_presets_count_is_seven():
    """After Sprint Manager, get_builtin_presets() has 7 entries."""
    presets = get_builtin_presets()
    assert len(presets) == 7
    names = {p["name"] for p in presets}
    assert names == {
        "Full Control", "Executor", "Validator", "QA",
        "Reporter", "Sprint Manager", "Spec",
    }


def test_sprint_manager_sprint_lifecycle(presets_by_name):
    """Sprint Manager owns the full sprint state machine + evaluation."""
    flags = presets_by_name["Sprint Manager"]["flags"]
    assert _get_nested(flags, "sprint.entity.create") is True
    assert _get_nested(flags, "sprint.entity.edit_fields") is True
    assert _get_nested(flags, "sprint.move.draft_to_active") is True
    assert _get_nested(flags, "sprint.move.active_to_review") is True
    assert _get_nested(flags, "sprint.move.review_to_closed") is True
    assert _get_nested(flags, "sprint.move.any_to_cancelled") is True
    assert _get_nested(flags, "sprint.evaluations.submit") is True


def test_sprint_manager_card_assign_not_create(presets_by_name):
    """Sprint Manager plans (assigns), does not create cards."""
    flags = presets_by_name["Sprint Manager"]["flags"]
    assert _get_nested(flags, "card.entity.assign") is True
    assert _get_nested(flags, "card.entity.label") is True
    assert _get_nested(flags, "card.entity.create") is False


def test_sprint_manager_spec_read_only(presets_by_name):
    """Sprint Manager reads specs for context — never creates or edits."""
    flags = presets_by_name["Sprint Manager"]["flags"]
    assert _get_nested(flags, "spec.entity.read") is True
    assert _get_nested(flags, "spec.entity.create") is False
    assert _get_nested(flags, "spec.entity.edit_fields") is False


def test_sprint_manager_no_tech_gate_submits(presets_by_name):
    """Sprint Manager is a delivery gate, not a technical gate. No spec/task submits."""
    flags = presets_by_name["Sprint Manager"]["flags"]
    assert _get_nested(flags, "spec.validation.submit") is False
    assert _get_nested(flags, "spec.evaluations.submit") is False
    assert _get_nested(flags, "card.validation.submit") is False


def test_sprint_manager_kg_baseline(presets_by_name):
    """Sprint Manager KG: query + natural + schema only. No cypher/session."""
    flags = presets_by_name["Sprint Manager"]["flags"]
    assert _get_nested(flags, "kg.power.natural") is True
    assert _get_nested(flags, "kg.power.schema_info") is True
    assert _get_nested(flags, "kg.power.cypher") is False
    for flag in KG_SESSION_FLAGS:
        assert _get_nested(flags, flag) is False, f"Sprint Manager has unexpected {flag}"
    assert _get_nested(flags, "kg.admin.settings_write") is False
    assert _get_nested(flags, "kg.admin.historical_consolidation") is False


def test_validator_retains_sprint_evaluation_submit(presets_by_name):
    """Regression: Validator coexists with Sprint Manager — both have the flag."""
    flags = presets_by_name["Validator"]["flags"]
    assert _get_nested(flags, "sprint.evaluations.submit") is True


# ---------------------------------------------------------------------------
# Ideação 3 — Spec interact_in validated/in_progress (Opção A MVP)
# ---------------------------------------------------------------------------


def test_spec_interact_in_validated_and_in_progress(presets_by_name):
    """Spec preset can now interact with spec in validated/in_progress
    to reduce the back-to-draft dance for cosmetic fixes."""
    flags = presets_by_name["Spec"]["flags"]
    assert _get_nested(flags, "spec.interact_in.validated") is True
    assert _get_nested(flags, "spec.interact_in.in_progress") is True


def test_spec_retains_earlier_interact_in(presets_by_name):
    """Regression — existing draft/review/approved interact_in still True."""
    flags = presets_by_name["Spec"]["flags"]
    assert _get_nested(flags, "spec.interact_in.draft") is True
    assert _get_nested(flags, "spec.interact_in.review") is True
    assert _get_nested(flags, "spec.interact_in.approved") is True


def test_spec_check_with_state_allows_knowledge_in_validated(presets_by_name):
    """Spec can add/edit knowledge in validated spec — flag chain clears."""
    from okto_pulse.core.infra.permissions import resolve_permissions

    spec = resolve_permissions(None, presets_by_name["Spec"]["flags"], None)
    err = spec.check_with_state(
        "spec.knowledge.create", entity="spec", status="validated"
    )
    assert err is None, f"Expected allow; got: {err}"


def test_spec_check_with_state_allows_mockup_annotate_in_in_progress(presets_by_name):
    """Spec can annotate mockups mid-flight."""
    from okto_pulse.core.infra.permissions import resolve_permissions

    spec = resolve_permissions(None, presets_by_name["Spec"]["flags"], None)
    err = spec.check_with_state(
        "spec.mockups.annotate", entity="spec", status="in_progress"
    )
    assert err is None, f"Expected allow; got: {err}"


# ---------------------------------------------------------------------------
# Existing role_summary test — kept after the Sprint Manager block
# ---------------------------------------------------------------------------


def test_role_summary_legacy_list_permissions():
    """Legacy flat list permissions are mapped and summarized."""
    from okto_pulse.core.infra.permissions import generate_role_summary

    # A legacy agent with the default flat permission set
    legacy_perms = [
        "board:read",
        "cards:create",
        "cards:update",
        "specs:create",
        "specs:update",
        "specs:move",
        "qa:create",
    ]
    summary = generate_role_summary(legacy_perms)
    assert summary.startswith("Role:")
    assert "\n" not in summary
    # Should identify as (legacy) custom or matched preset
    assert "legacy" in summary.lower() or "Custom" in summary
