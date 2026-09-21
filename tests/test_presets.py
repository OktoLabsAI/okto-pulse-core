"""Built-in permissions for active entities; Sprint authority is retired."""

from __future__ import annotations

import sys

import pytest

from okto_pulse.core.infra.permissions import (  # noqa: E402
    PROJECT_STRUCTURE_ENTITY_OPERATIONS,
    STRUCTURED_SPEC_ENTITY_OPERATIONS,
    STRUCTURED_SPEC_ENTITY_TYPES,
    _get_nested,
    get_builtin_presets,
)


@pytest.fixture(scope="module")
def presets_by_name() -> dict:
    return {p["name"]: p for p in get_builtin_presets()}


# ---------------------------------------------------------------------------
# Structured spec entity editing
# ---------------------------------------------------------------------------


def test_full_control_has_all_structured_spec_entity_permissions(presets_by_name):
    flags = presets_by_name["Full Control"]["flags"]
    for entity_type in STRUCTURED_SPEC_ENTITY_TYPES:
        operations = (
            PROJECT_STRUCTURE_ENTITY_OPERATIONS
            if entity_type == "project_structure_node"
            else STRUCTURED_SPEC_ENTITY_OPERATIONS
        )
        for operation in operations:
            flag = f"spec.structured_entity.{entity_type}.{operation}"
            assert _get_nested(flags, flag) is True, f"Full Control missing {flag}"


def test_spec_writer_has_all_structured_spec_entity_permissions(presets_by_name):
    flags = presets_by_name["Spec"]["flags"]
    for entity_type in STRUCTURED_SPEC_ENTITY_TYPES:
        operations = (
            PROJECT_STRUCTURE_ENTITY_OPERATIONS
            if entity_type == "project_structure_node"
            else STRUCTURED_SPEC_ENTITY_OPERATIONS
        )
        for operation in operations:
            flag = f"spec.structured_entity.{entity_type}.{operation}"
            assert _get_nested(flags, flag) is True, f"Spec preset missing {flag}"


@pytest.mark.parametrize("preset_name", ["Executor", "QA", "Validator", "Reporter"])
def test_non_spec_presets_do_not_mutate_structured_spec_entities(
    presets_by_name, preset_name
):
    flags = presets_by_name[preset_name]["flags"]
    for entity_type in STRUCTURED_SPEC_ENTITY_TYPES:
        operations = (
            PROJECT_STRUCTURE_ENTITY_OPERATIONS
            if entity_type == "project_structure_node"
            else STRUCTURED_SPEC_ENTITY_OPERATIONS
        )
        for operation in operations:
            flag = f"spec.structured_entity.{entity_type}.{operation}"
            assert _get_nested(flags, flag) is False, (
                f"{preset_name} must not own {flag}"
            )


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
    suite = sys.modules[__name__]

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
    """Trusted legacy callers keep active capabilities, never retired operations."""
    from okto_pulse.core.infra.permissions import check_permission, has_permission

    # Simulates an agent pre-granular-flag system (permissions column NULL).
    assert has_permission(None, "kg.power.cypher") is True
    assert has_permission(None, "kg.admin.settings_write") is True
    assert check_permission(None, "kg.session.commit") is None
    assert check_permission(None, "sprint.move.active_to_review") is not None
    assert has_permission(None, "sprint.move.active_to_review") is False


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
    assert (
        "Observador" in reporter["description"]
        or "observador" in reporter["description"].lower()
    )


def test_builtin_presets_includes_reporter():
    """Reporter remains in the six active built-in presets."""
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
    """Reporter keeps read interaction in registered, non-retired states."""
    flags = presets_by_name["Reporter"]["flags"]
    for state in ("draft", "evaluating"):
        assert _get_nested(flags, f"ideation.interact_in.{state}") is True
    assert _get_nested(flags, "ideation.interact_in.refined") is None
    for state in ("draft", "review", "approved"):
        assert _get_nested(flags, f"refinement.interact_in.{state}") is True
    assert _get_nested(flags, "refinement.interact_in.in_progress") is None
    for state in ("draft", "review", "approved", "validated", "in_progress", "done"):
        assert _get_nested(flags, f"spec.interact_in.{state}") is True
    assert "sprint" not in flags
    assert _get_nested(flags, "card.interact_in.not_started") is True


def test_reporter_addition_does_not_break_other_presets(presets_by_name):
    """Regression — adding Reporter left the other 5 presets unchanged
    on their critical flags."""
    # Validator keeps gate ownership
    assert (
        _get_nested(presets_by_name["Validator"]["flags"], "spec.validation.submit")
        is True
    )
    assert (
        _get_nested(presets_by_name["Validator"]["flags"], "card.validation.submit")
        is True
    )
    # Executor still owns card lifecycle
    assert (
        _get_nested(presets_by_name["Executor"]["flags"], "card.entity.edit_fields")
        is True
    )
    assert (
        _get_nested(
            presets_by_name["Executor"]["flags"], "card.move.in_progress_to_validation"
        )
        is True
    )
    # QA still owns test scenarios
    assert _get_nested(presets_by_name["QA"]["flags"], "spec.tests.create") is True
    # Spec still owns ideation/refinement/spec derivation
    assert _get_nested(presets_by_name["Spec"]["flags"], "spec.cards_derive") is True
    assert (
        _get_nested(presets_by_name["Spec"]["flags"], "ideation.specs_derive") is True
    )


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


def test_builtin_presets_count_is_six():
    """New installations expose only the six remaining built-in presets."""
    presets = get_builtin_presets()
    assert len(presets) == 6
    names = {p["name"] for p in presets}
    assert names == {
        "Full Control",
        "Executor",
        "Validator",
        "QA",
        "Reporter",
        "Spec",
    }


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
# Architecture Design permissions
# ---------------------------------------------------------------------------


ARCHITECTURE_PARENTS = ("ideation", "refinement", "spec", "card")
ARCHITECTURE_ACTIONS = ("read", "create", "edit", "delete", "import", "render")


def test_architecture_registry_contains_parent_actions():
    from okto_pulse.core.infra.permissions import ALL_FLAGS

    for parent in ARCHITECTURE_PARENTS:
        for action in ARCHITECTURE_ACTIONS:
            assert f"{parent}.architecture.{action}" in ALL_FLAGS
    assert "card.copy_from_spec.architecture" in ALL_FLAGS


def test_spec_preset_owns_architecture_authoring_and_copy(presets_by_name):
    flags = presets_by_name["Spec"]["flags"]
    for parent in ARCHITECTURE_PARENTS:
        for action in ARCHITECTURE_ACTIONS:
            assert _get_nested(flags, f"{parent}.architecture.{action}") is True
    assert _get_nested(flags, "card.copy_from_spec.architecture") is True


@pytest.mark.parametrize(
    "preset_name", ["Executor", "QA", "Validator", "Reporter"]
)
def test_operational_presets_read_architecture_without_editing(
    presets_by_name, preset_name
):
    flags = presets_by_name[preset_name]["flags"]
    for parent in ARCHITECTURE_PARENTS:
        assert _get_nested(flags, f"{parent}.architecture.read") is True
        assert _get_nested(flags, f"{parent}.architecture.edit") is False
    assert _get_nested(flags, "card.copy_from_spec.architecture") is False


def test_legacy_permission_map_includes_architecture_flags():
    from okto_pulse.core.infra.permissions import map_legacy_permissions

    read_flags = map_legacy_permissions(["board:read"])
    for parent in ARCHITECTURE_PARENTS:
        assert _get_nested(read_flags, f"{parent}.architecture.read") is True

    spec_update_flags = map_legacy_permissions(["specs:update"])
    for parent in ("ideation", "refinement", "spec"):
        for action in ("create", "edit", "delete", "import", "render"):
            assert (
                _get_nested(spec_update_flags, f"{parent}.architecture.{action}")
                is True
            )

    card_update_flags = map_legacy_permissions(["cards:update"])
    for action in ("create", "edit", "delete", "import", "render"):
        assert _get_nested(card_update_flags, f"card.architecture.{action}") is True
    assert _get_nested(card_update_flags, "card.copy_from_spec.architecture") is True


# ---------------------------------------------------------------------------
# IR / OR permissions
# ---------------------------------------------------------------------------


IR_OR_RESOURCE_FLAGS = (
    "spec.integration_requirements.read",
    "spec.integration_requirements.create",
    "spec.integration_requirements.edit",
    "spec.integration_requirements.delete",
    "spec.integration_requirements.link_task",
    "spec.observability_requirements.read",
    "spec.observability_requirements.create",
    "spec.observability_requirements.edit",
    "spec.observability_requirements.delete",
    "spec.observability_requirements.link_task",
)


def test_ir_or_registry_contains_first_class_resource_flags():
    from okto_pulse.core.infra.permissions import ALL_FLAGS

    for flag in IR_OR_RESOURCE_FLAGS:
        assert flag in ALL_FLAGS
    assert "card.link_to.ir" in ALL_FLAGS
    assert "card.link_to.or" in ALL_FLAGS


def test_spec_preset_owns_ir_or_authoring_and_task_links(presets_by_name):
    flags = presets_by_name["Spec"]["flags"]

    for flag in IR_OR_RESOURCE_FLAGS:
        assert _get_nested(flags, flag) is True
    assert _get_nested(flags, "card.link_to.ir") is True
    assert _get_nested(flags, "card.link_to.or") is True


def test_executor_can_read_and_link_ir_or_without_authoring(presets_by_name):
    flags = presets_by_name["Executor"]["flags"]

    assert _get_nested(flags, "spec.integration_requirements.read") is True
    assert _get_nested(flags, "spec.integration_requirements.link_task") is True
    assert _get_nested(flags, "spec.observability_requirements.read") is True
    assert _get_nested(flags, "spec.observability_requirements.link_task") is True
    assert _get_nested(flags, "card.link_to.ir") is True
    assert _get_nested(flags, "card.link_to.or") is True

    for flag in (
        "spec.integration_requirements.create",
        "spec.integration_requirements.edit",
        "spec.integration_requirements.delete",
        "spec.observability_requirements.create",
        "spec.observability_requirements.edit",
        "spec.observability_requirements.delete",
    ):
        assert _get_nested(flags, flag) is False


@pytest.mark.parametrize(
    "preset_name", ["QA", "Validator", "Reporter"]
)
def test_non_authoring_presets_read_ir_or_without_editing(presets_by_name, preset_name):
    flags = presets_by_name[preset_name]["flags"]

    assert _get_nested(flags, "spec.integration_requirements.read") is True
    assert _get_nested(flags, "spec.observability_requirements.read") is True
    for flag in (
        "spec.integration_requirements.create",
        "spec.integration_requirements.edit",
        "spec.integration_requirements.delete",
        "spec.integration_requirements.link_task",
        "spec.observability_requirements.create",
        "spec.observability_requirements.edit",
        "spec.observability_requirements.delete",
        "spec.observability_requirements.link_task",
        "card.link_to.ir",
        "card.link_to.or",
    ):
        assert _get_nested(flags, flag) is False


def test_legacy_permission_map_includes_ir_or_flags():
    from okto_pulse.core.infra.permissions import map_legacy_permissions

    read_flags = map_legacy_permissions(["board:read"])
    assert _get_nested(read_flags, "spec.integration_requirements.read") is True
    assert _get_nested(read_flags, "spec.observability_requirements.read") is True

    spec_update_flags = map_legacy_permissions(["specs:update"])
    for flag in (
        "spec.integration_requirements.create",
        "spec.integration_requirements.edit",
        "spec.integration_requirements.delete",
        "spec.integration_requirements.link_task",
        "spec.observability_requirements.create",
        "spec.observability_requirements.edit",
        "spec.observability_requirements.delete",
        "spec.observability_requirements.link_task",
    ):
        assert _get_nested(spec_update_flags, flag) is True

    card_update_flags = map_legacy_permissions(["cards:update"])
    assert _get_nested(card_update_flags, "card.link_to.ir") is True
    assert _get_nested(card_update_flags, "card.link_to.or") is True


# ---------------------------------------------------------------------------
# Stories and Topics permissions
# ---------------------------------------------------------------------------


STORY_FLAGS = (
    "story.entity.read",
    "story.entity.create",
    "story.entity.edit_fields",
    "story.entity.assign",
    "story.entity.label",
    "story.entity.archive",
    "story.entity.restore",
    "story.entity.delete",
    "story.move.draft_to_triage",
    "story.move.draft_to_ready",
    "story.move.triage_to_draft",
    "story.move.triage_to_ready",
    "story.move.ready_to_triage",
    "story.interact_in.draft",
    "story.interact_in.triage",
    "story.interact_in.ready",
    "story.interact_in.converted",
    "story.interact_in.archived",
    "story.links.ideation",
    "story.conversion.to_ideation",
    "story.history_read",
)

TOPIC_FLAGS = (
    "topic.entity.read",
    "topic.entity.create",
    "topic.entity.edit_fields",
    "topic.entity.archive",
    "topic.entity.restore",
    "topic.entity.merge",
    "topic.entity.delete",
)


def test_story_topic_registry_contains_expected_flags():
    from okto_pulse.core.infra.permissions import ALL_FLAGS

    for flag in STORY_FLAGS + TOPIC_FLAGS:
        assert flag in ALL_FLAGS


def test_all_builtin_presets_include_story_topic_sections(presets_by_name):
    for preset in presets_by_name.values():
        flags = preset["flags"]
        for flag in STORY_FLAGS + TOPIC_FLAGS:
            assert _get_nested(flags, flag) is not None, (
                f"{preset['name']} missing {flag}"
            )


def test_spec_preset_owns_story_topic_authoring(presets_by_name):
    flags = presets_by_name["Spec"]["flags"]
    for flag in STORY_FLAGS + TOPIC_FLAGS:
        assert _get_nested(flags, flag) is True, f"Spec missing {flag}"


@pytest.mark.parametrize(
    "preset_name", ["Executor", "QA", "Validator", "Reporter"]
)
def test_operational_presets_read_stories_topics_without_editing(
    presets_by_name, preset_name
):
    flags = presets_by_name[preset_name]["flags"]

    assert _get_nested(flags, "story.entity.read") is True
    assert _get_nested(flags, "story.history_read") is True
    assert _get_nested(flags, "topic.entity.read") is True

    assert _get_nested(flags, "story.entity.create") is False
    assert _get_nested(flags, "story.entity.edit_fields") is False
    assert _get_nested(flags, "story.links.ideation") is False
    assert _get_nested(flags, "story.conversion.to_ideation") is False
    assert _get_nested(flags, "topic.entity.create") is False
    assert _get_nested(flags, "topic.entity.merge") is False
    assert _get_nested(flags, "topic.entity.delete") is False


def test_legacy_permission_map_includes_story_topic_flags():
    from okto_pulse.core.infra.permissions import map_legacy_permissions

    read_flags = map_legacy_permissions(["board:read"])
    assert _get_nested(read_flags, "story.entity.read") is True
    assert _get_nested(read_flags, "story.history_read") is True
    assert _get_nested(read_flags, "topic.entity.read") is True

    create_flags = map_legacy_permissions(["specs:create"])
    assert _get_nested(create_flags, "story.entity.create") is True
    assert _get_nested(create_flags, "topic.entity.create") is True

    update_flags = map_legacy_permissions(["specs:update"])
    assert _get_nested(update_flags, "story.entity.edit_fields") is True
    assert _get_nested(update_flags, "story.links.ideation") is True
    assert _get_nested(update_flags, "story.conversion.to_ideation") is True
    assert _get_nested(update_flags, "topic.entity.edit_fields") is True
    assert _get_nested(update_flags, "topic.entity.merge") is True

    move_flags = map_legacy_permissions(["specs:move"])
    assert _get_nested(move_flags, "story.interact_in.ready") is True
    assert _get_nested(move_flags, "story.move.ready_to_triage") is True

    delete_flags = map_legacy_permissions(["specs:delete"])
    assert _get_nested(delete_flags, "story.entity.delete") is True
    assert _get_nested(delete_flags, "topic.entity.delete") is True


def test_merge_missing_flags_backfills_story_topic_as_allowed():
    from okto_pulse.core.infra.permissions import (
        PERMISSION_REGISTRY,
        merge_missing_flags,
    )

    stored = {"board": {"read": False}}
    merged, added = merge_missing_flags(stored, PERMISSION_REGISTRY)

    assert added > 0
    assert _get_nested(merged, "board.read") is False
    assert _get_nested(merged, "story.entity.create") is True
    assert _get_nested(merged, "topic.entity.merge") is True


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
