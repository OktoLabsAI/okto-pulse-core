from __future__ import annotations

import logging

import pytest

from okto_pulse.core.domain.mcp_permission_registry import (
    HUMAN_ONLY_MCP_TOOL_EXEMPTIONS,
    MCP_TOOL_PERMISSION_POLICIES,
    HumanOnlyToolExemption,
    McpPermissionRegistryError,
    McpToolPermissionPolicy,
    build_mcp_permission_registry_report,
)
from okto_pulse.core.domain.permissions import (
    ALL_FLAGS,
    registry_vs_tools_report,
    validate_registry_vs_tools,
)
from okto_pulse.core.domain.sdlc_registry import transition_permission_flags
from okto_pulse.core.mcp import server
from okto_pulse.core.mcp.catalog import (
    CoreMcpCatalog,
    DuplicateMcpToolNameError,
)


LIVE_TOOL_NAMES = tuple(tool.name for tool in server.mcp.resolve().iter_tools())


# Reviewed semantic allowlist for tools whose required leaf depends on an input,
# current lifecycle state, or an opt-in projection.  Keep this independent from
# the production manifest so replacing a conditional policy with a broad CRUD
# leaf cannot pass CI merely because that leaf exists in ALL_FLAGS.
EXPECTED_TRANSITION_SUFFIXES = {
    "card": (
        "not_started_to_started",
        "not_started_to_in_progress",
        "not_started_to_cancelled",
        "started_to_not_started",
        "started_to_in_progress",
        "started_to_validation",
        "started_to_on_hold",
        "started_to_cancelled",
        "in_progress_to_started",
        "in_progress_to_validation",
        "in_progress_to_done",
        "in_progress_to_on_hold",
        "in_progress_to_cancelled",
        "validation_to_in_progress",
        "validation_to_done",
        "validation_to_on_hold",
        "validation_to_cancelled",
        "on_hold_to_started",
        "on_hold_to_in_progress",
        "on_hold_to_cancelled",
        "done_to_in_progress",
        "rejected_to_in_progress",
        "cancelled_to_not_started",
    ),
    "story": (
        "draft_to_triage",
        "draft_to_ready",
        "triage_to_draft",
        "triage_to_ready",
        "ready_to_triage",
    ),
    "ideation": (
        "draft_to_review",
        "draft_to_cancelled",
        "review_to_draft",
        "review_to_approved",
        "review_to_cancelled",
        "approved_to_review",
        "approved_to_evaluating",
        "approved_to_cancelled",
        "evaluating_to_approved",
        "evaluating_to_done",
        "evaluating_to_cancelled",
        "done_to_draft",
        "cancelled_to_draft",
    ),
    "refinement": (
        "draft_to_review",
        "draft_to_cancelled",
        "review_to_draft",
        "review_to_approved",
        "review_to_cancelled",
        "approved_to_review",
        "approved_to_done",
        "approved_to_cancelled",
        "done_to_draft",
        "cancelled_to_draft",
    ),
    "spec": (
        "draft_to_review",
        "draft_to_cancelled",
        "review_to_draft",
        "review_to_approved",
        "review_to_cancelled",
        "approved_to_review",
        "approved_to_validated",
        "approved_to_draft",
        "approved_to_cancelled",
        "validated_to_approved",
        "validated_to_in_progress",
        "validated_to_draft",
        "validated_to_cancelled",
        "in_progress_to_validated",
        "in_progress_to_draft",
        "in_progress_to_done",
        "in_progress_to_cancelled",
        "done_to_draft",
        "cancelled_to_draft",
    ),
    "sprint": (
        "draft_to_active",
        "draft_to_cancelled",
        "active_to_draft",
        "active_to_review",
        "active_to_cancelled",
        "review_to_active",
        "review_to_closed",
        "review_to_cancelled",
        "closed_to_draft",
        "cancelled_to_draft",
    ),
    "test_scenario": (
        "draft_to_ready",
        "draft_to_automated",
        "draft_to_passed",
        "draft_to_failed",
        "ready_to_draft",
        "ready_to_automated",
        "ready_to_passed",
        "ready_to_failed",
        "automated_to_ready",
        "automated_to_passed",
        "failed_to_ready",
        "failed_to_passed",
        "passed_to_ready",
    ),
}


def _reviewed_transition_flags(entity: str) -> tuple[str, ...]:
    return tuple(
        f"{entity}.move.{suffix}" for suffix in EXPECTED_TRANSITION_SUFFIXES[entity]
    )


EXPECTED_CONDITIONAL_PERMISSION_POLICIES = {
    "okto_pulse_archive_tree": (
        "ideation.entity.archive",
        "refinement.entity.archive",
        "spec.entity.archive",
    ),
    "okto_pulse_get_architecture_design": (
        "ideation.architecture.read",
        "refinement.architecture.read",
        "spec.architecture.read",
        "card.architecture.read",
        "ideation.architecture.render",
        "refinement.architecture.render",
        "spec.architecture.render",
        "card.architecture.render",
    ),
    "okto_pulse_get_spec": (
        "spec.entity.read",
        "spec.integration_requirements.read",
        "spec.observability_requirements.read",
    ),
    "okto_pulse_get_spec_context": (
        "spec.entity.read",
        "spec.integration_requirements.read",
        "spec.observability_requirements.read",
        "spec.checklist.read",
    ),
    "okto_pulse_get_task_context": (
        "card.entity.context_read",
        "card.validation.read",
        "spec.integration_requirements.read",
        "spec.observability_requirements.read",
    ),
    "okto_pulse_link_task": (
        "card.entity.link_spec",
        "card.link_to.scenario",
        "spec.structured_entity.functional_requirement.link_task",
        "card.link_to.rule",
        "spec.structured_entity.decision.link_task",
        "card.link_to.tr",
        "card.link_to.contract",
        "spec.integration_requirements.link_task",
        "card.link_to.ir",
        "spec.observability_requirements.link_task",
        "card.link_to.or",
    ),
    "okto_pulse_list_architecture_designs": (
        "ideation.architecture.read",
        "refinement.architecture.read",
        "spec.architecture.read",
        "card.architecture.read",
        "ideation.architecture.render",
        "refinement.architecture.render",
        "spec.architecture.render",
        "card.architecture.render",
    ),
    "okto_pulse_list_architecture_propagation_legacy": ("spec.architecture.read",),
    "okto_pulse_move_card": _reviewed_transition_flags("card"),
    "okto_pulse_move_ideation": _reviewed_transition_flags("ideation"),
    "okto_pulse_move_refinement": _reviewed_transition_flags("refinement"),
    "okto_pulse_move_spec": _reviewed_transition_flags("spec"),
    "okto_pulse_move_sprint": _reviewed_transition_flags("sprint"),
    "okto_pulse_move_story": _reviewed_transition_flags("story"),
    "okto_pulse_remove_spec_entity": (
        "spec.rules.delete",
        "spec.contracts.delete",
        "spec.structured_entity.decision.revoke",
    ),
    "okto_pulse_restore_tree": (
        "ideation.entity.restore",
        "refinement.entity.restore",
        "spec.entity.restore",
    ),
    "okto_pulse_submit_task_validation": (
        "card.validation.submit",
        "card.validation.read",
    ),
    "okto_pulse_update_test_scenario_status": (
        "spec.tests.execute",
        *_reviewed_transition_flags("test_scenario"),
    ),
    "okto_pulse_validate_architecture_design_payload": (
        "ideation.architecture.create",
        "refinement.architecture.create",
        "spec.architecture.create",
        "card.architecture.create",
        "ideation.architecture.edit",
        "refinement.architecture.edit",
        "spec.architecture.edit",
        "card.architecture.edit",
    ),
}


def test_live_catalog_has_one_exact_policy_or_audited_human_only_exemption() -> None:
    report = registry_vs_tools_report(list(LIVE_TOOL_NAMES))

    assert report.is_valid
    assert len(report.live_tools) == 337
    assert len(MCP_TOOL_PERMISSION_POLICIES) == 334
    assert len(HUMAN_ONLY_MCP_TOOL_EXEMPTIONS) == 3
    assert tuple(policy.tool_name for policy in MCP_TOOL_PERMISSION_POLICIES) == tuple(
        sorted(policy.tool_name for policy in MCP_TOOL_PERMISSION_POLICIES)
    )
    assert tuple(
        exemption.tool_name for exemption in HUMAN_ONLY_MCP_TOOL_EXEMPTIONS
    ) == tuple(
        sorted(exemption.tool_name for exemption in HUMAN_ONLY_MCP_TOOL_EXEMPTIONS)
    )
    assert set(report.policy_tools).isdisjoint(report.exempt_tools)
    assert {exemption.tool_name for exemption in HUMAN_ONLY_MCP_TOOL_EXEMPTIONS} == {
        "okto_pulse_kg_clear_cognitive_skip",
        "okto_pulse_kg_record_cognitive_skip",
        "okto_pulse_set_ideation_ambiguity_gate_skip",
    }
    assert all(exemption.reason.strip() for exemption in HUMAN_ONLY_MCP_TOOL_EXEMPTIONS)
    assert all(
        flag in ALL_FLAGS
        for policy in MCP_TOOL_PERMISSION_POLICIES
        for flag in policy.permission_flags
    )


def test_conditional_and_dynamic_tools_match_reviewed_semantic_contract() -> None:
    policies_by_tool = {
        policy.tool_name: policy.permission_flags
        for policy in MCP_TOOL_PERMISSION_POLICIES
    }

    for entity in EXPECTED_TRANSITION_SUFFIXES:
        assert transition_permission_flags(entity) == _reviewed_transition_flags(entity)
    assert {
        tool_name: policies_by_tool[tool_name]
        for tool_name in EXPECTED_CONDITIONAL_PERMISSION_POLICIES
    } == EXPECTED_CONDITIONAL_PERMISSION_POLICIES


@pytest.mark.parametrize(
    ("tool_names", "field", "expected"),
    [
        (
            (*LIVE_TOOL_NAMES, "okto_pulse_new_unclassified_action"),
            "new_tools",
            ("okto_pulse_new_unclassified_action",),
        ),
        (
            LIVE_TOOL_NAMES[1:],
            "missing_tools",
            (LIVE_TOOL_NAMES[0],),
        ),
        (
            (*LIVE_TOOL_NAMES, LIVE_TOOL_NAMES[0]),
            "duplicate_live_tools",
            (LIVE_TOOL_NAMES[0],),
        ),
    ],
)
def test_live_inventory_drift_is_reported_and_strict_mode_fails(
    tool_names: tuple[str, ...],
    field: str,
    expected: tuple[str, ...],
) -> None:
    report = registry_vs_tools_report(list(tool_names))

    assert not report.is_valid
    assert getattr(report, field) == expected
    with pytest.raises(McpPermissionRegistryError) as exc_info:
        validate_registry_vs_tools(list(tool_names), strict=True)
    assert exc_info.value.report == report


def test_pure_report_rejects_invalid_and_duplicate_policy_records() -> None:
    policies = (
        McpToolPermissionPolicy("tool_a", ("board.read",)),
        McpToolPermissionPolicy("tool_a", ("not.in.all_flags",)),
        McpToolPermissionPolicy("tool_b", ("board.read", "board.read")),
    )

    report = build_mcp_permission_registry_report(
        ("tool_a", "tool_b"),
        all_flags={"board.read"},
        policies=policies,
        exemptions=(),
    )

    assert not report.is_valid
    assert report.duplicate_policy_tools == ("tool_a",)
    assert report.invalid_policy_flags == (("tool_a", "not.in.all_flags"),)
    assert report.invalid_policy_records == ("policy[2]",)


def test_pure_report_rejects_conflicting_or_invalid_human_only_exemptions() -> None:
    policies = (McpToolPermissionPolicy("tool_a", ("board.read",)),)
    exemptions = (
        HumanOnlyToolExemption("tool_a", "Conflicts with a policy."),
        HumanOnlyToolExemption("tool_b", "   "),
        HumanOnlyToolExemption("tool_b", "Duplicate exact name."),
    )

    report = build_mcp_permission_registry_report(
        ("tool_a", "tool_b"),
        all_flags={"board.read"},
        policies=policies,
        exemptions=exemptions,
        exemption_limit=2,
    )

    assert not report.is_valid
    assert report.conflicting_tools == ("tool_a",)
    assert report.duplicate_exemption_tools == ("tool_b",)
    assert report.invalid_exemption_records == ("exemption[1]",)
    assert report.exemption_limit_exceeded


def test_pure_report_is_deterministic_for_live_input_order() -> None:
    forward = registry_vs_tools_report(list(LIVE_TOOL_NAMES))
    reverse = registry_vs_tools_report(list(reversed(LIVE_TOOL_NAMES)))

    assert forward == reverse
    assert forward.render() == reverse.render()


def test_runtime_compatibility_mode_logs_drift_without_raising(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING, logger="okto_pulse.permissions")

    report = validate_registry_vs_tools(
        [*LIVE_TOOL_NAMES, "okto_pulse_unclassified_runtime_tool"]
    )

    assert not report.is_valid
    assert report.new_tools == ("okto_pulse_unclassified_runtime_tool",)
    assert "MCP permission registry drift" in caplog.text


def test_catalog_rejects_duplicate_registration_instead_of_overwriting() -> None:
    catalog = CoreMcpCatalog(name="test", version="1")

    @catalog.tool(name="same_name")
    def first() -> None:
        pass

    with pytest.raises(DuplicateMcpToolNameError, match="same_name"):

        @catalog.tool(name="same_name")
        def second() -> None:
            pass

    assert catalog._tool_manager._tools["same_name"].fn is first.fn
