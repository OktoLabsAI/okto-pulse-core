"""AF35-S4 MCP residual inventory and AST gate.

The residual ``get_db_for_mcp`` migration is intentionally incremental. This
file makes the current surface explicit: every direct MCP session opener must be
present in the ledger below, and migrated wrappers must keep using the MCP UoW
path instead of reopening raw MCP database sessions.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from okto_pulse.core.mcp import server as mcp_server

AF35_S4_EXPECTED_DIRECT_GET_DB_COUNT = 0
AF35_S4_UOW_CALLSITE_FLOOR = 193

AF35_S4_DIRECT_GET_DB_LEDGER: dict[str, frozenset[str]] = {
    "cards_and_task_context": frozenset(
        {
            "okto_pulse_resolve_bug_regression_scenarios",
            "okto_pulse_get_task_context",
            "okto_pulse_get_task_conclusions",
            "okto_pulse_list_cards_by_status",
        }
    ),
    "linking_archive": frozenset(
        {
            "_link_task_to_rule_internal",
            "_link_task_to_fr_internal",
            "_link_task_to_contract_internal",
            "_link_task_to_tr_internal",
            "okto_pulse_archive_tree",
            "okto_pulse_restore_tree",
        }
    ),
    "linking_requirements": frozenset(
        {
            "_link_task_to_integration_requirement_internal",
            "_link_task_to_observability_requirement_internal",
            "_link_task_to_decision_internal",
        }
    ),
    "spec_eval_knowledge_traceability": frozenset(
        {
            "_link_card_to_spec_internal",
            "okto_pulse_submit_spec_evaluation",
            "okto_pulse_list_spec_evaluations",
            "okto_pulse_get_spec_evaluation",
            "okto_pulse_delete_spec_evaluation",
            "okto_pulse_ask_spec_choice_question",
            "okto_pulse_answer_spec_question",
            "okto_pulse_get_traceability_report",
            "okto_pulse_get_spec_knowledge",
            "okto_pulse_add_spec_knowledge",
            "okto_pulse_delete_spec_knowledge",
            "okto_pulse_delete_spec_question",
        }
    ),
    "validation_amendments_default_config": frozenset(
        {
            "okto_pulse_confirm_amendment_coverage",
            "okto_pulse_create_amendment_revision",
            "okto_pulse_list_amendment_revisions",
            "okto_pulse_get_amendment_revision",
            "okto_pulse_associate_amendment_revision_artifacts",
            "okto_pulse_transition_amendment_revision",
            "_refuse_mcp_default_config_activation_if_human_skip_changes",
            "_refuse_mcp_default_config_deactivation_if_human_skip_changes",
        }
    ),
    "kg_residuals": frozenset(
        {
            "_kg_orphan_backfill_health_refusal",
            "okto_pulse_kg_migrate_schema",
            "okto_pulse_kg_tick_run_now",
        }
    ),
}

AF35_S4_REQUIRED_LEDGER_METADATA_FIELDS = frozenset(
    {"owner", "rationale", "removal_trigger"}
)

AF35_S4_RESIDUAL_GROUP_METADATA: dict[str, dict[str, str]] = {
    "cards_and_task_context": {
        "owner": "AF35-S4 card context migration",
        "rationale": "Residual card context helpers still compose read models in the MCP wrapper after the core card CRUD migration.",
        "removal_trigger": "Bug-regression, task-context, task-conclusion and status-list surfaces run through card-context read models over MCP UoW.",
    },
    "linking_archive": {
        "owner": "AF35-S4 linking/archive migration",
        "rationale": "Requirement-link and tree archive/restore helpers span several domains and were left outside earlier family oracles.",
        "removal_trigger": "Requirement-linking and archive/restore operations are moved behind cohesive UoW use cases or ledgered into AF35-S5.",
    },
    "linking_requirements": {
        "owner": "AF35-S4 requirement linking migration",
        "rationale": "IR/OR/decision link helpers remain direct because they span structured spec children and card linkage side effects.",
        "removal_trigger": "Requirement-link helpers execute through cohesive structured-link use cases over MCP UoW.",
    },
    "spec_eval_knowledge_traceability": {
        "owner": "AF35-S4 spec-adjacent residual migration",
        "rationale": "Spec evaluation, spec Q&A, knowledge and traceability were not fully covered by the spec UoW oracle.",
        "removal_trigger": "Spec-adjacent residual tools delegate to existing or narrowly extended spec use cases over MCP UoW.",
    },
    "validation_amendments_default_config": {
        "owner": "AF35-S4 amendment and default-config guard migration",
        "rationale": "Amendment revisions and default-config human-control refusal helpers still have wrapper-level persistence.",
        "removal_trigger": "Amendment/default-config guard helpers use transport-free use cases or explicit AF35-S5 exception records.",
    },
    "kg_residuals": {
        "owner": "AF35-S4 KG residual migration",
        "rationale": "Some KG operational helpers were outside the already migrated KG health/DLQ seams.",
        "removal_trigger": "KG residual tools are either UoW-backed or explicitly deferred to AF35-S5 with this ledger updated.",
    },
}

# The former 36 direct session openers are retained in the source history above
# for traceability. The active catalog routes all of them through the composed
# UoW session scope, so the live residual ledger is empty.
AF35_S4_RETIRED_DIRECT_GET_DB_LEDGER = AF35_S4_DIRECT_GET_DB_LEDGER
AF35_S4_RETIRED_RESIDUAL_GROUP_METADATA = AF35_S4_RESIDUAL_GROUP_METADATA
AF35_S4_DIRECT_GET_DB_LEDGER: dict[str, frozenset[str]] = {}
AF35_S4_RESIDUAL_GROUP_METADATA: dict[str, dict[str, str]] = {}

AF35_S4_MIGRATED_MCP_WRAPPERS = frozenset(
    {
        "okto_pulse_get_board",
        "okto_pulse_create_card",
        "okto_pulse_get_card",
        "okto_pulse_update_card",
        "okto_pulse_delete_card",
        "okto_pulse_add_card_dependency",
        "okto_pulse_copy_knowledge_to_card",
        "okto_pulse_get_ideation",
        "okto_pulse_update_my_profile",
        "okto_pulse_list_my_boards",
        "okto_pulse_list_my_mentions",
        "okto_pulse_mark_as_seen",
        "okto_pulse_get_unseen_summary",
        "okto_pulse_list_agents",
        "okto_pulse_get_activity_log",
        "_ask_question_impl",
        "okto_pulse_answer_question",
        "okto_pulse_delete_question",
        "okto_pulse_add_comment",
        "okto_pulse_add_choice_comment",
        "okto_pulse_respond_to_choice",
        "okto_pulse_get_choice_responses",
        "okto_pulse_list_comments",
        "okto_pulse_update_comment",
        "okto_pulse_delete_comment",
        "okto_pulse_upload_attachment",
        "okto_pulse_list_attachments",
        "okto_pulse_delete_attachment",
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
        "okto_pulse_kg_health",
        "okto_pulse_kg_dead_letter_list",
        "okto_pulse_list_by_board",
        "okto_pulse_list_architecture_designs",
        "okto_pulse_list_architecture_propagation_legacy",
        "okto_pulse_get_architecture_design",
        "okto_pulse_validate_architecture_design_payload",
        "okto_pulse_add_architecture_design",
        "okto_pulse_update_architecture_design",
        "okto_pulse_delete_architecture_design",
        "okto_pulse_import_excalidraw_architecture_diagram",
        "okto_pulse_dump_architecture_diagram",
        "okto_pulse_copy_architecture_to_card",
        "okto_pulse_copy_mockups_to_card",
        "okto_pulse_get_card_knowledge",
        "okto_pulse_copy_qa_to_card",
        "okto_pulse_add_screen_mockup",
        "okto_pulse_update_screen_mockup",
        "okto_pulse_annotate_mockup",
        "okto_pulse_list_screen_mockups",
        "okto_pulse_delete_screen_mockup",
        "okto_pulse_list_qa",
        "okto_pulse_list_knowledge",
        "okto_pulse_list_snapshots",
        "okto_pulse_get_analytics",
        "okto_pulse_list_blockers",
        "okto_pulse_submit_task_validation",
        "okto_pulse_list_default_guideline_candidates",
        "okto_pulse_update_default_guideline_refs",
        "okto_pulse_set_default_design_system",
        "okto_pulse_list_design_systems",
        "okto_pulse_get_design_system",
        "okto_pulse_create_design_system",
        "okto_pulse_update_design_system",
        "okto_pulse_delete_design_system",
        "okto_pulse_list_task_validations",
        "okto_pulse_get_task_validation",
        "okto_pulse_submit_spec_validation",
        "okto_pulse_list_spec_validations",
    }
)


def _server_source() -> str:
    return Path(mcp_server.__file__).read_text(encoding="utf-8")


def _server_tree() -> ast.Module:
    return ast.parse(_server_source())


def _function_nodes() -> dict[str, ast.AsyncFunctionDef | ast.FunctionDef]:
    return {
        node.name: node
        for node in ast.walk(_server_tree())
        if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef))
    }


def _callee_name(call: ast.Call) -> str | None:
    func = call.func
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return None


def _opens_direct_get_db_for_mcp(node: ast.AsyncFunctionDef | ast.FunctionDef) -> bool:
    for candidate in ast.walk(node):
        if not isinstance(candidate, ast.AsyncWith):
            continue
        for item in candidate.items:
            context_expr = item.context_expr
            if (
                isinstance(context_expr, ast.Call)
                and _callee_name(context_expr) == "get_db_for_mcp"
            ):
                return True
    return False


def _calls(node: ast.AST, callee: str) -> bool:
    return any(
        isinstance(candidate, ast.Call) and _callee_name(candidate) == callee
        for candidate in ast.walk(node)
    )


def _ledger_functions() -> set[str]:
    out: set[str] = set()
    for group_name, group_functions in AF35_S4_DIRECT_GET_DB_LEDGER.items():
        assert group_functions, f"{group_name} must not be empty"
        assert out.isdisjoint(group_functions), f"{group_name} duplicates ledger entries"
        out.update(group_functions)
    return out


def _ledger_records() -> dict[str, dict[str, str]]:
    records: dict[str, dict[str, str]] = {}
    for group_name, group_functions in AF35_S4_DIRECT_GET_DB_LEDGER.items():
        metadata = AF35_S4_RESIDUAL_GROUP_METADATA[group_name]
        for function_name in group_functions:
            records[function_name] = {"group": group_name, **metadata}
    return records


def _uses_session_execute(node: ast.AST) -> bool:
    for candidate in ast.walk(node):
        if not isinstance(candidate, ast.Call):
            continue
        if not isinstance(candidate.func, ast.Attribute):
            continue
        if candidate.func.attr != "execute":
            continue
        receiver = candidate.func.value
        if isinstance(receiver, ast.Name) and receiver.id in {"db", "session"}:
            return True
        if isinstance(receiver, ast.Attribute) and receiver.attr == "session":
            return True
    return False


def _uses_uow_session(node: ast.AST) -> bool:
    return any(
        isinstance(candidate, ast.Attribute)
        and candidate.attr == "session"
        and isinstance(candidate.value, ast.Name)
        and candidate.value.id == "uow"
        for candidate in ast.walk(node)
    )


def _wrapper_coupling_issues(
    node: ast.AsyncFunctionDef | ast.FunctionDef,
) -> list[str]:
    names = {
        candidate.id for candidate in ast.walk(node) if isinstance(candidate, ast.Name)
    }
    issues: list[str] = []
    if _opens_direct_get_db_for_mcp(node):
        issues.append("direct get_db_for_mcp opener")
    for forbidden_name in ("AsyncSession", "get_db", "get_session_factory"):
        if forbidden_name in names:
            issues.append(forbidden_name)
    if _uses_session_execute(node):
        issues.append("session.execute")
    if _calls(node, "select"):
        issues.append("select")
    if _uses_uow_session(node):
        issues.append("uow.session")
    return issues


def test_af35_s4_inventory_matches_current_ledger() -> None:
    tree = _server_tree()
    nodes = _function_nodes()
    discovered = {
        name for name, node in nodes.items() if _opens_direct_get_db_for_mcp(node)
    }
    ledger = _ledger_functions()
    uow_calls = sum(
        1
        for candidate in ast.walk(tree)
        if isinstance(candidate, ast.Call)
        and _callee_name(candidate) == "get_unit_of_work_factory_for_mcp"
    )

    assert len(ledger) == AF35_S4_EXPECTED_DIRECT_GET_DB_COUNT
    assert discovered == ledger
    assert uow_calls >= AF35_S4_UOW_CALLSITE_FLOOR


def test_af35_s4_residual_ledger_has_metadata() -> None:
    ledger = _ledger_functions()
    records = _ledger_records()

    assert set(AF35_S4_RESIDUAL_GROUP_METADATA) == set(AF35_S4_DIRECT_GET_DB_LEDGER)
    assert set(records) == ledger
    for function_name, record in records.items():
        missing = AF35_S4_REQUIRED_LEDGER_METADATA_FIELDS - set(record)
        assert not missing, f"{function_name} missing metadata: {sorted(missing)}"
        for field in AF35_S4_REQUIRED_LEDGER_METADATA_FIELDS:
            value = record[field].strip()
            assert value and value.upper() not in {"TBD", "TODO", "N/A"}, (
                f"{function_name} has invalid {field}: {value!r}"
            )


def test_af35_s4_migrated_wrapper_gate_blocks_raw_mcp_db() -> None:
    nodes = _function_nodes()
    missing = AF35_S4_MIGRATED_MCP_WRAPPERS - set(nodes)
    assert not missing, f"missing migrated MCP wrappers: {sorted(missing)}"

    missing_uow = [
        name
        for name in sorted(AF35_S4_MIGRATED_MCP_WRAPPERS)
        if not _calls(nodes[name], "get_unit_of_work_factory_for_mcp")
    ]
    coupling_issues = {
        name: _wrapper_coupling_issues(nodes[name])
        for name in AF35_S4_MIGRATED_MCP_WRAPPERS
    }
    coupling_issues = {
        name: issues for name, issues in coupling_issues.items() if issues
    }

    assert not missing_uow
    assert not coupling_issues


@pytest.mark.asyncio
async def test_af35_s4_migrated_mcp_tool_fails_closed_without_uow_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.runtime_registry import (
        is_unit_of_work_factory_registered,
        reset_unit_of_work_factory,
    )

    async def _ctx(board_id: str) -> mcp_server.AgentContext:
        return mcp_server.AgentContext(
            agent_id="agent-1",
            agent_name="AF35 Test Agent",
            board_id=board_id,
            permissions=None,
        )

    def _unexpected_db_fallback():
        raise AssertionError("migrated MCP wrapper must not fall back to get_db_for_mcp")

    monkeypatch.setattr(mcp_server, "_get_agent_ctx", _ctx)
    monkeypatch.setattr(mcp_server, "check_permission", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(mcp_server, "get_db_for_mcp", _unexpected_db_fallback)

    reset_unit_of_work_factory()
    assert not is_unit_of_work_factory_registered()

    with pytest.raises(RuntimeError, match="No relational UnitOfWorkFactory"):
        await mcp_server.okto_pulse_get_analytics.fn(
            board_id="board-1",
            metric_type="overview",
        )
