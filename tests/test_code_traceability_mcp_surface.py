"""MCP inventory and schema guards for agent-mediated Code Traceability."""

from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest

from okto_pulse.core.domain.code_traceability import (
    CodeEvidenceSelectorKind,
    CodeEvidenceType,
    CodeTraceabilitySubjectType,
    ImplementationTargetRole,
)
from okto_pulse.core.domain.mcp_permission_registry import (
    MCP_TOOL_PERMISSION_POLICIES,
)
from okto_pulse.core.mcp.catalog import CoreMcpCatalog
from okto_pulse.core.mcp.code_traceability_tools import (
    _closed_input,
    register_code_traceability_tools,
)
from okto_pulse.core.mcp import server
from okto_pulse.core.models.code_traceability import ImplementationTargetUpdateInput


EXPECTED_TOOLS = {
    "okto_pulse_acknowledge_implementation_overlap": "code_traceability.overlap.acknowledge",
    "okto_pulse_clear_code_traceability_not_applicable": "code_traceability.waiver.clear",
    "okto_pulse_create_implementation_target": "code_traceability.target.suggest",
    "okto_pulse_get_code_evidence": "code_traceability.evidence.read",
    "okto_pulse_get_code_investigation_receipt": "code_traceability.investigation.read",
    "okto_pulse_get_implementation_overlaps": "code_traceability.overlap.read",
    "okto_pulse_link_code_evidence": "code_traceability.spec_link.create",
    "okto_pulse_list_code_evidence": "code_traceability.evidence.read",
    "okto_pulse_list_implementation_targets": "code_traceability.target.read",
    "okto_pulse_mark_code_traceability_not_applicable": "code_traceability.waiver.create",
    "okto_pulse_set_code_evidence_disposition": "code_traceability.spec_link.set_disposition",
    "okto_pulse_start_code_investigation": "code_traceability.investigation.start",
    "okto_pulse_submit_code_evidence": "code_traceability.evidence.submit",
    "okto_pulse_submit_code_investigation_receipt": "code_traceability.investigation.receipt_submit",
    "okto_pulse_submit_implementation_target_execution_receipt": "code_traceability.target.execution_submit",
    "okto_pulse_submit_implementation_target_resolution": "code_traceability.target.resolution_submit",
    "okto_pulse_supersede_code_evidence": "code_traceability.evidence.supersede",
    "okto_pulse_unlink_code_evidence": "code_traceability.spec_link.delete",
    "okto_pulse_update_implementation_target": "code_traceability.target.edit",
}


def test_code_traceability_registers_exact_reviewed_inventory() -> None:
    assert server._CODE_TRACEABILITY_TOOL_NAMES == frozenset(EXPECTED_TOOLS)
    live = {tool.name for tool in server.mcp.iter_tools()}
    assert set(EXPECTED_TOOLS).issubset(live)
    assert len(live) == 337


def test_every_code_traceability_tool_has_a_closed_specific_schema() -> None:
    for name in EXPECTED_TOOLS:
        tool = server.mcp._tool_manager._tools[name]
        schema = tool.parameters
        assert schema["type"] == "object"
        assert schema["additionalProperties"] is False
        assert "board_id" in schema["properties"]
        assert "payload" not in schema["properties"]
        assert "target_type" not in schema["properties"]

    submit = server.mcp._tool_manager._tools[
        "okto_pulse_submit_code_investigation_receipt"
    ].parameters["properties"]
    for server_owned in (
        "actor_id",
        "attestor_actor_id",
        "source_ref",
        "subject_id",
        "subject_version",
        "generation",
        "predecessor_receipt_id",
        "trust",
        "acceptance_status",
        "received_at",
    ):
        assert server_owned not in submit

    for evidence_tool in (
        "okto_pulse_submit_code_evidence",
        "okto_pulse_supersede_code_evidence",
    ):
        evidence_properties = server.mcp._tool_manager._tools[
            evidence_tool
        ].parameters["properties"]
        assert "excerpt_omitted_reason" not in evidence_properties


def test_code_traceability_tools_have_one_exact_granular_permission() -> None:
    policies = {
        policy.tool_name: policy.permission_flags
        for policy in MCP_TOOL_PERMISSION_POLICIES
    }
    for tool_name, expected_flag in EXPECTED_TOOLS.items():
        assert policies[tool_name] == (expected_flag,)
    assert len(MCP_TOOL_PERMISSION_POLICIES) == 334


def test_code_traceability_lazy_docs_are_canonical_and_complete() -> None:
    uri = "okto-pulse://reference/tool-docs/code-traceability"
    registered = {entry[0] for entry in server._RESOURCE_REGISTRY}
    assert uri in registered
    content = server._load_resource_file(
        "reference/tool-docs/code-traceability.md"
    )
    for tool_name in EXPECTED_TOOLS:
        assert server.tool_docs_uri(tool_name) == uri
        assert tool_name in content


def test_mcp_adapter_has_no_source_acquisition_capability() -> None:
    path = (
        Path(__file__).parents[1]
        / "src"
        / "okto_pulse"
        / "core"
        / "mcp"
        / "code_traceability_tools.py"
    )
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    } | {
        node.module or ""
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
    }
    forbidden = (
        "git",
        "pathlib",
        "subprocess",
        "socket",
        "urllib",
        "aiohttp",
        "requests",
        "content_ingestion",
    )
    assert not any(
        module == token or module.startswith(f"{token}.")
        for module in imported
        for token in forbidden
    )
    called_names = {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    assert not {"open", "exec", "eval", "compile"}.intersection(called_names)


@pytest.mark.asyncio
async def test_invalid_relative_path_is_a_serializable_error_before_uow() -> None:
    calls = {"agent": 0, "uow": 0}

    async def get_board_agent(_board_id: str) -> object:
        calls["agent"] += 1
        return object()

    def get_uow() -> object:
        calls["uow"] += 1
        raise AssertionError("invalid input must not open a unit of work")

    catalog = CoreMcpCatalog(name="traceability-test", version="1")
    register_code_traceability_tools(
        catalog,
        get_board_agent=get_board_agent,
        get_uow=get_uow,
        get_settings=object,
    )
    tool = await catalog.get_tool("okto_pulse_submit_code_evidence")

    outcome = await tool.fn(
        board_id="board-1",
        investigation_receipt_id="receipt-1",
        parent_type=CodeTraceabilitySubjectType.CARD,
        parent_id="card-1",
        evidence_type=CodeEvidenceType.BEHAVIOR,
        claim="The service applies the accepted rule.",
        selector_kind=CodeEvidenceSelectorKind.FILE,
        declared_source_content_sha256="a" * 64,
        idempotency_key="evidence-submit-1",
        relative_path="../secret.py",
    )

    assert outcome.is_error is True
    assert outcome.code == "code_path_invalid"
    encoded = json.dumps(outcome.structured_content())
    assert "not JSON serializable" not in encoded
    assert calls == {"agent": 0, "uow": 0}


def test_update_target_closed_input_keeps_omitted_fields_out_of_patch() -> None:
    command = _closed_input(
        ImplementationTargetUpdateInput,
        {
            "board_id": "board-1",
            "card_id": "card-1",
            "target_id": "target-1",
            "expected_revision": 2,
            "change_reason": "Clarify the implementation intent.",
            "selector_kind": None,
            "relative_path_hint": None,
            "qualified_symbol": None,
            "role": None,
            "intent": "Apply the accepted validation rule.",
            "required": None,
            "spec_links": None,
            "evidence_links": None,
        },
    )

    assert command.intent == "Apply the accepted validation rule."
    assert command.model_fields_set == {
        "board_id",
        "card_id",
        "target_id",
        "expected_revision",
        "change_reason",
        "intent",
    }
    assert command.selector_kind is None
    assert command.role is None


def test_update_target_closed_input_preserves_false_and_empty_patch_values() -> None:
    command = _closed_input(
        ImplementationTargetUpdateInput,
        {
            "board_id": "board-1",
            "card_id": "card-1",
            "target_id": "target-1",
            "expected_revision": 2,
            "change_reason": "Make the target optional and clear link sets.",
            "selector_kind": None,
            "role": ImplementationTargetRole.MODIFY,
            "required": False,
            "spec_links": [],
            "evidence_links": [],
        },
    )

    assert {"role", "required", "spec_links", "evidence_links"}.issubset(
        command.model_fields_set
    )
    assert command.required is False
    assert command.spec_links == ()
    assert command.evidence_links == ()
