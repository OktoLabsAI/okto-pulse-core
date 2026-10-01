"""Closed MCP routing for the sole contextual Code Traceability contract."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

from pydantic import ValidationError
import pytest

from okto_pulse.core.domain.code_traceability import (
    CodeEvidenceBaselinePresence,
    CodeEvidenceBaselineProvenance,
    CodeEvidenceContextOrigin,
    CodeEvidenceSourceRole,
    CodeTraceabilityContext,
    CodeTraceabilityProjectionProfile,
    CodeTraceabilitySubjectType,
    SourceContextEvidenceItemV2,
)
from okto_pulse.core.mcp import server
from okto_pulse.core.mcp.catalog import CoreMcpCatalog
from okto_pulse.core.mcp.code_traceability_tools import (
    _evidence_command,
    _investigation_receipt_command,
    register_code_traceability_tools,
)
from okto_pulse.core.models.code_traceability import (
    CodeEvidenceSubmission,
    CodeEvidenceSupersessionSubmission,
    CodeInvestigationReceiptSubmission,
    CodeInvestigationToolingInput,
)
from okto_pulse.core.models.schemas import CodeTraceabilitySettings
from okto_pulse.core.services.code_traceability_gate import (
    CodeTraceabilityGateEvaluator,
)


NOW = datetime(2026, 8, 22, 12, 0, tzinfo=UTC)
SHA = "a" * 64


def _tooling() -> CodeInvestigationToolingInput:
    return CodeInvestigationToolingInput(
        tool_id="codex",
        tool_version="1",
        method_id="source-blind-context/v2",
    )


def _evidence_values() -> dict[str, object]:
    return {
        "board_id": "board-1",
        "investigation_receipt_id": "receipt-1",
        "parent_type": "spec",
        "parent_id": "spec-1",
        "evidence_type": "structure",
        "claim": "The accepted baseline contains the service shell.",
        "selector_kind": "file",
        "relative_path": "src/service.py",
        "language": None,
        "symbol_kind": None,
        "qualified_symbol": None,
        "symbol_signature": None,
        "line_start": None,
        "line_end": None,
        "excerpt": None,
        "excerpt_sha256": None,
        "declared_file_blob_sha256": None,
        "declared_source_content_sha256": SHA,
        "idempotency_key": "evidence-1",
    }


def _context_values() -> dict[str, object]:
    return {
        "source_role": CodeEvidenceSourceRole.EXISTING_SCAFFOLD,
        "relevance_summary": "Defines the package layout baseline.",
        "scope_relation": "Directly constrains the in-scope service.",
        "source_origin": "Observed in the accepted repository snapshot.",
        "interpretation_limit": "It does not prove requested behavior exists.",
        "baseline_provenance": CodeEvidenceBaselineProvenance(
            presence=CodeEvidenceBaselinePresence.COMMITTED_SNAPSHOT,
            workspace_state_id="workspace-1",
        ),
    }


def test_mcp_tools_advertise_only_required_contextual_contract() -> None:
    receipt = server.mcp._tool_manager._tools[
        "okto_pulse_submit_code_investigation_receipt"
    ].parameters
    assert receipt["properties"]["contract_version"] == {
        "const": 2,
        "type": "integer",
    }
    assert "evidence_applicable" in receipt["properties"]["outcome"]["pattern"]
    assert (
        "no_relevant_existing_implementation"
        in receipt["properties"]["outcome"]["pattern"]
    )

    contextual_fields = {
        "contract_version",
        "source_role",
        "relevance_summary",
        "scope_relation",
        "source_origin",
        "interpretation_limit",
        "baseline_provenance",
    }
    for name in (
        "okto_pulse_submit_code_evidence",
        "okto_pulse_supersede_code_evidence",
    ):
        schema = server.mcp._tool_manager._tools[name].parameters
        assert schema["additionalProperties"] is False
        assert contextual_fields.issubset(schema["properties"])
        assert schema["properties"]["contract_version"]["const"] == 2
        assert contextual_fields - {"interpretation_limit"} <= set(schema["required"])
        role_schema = schema["properties"]["source_role"]
        authored_roles = role_schema["enum"]
        assert set(authored_roles) == {
            "current_implementation",
            "existing_scaffold",
            "existing_constraint",
            "reference_pattern",
        }
        assert "uncategorized_legacy" not in authored_roles
        assert not {
            "repository_path",
            "checkout_path",
            "source_url",
            "credential",
        }.intersection(schema["properties"])


def test_receipt_rejects_prior_contract_and_requires_contextual_outcome() -> None:
    common = {
        "board_id": "board-1",
        "request_id": "request-1",
        "challenge_token": "challenge",
        "capabilities": [],
        "tooling": _tooling(),
        "observed_at": NOW,
        "idempotency_key": "receipt-1",
    }

    contextual = _investigation_receipt_command(
        contract_version=2,
        outcome="evidence_applicable",
        **common,
    )

    assert type(contextual) is CodeInvestigationReceiptSubmission
    assert contextual.contract_version == 2

    with pytest.raises(ValidationError):
        _investigation_receipt_command(
            contract_version=1,
            outcome="evidence_applicable",
            **common,
        )
    with pytest.raises(ValidationError):
        _investigation_receipt_command(
            contract_version=2,
            outcome="accessible",
            **common,
        )


def test_evidence_contract_selection_is_explicit_for_submit_and_supersede() -> None:
    values = _evidence_values()
    contextual = _evidence_command(
        **values,
        **_context_values(),
        contract_version=2,
    )
    contextual_supersession = _evidence_command(
        **values,
        **_context_values(),
        contract_version=2,
        supersedes_evidence_id="evidence-old",
        supersession_reason="Correct the prior contextual observation.",
    )

    assert type(contextual) is CodeEvidenceSubmission
    assert type(contextual_supersession) is CodeEvidenceSupersessionSubmission
    assert contextual.source_role is CodeEvidenceSourceRole.EXISTING_SCAFFOLD
    assert contextual_supersession.contract_version == 2


@pytest.mark.parametrize(
    "values",
    [
        {"contract_version": 1, **_context_values()},
        {
            "contract_version": 2,
            **_context_values(),
            "baseline_provenance": None,
        },
        {"contract_version": 2, **_context_values(), "source_role": "uncategorized_legacy"},
    ],
)
def test_evidence_contract_mixtures_fail_closed(values: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        _evidence_command(**_evidence_values(), **values)


@pytest.mark.asyncio
async def test_invalid_v1_v2_mix_is_rejected_before_authentication_or_uow() -> None:
    calls = {"agent": 0, "uow": 0}

    async def get_board_agent(_board_id: str) -> object:
        calls["agent"] += 1
        raise AssertionError("invalid input must fail before authentication")

    def get_uow() -> object:
        calls["uow"] += 1
        raise AssertionError("invalid input must fail before persistence")

    catalog = CoreMcpCatalog(name="contextual-traceability-test", version="1")
    register_code_traceability_tools(
        catalog,
        get_board_agent=get_board_agent,
        get_uow=get_uow,
        get_settings=object,
    )
    tool = await catalog.get_tool("okto_pulse_submit_code_evidence")

    outcome = await tool.fn(
        **_evidence_values(),
        contract_version=1,
        **_context_values(),
    )

    assert outcome.is_error is True
    assert outcome.code == "validation_failed"
    assert calls == {"agent": 0, "uow": 0}


@pytest.mark.asyncio
async def test_existing_mcp_handlers_route_all_three_v2_commands(monkeypatch) -> None:
    from okto_pulse.core.application.use_cases.code_traceability import (
            SubmitCodeEvidenceUseCase,
        SubmitCodeInvestigationReceiptUseCase,
        SupersedeCodeEvidenceUseCase,
    )

    captured: list[object] = []

    async def capture(
        _self: object,
        command: object,
        *,
        actor: object,
        uow: object,
    ) -> object:
        assert actor is not None
        assert uow is not None
        captured.append(command)
        return command

    for use_case in (
        SubmitCodeInvestigationReceiptUseCase,
        SubmitCodeEvidenceUseCase,
        SupersedeCodeEvidenceUseCase,
        ):
        monkeypatch.setattr(use_case, "execute", capture)

    class Scope:
        async def __aenter__(self) -> object:
            return self

        async def __aexit__(self, *_args: object) -> None:
            return None

    async def get_board_agent(_board_id: str) -> object:
        return SimpleNamespace(
            agent_id="agent-1",
            agent_name="Agent One",
            realm_id=None,
            permissions=(),
        )

    def get_uow() -> object:
        return lambda **_kwargs: Scope()

    catalog = CoreMcpCatalog(name="contextual-routing-test", version="1")
    register_code_traceability_tools(
        catalog,
        get_board_agent=get_board_agent,
        get_uow=get_uow,
        get_settings=SimpleNamespace,
    )

    receipt_tool = await catalog.get_tool(
        "okto_pulse_submit_code_investigation_receipt"
    )
    evidence_tool = await catalog.get_tool("okto_pulse_submit_code_evidence")
    supersede_tool = await catalog.get_tool("okto_pulse_supersede_code_evidence")
    receipt_outcome = await receipt_tool.fn(
        board_id="board-1",
        request_id="request-1",
        challenge_token="challenge",
        outcome="evidence_applicable",
        capabilities=[],
        tooling=_tooling(),
        observed_at=NOW,
        idempotency_key="receipt-1",
        contract_version=2,
    )
    evidence_outcome = await evidence_tool.fn(
        **_evidence_values(),
        **_context_values(),
        contract_version=2,
    )
    supersede_outcome = await supersede_tool.fn(
        **_evidence_values(),
        **_context_values(),
        contract_version=2,
        supersedes_evidence_id="evidence-old",
        supersession_reason="Correct the prior contextual observation.",
    )
    assert not receipt_outcome.is_error
    assert not evidence_outcome.is_error
    assert not supersede_outcome.is_error
    assert [type(command) for command in captured] == [
        CodeInvestigationReceiptSubmission,
        CodeEvidenceSubmission,
        CodeEvidenceSupersessionSubmission,
    ]


def test_mcp_inventory_has_no_legacy_classification_mutation() -> None:
    names = set(server._CODE_TRACEABILITY_TOOL_NAMES)
    assert len(names) == 21
    assert "okto_pulse_classify_legacy_code_evidence" not in {
        tool.name for tool in server.mcp.iter_tools()
    }


@pytest.mark.asyncio
async def test_mcp_projection_serializes_authored_context_without_classification_overlay(
    monkeypatch,
) -> None:
    from okto_pulse.core.application.use_cases.code_traceability import (
        GetCodeTraceabilityProjectionUseCase,
    )

    item = SourceContextEvidenceItemV2(
        evidence_id="evidence-1",
        **_context_values(),
        context_origin=CodeEvidenceContextOrigin.AUTHORED,
        context_contract_version=2,
    )
    projection = CodeTraceabilityGateEvaluator().project(
        CodeTraceabilityContext(
            board_id="board-1",
            subject_type=CodeTraceabilitySubjectType.REFINEMENT,
            subject_id="refinement-1",
            subject_version=3,
            profile=CodeTraceabilityProjectionProfile.DETAIL,
            source_context_items=(item,),
        ),
        CodeTraceabilitySettings(mode="advisory"),
    )

    async def execute(_self, _query, *, actor, uow):
        assert actor is not None
        assert uow is not None
        return projection

    monkeypatch.setattr(GetCodeTraceabilityProjectionUseCase, "execute", execute)
    payload = await server._mcp_code_traceability_projection(
        uow=SimpleNamespace(),
        actor=SimpleNamespace(),
        board_id="board-1",
        subject_type="refinement",
        subject_id="refinement-1",
        subject_version=3,
        profile="detail",
    )

    assert "source_context_classification_inputs" not in payload
    assert payload["source_context_items"][0]["context_origin"] == "authored"
    assert payload["source_context_items"][0]["source_role"] == "existing_scaffold"
