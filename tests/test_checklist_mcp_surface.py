"""Agent-facing A3 checklist MCP surface and aggregate outcome contract."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from okto_pulse.core.application.use_cases.checklist import (
    GetChecklistReceiptUseCase,
    StartChecklistExecutionUseCase,
    SubmitChecklistExecutionUseCase,
)
from okto_pulse.core.domain.checklist import (
    SPECIFY_CHECKLIST_ITEM_IDS,
    SPECIFY_CHECKLIST_TEMPLATE_V1,
    ChecklistCommitResult,
    ChecklistExecution,
    ChecklistExecutionStartResult,
    ChecklistItemOutcome,
    ChecklistItemResult,
    ChecklistMode,
    ChecklistReceipt,
    ChecklistReceiptSource,
)
from okto_pulse.core.mcp import server


class _UowContext:
    async def __aenter__(self):
        return object()

    async def __aexit__(self, exc_type, exc, traceback):
        del exc_type, exc, traceback


def _ctx():
    return type(
        "Ctx",
        (),
        {
            "agent_id": "agent-1",
            "agent_name": "Checklist agent",
            "permissions": ["*"],
        },
    )()


def _failed_items() -> tuple[ChecklistItemResult, ...]:
    return tuple(
        ChecklistItemResult(
            item_id=item_id,
            outcome=(
                ChecklistItemOutcome.FAIL
                if index == 0
                else ChecklistItemOutcome.PASS
            ),
            anchor=f"spec://spec-1/{item_id}",
            rationale="Observed mismatch" if index == 0 else None,
        )
        for index, item_id in enumerate(SPECIFY_CHECKLIST_ITEM_IDS)
    )


def _legacy_receipt() -> ChecklistReceipt:
    return ChecklistReceipt(
        id="receipt-legacy",
        board_id="board-1",
        spec_id="spec-1",
        spec_version=4,
        content_digest="c" * 64,
        input_digest="d" * 64,
        template_version="/specify/v1",
        template_digest=SPECIFY_CHECKLIST_TEMPLATE_V1.digest,
        binding_version=1,
        binding_digest="b" * 64,
        binding_mode=ChecklistMode.BLOCKING,
        items=(),
        source=ChecklistReceiptSource.LEGACY_UNVERIFIED,
        request_digest="f" * 64,
        created_by="legacy-import",
        created_at=datetime(2026, 7, 27, tzinfo=timezone.utc),
        head_revision=1,
        manual_checklist_ref="legacy://manual-checklist",
    )


def test_legacy_unverified_receipt_never_projects_a_vacuous_pass() -> None:
    assert _legacy_receipt().blocking_satisfied is False
    assert server._checklist_receipt_outcome(_legacy_receipt()) == "fail"


@pytest.mark.asyncio
@pytest.mark.parametrize("replayed", [False, True])
async def test_mcp_start_returns_the_frozen_ordered_template_items(
    monkeypatch,
    replayed,
) -> None:
    async def agent_ctx(_board_id):
        return _ctx()

    monkeypatch.setattr(server, "_get_agent_ctx", agent_ctx)
    monkeypatch.setattr(server, "check_permission", lambda *_args: None)
    monkeypatch.setattr(
        server,
        "get_unit_of_work_factory_for_mcp",
        lambda: lambda **_kwargs: _UowContext(),
    )

    async def start_execute(self, command, *, actor, uow):
        del self, command, actor, uow
        return ChecklistExecutionStartResult(
            execution=ChecklistExecution(
                id="execution-1",
                board_id="board-1",
                spec_id="spec-1",
                spec_version=4,
                content_digest="c" * 64,
                input_digest="d" * 64,
                template_version=SPECIFY_CHECKLIST_TEMPLATE_V1.version,
                template_digest=SPECIFY_CHECKLIST_TEMPLATE_V1.digest,
                binding_version=2,
                binding_digest="b" * 64,
                binding_mode=ChecklistMode.BLOCKING,
                request_digest="e" * 64,
                idempotency_key="start-1",
                created_by="agent-1",
                created_at=datetime(2026, 7, 27, tzinfo=timezone.utc),
            ),
            replayed=replayed,
        )

    monkeypatch.setattr(
        StartChecklistExecutionUseCase,
        "execute",
        start_execute,
    )

    payload = json.loads(
        await server.okto_pulse_start_checklist_execution.fn(
            board_id="board-1",
            spec_id="spec-1",
            binding_id="b" * 64,
            expected_spec_version=4,
            idempotency_key="start-1",
        )
    )

    assert payload["outcome"] == "success"
    data = payload["data"]
    assert data["template_version_id"] == SPECIFY_CHECKLIST_TEMPLATE_V1.version
    assert data["template_digest"] == SPECIFY_CHECKLIST_TEMPLATE_V1.digest
    assert data["replayed"] is replayed
    assert data["items"] == [
        {
            "item_id": item.item_id,
            "title_en": item.title_en,
            "title_pt": item.title_pt,
            "description_en": item.description_en,
            "description_pt": item.description_pt,
            "allow_na": item.allow_na,
        }
        for item in SPECIFY_CHECKLIST_TEMPLATE_V1.items
    ]
    assert [item["item_id"] for item in data["items"]] == list(
        SPECIFY_CHECKLIST_ITEM_IDS
    )


@pytest.mark.asyncio
async def test_live_registry_excludes_redundant_reads_and_closes_results_schema():
    tools = await server.mcp.get_tools()
    assert "okto_pulse_get_checklist_state" not in tools
    assert "okto_pulse_list_checklist_executions" not in tools

    results_schema = tools[
        "okto_pulse_submit_checklist_execution"
    ].parameters["properties"]["results"]
    item_schema = results_schema["$defs"]["_ChecklistItemResultInput"]
    assert item_schema["additionalProperties"] is False
    assert item_schema["properties"]["outcome"]["enum"] == [
        "pass",
        "fail",
        "not_applicable",
    ]


@pytest.mark.asyncio
async def test_mcp_submit_and_receipt_expose_failed_aggregate_outcome(
    monkeypatch,
) -> None:
    async def agent_ctx(_board_id):
        return _ctx()

    monkeypatch.setattr(server, "_get_agent_ctx", agent_ctx)
    monkeypatch.setattr(server, "check_permission", lambda *_args: None)
    monkeypatch.setattr(
        server,
        "get_unit_of_work_factory_for_mcp",
        lambda: (lambda **_kwargs: _UowContext()),
    )

    async def submit_execute(self, command, *, actor, uow):
        del self, command, actor, uow
        return ChecklistCommitResult(
            board_id="board-1",
            spec_id="spec-1",
            spec_version=4,
            receipt_id="receipt-1",
            request_digest="f" * 64,
            head_revision=3,
        )

    async def receipt_execute(self, command, *, actor, uow):
        del self, command, actor, uow
        return ChecklistReceipt(
            id="receipt-1",
            board_id="board-1",
            spec_id="spec-1",
            spec_version=4,
            content_digest="c" * 64,
            input_digest="d" * 64,
            template_version="/specify/v1",
            template_digest=SPECIFY_CHECKLIST_TEMPLATE_V1.digest,
            binding_version=2,
            binding_digest="b" * 64,
            binding_mode=ChecklistMode.BLOCKING,
            items=_failed_items(),
            source=ChecklistReceiptSource.NATIVE,
            request_digest="f" * 64,
            created_by="agent-1",
            created_at=datetime(2026, 7, 27, tzinfo=timezone.utc),
            head_revision=3,
            idempotency_key="submit-1",
        )

    monkeypatch.setattr(
        SubmitChecklistExecutionUseCase,
        "execute",
        submit_execute,
    )
    monkeypatch.setattr(
        GetChecklistReceiptUseCase,
        "execute",
        receipt_execute,
    )

    submit_payload = json.loads(
        await server.okto_pulse_submit_checklist_execution.fn(
            board_id="board-1",
            spec_id="spec-1",
            execution_id="execution-1",
            expected_execution_revision=1,
            idempotency_key="submit-1",
            results=[
                server._ChecklistItemResultInput(
                    item_id=item.item_id,
                    outcome=item.outcome.value,
                    anchor=item.anchor,
                    rationale=item.rationale,
                )
                for item in _failed_items()
            ],
        )
    )
    assert submit_payload["outcome"] == "success"
    assert submit_payload["data"]["outcome"] == "fail"

    receipt_payload = json.loads(
        await server.okto_pulse_get_checklist_receipt.fn(
            board_id="board-1",
            receipt_id="receipt-1",
        )
    )
    assert receipt_payload["outcome"] == "success"
    assert receipt_payload["data"]["outcome"] == "fail"
