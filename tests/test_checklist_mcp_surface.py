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
from okto_pulse.core.services.checklist import ChecklistConflictError
from okto_pulse.core.services.ska_observability import (
    METRIC_VALIDATION_EXTERNAL_COGNITION_UOW_DURATION_SECONDS,
    reset_ska_metric_samples_for_tests,
    ska_metric_samples,
    validation_edition_conflict_events,
)


class _UowContext:
    async def __aenter__(self):
        return object()

    async def __aexit__(self, exc_type, exc, traceback):
        del exc_type, exc, traceback


class _RollbackUowContext:
    def __init__(self, committed: list[str]) -> None:
        self.committed = committed
        self.staged: list[str] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        del exc, traceback
        if exc_type is None:
            self.committed.extend(self.staged)
        self.staged.clear()


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
async def test_mcp_start_returns_only_the_human_acknowledgement(
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
                spec_edition=3,
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
            spec_edition=3,
            expected_spec_version=4,
            binding_version=2,
        )
    )

    assert payload == {
        "outcome": "success",
        "data": {
            "execution_id": "execution-1",
            "spec_edition": 3,
            "status": "started",
        },
    }


@pytest.mark.asyncio
async def test_mcp_edition_conflict_emits_one_audit_and_rolls_back(
    monkeypatch,
) -> None:
    reset_ska_metric_samples_for_tests()
    committed: list[str] = []

    async def agent_ctx(_board_id):
        return _ctx()

    monkeypatch.setattr(server, "_get_agent_ctx", agent_ctx)
    monkeypatch.setattr(server, "check_permission", lambda *_args: None)
    monkeypatch.setattr(
        server,
        "get_unit_of_work_factory_for_mcp",
        lambda: (lambda **_kwargs: _RollbackUowContext(committed)),
    )

    async def reject_old_edition(self, command, *, actor, uow):
        del self, command, actor
        uow.staged.append("execution-would-have-been-written")
        raise ChecklistConflictError(
            "checklist_spec_edition_conflict",
            details={"expected": 3, "current": 4},
        )

    monkeypatch.setattr(
        StartChecklistExecutionUseCase,
        "execute",
        reject_old_edition,
    )

    payload = json.loads(
        await server.okto_pulse_start_checklist_execution.fn(
            board_id="board-1",
            spec_id="spec-1",
            spec_edition=3,
            expected_spec_version=4,
            binding_version=2,
        )
    )

    assert payload["error_code"] == "checklist_spec_edition_conflict"
    assert committed == []
    assert validation_edition_conflict_events() == (
        {
            "event": "validation.edition_conflict",
            "operation": "curated_checklist",
            "subject_type": "spec",
            "subject_id": "spec-1",
            "expected_edition": 3,
            "actual_edition": 4,
            "correlation_id": validation_edition_conflict_events()[0][
                "correlation_id"
            ],
            "conflict_code": "checklist_spec_edition_conflict",
        },
    )
    uow_samples = tuple(
        item
        for item in ska_metric_samples()
        if item["metric_name"]
        == METRIC_VALIDATION_EXTERNAL_COGNITION_UOW_DURATION_SECONDS
    )
    assert len(uow_samples) == 1
    assert uow_samples[0]["assessment_kind"] == "curated_checklist"
    assert uow_samples[0]["subject_type"] == "spec"
    assert uow_samples[0]["outcome"] == "conflict"


@pytest.mark.asyncio
async def test_live_registry_excludes_redundant_reads_and_closes_results_schema():
    tools = await server.mcp.get_tools()
    assert "okto_pulse_get_checklist_state" not in tools
    assert "okto_pulse_list_checklist_executions" not in tools

    results_schema = tools[
        "okto_pulse_submit_checklist_execution"
    ].parameters["properties"]["item_results"]
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
            spec_edition=3,
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
            spec_edition=3,
            expected_spec_version=4,
            execution_id="execution-1",
            item_results=[
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
    assert submit_payload == {
        "outcome": "success",
        "data": {
            "result_id": "receipt-1",
            "spec_edition": 3,
            "status": "failed",
        },
    }

    receipt_payload = json.loads(
        await server.okto_pulse_get_checklist_receipt.fn(
            board_id="board-1",
            receipt_id="receipt-1",
        )
    )
    assert receipt_payload["outcome"] == "success"
    assert receipt_payload["data"]["outcome"] == "fail"
