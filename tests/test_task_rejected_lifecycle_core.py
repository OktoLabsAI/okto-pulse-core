"""Core contracts for the governed Task/Bug Rejected rework handoff."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from pydantic import ValidationError

from okto_pulse.core.application.use_cases.allowed_transitions import (
    allowed_transitions_for_status,
)
from okto_pulse.core.application.processors.deterministic_kg import (
    DeterministicWorker,
)
from okto_pulse.core.domain.card_completion import (
    CardCompletionOutcome,
    CardRejectionKind,
    CompletionGateFailure,
    REJECTION_REASON_CODE_MAX_COUNT,
    REJECTION_SUMMARY_MAX_LENGTH,
    TaskValidationOutcome,
    current_rejection_cause,
    decide_card_completion,
    resolve_current_rejection_record,
)
from okto_pulse.core.domain.enums import CardType
from okto_pulse.core.domain.sdlc_registry import (
    is_internal_transition_allowed,
    is_transition_allowed,
)
from okto_pulse.core.kg.source_maturity import (
    DISPOSITION_WORKING,
    GRAPH_LAYER_WORKING,
    classify_source_for_kg,
)
from okto_pulse.core.models.schemas import (
    CardCreate,
    CardMove,
    TaskValidationSubmit,
    redact_card_validation_projection,
)
from okto_pulse.core.services.card_errors import CardOperationError
from okto_pulse.core.services.card_operational_freeze import (
    require_card_operational_mutation_allowed,
)
from okto_pulse.core.services.main import CardService


@pytest.mark.parametrize("card_type", ["normal", "bug"])
def test_rejection_is_internal_and_only_rework_exit_is_public(card_type: str) -> None:
    assert is_internal_transition_allowed(
        "card", "validation", "rejected", card_type=card_type
    )
    assert not is_transition_allowed(
        "card", "validation", "rejected", card_type=card_type
    )
    assert [
        item.to_status
        for item in allowed_transitions_for_status(
            "card", "rejected", card_type=card_type
        )
    ] == ["in_progress"]


def test_test_cards_never_enter_rejected() -> None:
    assert not is_internal_transition_allowed(
        "card", "validation", "rejected", card_type="test"
    )
    assert allowed_transitions_for_status("card", "rejected", card_type="test") == []
    assert [
        item.to_status
        for item in allowed_transitions_for_status(
            "card", "validation", card_type="test"
        )
        if item.to_status == "in_progress"
    ] == ["in_progress"]
    assert all(
        item.to_status != "in_progress"
        for item in allowed_transitions_for_status(
            "card", "validation", card_type="normal"
        )
    )


def test_completion_decision_keeps_validation_and_completion_outcomes_distinct() -> (
    None
):
    blocked = decide_card_completion(
        validation_outcome=TaskValidationOutcome.SUCCESS,
        gate_failures=(
            CompletionGateFailure(
                code="dependencies_incomplete",
                summary="Dependency A is unfinished.",
            ),
        ),
    )
    assert blocked.validation_outcome is TaskValidationOutcome.SUCCESS
    assert blocked.completion_outcome is CardCompletionOutcome.REJECTED

    failed = decide_card_completion(validation_outcome=TaskValidationOutcome.FAILED)
    assert failed.completion_outcome is CardCompletionOutcome.REJECTED

    completed = decide_card_completion(validation_outcome=TaskValidationOutcome.SUCCESS)
    assert completed.completion_outcome is CardCompletionOutcome.COMPLETED


def _sealed_task_validation_rejection() -> dict[str, object]:
    summary = "Acceptance criterion AC-2 was not met."
    return {
        "id": "card-123",
        "board_id": "board-123",
        "validations": [
            {
                "id": "val_123",
                "card_id": "card-123",
                "board_id": "board-123",
                "expected_subject_version": 7,
                "validation_outcome": "failed",
                "completion_outcome": "rejected",
            }
        ],
        "rejection_records": [
            {
                "id": "rej_123",
                "card_id": "card-123",
                "board_id": "board-123",
                "kind": "task_validation",
                "source_id": "val_123",
                "code": "task_validation_failed",
                "summary": summary,
                "reason_codes": ["reject_recommendation"],
                "created_by": "reviewer-123",
                "created_at": "2026-08-14T12:00:00+00:00",
                "subject_version": 7,
            }
        ],
        "current_rejection_kind": "task_validation",
        "current_rejection_id": "rej_123",
        "current_rejection_code": "task_validation_failed",
        "current_rejection_summary": summary,
    }


def test_current_cause_requires_a_sealed_record_and_admitted_validation() -> None:
    payload = _sealed_task_validation_rejection()
    cause = current_rejection_cause(payload)
    assert cause is not None
    assert cause.kind is CardRejectionKind.TASK_VALIDATION
    assert cause.id == "rej_123"
    assert resolve_current_rejection_record(payload).source_id == "val_123"

    incomplete = dict(payload)
    incomplete["current_rejection_summary"] = None
    assert current_rejection_cause(incomplete) is None

    legacy_direct_pointer = dict(payload)
    legacy_direct_pointer["current_rejection_id"] = "val_123"
    assert current_rejection_cause(legacy_direct_pointer) is None

    mismatched_record = dict(payload)
    mismatched_record["current_rejection_code"] = "different_code"
    assert current_rejection_cause(mismatched_record) is None

    missing_source = dict(payload)
    missing_source["validations"] = []
    assert current_rejection_cause(missing_source) is None

    inadmissible_source = dict(payload)
    inadmissible_source["validations"] = [
        {
            **payload["validations"][0],
            "completion_outcome": "completed",
        }
    ]
    assert current_rejection_cause(inadmissible_source) is None


def test_rejection_projection_is_bounded_without_rewriting_source_history() -> None:
    original = "Human validation detail " * 500
    failure = CompletionGateFailure(
        code="dependencies_incomplete",
        summary=original,
        reason_codes=tuple(f"reason_{index}" for index in range(100)),
    )

    assert len(failure.summary) == REJECTION_SUMMARY_MAX_LENGTH
    assert failure.summary.endswith("…")
    assert len(failure.reason_codes) == REJECTION_REASON_CODE_MAX_COUNT
    assert original.startswith(failure.summary[:-1])
    assert len(original) > len(failure.summary)


def test_rejected_task_remains_visible_in_working_kg_without_ttl_expiry() -> None:
    result = classify_source_for_kg(
        artifact_type="task",
        artifact_status="rejected",
        content_hash="a" * 64,
        updated_at=datetime(2000, 1, 1, tzinfo=timezone.utc),
        now=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    assert result.graph_layer == GRAPH_LAYER_WORKING
    assert result.disposition == DISPOSITION_WORKING
    assert result.expires_at is None


def test_kg_projects_only_generic_rework_signal_not_rejection_cause() -> None:
    secret = "Human validator details that require card.validation.read"
    result = DeterministicWorker().process_card(
        {
            "id": "card-rejected-1",
            "board_id": "board-1",
            "title": "Rejected Task",
            "description": "Public implementation description",
            "status": "rejected",
            "card_type": "normal",
            "current_rejection_code": "task_validation_failed",
            "current_rejection_summary": secret,
        }
    )

    assert secret not in result.raw_content
    assert "task_validation_failed" not in result.raw_content
    assert "Lifecycle: Rejected" in result.raw_content
    assert all(secret not in node.content for node in result.nodes)


def test_validation_redaction_hides_body_cause_and_aggregate_scores() -> None:
    projected = redact_card_validation_projection(
        {
            "status": "rejected",
            "validations": [{"general_justification": "secret"}],
            "rejection_records": [{"summary": "secret"}],
            "current_rejection_kind": "task_validation",
            "current_rejection_id": "val-1",
            "current_rejection_code": "task_validation_failed",
            "current_rejection_summary": "secret",
            "validations_count": 2,
            "validations_fail_count": 1,
            "validations_has_pass": True,
            "first_pass_confidence": 91,
            "first_pass_completeness": 92,
            "first_pass_drift": 3,
        }
    )

    assert projected["status"] == "rejected"
    assert projected["validations"] is None
    assert projected["rejection_records"] == []
    assert projected["current_rejection_summary"] is None
    assert projected["current_rejection_code"] is None
    assert projected["validations_count"] == 0
    assert projected["validations_fail_count"] == 0
    assert projected["validations_has_pass"] is False
    assert projected["first_pass_confidence"] is None
    assert "secret" not in json.dumps(projected)


def test_operational_freeze_error_does_not_leak_current_cause() -> None:
    card = SimpleNamespace(
        id="card-1",
        status="rejected",
        current_rejection_kind="task_validation",
        current_rejection_id="val-1",
        current_rejection_code="task_validation_failed",
        current_rejection_summary="private human validation detail",
    )

    with pytest.raises(CardOperationError) as caught:
        require_card_operational_mutation_allowed(card, operation="update_card")

    payload = caught.value.to_dict()
    assert payload["code"] == "card_rejected_rework_handoff_required"
    assert payload["facts"] == {
        "card_id": "card-1",
        "operation": "update_card",
        "status": "rejected",
    }
    assert "private human validation detail" not in json.dumps(payload)


@pytest.mark.asyncio
async def test_bug_completion_rechecks_missing_regression_evidence() -> None:
    card = SimpleNamespace(
        id="bug-1",
        board_id="board-1",
        card_type=CardType.BUG,
        spec_id="spec-1",
        severity="major",
        linked_test_task_ids=[],
    )
    board = SimpleNamespace(
        settings={
            "require_test_task_for_bug": True,
            "bug_test_gate_min_severity": "minor",
        }
    )
    with patch(
        "okto_pulse.core.services.main.AmendmentRevisionService.list_for_bug",
        AsyncMock(return_value=[]),
    ):
        failure = await CardService(object())._bug_regression_completion_failure(
            card=card,
            board=board,
        )

    assert failure is not None
    assert failure.code == "missing_regression_test_task"
    assert failure.reason_codes == ("missing_regression_test_task",)


@pytest.mark.asyncio
async def test_nested_board_cards_require_validation_read() -> None:
    from okto_pulse.core.application.use_cases.base import ActorContext
    from okto_pulse.core.application.use_cases.boards_crud import (
        _project_board_validation_visibility,
    )
    from okto_pulse.core.domain.enums import CardPriority, CardStatus

    now = datetime.now(timezone.utc)
    secret = "private validation cause"
    card = SimpleNamespace(
        id="card-1",
        board_id="board-1",
        spec_id=None,
        sprint_id=None,
        title="Rejected task",
        description=None,
        details=None,
        status=CardStatus.REJECTED,
        policy_version=2,
        priority=CardPriority.NONE,
        position=0,
        assignee_id=None,
        created_by="creator-1",
        created_at=now,
        updated_at=now,
        due_date=None,
        labels=[],
        validations=[{"id": "validation-1", "general_justification": secret}],
        rejection_records=[{"id": "rejection-1", "summary": secret}],
        current_rejection_kind="task_validation",
        current_rejection_id="rejection-1",
        current_rejection_code="task_validation_failed",
        current_rejection_summary=secret,
    )
    board = SimpleNamespace(
        id="board-1",
        name="Board",
        description=None,
        owner_id="owner-1",
        realm_id=None,
        settings=None,
        default_config_snapshot=None,
        created_at=now,
        updated_at=now,
        cards=[card],
        agents=[],
        counts={"cards": 1, "agents": 0},
    )
    sparse_actor = ActorContext(
        "reader-1",
        "rest",
        board_id="board-1",
        permissions=[],
    )
    projected = await _project_board_validation_visibility(
        board,
        actor=sparse_actor,
        uow=SimpleNamespace(),
        board_id="board-1",
    )

    assert projected.cards[0].status is CardStatus.REJECTED
    assert projected.cards[0].validations is None
    assert projected.cards[0].rejection_records == []
    assert projected.cards[0].current_rejection_summary is None
    assert secret not in projected.model_dump_json()


@pytest.mark.asyncio
async def test_board_blockers_redact_rejection_cause_without_validation_read() -> None:
    from okto_pulse.core.application.use_cases import analytics_helpers
    from okto_pulse.core.application.use_cases.analytics_helpers import (
        BoardBlockersCommand,
        BoardBlockersUseCase,
    )
    from okto_pulse.core.application.use_cases.base import ActorContext

    secret = "private rejection explanation"
    source = {
        "board_id": "board-1",
        "blockers": [
            {
                "type": "rework_required",
                "card_id": "card-1",
                "reason": secret,
                "evidence": {
                    "cause_kind": "task_validation",
                    "cause_code": "task_validation_failed",
                },
            }
        ],
    }
    uow = SimpleNamespace(
        services=SimpleNamespace(
            analytics=SimpleNamespace(blockers=AsyncMock(return_value=source))
        )
    )
    actor = ActorContext(
        "reader-1",
        "rest",
        board_id="board-1",
        permissions=[],
    )
    with patch.object(
        analytics_helpers,
        "_ensure_board_access",
        AsyncMock(return_value=None),
    ):
        result = await BoardBlockersUseCase().execute(
            BoardBlockersCommand("board-1"),
            actor=actor,
            uow=uow,
        )

    blocker = result.data["blockers"][0]
    assert blocker["reason"] == "A governed completion attempt requires rework"
    assert blocker["evidence"] == {}
    assert secret not in json.dumps(result.data)


@pytest.mark.asyncio
async def test_validation_analytics_are_leaf_aware_and_strip_ledger_plumbing() -> None:
    from okto_pulse.core.application.use_cases import analytics_helpers
    from okto_pulse.core.application.use_cases.analytics_helpers import (
        BoardEntityDetailCommand,
        BoardEntityDetailUseCase,
        BoardValidationsCommand,
        BoardValidationsUseCase,
    )
    from okto_pulse.core.application.use_cases.base import ActorContext

    secret = "private validation body"
    validations_payload = {
        "spec_validation_gate": {"total_submitted": 7},
        "task_validation_gate": {
            "total_submitted": 1,
            "per_card": [{"card_id": "card-1", "last_outcome": "failed"}],
            "secret": secret,
        },
        "spec_evaluation": {"total": 3},
        "sprint_evaluation": {"total": 2},
    }
    card_detail = {
        "card_id": "card-1",
        "title": "Rejected card",
        "status": "rejected",
        "validations": [
            {
                "id": "validation-1",
                "outcome": "failed",
                "general_justification": secret,
                "idempotency_key": "private-key",
                "request_digest": "private-digest",
                "response": {"outcome": "failed", "replayed": False},
            }
        ],
        "current_rejection_summary": secret,
    }
    services = SimpleNamespace(
        analytics=SimpleNamespace(
            validations=AsyncMock(return_value=validations_payload),
            entity_detail=AsyncMock(return_value=card_detail),
        )
    )
    sparse_actor = ActorContext(
        "reader-1",
        "rest",
        board_id="board-1",
        permissions=[],
    )
    with patch.object(
        analytics_helpers,
        "_ensure_board_access",
        AsyncMock(return_value=None),
    ):
        validations_result = await BoardValidationsUseCase().execute(
            BoardValidationsCommand("board-1"),
            actor=sparse_actor,
            uow=SimpleNamespace(services=services),
        )
        card_result = await BoardEntityDetailUseCase().execute(
            BoardEntityDetailCommand("board-1", "card", "card-1"),
            actor=sparse_actor,
            uow=SimpleNamespace(services=services),
        )

    assert validations_result.data["spec_validation_gate"] == {"total_submitted": 7}
    assert validations_result.data["spec_evaluation"] == {"total": 3}
    task_gate = validations_result.data["task_validation_gate"]
    assert task_gate["redacted"] is True
    assert task_gate["total_submitted"] == 0
    assert task_gate["per_card"] == []
    assert secret not in json.dumps(validations_result.data)
    assert card_result.data["status"] == "rejected"
    assert card_result.data["validations"] is None
    assert card_result.data["current_rejection_summary"] is None
    assert secret not in json.dumps(card_result.data)

    validation_actor = ActorContext(
        "validator-1",
        "rest",
        board_id="board-1",
        permissions=["card.validation.read"],
    )
    with patch.object(
        analytics_helpers,
        "_ensure_board_access",
        AsyncMock(return_value=None),
    ):
        allowed = await BoardEntityDetailUseCase().execute(
            BoardEntityDetailCommand("board-1", "card", "card-1"),
            actor=validation_actor,
            uow=SimpleNamespace(services=services),
        )
    validation = allowed.data["validations"][0]
    assert validation["general_justification"] == secret
    assert "idempotency_key" not in validation
    assert "request_digest" not in validation
    assert "response" not in validation


@pytest.mark.asyncio
async def test_discovery_outputs_only_generic_rework_and_activity_signals() -> None:
    from okto_pulse.core.domain.enums import CardStatus, SprintStatus
    from okto_pulse.core.services import discovery_executor

    secret = "private validation rejection cause"
    now = datetime.now(timezone.utc)
    rejected_card = SimpleNamespace(
        id="card-1",
        title="Rejected card",
        status=CardStatus.REJECTED,
        sprint_id="sprint-1",
        updated_at=now,
        current_rejection_kind="task_validation",
        current_rejection_code="task_validation_failed",
        current_rejection_summary=secret,
    )
    validation_activity = SimpleNamespace(
        id="activity-1",
        action="validation_submitted",
        details={"rejection_cause": {"summary": secret}},
        card_id="card-1",
        actor_id="reviewer-1",
        actor_type="agent",
        actor_name="Reviewer",
        created_at=now,
    )
    reader = SimpleNamespace(
        list_sprints=AsyncMock(
            return_value=[
                SimpleNamespace(
                    id="sprint-1",
                    title="Sprint",
                    status=SprintStatus.ACTIVE,
                )
            ]
        ),
        list_cards_for_sprints=AsyncMock(return_value=[rejected_card]),
        list_dependencies_for_cards=AsyncMock(return_value=[]),
        list_recent_activity=AsyncMock(return_value=[validation_activity]),
        resolve_entity_titles=AsyncMock(
            return_value={("card", "card-1"): "Rejected card"}
        ),
    )
    with patch.object(
        discovery_executor,
        "get_discovery_execution_read_port",
        return_value=reader,
    ):
        blockers = await discovery_executor._exec_blockers(None, "board-1")
        activity = await discovery_executor._exec_activity_log(None, "board-1")

    rejected = next(row for row in blockers["rows"] if row["type"] == "rejected_card")
    assert rejected["summary"] == "Sprint 'Sprint' · rework required"
    assert "cause_kind" not in rejected["meta"]
    assert secret not in json.dumps(blockers)
    assert activity["rows"][0]["meta"]["details"] == {"redacted": True}
    assert secret not in json.dumps(activity)


@pytest.mark.asyncio
async def test_card_and_board_activity_redact_task_validation_details() -> None:
    from okto_pulse.core.application.use_cases import card_crud
    from okto_pulse.core.application.use_cases.base import ActorContext
    from okto_pulse.core.application.use_cases.card_crud import (
        GetCardActivityCommand,
        GetCardActivityUseCase,
    )
    from okto_pulse.core.application.use_cases.mcp_profile_activity import (
        McpGetActivityLogCommand,
        McpGetActivityLogUseCase,
    )
    from okto_pulse.core.models.schemas import ActivityLogResponse

    now = datetime.now(timezone.utc)
    secret = "private rejection summary"
    activity = ActivityLogResponse(
        id="activity-1",
        board_id="board-1",
        card_id="card-1",
        action="validation_submitted",
        actor_type="agent",
        actor_id="reviewer-1",
        actor_name="Reviewer",
        summary=secret,
        details={"rejection_cause": {"summary": secret}},
        created_at=now,
    )
    card_uow = SimpleNamespace(
        services=SimpleNamespace(
            compute_card_activity=AsyncMock(return_value=[activity])
        )
    )
    card_actor = ActorContext(
        "reader-1",
        "rest",
        board_id="board-1",
        permissions=["card.activity_read"],
    )
    with patch.object(
        card_crud,
        "_get_card_for_actor",
        AsyncMock(return_value=SimpleNamespace(board_id="board-1")),
    ):
        card_result = await GetCardActivityUseCase().execute(
            GetCardActivityCommand("card-1"),
            actor=card_actor,
            uow=card_uow,
        )
    assert card_result.activity[0].summary == "Task validation submitted"
    assert card_result.activity[0].details == {"redacted": True}

    board_uow = SimpleNamespace(
        services=SimpleNamespace(
            get_activity_log_rows=AsyncMock(
                return_value=(
                    [
                        {
                            "id": "activity-1",
                            "action": "validation_submitted",
                            "summary": secret,
                            "details": {"rejection_cause": {"summary": secret}},
                        }
                    ],
                    None,
                )
            )
        ),
        commit=AsyncMock(),
    )
    board_actor = ActorContext(
        "reader-1",
        "mcp",
        board_id="board-1",
        permissions=["board.activity_read"],
    )
    board_result = await McpGetActivityLogUseCase().execute(
        McpGetActivityLogCommand(
            "board-1",
            limit=50,
            cursor_pair=None,
            effective_offset=0,
            include_details=True,
        ),
        actor=board_actor,
        uow=board_uow,
    )
    assert board_result.rows[0]["summary"] == "Task validation submitted"
    assert board_result.rows[0]["details"] == {"redacted": True}
    assert secret not in json.dumps(board_result.rows)


@pytest.mark.asyncio
async def test_delete_validation_authorizes_read_before_id_lookup() -> None:
    from okto_pulse.core.application.use_cases import card_crud
    from okto_pulse.core.application.use_cases.base import (
        ActorContext,
        PermissionDeniedError,
    )
    from okto_pulse.core.application.use_cases.card_crud import (
        DeleteTaskValidationCommand,
        DeleteTaskValidationUseCase,
    )

    service = SimpleNamespace(
        get_task_validation=AsyncMock(return_value={"id": "validation-1"}),
        delete_task_validation=AsyncMock(),
    )
    uow = SimpleNamespace(services=SimpleNamespace(cards=service))
    actor = ActorContext(
        "reader-1",
        "rest",
        board_id="board-1",
        permissions=[],
    )
    with (
        patch.object(
            card_crud,
            "_get_card_for_actor",
            AsyncMock(
                return_value=SimpleNamespace(board_id="board-1", status="validation")
            ),
        ),
        pytest.raises(PermissionDeniedError),
    ):
        await DeleteTaskValidationUseCase().execute(
            DeleteTaskValidationCommand("card-1", "validation-1"),
            actor=actor,
            uow=uow,
        )

    service.get_task_validation.assert_not_awaited()
    service.delete_task_validation.assert_not_awaited()


def test_task_validation_transport_requires_fence_and_idempotency() -> None:
    base = {
        "confidence": 90,
        "confidence_justification": "Reviewed the implementation in detail.",
        "estimated_completeness": 95,
        "completeness_justification": "All requested behavior is present.",
        "estimated_drift": 2,
        "drift_justification": "Only a small documented adjustment was made.",
        "general_justification": "The implementation satisfies the task contract.",
        "recommendation": "approve",
    }
    with pytest.raises(ValidationError):
        TaskValidationSubmit.model_validate(base)

    request = TaskValidationSubmit.model_validate(
        {
            **base,
            "expected_card_version": 7,
            "idempotency_key": "validation-attempt-7",
        }
    )
    assert request.expected_subject_version == 7


def test_rest_move_schema_accepts_rejected_only_for_domain_checked_reorder() -> None:
    request = CardMove.model_validate({"status": "rejected", "position": 2})
    assert request.status.value == "rejected"
    assert "rejected" in CardMove.model_json_schema()["properties"]["status"]["enum"]


def test_card_create_schema_exposes_only_lifecycle_entry_states() -> None:
    with pytest.raises(ValidationError):
        CardCreate.model_validate(
            {"title": "Cannot be born rejected", "status": "rejected"}
        )
    assert CardCreate.model_json_schema()["properties"]["status"]["enum"] == [
        "not_started",
        "started",
    ]


@pytest.mark.asyncio
async def test_mcp_create_rejects_consequence_only_initial_status_before_uow() -> None:
    from okto_pulse.core.mcp import server as mcp_server

    tool = await mcp_server.mcp.get_tool("okto_pulse_create_card")
    with patch.object(
        mcp_server,
        "_get_agent_ctx",
        AsyncMock(return_value=SimpleNamespace(agent_id="executor-1")),
    ):
        payload = json.loads(
            await tool.fn(
                board_id="board-1",
                spec_id="spec-1",
                title="Cannot be born rejected",
                status="rejected",
            )
        )
    assert payload == {
        "error": "card_initial_status_invalid",
        "detail": "A card can only be created in not_started or started.",
        "allowed_statuses": ["not_started", "started"],
    }
