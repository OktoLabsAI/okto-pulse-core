"""Task-validation reviewer separation: policy, persistence, and MCP projection."""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from mcp_runtime_testing import register_mcp_test_runtime
from sqlalchemy_test_models import (
    Board,
    Card,
    CardPriority,
    CardStatus,
    CardType,
)

from okto_pulse.core.mcp import server as mcp_server
from okto_pulse.core.mcp.outcome import coerce_mcp_tool_outcome
from okto_pulse.core.services.board_governance import BoardGovernanceService
from okto_pulse.core.services.default_board_configuration import (
    DefaultBoardConfigurationService,
)
from okto_pulse.core.services.main import CardOperationError, CardService
from okto_pulse.core.services.reviewer_separation import (
    evaluate_task_reviewer_separation,
)


REVIEWER_ID = "task-reviewer"


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def _validation_payload() -> dict:
    return {
        "expected_subject_version": 1,
        "idempotency_key": "reviewer-separation-validation",
        "confidence": 95,
        "confidence_justification": "The implementation was inspected.",
        "estimated_completeness": 95,
        "completeness_justification": "The requested scope is present.",
        "estimated_drift": 5,
        "drift_justification": "Only a minor implementation variation exists.",
        # Reject keeps this policy test independent from completion/resource gates.
        "general_justification": "Role-policy test submission.",
        "recommendation": "reject",
    }


async def _seed_conflicted_card(db_factory, *, mode: str | None) -> tuple[str, str]:
    board_id = _id("review-board")
    card_id = _id("review-card")
    settings: dict[str, object] = {
        "require_full_context_for_critical_actions": False,
    }
    if mode is not None:
        settings["reviewer_separation_mode"] = mode
    async with db_factory() as db:
        db.add(
            Board(
                id=board_id,
                name="Reviewer separation board",
                owner_id=REVIEWER_ID,
                settings=settings,
            )
        )
        db.add(
            Card(
                id=card_id,
                board_id=board_id,
                title="Conflicted task",
                status=CardStatus.VALIDATION,
                card_type=CardType.NORMAL,
                priority=CardPriority.MEDIUM,
                position=0,
                created_by=REVIEWER_ID,
                assignee_id=REVIEWER_ID,
                conclusions=[
                    {
                        "text": "Executor report",
                        "author_id": REVIEWER_ID,
                        "source": "move_to_validation",
                    }
                ],
            )
        )
        await db.commit()
    return board_id, card_id


def test_task_decision_evaluates_creator_assignee_and_executor_conflicts() -> None:
    card_id = "card-1"
    decision = evaluate_task_reviewer_separation(
        board=type(
            "BoardFact",
            (),
            {"settings": {"reviewer_separation_mode": "enforce"}},
        )(),
        reviewer_id=REVIEWER_ID,
        card=type(
            "CardFact",
            (),
            {
                "id": card_id,
                "created_by": REVIEWER_ID,
                "assignee_id": REVIEWER_ID,
                "conclusions": [{"actor_id": REVIEWER_ID}],
            },
        )(),
    )
    assert decision.allowed is False
    assert decision.conflicts == (
        f"card_assignee:{card_id}",
        f"card_creator:{card_id}",
        f"card_executor:{card_id}",
    )


def test_unrelated_legacy_settings_patch_preserves_absent_compat_source() -> None:
    merged = BoardGovernanceService.merge_settings_patch(
        {"max_scenarios_per_card": 3},
        {"max_scenarios_per_card": 4},
    )
    assert "reviewer_separation_mode" not in merged


def test_enforce_allows_an_independent_task_reviewer() -> None:
    decision = evaluate_task_reviewer_separation(
        board=type(
            "BoardFact",
            (),
            {"settings": {"reviewer_separation_mode": "enforce"}},
        )(),
        reviewer_id="independent-reviewer",
        card=type(
            "CardFact",
            (),
            {
                "id": "card-1",
                "created_by": "creator",
                "assignee_id": "assignee",
                "conclusions": [{"author_id": "executor"}],
            },
        )(),
    )
    assert decision.allowed is True
    assert decision.warning is False
    assert decision.conflicts == ()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mode", "expected_source", "expected_warning"),
    [
        (None, "legacy_absent_compat", False),
        ("off", "board_settings", False),
        ("warn", "board_settings", True),
    ],
)
async def test_off_warn_and_legacy_modes_persist_transparent_decision(
    db_factory,
    mode: str | None,
    expected_source: str,
    expected_warning: bool,
) -> None:
    board_id, card_id = await _seed_conflicted_card(db_factory, mode=mode)

    async with db_factory() as db:
        result = await CardService(db).submit_task_validation(
            card_id,
            REVIEWER_ID,
            "Task Reviewer",
            _validation_payload(),
        )
        await db.commit()

        decision = result["reviewer_separation"]
        assert decision["allowed"] is True
        assert decision["warning"] is expected_warning
        assert decision["source"] == expected_source
        assert decision["conflicts"]

        persisted = await db.get(Card, card_id)
        assert persisted.board_id == board_id
        assert persisted.status == CardStatus.REJECTED
        assert result["validation_outcome"] == "failed"
        assert result["completion_outcome"] == "rejected"
        assert result["reviewer_name"] == "Task Reviewer"
        assert result["evaluator_name"] == "Task Reviewer"
        assert persisted.validations[-1]["reviewer_separation"] == decision
        assert persisted.validations[-1]["reviewer_name"] == "Task Reviewer"
        assert persisted.validations[-1]["evaluator_name"] == "Task Reviewer"
        assert len(persisted.rejection_records) == 1
        rejection = persisted.rejection_records[0]
        assert persisted.current_rejection_id == rejection["id"]
        assert rejection["source_id"] == result["id"]
        assert rejection["kind"] == "task_validation"

        replay = await CardService(db).submit_task_validation(
            card_id,
            REVIEWER_ID,
            "Task Reviewer",
            _validation_payload(),
        )
        assert replay["id"] == result["id"]
        assert replay["reviewer_name"] == "Task Reviewer"
        assert replay["evaluator_name"] == "Task Reviewer"
        assert replay["replayed"] is True
        await db.refresh(persisted)
        assert len(persisted.rejection_records) == 1


@pytest.mark.asyncio
async def test_enforce_fails_closed_before_validation_or_status_mutation(
    db_factory,
) -> None:
    _, card_id = await _seed_conflicted_card(db_factory, mode="enforce")

    async with db_factory() as db:
        with pytest.raises(CardOperationError) as raised:
            await CardService(db).submit_task_validation(
                card_id,
                REVIEWER_ID,
                "Task Reviewer",
                _validation_payload(),
            )

        payload = raised.value.to_dict()
        assert payload["code"] == "reviewer_separation_required"
        assert payload["remediation"] == "request_independent_task_validator"
        assert payload["facts"]["reviewer_separation"]["mode"] == "enforce"
        assert payload["facts"]["reviewer_separation"]["allowed"] is False

        persisted = await db.get(Card, card_id)
        assert persisted.status == CardStatus.VALIDATION
        assert not (persisted.validations or [])


class _Ctx:
    agent_id = REVIEWER_ID
    agent_name = "Task Reviewer"
    permissions = [
        "card.validation.submit",
        "card.validation.read",
        "code_traceability.investigation.read",
        "code_traceability.evidence.read",
        "code_traceability.target.read",
        "code_traceability.overlap.read",
    ]


async def _call_tool(name: str, db_factory, **kwargs) -> dict:
    register_mcp_test_runtime(db_factory)
    with (
        patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_Ctx())),
        patch.object(mcp_server, "check_permission", return_value=None),
        patch.object(mcp_server, "_mcp_check_permission", return_value=None),
        patch.object(
            mcp_server,
            "_cognitive_enforcement_active",
            AsyncMock(return_value=False),
        ),
        patch.object(
            mcp_server,
            "_evaluate_card_cognitive_verdict",
            AsyncMock(return_value={"blocking": False, "would_block_done": False}),
        ),
    ):
        tool = await mcp_server.mcp.get_tool(name)
        return json.loads(await tool.fn(**kwargs))


@pytest.mark.asyncio
async def test_mcp_enforce_response_projects_action_required_and_remediation(
    db_factory,
) -> None:
    board_id, card_id = await _seed_conflicted_card(db_factory, mode="enforce")
    raw = await _call_tool(
        "okto_pulse_submit_task_validation",
        db_factory,
        board_id=board_id,
        card_id=card_id,
        **_validation_payload(),
    )

    assert raw["error"] == "reviewer_separation_required"
    assert raw["remediation"] == "request_independent_task_validator"
    assert raw["facts"]["reviewer_separation"]["conflicts"]

    outcome = coerce_mcp_tool_outcome(
        json.dumps(raw),
        tool_name="okto_pulse_submit_task_validation",
    ).structured_content(tool_name="okto_pulse_submit_task_validation")
    assert outcome["outcome"] == "action_required"
    assert outcome["error_code"] == "reviewer_separation_required"
    assert outcome["retryable"] is True
    assert outcome["next_action"] == {"hint": "request_independent_task_validator"}


@pytest.mark.asyncio
async def test_full_task_context_projects_legacy_compat_decision(db_factory) -> None:
    board_id, card_id = await _seed_conflicted_card(db_factory, mode=None)
    context = await _call_tool(
        "okto_pulse_get_task_context",
        db_factory,
        board_id=board_id,
        card_id=card_id,
        profile="full",
    )

    decision = context["reviewer_separation"]
    assert decision["applies_to_task_validation"] is True
    assert decision["mode"] == "off"
    assert decision["source"] == "legacy_absent_compat"
    assert decision["allowed"] is True
    assert decision["conflicts"]


@pytest.mark.asyncio
async def test_new_boards_and_default_template_versions_keep_enforce_default(
    db_factory,
) -> None:
    scope = _id("default-scope")
    async with db_factory() as db:
        service = DefaultBoardConfigurationService(db)
        fallback, snapshot = await service.build_snapshot_for_create(
            applied_by=REVIEWER_ID,
            scope=scope,
        )
        assert snapshot is None
        assert fallback["reviewer_separation_mode"] == "enforce"

        explicit_off, _ = await service.build_snapshot_for_create(
            settings_override={"reviewer_separation_mode": "off"},
            applied_by=REVIEWER_ID,
            scope=scope,
        )
        assert explicit_off["reviewer_separation_mode"] == "off"

        template = await service.create_version(
            settings_payload={},
            actor=REVIEWER_ID,
            scope=scope,
        )
        assert template.settings_payload["reviewer_separation_mode"] == "enforce"


def test_mcp_resources_describe_task_reviewer_separation_contract() -> None:
    root = (
        Path(__file__).parents[1] / "src" / "okto_pulse" / "core" / "mcp" / "resources"
    )
    cards = (root / "workflows" / "cards.md").read_text(encoding="utf-8")
    errors = (root / "reference" / "errors.md").read_text(encoding="utf-8")
    gates = (root / "reference" / "spec_gates.md").read_text(encoding="utf-8")
    for text in (cards, errors, gates):
        assert "reviewer_separation_required" in text
        assert "legacy_absent_compat" in text
    assert "ready for validation only after the move succeeds" in cards
    assert "the implementor has not completed the handoff" in cards
    for coupling in ("notify", "notification", "nexus", "messaging"):
        assert coupling not in cards.lower()
    assert "\"Card is not in 'validation' status\"" in errors
