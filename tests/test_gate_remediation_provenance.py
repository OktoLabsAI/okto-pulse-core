"""Focused regressions for E2E findings #9, #10 and #50."""

from __future__ import annotations

import uuid

import pytest

from okto_pulse.core.application.use_cases.allowed_transitions import (
    ListAllowedTransitionsCommand,
    ListAllowedTransitionsUseCase,
)
from okto_pulse.core.application.use_cases.base import ActorContext
from okto_pulse.core.domain.enums import CardStatus, CardType, SpecStatus
from okto_pulse.core.domain.realm import LOCAL_REALM_ID
from okto_pulse.core.models.schemas import CardMove
from okto_pulse.core.runtime_registry import resolve_unit_of_work_factory
from okto_pulse.core.services.gate_contracts import (
    GateContractError,
    operational_flow_for_test_card,
)
from okto_pulse.core.services.main import CardOperationError, CardService
from sqlalchemy_test_models import Board, Card, Spec


USER_ID = "gate-remediation-regression-user"


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


async def _persist(db_factory, *rows: object) -> None:
    async with db_factory() as db:
        db.add_all(list(rows))
        await db.commit()


async def _preview(db, *, board_id: str, card_id: str, target: str):
    result = await ListAllowedTransitionsUseCase().execute(
        ListAllowedTransitionsCommand(
            board_id,
            "card",
            entity_id=card_id,
        ),
        actor=ActorContext(
            USER_ID,
            "test",
            board_id=board_id,
            realm_id=LOCAL_REALM_ID,
        ),
        uow=resolve_unit_of_work_factory().wrap(db),
    )
    return next(
        transition
        for transition in result.read_model.allowed_transitions
        if transition.to_status == target
    )


@pytest.mark.asyncio
async def test_missing_regression_task_projects_real_positive_eligibility_count(
    db_factory,
) -> None:
    board_id = _id("bug-path-a-board")
    spec_id = _id("bug-path-a-spec")
    origin_id = _id("bug-path-a-origin")
    bug_id = _id("bug-path-a")
    scenario_id = "ts_path_a_eligible"
    await _persist(
        db_factory,
        Board(
            id=board_id,
            name="Bug Path A",
            owner_id=USER_ID,
            realm_id=LOCAL_REALM_ID,
            settings={"require_test_task_for_bug": True},
        ),
        Spec(
            id=spec_id,
            board_id=board_id,
            title="Bug Path A spec",
            status=SpecStatus.IN_PROGRESS,
            test_scenarios=[
                {
                    "id": scenario_id,
                    "title": "Eligible regression",
                    "status": "ready",
                    "linked_task_ids": [origin_id],
                }
            ],
            created_by=USER_ID,
        ),
        Card(
            id=origin_id,
            board_id=board_id,
            spec_id=spec_id,
            title="Origin",
            status=CardStatus.DONE,
            card_type=CardType.NORMAL,
            position=0,
            created_by=USER_ID,
        ),
        Card(
            id=bug_id,
            board_id=board_id,
            spec_id=spec_id,
            title="Bug",
            status=CardStatus.NOT_STARTED,
            card_type=CardType.BUG,
            origin_task_id=origin_id,
            linked_test_task_ids=[],
            position=1,
            created_by=USER_ID,
        ),
    )

    async with db_factory() as db:
        preview = await _preview(
            db,
            board_id=board_id,
            card_id=bug_id,
            target=CardStatus.IN_PROGRESS.value,
        )
        with pytest.raises(CardOperationError) as exc_info:
            await CardService(db).move_card(
                bug_id,
                USER_ID,
                CardMove(status=CardStatus.IN_PROGRESS),
            )

    assert preview.blocked_reason is not None
    assert "1 eligible existing scenario" in preview.blocked_reason
    payload = exc_info.value.to_dict()
    assert payload["eligible_scenarios_count"] == 1
    assert payload["remediation_path"] == "path_a_reuse_existing_scenario"
    assert payload["next_action"] == "create_regression_test_card"
    assert payload["semantic_gap_required"] is False


@pytest.mark.asyncio
async def test_missing_regression_task_with_zero_eligibility_routes_path_b(
    db_factory,
) -> None:
    board_id = _id("bug-path-b-board")
    spec_id = _id("bug-path-b-spec")
    origin_id = _id("bug-path-b-origin")
    bug_id = _id("bug-path-b")
    await _persist(
        db_factory,
        Board(
            id=board_id,
            name="Bug Path B",
            owner_id=USER_ID,
            realm_id=LOCAL_REALM_ID,
            settings={"require_test_task_for_bug": True},
        ),
        Spec(
            id=spec_id,
            board_id=board_id,
            title="Bug Path B spec",
            status=SpecStatus.IN_PROGRESS,
            test_scenarios=[
                {
                    "id": "ts_unrelated",
                    "title": "Unrelated regression",
                    "status": "ready",
                    "linked_task_ids": [],
                }
            ],
            created_by=USER_ID,
        ),
        Card(
            id=origin_id,
            board_id=board_id,
            spec_id=spec_id,
            title="Origin",
            status=CardStatus.DONE,
            card_type=CardType.NORMAL,
            position=0,
            created_by=USER_ID,
        ),
        Card(
            id=bug_id,
            board_id=board_id,
            spec_id=spec_id,
            title="Bug",
            status=CardStatus.NOT_STARTED,
            card_type=CardType.BUG,
            origin_task_id=origin_id,
            linked_test_task_ids=[],
            position=1,
            created_by=USER_ID,
        ),
    )

    async with db_factory() as db:
        preview = await _preview(
            db,
            board_id=board_id,
            card_id=bug_id,
            target=CardStatus.IN_PROGRESS.value,
        )
        with pytest.raises(CardOperationError) as exc_info:
            await CardService(db).move_card(
                bug_id,
                USER_ID,
                CardMove(status=CardStatus.IN_PROGRESS),
            )

    assert preview.blocked_reason is not None
    assert "zero eligible existing scenarios" in preview.blocked_reason
    assert "Path B" in preview.blocked_reason
    payload = exc_info.value.to_dict()
    assert payload["eligible_scenarios_count"] == 0
    assert payload["remediation_path"] == "path_b_semantic_gap"
    assert payload["next_action"] == "escalate_semantic_gap"
    assert payload["semantic_gap_required"] is True
    assert "Path A" not in payload["remediation_message"]["detail"]


@pytest.mark.parametrize("scenario_ids", [[], ["ts_dangling"]])
@pytest.mark.asyncio
async def test_test_card_done_gate_fails_closed_for_missing_scenario_linkage(
    db_factory,
    scenario_ids: list[str],
) -> None:
    board_id = _id("test-card-board")
    spec_id = _id("test-card-spec")
    card_id = _id("test-card")
    await _persist(
        db_factory,
        Board(
            id=board_id,
            name="Test-card gate",
            owner_id=USER_ID,
            realm_id=LOCAL_REALM_ID,
            settings={
                "require_task_validation": False,
                "skip_test_coverage_global": False,
            },
        ),
        Spec(
            id=spec_id,
            board_id=board_id,
            title="Test-card spec",
            status=SpecStatus.IN_PROGRESS,
            test_scenarios=[],
            created_by=USER_ID,
        ),
        Card(
            id=card_id,
            board_id=board_id,
            spec_id=spec_id,
            title="Test card",
            status=CardStatus.IN_PROGRESS,
            card_type=CardType.TEST,
            test_scenario_ids=scenario_ids,
            position=0,
            created_by=USER_ID,
        ),
    )

    async with db_factory() as db:
        preview = await _preview(
            db,
            board_id=board_id,
            card_id=card_id,
            target=CardStatus.DONE.value,
        )
        with pytest.raises(GateContractError) as exc_info:
            await CardService(db).move_card(
                card_id,
                USER_ID,
                CardMove(status=CardStatus.DONE),
            )

    assert preview.blocked_reason is not None
    assert "test_scenarios_pending" in preview.blocked_reason
    assert "missing" in preview.blocked_reason
    payload = exc_info.value.to_dict()
    assert payload["code"] == "test_card_completion_blocked"
    assert payload["details"]["would_block_done"] is True
    assert payload["details"]["required_tool"] == "okto_pulse_link_task"
    assert all(
        item["status"] == "missing"
        for item in payload["details"]["pending_scenarios"]
    )

    async with db_factory() as db:
        persisted = await db.get(Card, card_id)
        assert persisted.status == CardStatus.IN_PROGRESS


def test_operational_flow_never_claims_empty_linkage_passed() -> None:
    flow = operational_flow_for_test_card(
        card_id="test-card",
        board_id="board",
        spec_id="spec",
        current_status=CardStatus.IN_PROGRESS.value,
        linked_scenarios=[],
    )

    assert flow["would_block_done"] is True
    assert flow["pending_scenarios"][0]["status"] == "missing"
    assert flow["required_tool"] == "okto_pulse_link_task"
    assert flow["next_action"]["tool"] == "okto_pulse_link_task"
    assert "All linked scenarios" not in flow["operator_action"]


def test_operational_flow_fails_closed_for_partially_unresolved_linkage() -> None:
    flow = operational_flow_for_test_card(
        card_id="test-card",
        board_id="board",
        spec_id="spec",
        current_status=CardStatus.IN_PROGRESS.value,
        linked_scenarios=[
            {
                "id": "ts_passed",
                "title": "Resolved scenario",
                "status": "passed",
                "evidence": {
                    "last_run_at": "2026-07-25T00:00:00+00:00",
                    "output_snippet": "passed",
                },
            }
        ],
        expected_scenario_ids=["ts_passed", "ts_dangling"],
    )

    assert flow["would_block_done"] is True
    assert any(
        item["id"] == "ts_dangling" and item["status"] == "missing"
        for item in flow["pending_scenarios"]
    )
    assert flow["required_tool"] == "okto_pulse_link_task"
    assert flow["next_action"]["tool"] == "okto_pulse_link_task"
    assert "move the test card to done" not in flow["operator_action"].lower()
