"""Create-card origin lineage is governed before the card FK is flushed."""

from __future__ import annotations

import json
import uuid
from unittest.mock import AsyncMock, patch

import pytest
from mcp_runtime_testing import register_mcp_test_runtime
from sqlalchemy import func, select

from sqlalchemy_test_models import Board, Card, CardType, Spec, SpecStatus
from okto_pulse.core.mcp import server as mcp_server
from okto_pulse.core.models.schemas import CardCreate
from okto_pulse.core.services.main import CardService


USER_ID = "card-origin-invariant-agent"


@pytest.fixture
async def _origin_graph():
    from okto_pulse.core.infra.database import get_session_factory

    token = uuid.uuid4().hex[:10]
    board_a = f"card-origin-a-{token}"
    board_b = f"card-origin-b-{token}"
    spec_a = f"card-origin-spec-a-{token}"
    spec_b = f"card-origin-spec-b-{token}"
    origin_a = f"card-origin-task-a-{token}"
    origin_b = f"card-origin-task-b-{token}"
    scenario_id = f"ts-card-origin-{token}"
    db_factory = get_session_factory()

    async with db_factory() as db:
        db.add_all(
            [
                Board(id=board_a, name="Origin Board A", owner_id=USER_ID),
                Board(id=board_b, name="Origin Board B", owner_id=USER_ID),
            ]
        )
        await db.flush()
        db.add_all(
            [
                Spec(
                    id=spec_a,
                    board_id=board_a,
                    title="Origin Spec A",
                    status=SpecStatus.APPROVED,
                    created_by=USER_ID,
                    functional_requirements=["FR1"],
                    acceptance_criteria=["AC1"],
                    test_scenarios=[
                        {
                            "id": scenario_id,
                            "title": "Origin scenario",
                            "status": "draft",
                            "linked_task_ids": [],
                        }
                    ],
                    business_rules=[],
                    api_contracts=[],
                ),
                Spec(
                    id=spec_b,
                    board_id=board_b,
                    title="Origin Spec B",
                    status=SpecStatus.APPROVED,
                    created_by=USER_ID,
                    functional_requirements=["FR1"],
                    acceptance_criteria=["AC1"],
                    test_scenarios=[],
                    business_rules=[],
                    api_contracts=[],
                ),
            ]
        )
        await db.flush()
        db.add_all(
            [
                Card(
                    id=origin_a,
                    board_id=board_a,
                    spec_id=spec_a,
                    title="Origin task A",
                    created_by=USER_ID,
                    card_type=CardType.NORMAL,
                ),
                Card(
                    id=origin_b,
                    board_id=board_b,
                    spec_id=spec_b,
                    title="Origin task B",
                    created_by=USER_ID,
                    card_type=CardType.NORMAL,
                ),
            ]
        )
        await db.commit()

    return {
        "db_factory": db_factory,
        "board_a": board_a,
        "board_b": board_b,
        "spec_a": spec_a,
        "spec_b": spec_b,
        "origin_a": origin_a,
        "origin_b": origin_b,
        "scenario_id": scenario_id,
    }


def _card_create(
    graph: dict,
    *,
    title: str,
    card_type: str,
    origin_task_id: str | None,
) -> CardCreate:
    kwargs = {
        "title": title,
        "spec_id": graph["spec_a"],
        "card_type": card_type,
        "origin_task_id": origin_task_id,
    }
    if card_type == "test":
        kwargs["test_scenario_ids"] = [graph["scenario_id"]]
    if card_type == "bug":
        kwargs.update(
            severity="major",
            expected_behavior="Expected behavior",
            observed_behavior="Observed behavior",
        )
    return CardCreate(**kwargs)


async def _count_cards_with_title(graph: dict, title: str) -> int:
    async with graph["db_factory"]() as db:
        count = await db.scalar(
            select(func.count()).select_from(Card).where(Card.title == title)
        )
    return int(count or 0)


@pytest.mark.asyncio
@pytest.mark.parametrize("card_type", ["normal", "test"])
async def test_service_rejects_origin_for_non_bug_before_flush(
    _origin_graph, card_type: str
) -> None:
    title = f"service-{card_type}-with-origin-{uuid.uuid4().hex[:8]}"
    async with _origin_graph["db_factory"]() as db:
        with pytest.raises(ValueError, match="only allowed for bug cards"):
            await CardService(db).create_card(
                _origin_graph["board_a"],
                USER_ID,
                _card_create(
                    _origin_graph,
                    title=title,
                    card_type=card_type,
                    origin_task_id=_origin_graph["origin_a"],
                ),
            )
    assert await _count_cards_with_title(_origin_graph, title) == 0


@pytest.mark.asyncio
async def test_service_rejects_missing_bug_origin_before_flush(_origin_graph) -> None:
    title = f"service-bug-missing-origin-{uuid.uuid4().hex[:8]}"
    async with _origin_graph["db_factory"]() as db:
        with pytest.raises(ValueError, match="Origin task not found on this board"):
            await CardService(db).create_card(
                _origin_graph["board_a"],
                USER_ID,
                _card_create(
                    _origin_graph,
                    title=title,
                    card_type="bug",
                    origin_task_id=f"missing-{uuid.uuid4().hex}",
                ),
            )
    assert await _count_cards_with_title(_origin_graph, title) == 0


@pytest.mark.asyncio
async def test_service_rejects_cross_board_bug_origin_before_flush(
    _origin_graph,
) -> None:
    title = f"service-bug-cross-board-{uuid.uuid4().hex[:8]}"
    async with _origin_graph["db_factory"]() as db:
        with pytest.raises(ValueError, match="Origin task not found on this board"):
            await CardService(db).create_card(
                _origin_graph["board_a"],
                USER_ID,
                _card_create(
                    _origin_graph,
                    title=title,
                    card_type="bug",
                    origin_task_id=_origin_graph["origin_b"],
                ),
            )
    assert await _count_cards_with_title(_origin_graph, title) == 0


@pytest.mark.asyncio
async def test_service_accepts_same_board_bug_origin(_origin_graph) -> None:
    title = f"service-valid-bug-origin-{uuid.uuid4().hex[:8]}"
    async with _origin_graph["db_factory"]() as db:
        card = await CardService(db).create_card(
            _origin_graph["board_a"],
            USER_ID,
            _card_create(
                _origin_graph,
                title=title,
                card_type="bug",
                origin_task_id=_origin_graph["origin_a"],
            ),
        )
        assert card is not None
        assert card.board_id == _origin_graph["board_a"]
        assert card.spec_id == _origin_graph["spec_a"]
        assert card.origin_task_id == _origin_graph["origin_a"]


def _stub_ctx():
    return type(
        "Ctx",
        (),
        {
            "agent_id": USER_ID,
            "agent_name": "card-origin-test",
            "permissions": ["card.entity.create", "card.entity.create_test"],
        },
    )()


async def _call_create_card(**kwargs) -> dict:
    from okto_pulse.core.infra.database import get_session_factory

    register_mcp_test_runtime(get_session_factory())
    tool = await mcp_server.mcp.get_tool("okto_pulse_create_card")
    return json.loads(await tool.fn(**kwargs))


async def _call_create_card_as_agent(**kwargs) -> dict:
    with (
        patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx())),
        patch.object(mcp_server, "check_permission", return_value=None),
    ):
        return await _call_create_card(**kwargs)


@pytest.mark.asyncio
@pytest.mark.parametrize("card_type", ["normal", "test"])
async def test_mcp_rejects_origin_for_non_bug_and_persists_zero(
    _origin_graph, card_type: str
) -> None:
    title = f"mcp-{card_type}-with-origin-{uuid.uuid4().hex[:8]}"
    kwargs = {
        "board_id": _origin_graph["board_a"],
        "title": title,
        "spec_id": _origin_graph["spec_a"],
        "card_type": card_type,
        "origin_task_id": _origin_graph["origin_a"],
    }
    if card_type == "test":
        kwargs["test_scenario_ids"] = [_origin_graph["scenario_id"]]

    payload = await _call_create_card_as_agent(**kwargs)

    assert payload == {"error": "origin_task_id is only allowed for bug cards"}
    assert await _count_cards_with_title(_origin_graph, title) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("origin_key", ["missing", "origin_b"])
async def test_mcp_rejects_unresolvable_bug_origin_and_persists_zero(
    _origin_graph, origin_key: str
) -> None:
    title = f"mcp-bug-{origin_key}-{uuid.uuid4().hex[:8]}"
    origin_id = (
        f"missing-{uuid.uuid4().hex}"
        if origin_key == "missing"
        else _origin_graph[origin_key]
    )

    payload = await _call_create_card_as_agent(
        board_id=_origin_graph["board_a"],
        title=title,
        spec_id=_origin_graph["spec_a"],
        card_type="bug",
        origin_task_id=origin_id,
        severity="major",
        expected_behavior="Expected behavior",
        observed_behavior="Observed behavior",
    )

    assert payload == {"error": "Origin task not found on this board"}
    assert await _count_cards_with_title(_origin_graph, title) == 0


@pytest.mark.asyncio
async def test_mcp_accepts_and_persists_same_board_bug_origin(_origin_graph) -> None:
    title = f"mcp-valid-bug-origin-{uuid.uuid4().hex[:8]}"

    payload = await _call_create_card_as_agent(
        board_id=_origin_graph["board_a"],
        title=title,
        spec_id=_origin_graph["spec_a"],
        card_type="bug",
        origin_task_id=_origin_graph["origin_a"],
        severity="major",
        expected_behavior="Expected behavior",
        observed_behavior="Observed behavior",
    )

    assert payload["success"] is True
    assert payload["card"]["origin_task_id"] == _origin_graph["origin_a"]
    assert payload["card"]["spec_id"] == _origin_graph["spec_a"]
    assert await _count_cards_with_title(_origin_graph, title) == 1
