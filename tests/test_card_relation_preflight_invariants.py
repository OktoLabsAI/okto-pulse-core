"""Card relationship writes fail before FK flushes or partial mutation."""

from __future__ import annotations

import json
import uuid
from unittest.mock import AsyncMock, patch

import pytest
from mcp_runtime_testing import register_mcp_test_runtime
from sqlalchemy import func, select, text

from okto_pulse.core.application.use_cases import UpdateCardCommand, UpdateCardUseCase
from okto_pulse.core.application.use_cases.base import ActorContext
from okto_pulse.core.mcp import server as mcp_server
from okto_pulse.core.models.schemas import CardUpdate
from okto_pulse.core.services.main import CardService
from sqlalchemy_test_models import (
    ActivityLog,
    Board,
    Card,
    CardDependency,
    Spec,
    SpecStatus,
    Sprint,
)
from sqlalchemy_test_unit_of_work import SQLAlchemyUnitOfWorkFactory


USER_ID = "card-relation-preflight-agent"


@pytest.fixture
async def _relation_graph():
    from okto_pulse.core.infra.database import get_session_factory

    token = uuid.uuid4().hex[:8]
    ids = {
        "board_a": f"rel-board-a-{token}",
        "board_b": f"rel-board-b-{token}",
        "spec_a1": f"rel-spec-a1-{token}",
        "spec_a2": f"rel-spec-a2-{token}",
        "spec_b": f"rel-spec-b-{token}",
        "sprint_a1": f"rel-sprint-a1-{token}",
        "sprint_a2": f"rel-sprint-a2-{token}",
        "sprint_b": f"rel-sprint-b-{token}",
        "update_card": f"rel-update-{token}",
        "dep_card": f"rel-dep-card-{token}",
        "dep_target": f"rel-dep-target-{token}",
        "foreign_card": f"rel-foreign-card-{token}",
    }
    factory = get_session_factory()

    async with factory() as db:
        assert await db.scalar(text("PRAGMA foreign_keys")) == 1
        db.add_all(
            [
                Board(id=ids["board_a"], name="Relation Board A", owner_id=USER_ID),
                Board(id=ids["board_b"], name="Relation Board B", owner_id=USER_ID),
            ]
        )
        await db.flush()
        db.add_all(
            [
                Spec(
                    id=ids["spec_a1"],
                    board_id=ids["board_a"],
                    title="Relation Spec A1",
                    status=SpecStatus.APPROVED,
                    created_by=USER_ID,
                    functional_requirements=[],
                    acceptance_criteria=[],
                    test_scenarios=[],
                    business_rules=[],
                    api_contracts=[],
                ),
                Spec(
                    id=ids["spec_a2"],
                    board_id=ids["board_a"],
                    title="Relation Spec A2",
                    status=SpecStatus.APPROVED,
                    created_by=USER_ID,
                    functional_requirements=[],
                    acceptance_criteria=[],
                    test_scenarios=[],
                    business_rules=[],
                    api_contracts=[],
                ),
                Spec(
                    id=ids["spec_b"],
                    board_id=ids["board_b"],
                    title="Relation Spec B",
                    status=SpecStatus.APPROVED,
                    created_by=USER_ID,
                    functional_requirements=[],
                    acceptance_criteria=[],
                    test_scenarios=[],
                    business_rules=[],
                    api_contracts=[],
                ),
            ]
        )
        await db.flush()
        db.add_all(
            [
                Sprint(
                    id=ids["sprint_a1"],
                    board_id=ids["board_a"],
                    spec_id=ids["spec_a1"],
                    title="Relation Sprint A1",
                    created_by=USER_ID,
                ),
                Sprint(
                    id=ids["sprint_a2"],
                    board_id=ids["board_a"],
                    spec_id=ids["spec_a2"],
                    title="Relation Sprint A2",
                    created_by=USER_ID,
                ),
                Sprint(
                    id=ids["sprint_b"],
                    board_id=ids["board_b"],
                    spec_id=ids["spec_b"],
                    title="Relation Sprint B",
                    created_by=USER_ID,
                ),
            ]
        )
        await db.flush()
        db.add_all(
            [
                Card(
                    id=ids["update_card"],
                    board_id=ids["board_a"],
                    spec_id=ids["spec_a1"],
                    sprint_id=ids["sprint_a1"],
                    title="Update sentinel",
                    created_by=USER_ID,
                ),
                Card(
                    id=ids["dep_card"],
                    board_id=ids["board_a"],
                    spec_id=ids["spec_a1"],
                    title="Dependency source",
                    created_by=USER_ID,
                ),
                Card(
                    id=ids["dep_target"],
                    board_id=ids["board_a"],
                    spec_id=ids["spec_a1"],
                    title="Dependency target",
                    created_by=USER_ID,
                ),
                Card(
                    id=ids["foreign_card"],
                    board_id=ids["board_b"],
                    spec_id=ids["spec_b"],
                    title="Foreign dependency target",
                    created_by=USER_ID,
                ),
            ]
        )
        await db.commit()

    return {"factory": factory, **ids}


def _stub_ctx():
    return type(
        "Ctx",
        (),
        {
            "agent_id": USER_ID,
            "agent_name": "card-relation-test",
            "permissions": ["*"],
        },
    )()


async def _call_add_dependency(
    graph: dict, *, card_id: str, depends_on_id: str
) -> dict:
    register_mcp_test_runtime(graph["factory"])
    tool = await mcp_server.mcp.get_tool("okto_pulse_add_card_dependency")
    with (
        patch.object(mcp_server, "_get_agent_ctx", AsyncMock(return_value=_stub_ctx())),
        patch.object(mcp_server, "check_permission", return_value=None),
    ):
        return json.loads(
            await tool.fn(
                board_id=graph["board_a"],
                card_id=card_id,
                depends_on_id=depends_on_id,
            )
        )


async def _edge_count(graph: dict, card_id: str, depends_on_id: str) -> int:
    async with graph["factory"]() as db:
        count = await db.scalar(
            select(func.count())
            .select_from(CardDependency)
            .where(
                CardDependency.card_id == card_id,
                CardDependency.depends_on_id == depends_on_id,
            )
        )
    return int(count or 0)


@pytest.mark.asyncio
@pytest.mark.parametrize("missing_side", ["card", "depends_on"])
async def test_mcp_dependency_missing_endpoint_returns_governed_error_and_zero_edge(
    _relation_graph, missing_side: str
) -> None:
    missing = f"missing-{uuid.uuid4().hex}"
    card_id = missing if missing_side == "card" else _relation_graph["dep_card"]
    depends_on_id = (
        missing if missing_side == "depends_on" else _relation_graph["dep_target"]
    )

    payload = await _call_add_dependency(
        _relation_graph,
        card_id=card_id,
        depends_on_id=depends_on_id,
    )

    assert payload == {"error": "Card not found"}
    assert await _edge_count(_relation_graph, card_id, depends_on_id) == 0


@pytest.mark.asyncio
async def test_mcp_dependency_cross_board_returns_not_found_and_zero_edge(
    _relation_graph,
) -> None:
    payload = await _call_add_dependency(
        _relation_graph,
        card_id=_relation_graph["dep_card"],
        depends_on_id=_relation_graph["foreign_card"],
    )

    assert payload == {"error": "Card not found"}
    assert (
        await _edge_count(
            _relation_graph,
            _relation_graph["dep_card"],
            _relation_graph["foreign_card"],
        )
        == 0
    )


@pytest.mark.asyncio
async def test_mcp_dependency_duplicate_is_conflict_and_keeps_single_edge(
    _relation_graph,
) -> None:
    kwargs = {
        "card_id": _relation_graph["dep_card"],
        "depends_on_id": _relation_graph["dep_target"],
    }
    created = await _call_add_dependency(_relation_graph, **kwargs)
    duplicate = await _call_add_dependency(_relation_graph, **kwargs)

    assert created["success"] is True
    assert "existe" in duplicate.get("error", "").lower()
    assert await _edge_count(_relation_graph, **kwargs) == 1


async def _run_update(graph: dict, data: CardUpdate):
    actor = ActorContext(USER_ID, "mcp", board_id=graph["board_a"])
    uow_factory = SQLAlchemyUnitOfWorkFactory(graph["factory"])
    async with uow_factory(actor=actor) as uow:
        return await UpdateCardUseCase().execute(
            UpdateCardCommand(graph["update_card"], data),
            actor=actor,
            uow=uow,
        )


async def _stored_update_card(graph: dict) -> Card:
    async with graph["factory"]() as db:
        return await db.get(Card, graph["update_card"])


async def _update_activity_count(graph: dict) -> int:
    async with graph["factory"]() as db:
        count = await db.scalar(
            select(func.count())
            .select_from(ActivityLog)
            .where(ActivityLog.card_id == graph["update_card"])
        )
    return int(count or 0)


@pytest.mark.asyncio
async def test_service_update_rejects_relation_before_in_memory_mutation(
    _relation_graph,
) -> None:
    async with _relation_graph["factory"]() as db:
        with pytest.raises(ValueError, match="Spec not found on this board"):
            await CardService(db).update_card(
                _relation_graph["update_card"],
                USER_ID,
                CardUpdate(
                    title="must not reach setattr",
                    spec_id=f"missing-{uuid.uuid4().hex}",
                ),
            )

        stored_in_same_session = await db.get(Card, _relation_graph["update_card"])
        assert stored_in_same_session.title == "Update sentinel"
        assert stored_in_same_session.spec_id == _relation_graph["spec_a1"]
        assert stored_in_same_session.sprint_id == _relation_graph["sprint_a1"]

    assert await _update_activity_count(_relation_graph) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("data_factory", "message"),
    [
        (
            lambda graph: CardUpdate(
                title="must not persist", spec_id=f"missing-{uuid.uuid4().hex}"
            ),
            "Spec not found on this board",
        ),
        (
            lambda graph: CardUpdate(title="must not persist", spec_id=graph["spec_b"]),
            "Spec not found on this board",
        ),
        (
            lambda graph: CardUpdate(
                title="must not persist", sprint_id=f"missing-{uuid.uuid4().hex}"
            ),
            "Sprint not found on this board",
        ),
        (
            lambda graph: CardUpdate(
                title="must not persist", sprint_id=graph["sprint_b"]
            ),
            "Sprint not found on this board",
        ),
        (
            lambda graph: CardUpdate(
                title="must not persist", sprint_id=graph["sprint_a2"]
            ),
            "Sprint must belong to the card's resulting spec",
        ),
        (
            lambda graph: CardUpdate(
                title="must not persist", spec_id=graph["spec_a2"]
            ),
            "Sprint must belong to the card's resulting spec",
        ),
    ],
    ids=[
        "missing-spec",
        "cross-board-spec",
        "missing-sprint",
        "cross-board-sprint",
        "sprint-mismatches-current-spec",
        "spec-mismatches-current-sprint",
    ],
)
async def test_update_relation_preflight_rejects_without_partial_card_change(
    _relation_graph, data_factory, message: str
) -> None:
    with pytest.raises(ValueError, match=message.replace("'", "\\'")):
        await _run_update(_relation_graph, data_factory(_relation_graph))

    stored = await _stored_update_card(_relation_graph)
    assert stored.title == "Update sentinel"
    assert stored.spec_id == _relation_graph["spec_a1"]
    assert stored.sprint_id == _relation_graph["sprint_a1"]
    assert await _update_activity_count(_relation_graph) == 0


@pytest.mark.asyncio
async def test_update_accepts_coherent_same_board_spec_and_sprint(
    _relation_graph,
) -> None:
    result = await _run_update(
        _relation_graph,
        CardUpdate(
            title="Coherent update",
            spec_id=_relation_graph["spec_a2"],
            sprint_id=_relation_graph["sprint_a2"],
        ),
    )

    assert result.card.spec_id == _relation_graph["spec_a2"]
    assert result.card.sprint_id == _relation_graph["sprint_a2"]
    stored = await _stored_update_card(_relation_graph)
    assert stored.title == "Coherent update"
    assert stored.spec_id == _relation_graph["spec_a2"]
    assert stored.sprint_id == _relation_graph["sprint_a2"]


@pytest.mark.asyncio
async def test_update_can_clear_spec_only_when_resulting_sprint_is_also_clear(
    _relation_graph,
) -> None:
    result = await _run_update(
        _relation_graph,
        CardUpdate(spec_id=None, sprint_id=None),
    )

    assert result.card.spec_id is None
    assert result.card.sprint_id is None
    stored = await _stored_update_card(_relation_graph)
    assert stored.spec_id is None
    assert stored.sprint_id is None
