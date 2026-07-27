"""Spec R01A REST-FU3c-S2 — spec card / scenario linking on the UnitOfWork.

The five linking endpoints now route through transport-free use cases +
``get_unit_of_work``; each adapter only maps the result/errors back to HTTP:

  link_card_to_spec            -> LinkCardToSpecUseCase
  unlink_card_from_spec        -> UnlinkCardFromSpecUseCase
  link_task_to_scenario        -> LinkTaskToScenarioUseCase
  unlink_task_from_scenario    -> UnlinkTaskFromScenarioUseCase
  update_test_scenario_status  -> SetTestScenarioStatusUseCase

Oracles: every endpoint's happy path (200, bidirectional state verified) AND its
legacy 404 details (spec/card/scenario, each distinct), the scenario-status
404 (``scenario_not_found``) and 422 (invalid status) branches, a use-case-level
``EntityNotFoundError`` for a missing spec, and an AST signature check proving
the endpoints take ``uow`` (not a raw ``AsyncSession``).
"""

from __future__ import annotations

import inspect
from types import SimpleNamespace
from unittest.mock import AsyncMock
import uuid

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import func, select

from okto_pulse.community.api import specs as specs_api
from okto_pulse.community.api.specs import router as specs_router
from okto_pulse.community.api.deps import get_unit_of_work
from okto_pulse.community.api.auth_deps import require_user
from okto_pulse.core.infra.database import get_db, get_session_factory
from sqlalchemy_test_models import Board, Card, Spec, SpecStatus

USER = "r01a-fu3c-s2-user"
OTHER = "r01a-fu3c-s2-other"
PREFIX = "/api/v1"
_ENDPOINTS = (
    "link_card_to_spec",
    "unlink_card_from_spec",
    "link_task_to_scenario",
    "unlink_task_from_scenario",
    "update_test_scenario_status",
)


@pytest.fixture
def client():
    app = FastAPI()
    app.include_router(specs_router, prefix=PREFIX)
    session_factory = get_session_factory()

    async def _override_db():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[require_user] = lambda: USER
    return TestClient(app)


async def _seed(
    *,
    spec_status: SpecStatus = SpecStatus.APPROVED,
    scenarios: list[dict] | None = None,
    card_spec_id_to_self: bool = False,
    card_scenarios: list[str] | None = None,
    owner: str = USER,
) -> tuple[str, str, str]:
    """Seed a board + spec + card and return ``(board_id, spec_id, card_id)``.

    ``card_spec_id_to_self`` links the card to the seeded spec; the board carries
    ``skip_test_evidence_global`` so the scenario-status happy path clears the
    NC-9 evidence gate deterministically.
    """
    bid = f"board-fu3cs2-{uuid.uuid4().hex[:8]}"
    sid = f"spec-fu3cs2-{uuid.uuid4().hex[:8]}"
    cid = f"card-fu3cs2-{uuid.uuid4().hex[:8]}"
    async with get_session_factory()() as db:
        db.add(
            Board(
                id=bid,
                name="fu3cs2",
                owner_id=owner,
                settings={"skip_test_evidence_global": True},
            )
        )
        db.add(
            Spec(
                id=sid,
                board_id=bid,
                title="fu3cs2-spec",
                status=spec_status,
                created_by=owner,
                functional_requirements=[],
                acceptance_criteria=[],
                test_scenarios=scenarios or [],
                business_rules=[],
                api_contracts=[],
            )
        )
        db.add(
            Card(
                id=cid,
                board_id=bid,
                spec_id=sid if card_spec_id_to_self else None,
                title="fu3cs2-card",
                created_by=owner,
                test_scenario_ids=card_scenarios or [],
            )
        )
        await db.commit()
    return bid, sid, cid


async def _get_spec(spec_id: str) -> Spec | None:
    async with get_session_factory()() as db:
        return await db.get(Spec, spec_id)


async def _get_card(card_id: str) -> Card | None:
    async with get_session_factory()() as db:
        return await db.get(Card, card_id)


async def _card_link_state(card_id: str) -> dict:
    from sqlalchemy_test_models import ActivityLog

    async with get_session_factory()() as db:
        card = await db.get(Card, card_id)
        activity_count = await db.scalar(
            select(func.count())
            .select_from(ActivityLog)
            .where(ActivityLog.card_id == card_id)
        )
        return {"spec_id": card.spec_id, "activity_count": activity_count}


def _missing() -> str:
    return f"missing-{uuid.uuid4().hex[:8]}"


# --- link / unlink card -----------------------------------------------------


@pytest.mark.asyncio
async def test_link_card_to_spec_200(client) -> None:
    _, sid, cid = await _seed(card_spec_id_to_self=False)
    resp = client.post(f"{PREFIX}/specs/{sid}/link-card/{cid}")
    assert resp.status_code == 200, resp.text
    assert resp.json() == {"success": True, "spec_id": sid, "card_id": cid}
    card = await _get_card(cid)
    assert card.spec_id == sid


@pytest.mark.asyncio
async def test_link_card_to_spec_404(client) -> None:
    resp = client.post(f"{PREFIX}/specs/{_missing()}/link-card/{_missing()}")
    assert resp.status_code == 404
    assert (
        resp.json()["detail"]
        == "Spec or card not found, or they belong to different boards"
    )


@pytest.mark.asyncio
async def test_unlink_card_from_spec_200(client) -> None:
    _, sid, cid = await _seed(card_spec_id_to_self=True)
    resp = client.post(f"{PREFIX}/specs/{sid}/unlink-card/{cid}")
    assert resp.status_code == 200, resp.text
    assert resp.json() == {"success": True, "spec_id": sid, "card_id": cid}
    card = await _get_card(cid)
    assert card.spec_id is None


@pytest.mark.asyncio
async def test_unlink_card_not_linked_404(client) -> None:
    # Card exists but is not linked to any spec -> unlink_card returns False.
    _, sid, cid = await _seed(card_spec_id_to_self=False)
    resp = client.post(f"{PREFIX}/specs/{sid}/unlink-card/{cid}")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Card not found or not linked to any spec"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operation", "initially_linked", "expected_detail"),
    (
        (
            "link-card",
            False,
            "Spec or card not found, or they belong to different boards",
        ),
        ("unlink-card", True, "Card not found or not linked to any spec"),
    ),
)
async def test_card_spec_relation_foreign_owner_fails_closed_without_audit(
    client, operation: str, initially_linked: bool, expected_detail: str
) -> None:
    _, sid, cid = await _seed(
        card_spec_id_to_self=initially_linked,
        owner=OTHER,
    )
    before = await _card_link_state(cid)

    response = client.post(f"{PREFIX}/specs/{sid}/{operation}/{cid}")

    assert response.status_code == 404, response.text
    assert response.json()["detail"] == expected_detail
    assert await _card_link_state(cid) == before


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operation", "card_linked_to_own_spec", "expected_detail"),
    (
        (
            "link-card",
            False,
            "Spec or card not found, or they belong to different boards",
        ),
        ("unlink-card", True, "Card not found or not linked to any spec"),
    ),
)
async def test_card_spec_relation_cross_board_fails_closed_without_audit(
    client, operation: str, card_linked_to_own_spec: bool, expected_detail: str
) -> None:
    _, target_spec_id, _ = await _seed()
    _, _, foreign_card_id = await _seed(
        card_spec_id_to_self=card_linked_to_own_spec
    )
    before = await _card_link_state(foreign_card_id)

    response = client.post(
        f"{PREFIX}/specs/{target_spec_id}/{operation}/{foreign_card_id}"
    )

    assert response.status_code == 404, response.text
    assert response.json()["detail"] == expected_detail
    assert await _card_link_state(foreign_card_id) == before


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operation", "spec_board", "card_board", "card_spec_id", "expected_entity"),
    (
        ("link", "foreign-board", "actor-board", None, "spec"),
        ("link", "actor-board", "foreign-board", None, "card"),
        ("link", "actor-board", None, None, "card"),
        ("unlink", "foreign-board", "actor-board", "spec-1", "spec"),
        ("unlink", "actor-board", "foreign-board", "spec-1", "card"),
        ("unlink", "actor-board", None, None, "card"),
        ("unlink", "actor-board", "actor-board", "other-spec", "card"),
    ),
)
async def test_card_spec_relation_preflight_blocks_service_mutation(
    operation: str,
    spec_board: str,
    card_board: str | None,
    card_spec_id: str | None,
    expected_entity: str,
) -> None:
    from okto_pulse.core.application.use_cases import (
        LinkCardToSpecCommand,
        LinkCardToSpecUseCase,
        UnlinkCardFromSpecCommand,
        UnlinkCardFromSpecUseCase,
    )
    from okto_pulse.core.application.use_cases.base import (
        ActorContext,
        EntityNotFoundError,
    )

    spec = SimpleNamespace(id="spec-1", board_id=spec_board)
    card = (
        None
        if card_board is None
        else SimpleNamespace(
            id="card-1",
            board_id=card_board,
            spec_id=card_spec_id,
        )
    )
    specs = SimpleNamespace(
        get_spec=AsyncMock(return_value=spec),
        link_card=AsyncMock(return_value=True),
        unlink_card=AsyncMock(return_value=True),
    )
    cards = SimpleNamespace(get_card=AsyncMock(return_value=card))
    uow = SimpleNamespace(
        boards=SimpleNamespace(
            get=AsyncMock(return_value=SimpleNamespace(id="actor-board"))
        ),
        services=SimpleNamespace(specs=specs, cards=cards),
        commit=AsyncMock(),
    )

    command = (
        LinkCardToSpecCommand("spec-1", "card-1")
        if operation == "link"
        else UnlinkCardFromSpecCommand("spec-1", "card-1")
    )
    use_case = (
        LinkCardToSpecUseCase()
        if operation == "link"
        else UnlinkCardFromSpecUseCase()
    )
    with pytest.raises(EntityNotFoundError) as exc_info:
        await use_case.execute(
            command,
            actor=ActorContext("agent", "mcp", board_id="actor-board"),
            uow=uow,
        )

    assert exc_info.value.entity_type == expected_entity
    specs.link_card.assert_not_awaited()
    specs.unlink_card.assert_not_awaited()
    uow.commit.assert_not_awaited()


# --- link task to scenario --------------------------------------------------


@pytest.mark.asyncio
async def test_link_task_to_scenario_200_bidirectional(client) -> None:
    _, sid, cid = await _seed(
        spec_status=SpecStatus.DRAFT,
        scenarios=[{"id": "sc1", "title": "Scenario one", "linked_task_ids": []}],
        card_spec_id_to_self=True,
        card_scenarios=[],
    )
    resp = client.post(f"{PREFIX}/specs/{sid}/scenarios/sc1/link-task/{cid}")
    assert resp.status_code == 200, resp.text
    assert resp.json() == {
        "success": True,
        "spec_id": sid,
        "scenario_id": "sc1",
        "card_id": cid,
    }
    spec = await _get_spec(sid)
    assert spec.test_scenarios[0]["linked_task_ids"] == [cid]
    card = await _get_card(cid)
    assert "sc1" in (card.test_scenario_ids or [])


@pytest.mark.asyncio
async def test_link_task_to_scenario_spec_404(client) -> None:
    resp = client.post(f"{PREFIX}/specs/{_missing()}/scenarios/sc1/link-task/{_missing()}")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Spec not found"


@pytest.mark.asyncio
async def test_link_task_to_scenario_card_404(client) -> None:
    _, sid, _ = await _seed(scenarios=[{"id": "sc1", "title": "Scenario one", "linked_task_ids": []}])
    missing_card = _missing()
    resp = client.post(f"{PREFIX}/specs/{sid}/scenarios/sc1/link-task/{missing_card}")
    assert resp.status_code == 404
    assert (
        resp.json()["detail"]
        == f"Card '{missing_card}' not found — cannot link a non-existent card."
    )


@pytest.mark.asyncio
async def test_link_task_to_scenario_scenario_404(client) -> None:
    _, sid, cid = await _seed(
        scenarios=[{"id": "sc1", "title": "Scenario one", "linked_task_ids": []}],
        card_spec_id_to_self=True,
    )
    resp = client.post(f"{PREFIX}/specs/{sid}/scenarios/nope/link-task/{cid}")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Scenario 'nope' not found in spec."


# --- unlink task from scenario ----------------------------------------------


@pytest.mark.asyncio
async def test_unlink_task_from_scenario_200_bidirectional(client) -> None:
    _, sid, cid = await _seed(
        spec_status=SpecStatus.DRAFT,
        scenarios=[{"id": "sc1", "title": "Scenario one", "linked_task_ids": ["__placeholder__"]}],
        card_scenarios=["sc1"],
    )
    # Seed the scenario with the real card id so the unlink removes it.
    async with get_session_factory()() as db:
        spec = await db.get(Spec, sid)
        spec.test_scenarios = [{"id": "sc1", "title": "Scenario one", "linked_task_ids": [cid]}]
        await db.commit()

    resp = client.post(f"{PREFIX}/specs/{sid}/scenarios/sc1/unlink-task/{cid}")
    assert resp.status_code == 200, resp.text
    assert resp.json() == {
        "success": True,
        "spec_id": sid,
        "scenario_id": "sc1",
        "card_id": cid,
    }
    spec = await _get_spec(sid)
    assert cid not in spec.test_scenarios[0]["linked_task_ids"]
    card = await _get_card(cid)
    assert "sc1" not in (card.test_scenario_ids or [])


@pytest.mark.asyncio
async def test_unlink_task_from_scenario_spec_404(client) -> None:
    resp = client.post(
        f"{PREFIX}/specs/{_missing()}/scenarios/sc1/unlink-task/{_missing()}"
    )
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Spec not found"


@pytest.mark.asyncio
async def test_unlink_task_from_scenario_scenario_404(client) -> None:
    _, sid, cid = await _seed(spec_status=SpecStatus.DRAFT, scenarios=[])
    resp = client.post(f"{PREFIX}/specs/{sid}/scenarios/nope/unlink-task/{cid}")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Scenario not found"


# --- update test scenario status --------------------------------------------


@pytest.mark.asyncio
async def test_update_test_scenario_status_200(client) -> None:
    _, sid, _ = await _seed(
        spec_status=SpecStatus.IN_PROGRESS,
        scenarios=[{"id": "sc1", "status": "draft"}],
    )
    resp = client.patch(
        f"{PREFIX}/specs/{sid}/scenarios/sc1/status",
        json={"status": "passed"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["id"] == sid
    assert body["scenario"] == {"id": "sc1", "status": "passed"}
    assert body["result"]["new_status"] == "passed"
    spec = await _get_spec(sid)
    assert spec.test_scenarios[0]["status"] == "passed"


@pytest.mark.asyncio
async def test_update_test_scenario_status_scenario_not_found_404(client) -> None:
    _, sid, _ = await _seed(
        spec_status=SpecStatus.IN_PROGRESS,
        scenarios=[{"id": "sc1", "status": "draft"}],
    )
    resp = client.patch(
        f"{PREFIX}/specs/{sid}/scenarios/nope/status",
        json={"status": "passed"},
    )
    assert resp.status_code == 404
    assert resp.json()["detail"].startswith("scenario_not_found")


@pytest.mark.asyncio
async def test_update_test_scenario_status_invalid_status_422(client) -> None:
    _, sid, _ = await _seed(
        spec_status=SpecStatus.IN_PROGRESS,
        scenarios=[{"id": "sc1", "status": "draft"}],
    )
    resp = client.patch(
        f"{PREFIX}/specs/{sid}/scenarios/sc1/status",
        json={"status": "banana"},
    )
    assert resp.status_code == 422
    assert resp.json()["detail"].startswith("status_not_valid")


# --- use case + AST ---------------------------------------------------------


@pytest.mark.asyncio
async def test_link_task_use_case_raises_for_missing_spec() -> None:
    from okto_pulse.core.application.use_cases import (
        LinkTaskToScenarioCommand,
        LinkTaskToScenarioUseCase,
    )
    from okto_pulse.core.application.use_cases.base import (
        ActorContext,
        EntityNotFoundError,
    )
    from sqlalchemy_test_unit_of_work import SQLAlchemyUnitOfWorkFactory
    uowf = SQLAlchemyUnitOfWorkFactory(get_session_factory())
    actor = ActorContext(USER, "rest")
    with pytest.raises(EntityNotFoundError):
        async with uowf(actor=actor) as uow:
            await LinkTaskToScenarioUseCase().execute(
                LinkTaskToScenarioCommand(_missing(), "sc1", _missing()),
                actor=actor,
                uow=uow,
            )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("spec_board", "card_board", "expected_entity"),
    (
        ("foreign-board", "actor-board", "spec"),
        ("actor-board", "foreign-board", "card"),
        ("actor-board", None, "card"),
    ),
)
async def test_unlink_task_fails_closed_before_either_write(
    spec_board: str, card_board: str | None, expected_entity: str
) -> None:
    from okto_pulse.core.application.use_cases import (
        UnlinkTaskFromScenarioCommand,
        UnlinkTaskFromScenarioUseCase,
    )
    from okto_pulse.core.application.use_cases.base import (
        ActorContext,
        EntityNotFoundError,
    )

    spec = SimpleNamespace(
        id="spec-1",
        board_id=spec_board,
        test_scenarios=[{"id": "sc1", "linked_task_ids": ["card-1"]}],
    )
    card = (
        None
        if card_board is None
        else SimpleNamespace(
            id="card-1", board_id=card_board, test_scenario_ids=["sc1"]
        )
    )
    specs = SimpleNamespace(
        get_spec=AsyncMock(return_value=spec),
        update_spec=AsyncMock(),
    )
    cards = SimpleNamespace(
        get_card=AsyncMock(return_value=card),
        update_card=AsyncMock(),
    )
    uow = SimpleNamespace(
        boards=SimpleNamespace(
            get=AsyncMock(return_value=SimpleNamespace(id="actor-board"))
        ),
        services=SimpleNamespace(specs=specs, cards=cards),
        commit=AsyncMock(),
    )

    with pytest.raises(EntityNotFoundError) as exc_info:
        await UnlinkTaskFromScenarioUseCase().execute(
            UnlinkTaskFromScenarioCommand("spec-1", "sc1", "card-1"),
            actor=ActorContext(
                "agent", "mcp", board_id="actor-board"
            ),
            uow=uow,
        )

    assert exc_info.value.entity_type == expected_entity
    specs.update_spec.assert_not_awaited()
    cards.update_card.assert_not_awaited()
    uow.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_status_update_cross_board_is_governed_before_service_write() -> None:
    from okto_pulse.core.application.use_cases import (
        SetTestScenarioStatusCommand,
        SetTestScenarioStatusUseCase,
    )
    from okto_pulse.core.application.use_cases.base import ActorContext

    specs = SimpleNamespace(
        get_spec=AsyncMock(
            return_value=SimpleNamespace(id="spec-1", board_id="foreign-board")
        ),
        set_test_scenario_status=AsyncMock(),
    )
    uow = SimpleNamespace(services=SimpleNamespace(specs=specs))

    with pytest.raises(ValueError, match="scenario_not_found: spec not found"):
        await SetTestScenarioStatusUseCase().execute(
            SetTestScenarioStatusCommand("spec-1", "sc1", "ready"),
            actor=ActorContext("agent", "mcp", board_id="actor-board"),
            uow=uow,
        )

    specs.set_test_scenario_status.assert_not_awaited()


def test_fu3c_s2_endpoints_take_uow_not_raw_session() -> None:
    for name in _ENDPOINTS:
        sig = inspect.signature(getattr(specs_api, name))
        assert "db" not in sig.parameters, name
        assert "uow" in sig.parameters, name
        assert sig.parameters["uow"].default.dependency is get_unit_of_work, name
