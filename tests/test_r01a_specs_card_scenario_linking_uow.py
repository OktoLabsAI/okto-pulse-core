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
import uuid

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.core.api import specs as specs_api
from okto_pulse.core.api.specs import router as specs_router
from okto_pulse.core.api.deps import get_unit_of_work
from okto_pulse.core.infra.auth import require_user
from okto_pulse.core.infra.database import get_db, get_session_factory
from okto_pulse.core.models.db import Board, Card, Spec, SpecStatus

USER = "r01a-fu3c-s2-user"
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
                owner_id=USER,
                settings={"skip_test_evidence_global": True},
            )
        )
        db.add(
            Spec(
                id=sid,
                board_id=bid,
                title="fu3cs2-spec",
                status=spec_status,
                created_by=USER,
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
                created_by=USER,
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


# --- link task to scenario --------------------------------------------------


@pytest.mark.asyncio
async def test_link_task_to_scenario_200_bidirectional(client) -> None:
    _, sid, cid = await _seed(
        spec_status=SpecStatus.DRAFT,
        scenarios=[{"id": "sc1", "title": "Scenario one", "linked_task_ids": []}],
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
    _, sid, cid = await _seed(scenarios=[{"id": "sc1", "title": "Scenario one", "linked_task_ids": []}])
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
    from okto_pulse.core.repositories import SQLAlchemyUnitOfWorkFactory

    uowf = SQLAlchemyUnitOfWorkFactory(get_session_factory())
    actor = ActorContext(USER, "rest")
    with pytest.raises(EntityNotFoundError):
        async with uowf(actor=actor) as uow:
            await LinkTaskToScenarioUseCase().execute(
                LinkTaskToScenarioCommand(_missing(), "sc1", _missing()),
                actor=actor,
                uow=uow,
            )


def test_fu3c_s2_endpoints_take_uow_not_raw_session() -> None:
    for name in _ENDPOINTS:
        sig = inspect.signature(getattr(specs_api, name))
        assert "db" not in sig.parameters, name
        assert "uow" in sig.parameters, name
        assert sig.parameters["uow"].default.dependency is get_unit_of_work, name
