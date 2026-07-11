"""Spec R01A REST-FU4-S2 — Card lifecycle / move / dependencies / task-validation
gates on the UnitOfWork.

The nine remaining ``api/cards.py`` lifecycle endpoints now route through the
``card_crud`` use cases + ``get_unit_of_work``; each adapter only maps the
result/errors to HTTP. Oracles assert the legacy observable contract end-to-end
via TestClient (status codes + detail/body shape) for:

* move_card           — 200 (re-fetched body), 404, 409 (archived → ValueError)
* get_dependencies    — 200 projection + empty list
* get_dependents      — 200 projection
* add_dependency      — 201 envelope, 409 self-reference (ConflictError)
* remove_dependency   — 204, 404 ("Dependency not found")
* submit_task_validation — 400 (missing fields / bad recommendation),
                           422 (card not in 'validation'), 404 (missing card)
* list_task_validations  — 200 envelope, 404 (missing card → ValueError)
* get_task_validation    — 200, 404 (unknown id), 404 (missing card)
* delete_task_validation — 204, 404 (unknown id), 404 (missing card)

Plus a use-case-level ``EntityNotFoundError`` probe and an AST signature check
proving every migrated endpoint takes ``uow`` (not a raw ``AsyncSession``).
"""

from __future__ import annotations

import inspect
import uuid

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.community.api import cards as cards_api
from okto_pulse.community.api.cards import router as cards_router
from okto_pulse.community.api.deps import get_unit_of_work
from okto_pulse.community.api.auth_deps import require_user
from okto_pulse.core.infra.database import get_db, get_session_factory

USER = "r01a-fu4-s2-user"
PREFIX = "/api/v1/cards"
_ENDPOINTS = (
    "move_card",
    "get_dependencies",
    "get_dependents",
    "add_dependency",
    "remove_dependency",
    "submit_task_validation",
    "list_task_validations",
    "get_task_validation",
    "delete_task_validation",
)

_VALID_VALIDATION = {
    "confidence": 90,
    "confidence_justification": "high",
    "estimated_completeness": 100,
    "completeness_justification": "all done",
    "estimated_drift": 0,
    "drift_justification": "none",
    "general_justification": "ok",
    "recommendation": "approve",
}


@pytest.fixture
def client():
    app = FastAPI()
    app.include_router(cards_router, prefix=PREFIX)
    session_factory = get_session_factory()

    async def _override_db():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[require_user] = lambda: USER
    return TestClient(app)


async def _seed_board_spec() -> tuple[str, str]:
    from sqlalchemy_test_models import Board, Spec

    bid = f"board-fu4s2-{uuid.uuid4().hex[:8]}"
    sid = f"spec-fu4s2-{uuid.uuid4().hex[:8]}"
    async with get_session_factory()() as db:
        db.add(Board(id=bid, name="fu4s2", owner_id=USER))
        db.add(Spec(id=sid, board_id=bid, title="fu4s2-spec", created_by=USER))
        await db.commit()
    return bid, sid


async def _seed_card(
    *,
    board_id: str | None = None,
    spec_id: str | None = None,
    archived: bool = False,
    status=None,
    validations: list | None = None,
    title: str = "fu4-s2-card",
) -> str:
    # Seed Board + Spec + Card via raw models. We exercise the lifecycle/move/
    # dependency/validation endpoints, not create, so this deliberately bypasses
    # CardService.create_card's spec-status gate — the card just needs to satisfy
    # the "every card belongs to a spec" invariant.
    from sqlalchemy_test_models import Board, Card, CardStatus, Spec

    if board_id is None or spec_id is None:
        board_id, spec_id = await _seed_board_spec()
    cid = f"card-fu4s2-{uuid.uuid4().hex[:8]}"
    async with get_session_factory()() as db:
        # Board/Spec may already exist from _seed_board_spec; guard with a merge.
        if not await db.get(Board, board_id):
            db.add(Board(id=board_id, name="fu4s2", owner_id=USER))
        if not await db.get(Spec, spec_id):
            db.add(Spec(id=spec_id, board_id=board_id, title="fu4s2-spec", created_by=USER))
        db.add(
            Card(
                id=cid,
                board_id=board_id,
                spec_id=spec_id,
                title=f"{title}-{uuid.uuid4().hex[:6]}",
                created_by=USER,
                archived=archived,
                status=status or CardStatus.NOT_STARTED,
                validations=validations,
            )
        )
        await db.commit()
    return cid


def _missing() -> str:
    return f"card-missing-{uuid.uuid4().hex[:8]}"


# --- move -------------------------------------------------------------------


@pytest.mark.asyncio
async def test_move_card_200_refetched_body(client) -> None:
    card_id = await _seed_card()
    # Lateral move (not_started → not_started, level 0 → 0) skips the forward
    # spec/sprint gates; it is a reorder that returns the re-fetched card.
    resp = client.post(f"{PREFIX}/{card_id}/move", json={"status": "not_started", "position": 1})
    assert resp.status_code == 200, resp.text
    assert resp.json()["id"] == card_id


@pytest.mark.asyncio
async def test_move_card_404(client) -> None:
    resp = client.post(f"{PREFIX}/{_missing()}/move", json={"status": "not_started"})
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Card not found"


@pytest.mark.asyncio
async def test_move_card_409_archived(client) -> None:
    card_id = await _seed_card(archived=True)
    resp = client.post(f"{PREFIX}/{card_id}/move", json={"status": "not_started"})
    assert resp.status_code == 409, resp.text
    assert "archived" in str(resp.json()["detail"]).lower()


# --- dependencies -----------------------------------------------------------


@pytest.mark.asyncio
async def test_add_dependency_201_then_lists(client) -> None:
    board_id, spec_id = await _seed_board_spec()
    a = await _seed_card(board_id=board_id, spec_id=spec_id, title="dep-a")
    b = await _seed_card(board_id=board_id, spec_id=spec_id, title="dep-b")

    resp = client.post(f"{PREFIX}/{a}/dependencies/{b}")
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["card_id"] == a
    assert body["depends_on_id"] == b
    assert body["id"]

    deps = client.get(f"{PREFIX}/{a}/dependencies")
    assert deps.status_code == 200
    assert [d["id"] for d in deps.json()] == [b]
    assert deps.json()[0]["status"] == "not_started"

    dependents = client.get(f"{PREFIX}/{b}/dependents")
    assert dependents.status_code == 200
    assert [d["id"] for d in dependents.json()] == [a]


@pytest.mark.asyncio
async def test_get_dependencies_empty(client) -> None:
    card_id = await _seed_card()
    resp = client.get(f"{PREFIX}/{card_id}/dependencies")
    assert resp.status_code == 200
    assert resp.json() == []


@pytest.mark.asyncio
async def test_add_dependency_409_self_reference(client) -> None:
    card_id = await _seed_card()
    resp = client.post(f"{PREFIX}/{card_id}/dependencies/{card_id}")
    assert resp.status_code == 409, resp.text
    assert resp.json()["detail"] == "Dependência circular detectada ou auto-referência"


@pytest.mark.asyncio
async def test_remove_dependency_204_then_404(client) -> None:
    board_id, spec_id = await _seed_board_spec()
    a = await _seed_card(board_id=board_id, spec_id=spec_id, title="rm-a")
    b = await _seed_card(board_id=board_id, spec_id=spec_id, title="rm-b")
    assert client.post(f"{PREFIX}/{a}/dependencies/{b}").status_code == 201

    removed = client.delete(f"{PREFIX}/{a}/dependencies/{b}")
    assert removed.status_code == 204, removed.text
    # Already gone — a second remove is a 404.
    gone = client.delete(f"{PREFIX}/{a}/dependencies/{b}")
    assert gone.status_code == 404
    assert gone.json()["detail"] == "Dependency not found"


# --- submit_task_validation -------------------------------------------------


@pytest.mark.asyncio
async def test_submit_validation_400_missing_fields(client) -> None:
    card_id = await _seed_card()
    resp = client.post(f"{PREFIX}/{card_id}/validate", json={})
    assert resp.status_code == 400, resp.text
    assert resp.json()["detail"].startswith("Missing required fields:")


@pytest.mark.asyncio
async def test_submit_validation_400_bad_recommendation(client) -> None:
    card_id = await _seed_card()
    payload = dict(_VALID_VALIDATION, recommendation="maybe")
    resp = client.post(f"{PREFIX}/{card_id}/validate", json=payload)
    assert resp.status_code == 400, resp.text
    assert resp.json()["detail"] == "recommendation must be 'approve' or 'reject'"


@pytest.mark.asyncio
async def test_submit_validation_422_card_not_in_validation(client) -> None:
    # A freshly seeded card is NOT_STARTED — the service rejects with ValueError
    # which the adapter maps to 422 (not the 409 gate path).
    card_id = await _seed_card()
    resp = client.post(f"{PREFIX}/{card_id}/validate", json=dict(_VALID_VALIDATION))
    assert resp.status_code == 422, resp.text
    assert "validation" in str(resp.json()["detail"]).lower()


@pytest.mark.asyncio
async def test_submit_validation_404_missing_card(client) -> None:
    resp = client.post(f"{PREFIX}/{_missing()}/validate", json=dict(_VALID_VALIDATION))
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Card not found"


# --- list / get / delete validations ----------------------------------------


@pytest.mark.asyncio
async def test_list_validations_200_empty(client) -> None:
    card_id = await _seed_card()
    resp = client.get(f"{PREFIX}/{card_id}/validations")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body == {"card_id": card_id, "total": 0, "validations": []}


@pytest.mark.asyncio
async def test_list_validations_404_missing_card(client) -> None:
    resp = client.get(f"{PREFIX}/{_missing()}/validations")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Card not found"


@pytest.mark.asyncio
async def test_get_validation_200_and_404(client) -> None:
    vid = f"val-{uuid.uuid4().hex[:8]}"
    card_id = await _seed_card(validations=[{"id": vid, "recommendation": "approve"}])

    found = client.get(f"{PREFIX}/{card_id}/validations/{vid}")
    assert found.status_code == 200, found.text
    assert found.json()["id"] == vid

    missing_val = client.get(f"{PREFIX}/{card_id}/validations/nope-{uuid.uuid4().hex[:6]}")
    assert missing_val.status_code == 404
    assert missing_val.json()["detail"] == "Validation not found"


@pytest.mark.asyncio
async def test_get_validation_404_missing_card(client) -> None:
    resp = client.get(f"{PREFIX}/{_missing()}/validations/whatever")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Card not found"


@pytest.mark.asyncio
async def test_delete_validation_204_then_404(client) -> None:
    vid = f"val-{uuid.uuid4().hex[:8]}"
    card_id = await _seed_card(validations=[{"id": vid, "recommendation": "approve"}])

    removed = client.delete(f"{PREFIX}/{card_id}/validations/{vid}")
    assert removed.status_code == 204, removed.text
    # Gone now — a second delete is a 404 ("Validation not found").
    gone = client.delete(f"{PREFIX}/{card_id}/validations/{vid}")
    assert gone.status_code == 404
    assert gone.json()["detail"] == "Validation not found"


@pytest.mark.asyncio
async def test_delete_validation_404_missing_card(client) -> None:
    resp = client.delete(f"{PREFIX}/{_missing()}/validations/whatever")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Card not found"


# --- use case + AST ---------------------------------------------------------


@pytest.mark.asyncio
async def test_move_card_use_case_raises_for_missing_card() -> None:
    from okto_pulse.core.application.use_cases import MoveCardCommand, MoveCardUseCase
    from okto_pulse.core.application.use_cases.base import ActorContext, EntityNotFoundError
    from okto_pulse.core.models.schemas import CardMove
    from sqlalchemy_test_models import CardStatus
    from sqlalchemy_test_unit_of_work import SQLAlchemyUnitOfWorkFactory
    uowf = SQLAlchemyUnitOfWorkFactory(get_session_factory())
    actor = ActorContext(USER, "rest")
    with pytest.raises(EntityNotFoundError):
        async with uowf(actor=actor) as uow:
            await MoveCardUseCase().execute(
                MoveCardCommand(_missing(), CardMove(status=CardStatus.NOT_STARTED)),
                actor=actor,
                uow=uow,
            )


def test_fu4_s2_endpoints_take_uow_not_raw_session() -> None:
    for name in _ENDPOINTS:
        sig = inspect.signature(getattr(cards_api, name))
        assert "db" not in sig.parameters, name
        assert "uow" in sig.parameters, name
        assert sig.parameters["uow"].default.dependency is get_unit_of_work, name
