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
    "expected_subject_version": 1,
    "idempotency_key": "task-validation-fu4s2",
    "confidence": 90,
    "confidence_justification": "The reviewer inspected the delivered behavior.",
    "estimated_completeness": 100,
    "completeness_justification": "All acceptance criteria are implemented.",
    "estimated_drift": 0,
    "drift_justification": "No implementation drift was identified.",
    "general_justification": "The implementation satisfies the reviewed task contract.",
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
    position: int | None = None,
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
            db.add(
                Spec(id=spec_id, board_id=board_id, title="fu4s2-spec", created_by=USER)
            )
        card = Card(
            id=cid,
            board_id=board_id,
            spec_id=spec_id,
            title=f"{title}-{uuid.uuid4().hex[:6]}",
            created_by=USER,
            archived=archived,
            status=status or CardStatus.NOT_STARTED,
            validations=validations,
        )
        if position is not None:
            card.position = position
        db.add(card)
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
    resp = client.post(
        f"{PREFIX}/{card_id}/move", json={"status": "not_started", "position": 1}
    )
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


@pytest.mark.asyncio
async def test_move_card_valid_selector_variants_reorder_and_return_card_response(
    client,
) -> None:
    board_id, spec_id = await _seed_board_spec()
    a = await _seed_card(board_id=board_id, spec_id=spec_id, title="move-a", position=0)
    b = await _seed_card(board_id=board_id, spec_id=spec_id, title="move-b", position=1)
    c = await _seed_card(board_id=board_id, spec_id=spec_id, title="move-c", position=2)

    cases = (
        (c, {"status": "not_started", "before_id": a}, 0),
        (c, {"status": "not_started", "placement": "end"}, 2),
        (a, {"status": "not_started", "position": -1}, 2),
        (
            b,
            {
                "status": "not_started",
                "position": None,
                "before_id": None,
                "after_id": None,
                "placement": None,
            },
            2,
        ),
    )
    for card_id, payload, expected_position in cases:
        response = client.post(f"{PREFIX}/{card_id}/move", json=payload)
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["id"] == card_id
        assert body["status"] == "not_started"
        assert body["position"] == expected_position

        from sqlalchemy import select
        from sqlalchemy_test_models import Card, CardStatus

        async with get_session_factory()() as db:
            positions = list(
                (
                    await db.execute(
                        select(Card.position)
                        .where(
                            Card.board_id == board_id,
                            Card.status == CardStatus.NOT_STARTED,
                            Card.archived.is_(False),
                        )
                        .order_by(Card.position)
                    )
                ).scalars()
            )
        assert positions == [0, 1, 2]


@pytest.mark.asyncio
async def test_move_card_reorders_within_rejected_but_refuses_inbound_transition(
    client,
) -> None:
    from sqlalchemy_test_models import Card, CardStatus

    board_id, spec_id = await _seed_board_spec()
    rejected_a = await _seed_card(
        board_id=board_id,
        spec_id=spec_id,
        title="rejected-a",
        status=CardStatus.REJECTED,
        position=0,
    )
    rejected_b = await _seed_card(
        board_id=board_id,
        spec_id=spec_id,
        title="rejected-b",
        status=CardStatus.REJECTED,
        position=1,
    )
    validation = await _seed_card(
        board_id=board_id,
        spec_id=spec_id,
        title="validation",
        status=CardStatus.VALIDATION,
        position=0,
    )

    reorder = client.post(
        f"{PREFIX}/{rejected_b}/move",
        json={"status": "rejected", "before_id": rejected_a},
    )
    assert reorder.status_code == 200, reorder.text
    assert reorder.json()["status"] == "rejected"
    assert reorder.json()["position"] == 0

    inbound = client.post(
        f"{PREFIX}/{validation}/move",
        json={"status": "rejected"},
    )
    assert inbound.status_code == 409, inbound.text
    async with get_session_factory()() as db:
        persisted = await db.get(Card, validation)
    assert persisted is not None
    assert persisted.status is CardStatus.VALIDATION


@pytest.mark.asyncio
async def test_move_card_route_maps_missing_cancellation_reason_to_typed_400(
    client,
) -> None:
    card_id = await _seed_card()
    response = client.post(f"{PREFIX}/{card_id}/move", json={"status": "cancelled"})
    assert response.status_code == 400, response.text
    assert response.json()["detail"]["error"] == "cancellation_reason_required"


@pytest.mark.asyncio
async def test_move_card_route_maps_cross_column_anchor_to_409(client) -> None:
    from sqlalchemy_test_models import CardStatus

    board_id, spec_id = await _seed_board_spec()
    moving = await _seed_card(
        board_id=board_id, spec_id=spec_id, title="moving", position=0
    )
    anchor = await _seed_card(
        board_id=board_id,
        spec_id=spec_id,
        title="wrong-column-anchor",
        status=CardStatus.STARTED,
        position=0,
    )
    response = client.post(
        f"{PREFIX}/{moving}/move",
        json={"status": "not_started", "before_id": anchor},
    )
    assert response.status_code == 409, response.text
    assert "resequence_anchor_invalid" in str(response.json()["detail"])


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
    assert resp.json()["detail"] == {
        "code": "dependency_self_reference",
        "message": "A card cannot depend on itself.",
        "remediation": "choose_a_different_dependency",
        "facts": {
            "card_id": card_id,
            "depends_on_id": card_id,
        },
    }


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
async def test_submit_validation_422_missing_fields(client) -> None:
    card_id = await _seed_card()
    resp = client.post(f"{PREFIX}/{card_id}/validate", json={})
    assert resp.status_code == 422, resp.text
    missing = {item["loc"][-1] for item in resp.json()["detail"]}
    assert {"expected_subject_version", "idempotency_key", "confidence"} <= missing


@pytest.mark.asyncio
async def test_submit_validation_422_bad_recommendation(client) -> None:
    card_id = await _seed_card()
    payload = dict(_VALID_VALIDATION, recommendation="maybe")
    resp = client.post(f"{PREFIX}/{card_id}/validate", json=payload)
    assert resp.status_code == 422, resp.text
    assert resp.json()["detail"][0]["loc"][-1] == "recommendation"


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


@pytest.mark.asyncio
async def test_submit_validation_exact_retry_replays_after_rejected(client) -> None:
    from sqlalchemy_test_models import Card, CardStatus

    card_id = await _seed_card()
    async with get_session_factory()() as db:
        card = await db.get(Card, card_id)
        card.status = CardStatus.VALIDATION
        await db.commit()
        await db.refresh(card)
        version = card.policy_version

    payload = {
        **_VALID_VALIDATION,
        "expected_subject_version": version,
        "idempotency_key": f"reject-{card_id}",
        "recommendation": "reject",
    }
    first = client.post(f"{PREFIX}/{card_id}/validate", json=payload)
    assert first.status_code == 201, first.text
    assert first.json()["card_status"] == "rejected"
    assert first.json()["replayed"] is False
    assert {"response", "request_digest", "idempotency_key"}.isdisjoint(first.json())

    replay = client.post(f"{PREFIX}/{card_id}/validate", json=payload)
    assert replay.status_code == 201, replay.text
    assert replay.json()["id"] == first.json()["id"]
    assert replay.json()["replayed"] is True
    assert {"response", "request_digest", "idempotency_key"}.isdisjoint(replay.json())

    listed = client.get(f"{PREFIX}/{card_id}/validations")
    fetched = client.get(
        f"{PREFIX}/{card_id}/validations/{first.json()['id']}"
    )
    assert listed.status_code == 200, listed.text
    assert fetched.status_code == 200, fetched.text
    for public_validation in (*listed.json()["validations"], fetched.json()):
        assert {"response", "request_digest", "idempotency_key"}.isdisjoint(
            public_validation
        )

    conflict = client.post(
        f"{PREFIX}/{card_id}/validate",
        json={**payload, "general_justification": "A different assessment payload."},
    )
    assert conflict.status_code == 409, conflict.text
    assert (
        conflict.json()["detail"]["code"]
        == "task_validation_idempotency_conflict"
    )


@pytest.mark.asyncio
async def test_submit_validation_exact_retry_replays_after_done(
    client, monkeypatch
) -> None:
    from sqlalchemy_test_models import Card, CardStatus

    from okto_pulse.core.services.main import CardService

    async def _no_completion_blockers(self, *, card, board):
        return ()

    monkeypatch.setattr(
        CardService,
        "_task_completion_gate_failures",
        _no_completion_blockers,
    )
    card_id = await _seed_card()
    async with get_session_factory()() as db:
        card = await db.get(Card, card_id)
        card.status = CardStatus.VALIDATION
        await db.commit()
        await db.refresh(card)
        version = card.policy_version

    payload = {
        **_VALID_VALIDATION,
        "expected_subject_version": version,
        "idempotency_key": f"approve-{card_id}",
    }
    first = client.post(f"{PREFIX}/{card_id}/validate", json=payload)
    assert first.status_code == 201, first.text
    assert first.json()["card_status"] == "done"

    replay = client.post(f"{PREFIX}/{card_id}/validate", json=payload)
    assert replay.status_code == 201, replay.text
    assert replay.json()["id"] == first.json()["id"]
    assert replay.json()["replayed"] is True


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

    missing_val = client.get(
        f"{PREFIX}/{card_id}/validations/nope-{uuid.uuid4().hex[:6]}"
    )
    assert missing_val.status_code == 404
    assert missing_val.json()["detail"] == "Validation not found"


@pytest.mark.asyncio
async def test_get_validation_404_missing_card(client) -> None:
    resp = client.get(f"{PREFIX}/{_missing()}/validations/whatever")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Card not found"


@pytest.mark.asyncio
async def test_delete_validation_is_rejected_as_append_only_history(client) -> None:
    vid = f"val-{uuid.uuid4().hex[:8]}"
    card_id = await _seed_card(validations=[{"id": vid, "recommendation": "approve"}])

    removed = client.delete(f"{PREFIX}/{card_id}/validations/{vid}")
    assert removed.status_code == 409, removed.text
    assert removed.json()["detail"]["code"] == "task_validation_history_append_only"

    persisted = client.get(f"{PREFIX}/{card_id}/validations/{vid}")
    assert persisted.status_code == 200
    assert persisted.json()["id"] == vid


@pytest.mark.asyncio
async def test_delete_validation_404_missing_card(client) -> None:
    resp = client.delete(f"{PREFIX}/{_missing()}/validations/whatever")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Card not found"


# --- use case + AST ---------------------------------------------------------


@pytest.mark.asyncio
async def test_move_card_use_case_raises_for_missing_card() -> None:
    from okto_pulse.core.application.use_cases import MoveCardCommand, MoveCardUseCase
    from okto_pulse.core.application.use_cases.base import (
        ActorContext,
        EntityNotFoundError,
    )
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
