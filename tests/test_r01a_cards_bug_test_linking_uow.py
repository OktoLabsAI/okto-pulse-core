"""Spec R01A REST-FU4-S3 — Bug card / test-task linking on the UnitOfWork.

The three remaining bug-linking ``api/cards.py`` endpoints now route through the
``card_crud`` use cases + ``get_unit_of_work``; each adapter only maps the
result/errors to HTTP. Oracles assert the legacy observable contract end-to-end
via TestClient (status codes + detail/body shape) for:

* get_bug_regression_scenario_candidates — 200 (payload passthrough),
                                           400 (not a bug → preview error dict),
                                           404 (missing bug → preview error dict)
* link_test_task_to_bug   — 201 (envelope + is_unblocked), idempotent re-link,
                            400 (missing test_task_id / not a bug card),
                            404 (missing bug card / missing test task),
                            422 (different spec / created-before-bug /
                                 scenario-not-on-spec)
* unlink_test_task_from_bug — 204 (linked + no-op), 404 (missing card)

Plus a use-case-level ``EntityNotFoundError`` probe (entity_type discrimination
that drives the link adapter's two distinct 404 details) and an AST signature
check proving every migrated endpoint takes ``uow`` (not a raw ``AsyncSession``).
"""

from __future__ import annotations

import inspect
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.core.api import cards as cards_api
from okto_pulse.core.api.cards import router as cards_router
from okto_pulse.core.api.deps import get_unit_of_work
from okto_pulse.core.infra.auth import require_user
from okto_pulse.core.infra.database import get_db, get_session_factory

USER = "r01a-fu4-s3-user"
PREFIX = "/api/v1/cards"
_ENDPOINTS = (
    "get_bug_regression_scenario_candidates",
    "link_test_task_to_bug",
    "unlink_test_task_from_bug",
)

_BUG_TIME = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
_AFTER = _BUG_TIME + timedelta(hours=1)
_BEFORE = _BUG_TIME - timedelta(hours=1)


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


def _missing() -> str:
    return f"card-missing-{uuid.uuid4().hex[:8]}"


async def _seed_board_spec(*, scenarios: list | None = None) -> tuple[str, str]:
    from okto_pulse.core.models.db import Board, Spec

    bid = f"board-fu4s3-{uuid.uuid4().hex[:8]}"
    sid = f"spec-fu4s3-{uuid.uuid4().hex[:8]}"
    async with get_session_factory()() as db:
        db.add(Board(id=bid, name="fu4s3", owner_id=USER))
        db.add(
            Spec(
                id=sid,
                board_id=bid,
                title="fu4s3-spec",
                created_by=USER,
                test_scenarios=scenarios,
            )
        )
        await db.commit()
    return bid, sid


async def _seed_card(
    *,
    board_id: str,
    spec_id: str | None,
    card_type=None,
    created_at: datetime | None = None,
    test_scenario_ids: list[str] | None = None,
    origin_task_id: str | None = None,
    title: str = "fu4-s3-card",
) -> str:
    from okto_pulse.core.models.db import Card, CardStatus, CardType

    cid = f"card-fu4s3-{uuid.uuid4().hex[:8]}"
    kwargs = dict(
        id=cid,
        board_id=board_id,
        spec_id=spec_id,
        title=f"{title}-{uuid.uuid4().hex[:6]}",
        created_by=USER,
        status=CardStatus.NOT_STARTED,
        card_type=card_type or CardType.NORMAL,
        test_scenario_ids=test_scenario_ids,
        origin_task_id=origin_task_id,
    )
    # Only set created_at when provided — the column is NOT NULL with a
    # server_default, so passing an explicit None would emit a NULL and fail.
    if created_at is not None:
        kwargs["created_at"] = created_at
    async with get_session_factory()() as db:
        db.add(Card(**kwargs))
        await db.commit()
    return cid


async def _seed_bug_and_test(
    *,
    bug_card_type=None,
    same_spec: bool = True,
    test_created_at: datetime | None = None,
    test_scenario_ids: list[str] | None = None,
    spec_scenarios: list | None = None,
) -> tuple[str, str]:
    """Seed a bug card + a candidate test-task card. Returns (bug_id, test_id)."""
    from okto_pulse.core.models.db import CardType

    board_id, spec_id = await _seed_board_spec(scenarios=spec_scenarios)
    bug_id = await _seed_card(
        board_id=board_id,
        spec_id=spec_id,
        card_type=bug_card_type if bug_card_type is not None else CardType.BUG,
        created_at=_BUG_TIME,
        title="bug",
    )
    test_spec_id = spec_id
    if not same_spec:
        _, test_spec_id = await _seed_board_spec()
        # Re-point the foreign spec onto the same board so only the spec differs.
        async with get_session_factory()() as db:
            from okto_pulse.core.models.db import Spec

            foreign = await db.get(Spec, test_spec_id)
            foreign.board_id = board_id
            await db.commit()
    test_id = await _seed_card(
        board_id=board_id,
        spec_id=test_spec_id,
        card_type=CardType.TEST,
        created_at=test_created_at or _AFTER,
        test_scenario_ids=test_scenario_ids,
        title="test-task",
    )
    return bug_id, test_id


# --- link: happy path + idempotency -----------------------------------------


@pytest.mark.asyncio
async def test_link_201_envelope_and_unblocked(client) -> None:
    bug_id, test_id = await _seed_bug_and_test()
    resp = client.post(f"{PREFIX}/{bug_id}/test-tasks", json={"test_task_id": test_id})
    assert resp.status_code == 201, resp.text
    assert resp.json() == {
        "success": True,
        "bug_card_id": bug_id,
        "test_task_id": test_id,
        "is_unblocked": True,
    }


@pytest.mark.asyncio
async def test_link_idempotent_relink_stays_unblocked(client) -> None:
    bug_id, test_id = await _seed_bug_and_test()
    first = client.post(f"{PREFIX}/{bug_id}/test-tasks", json={"test_task_id": test_id})
    assert first.status_code == 201, first.text
    # A second link of the same id is a no-op append (no duplicate) but still 201
    # with is_unblocked True (len(linked) >= 1).
    second = client.post(f"{PREFIX}/{bug_id}/test-tasks", json={"test_task_id": test_id})
    assert second.status_code == 201, second.text
    assert second.json()["is_unblocked"] is True

    async with get_session_factory()() as db:
        from okto_pulse.core.models.db import Card

        bug = await db.get(Card, bug_id)
        assert bug.linked_test_task_ids == [test_id]


# --- link: 400 / 404 / 422 gates --------------------------------------------


@pytest.mark.asyncio
async def test_link_400_missing_test_task_id(client) -> None:
    bug_id, _ = await _seed_bug_and_test()
    resp = client.post(f"{PREFIX}/{bug_id}/test-tasks", json={})
    assert resp.status_code == 400, resp.text
    assert resp.json()["detail"] == "test_task_id is required"


@pytest.mark.asyncio
async def test_link_404_missing_bug_card(client) -> None:
    resp = client.post(f"{PREFIX}/{_missing()}/test-tasks", json={"test_task_id": "whatever"})
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Card not found"


@pytest.mark.asyncio
async def test_link_400_card_is_not_a_bug(client) -> None:
    from okto_pulse.core.models.db import CardType

    bug_id, test_id = await _seed_bug_and_test(bug_card_type=CardType.NORMAL)
    resp = client.post(f"{PREFIX}/{bug_id}/test-tasks", json={"test_task_id": test_id})
    assert resp.status_code == 400, resp.text
    assert resp.json()["detail"] == "Card is not a bug card"


@pytest.mark.asyncio
async def test_link_404_missing_test_task(client) -> None:
    bug_id, _ = await _seed_bug_and_test()
    resp = client.post(f"{PREFIX}/{bug_id}/test-tasks", json={"test_task_id": _missing()})
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Test task not found"


@pytest.mark.asyncio
async def test_link_422_different_spec(client) -> None:
    bug_id, test_id = await _seed_bug_and_test(same_spec=False)
    resp = client.post(f"{PREFIX}/{bug_id}/test-tasks", json={"test_task_id": test_id})
    assert resp.status_code == 422, resp.text
    assert resp.json()["detail"] == "Test task does not belong to the same spec as the bug"


@pytest.mark.asyncio
async def test_link_422_created_before_bug(client) -> None:
    bug_id, test_id = await _seed_bug_and_test(test_created_at=_BEFORE)
    resp = client.post(f"{PREFIX}/{bug_id}/test-tasks", json={"test_task_id": test_id})
    assert resp.status_code == 422, resp.text
    assert "created before the bug" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_link_422_scenario_not_on_spec(client) -> None:
    # Spec has no scenarios; the test task references a ghost scenario id.
    bug_id, test_id = await _seed_bug_and_test(
        test_scenario_ids=["ghost-scenario"],
        spec_scenarios=[{"id": "ts-real", "title": "real"}],
    )
    resp = client.post(f"{PREFIX}/{bug_id}/test-tasks", json={"test_task_id": test_id})
    assert resp.status_code == 422, resp.text
    assert resp.json()["detail"] == (
        "Test task references scenario 'ghost-scenario' that does not exist on the bug spec"
    )


# --- unlink -----------------------------------------------------------------


@pytest.mark.asyncio
async def test_unlink_204_after_link_then_noop(client) -> None:
    bug_id, test_id = await _seed_bug_and_test()
    assert client.post(f"{PREFIX}/{bug_id}/test-tasks", json={"test_task_id": test_id}).status_code == 201

    removed = client.delete(f"{PREFIX}/{bug_id}/test-tasks/{test_id}")
    assert removed.status_code == 204, removed.text
    async with get_session_factory()() as db:
        from okto_pulse.core.models.db import Card

        bug = await db.get(Card, bug_id)
        assert bug.linked_test_task_ids == []

    # Unlinking an id that is not present is a no-op 204 (not a 404).
    again = client.delete(f"{PREFIX}/{bug_id}/test-tasks/{test_id}")
    assert again.status_code == 204, again.text


@pytest.mark.asyncio
async def test_unlink_404_missing_card(client) -> None:
    resp = client.delete(f"{PREFIX}/{_missing()}/test-tasks/whatever")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Card not found"


# --- regression scenario candidates -----------------------------------------


@pytest.mark.asyncio
async def test_candidates_200_payload_passthrough(client) -> None:
    from okto_pulse.core.models.db import CardType

    board_id, spec_id = await _seed_board_spec(
        scenarios=[{"id": "ts-x", "title": "x", "linked_criteria": [0], "status": "passed"}]
    )
    origin_id = await _seed_card(
        board_id=board_id, spec_id=spec_id, card_type=CardType.NORMAL, title="origin"
    )
    bug_id = await _seed_card(
        board_id=board_id,
        spec_id=spec_id,
        card_type=CardType.BUG,
        origin_task_id=origin_id,
        title="bug",
    )
    resp = client.get(
        f"{PREFIX}/{bug_id}/regression-scenario-candidates",
        params=[("board_id", board_id)],
    )
    assert resp.status_code == 200, resp.text
    payload = resp.json()
    assert payload["bug_id"] == bug_id
    assert payload["spec_id"] == spec_id
    assert "next_action" in payload


@pytest.mark.asyncio
async def test_candidates_400_not_a_bug_card(client) -> None:
    from okto_pulse.core.models.db import CardType

    board_id, spec_id = await _seed_board_spec()
    normal_id = await _seed_card(
        board_id=board_id, spec_id=spec_id, card_type=CardType.NORMAL, title="normal"
    )
    resp = client.get(
        f"{PREFIX}/{normal_id}/regression-scenario-candidates",
        params=[("board_id", board_id)],
    )
    assert resp.status_code == 400, resp.text
    assert resp.json()["detail"]["error"] == "not_bug_card"


@pytest.mark.asyncio
async def test_candidates_404_missing_bug_card(client) -> None:
    board_id, _ = await _seed_board_spec()
    resp = client.get(
        f"{PREFIX}/{_missing()}/regression-scenario-candidates",
        params=[("board_id", board_id)],
    )
    assert resp.status_code == 404, resp.text
    assert resp.json()["detail"]["error"] == "bug_not_found"


# --- use case + AST ---------------------------------------------------------


@pytest.mark.asyncio
async def test_link_use_case_raises_entity_not_found_for_missing_card() -> None:
    from okto_pulse.core.application.use_cases.base import ActorContext, EntityNotFoundError
    from okto_pulse.core.application.use_cases.card_crud import (
        LinkTestTaskToBugCommand,
        LinkTestTaskToBugUseCase,
    )
    from okto_pulse.core.repositories import SQLAlchemyUnitOfWorkFactory

    uowf = SQLAlchemyUnitOfWorkFactory(get_session_factory())
    actor = ActorContext(USER, "rest")
    with pytest.raises(EntityNotFoundError) as excinfo:
        async with uowf(actor=actor) as uow:
            await LinkTestTaskToBugUseCase().execute(
                LinkTestTaskToBugCommand(_missing(), "some-test-task"),
                actor=actor,
                uow=uow,
            )
    # entity_type drives the adapter's two distinct 404 details ("Card not found"
    # vs "Test task not found").
    assert excinfo.value.entity_type == "card"


def test_fu4_s3_endpoints_take_uow_not_raw_session() -> None:
    for name in _ENDPOINTS:
        sig = inspect.signature(getattr(cards_api, name))
        assert "db" not in sig.parameters, name
        assert "uow" in sig.parameters, name
        assert sig.parameters["uow"].default.dependency is get_unit_of_work, name
