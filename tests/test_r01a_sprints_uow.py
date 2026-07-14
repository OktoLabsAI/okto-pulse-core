"""Spec R01A REST-FU7-S2 — Sprints REST on the UnitOfWork.

Every ``api/sprints.py`` endpoint (list-by-board; create; list-by-spec; get;
update; move; delete; submit-evaluation; assign-tasks; unassign-tasks; history;
suggest) now routes through a transport-free use case
(``application/use_cases/sprints_crud.py``) + ``get_unit_of_work``. The legacy
behavior preserved here, end-to-end through ``TestClient`` (commit really
persists across requests):

* create → re-fetched 201 body; ``Spec or board not found`` 404 (missing spec);
  invalid test-scenario ids → 400
* list-by-board / list-by-spec 200; get 200 + ``Sprint not found`` 404
* update 200 + 404; the move state machine gates (draft→review rejected 400;
  draft→active without cards rejected 400) + the missing-sprint 404
* the FULL lifecycle through the migrated surface — assign-tasks (lane envelope) →
  active → (test scenario passed) → review → submit-evaluation (``evaluation_id``
  envelope) → (card done) → closed — proving the sprint gates survive the
  strangle
* assign / unassign ``card_ids required`` 400 + the unassign clear-count;
  history 200; suggest 200
* a direct use-case assertion (``GetSprintUseCase`` raises ``EntityNotFoundError``)
  and AST signature guards proving every endpoint — and every registered route —
  takes ``uow`` (``get_unit_of_work``), not a raw ``AsyncSession``.

No agent row is seeded for the test user; the legacy sprint endpoints carried no
permission gate, so neither do these — denial is out of scope for this oracle.
"""

from __future__ import annotations

import inspect
import uuid

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.orm.attributes import flag_modified

from okto_pulse.community.api import sprints as sprints_api
from okto_pulse.community.api.deps import get_unit_of_work
from okto_pulse.community.api.sprints import router as sprints_router
from okto_pulse.community.api.auth_deps import get_realm_id, require_user
from okto_pulse.core.domain.realm import LOCAL_REALM_ID
from okto_pulse.core.infra.database import get_db, get_session_factory

USER = "r01a-fu7-s2-user"
PREFIX = "/api/v1"

_ENDPOINTS = (
    "list_board_sprints",
    "create_sprint",
    "list_sprints",
    "get_sprint",
    "update_sprint",
    "move_sprint",
    "delete_sprint",
    "submit_evaluation",
    "assign_tasks",
    "unassign_tasks",
    "list_history",
    "suggest_sprints",
)


@pytest.fixture
def client():
    app = FastAPI()
    app.include_router(sprints_router, prefix=PREFIX)
    session_factory = get_session_factory()

    async def _override_db():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[require_user] = lambda: USER
    app.dependency_overrides[get_realm_id] = lambda: LOCAL_REALM_ID
    return TestClient(app)


def _missing(kind: str = "sprint") -> str:
    return f"{kind}-missing-{uuid.uuid4().hex[:8]}"


async def _seed_board() -> str:
    from sqlalchemy_test_models import Board

    bid = f"board-fu7s2-{uuid.uuid4().hex[:8]}"
    async with get_session_factory()() as db:
        db.add(
            Board(
                id=bid,
                name="fu7s2",
                owner_id=USER,
                realm_id=LOCAL_REALM_ID,
            )
        )
        await db.commit()
        return bid


async def _seed_spec(board_id: str, *, ts_id: str | None = None, br_id: str | None = None) -> str:
    """Seed an in-progress Spec (optionally carrying one test scenario + one
    business rule) via the raw model — these endpoints read/sprint over the spec,
    they do not author it."""
    from sqlalchemy_test_models import Spec, SpecStatus

    sid = f"spec-fu7s2-{uuid.uuid4().hex[:8]}"
    test_scenarios = (
        [{"id": ts_id, "title": "TS", "linked_criteria": [0], "status": "draft", "linked_task_ids": []}]
        if ts_id
        else []
    )
    business_rules = (
        [{"id": br_id, "title": "BR", "linked_requirements": [0], "linked_task_ids": []}]
        if br_id
        else []
    )
    async with get_session_factory()() as db:
        db.add(
            Spec(
                id=sid,
                board_id=board_id,
                title="fu7s2-spec",
                status=SpecStatus.IN_PROGRESS,
                archived=False,
                acceptance_criteria=["AC1"],
                functional_requirements=["FR1"],
                test_scenarios=test_scenarios,
                business_rules=business_rules,
                api_contracts=[],
                technical_requirements=[],
                decisions=[],
                created_by=USER,
            )
        )
        await db.commit()
        return sid


async def _seed_card(board_id: str, spec_id: str) -> str:
    from sqlalchemy_test_models import Card, CardStatus, CardType

    cid = f"card-fu7s2-{uuid.uuid4().hex[:8]}"
    async with get_session_factory()() as db:
        db.add(
            Card(
                id=cid,
                board_id=board_id,
                spec_id=spec_id,
                title="fu7s2-card",
                status=CardStatus.NOT_STARTED,
                card_type=CardType.NORMAL,
                archived=False,
                created_by=USER,
            )
        )
        await db.commit()
        return cid


async def _mark_ts_passed(spec_id: str, ts_id: str) -> None:
    from sqlalchemy_test_models import Spec

    async with get_session_factory()() as db:
        spec = await db.get(Spec, spec_id)
        for ts in (spec.test_scenarios or []):
            if ts.get("id") == ts_id:
                ts["status"] = "passed"
        flag_modified(spec, "test_scenarios")
        await db.commit()


async def _mark_card_done(card_id: str) -> None:
    from sqlalchemy_test_models import Card, CardStatus

    async with get_session_factory()() as db:
        card = await db.get(Card, card_id)
        card.status = CardStatus.DONE
        await db.commit()


async def _create_sprint(board_id: str, spec_id: str, **kwargs) -> str:
    from okto_pulse.core.models.schemas import SprintCreate
    from okto_pulse.core.services.main import SprintService

    async with get_session_factory()() as db:
        sprint = await SprintService(db).create_sprint(
            board_id, USER, SprintCreate(title="seed sprint", spec_id=spec_id, **kwargs)
        )
        await db.commit()
        return sprint.id


# --- list / create ----------------------------------------------------------


@pytest.mark.asyncio
async def test_list_board_sprints_200(client) -> None:
    bid = await _seed_board()
    sid = await _seed_spec(bid)
    sprint_id = await _create_sprint(bid, sid)

    resp = client.get(f"{PREFIX}/boards/{bid}/sprints")
    assert resp.status_code == 200, resp.text
    assert sprint_id in {s["id"] for s in resp.json()}


@pytest.mark.asyncio
async def test_create_sprint_201_refetched_body(client) -> None:
    bid = await _seed_board()
    sid = await _seed_spec(bid)

    resp = client.post(
        f"{PREFIX}/boards/{bid}/specs/{sid}/sprints",
        json={"title": "Created Sprint", "spec_id": sid},
    )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["title"] == "Created Sprint"
    assert body["spec_id"] == sid
    assert body["status"] == "draft"


@pytest.mark.asyncio
async def test_create_sprint_missing_spec_404(client) -> None:
    bid = await _seed_board()
    missing_spec = _missing("spec")
    resp = client.post(
        f"{PREFIX}/boards/{bid}/specs/{missing_spec}/sprints",
        json={"title": "Nope", "spec_id": missing_spec},
    )
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Spec or board not found"


@pytest.mark.asyncio
async def test_create_sprint_invalid_test_scenarios_400(client) -> None:
    bid = await _seed_board()
    sid = await _seed_spec(bid)
    resp = client.post(
        f"{PREFIX}/boards/{bid}/specs/{sid}/sprints",
        json={"title": "Bad TS", "spec_id": sid, "test_scenario_ids": ["nonexistent-ts"]},
    )
    assert resp.status_code == 400, resp.text
    assert "Test scenario IDs not found in spec" in str(resp.json()["detail"])


@pytest.mark.asyncio
async def test_list_sprints_by_spec_200(client) -> None:
    bid = await _seed_board()
    sid = await _seed_spec(bid)
    sprint_id = await _create_sprint(bid, sid)

    resp = client.get(f"{PREFIX}/boards/{bid}/specs/{sid}/sprints")
    assert resp.status_code == 200, resp.text
    assert sprint_id in {s["id"] for s in resp.json()}


# --- get / update -----------------------------------------------------------


@pytest.mark.asyncio
async def test_get_sprint_200_and_missing_404(client) -> None:
    bid = await _seed_board()
    sid = await _seed_spec(bid)
    sprint_id = await _create_sprint(bid, sid)

    ok = client.get(f"{PREFIX}/sprints/{sprint_id}")
    assert ok.status_code == 200, ok.text
    assert ok.json()["id"] == sprint_id

    miss = client.get(f"{PREFIX}/sprints/{_missing()}")
    assert miss.status_code == 404
    assert miss.json()["detail"] == "Sprint not found"


@pytest.mark.asyncio
async def test_update_sprint_200_and_missing_404(client) -> None:
    bid = await _seed_board()
    sid = await _seed_spec(bid)
    sprint_id = await _create_sprint(bid, sid)

    ok = client.patch(f"{PREFIX}/sprints/{sprint_id}", json={"title": "Renamed Sprint"})
    assert ok.status_code == 200, ok.text
    assert ok.json()["title"] == "Renamed Sprint"

    miss = client.patch(f"{PREFIX}/sprints/{_missing()}", json={"title": "x"})
    assert miss.status_code == 404
    assert miss.json()["detail"] == "Sprint not found"


# --- move (state-machine gates) ---------------------------------------------


@pytest.mark.asyncio
async def test_move_sprint_invalid_transition_400(client) -> None:
    bid = await _seed_board()
    sid = await _seed_spec(bid)
    sprint_id = await _create_sprint(bid, sid)

    # draft → review is not allowed (must go through active).
    resp = client.post(f"{PREFIX}/sprints/{sprint_id}/move", json={"status": "review"})
    assert resp.status_code == 400, resp.text
    assert "Cannot move sprint" in str(resp.json()["detail"])


@pytest.mark.asyncio
async def test_move_sprint_active_without_cards_400(client) -> None:
    bid = await _seed_board()
    sid = await _seed_spec(bid)
    sprint_id = await _create_sprint(bid, sid)

    resp = client.post(f"{PREFIX}/sprints/{sprint_id}/move", json={"status": "active"})
    assert resp.status_code == 400, resp.text
    assert "no cards assigned" in str(resp.json()["detail"])


@pytest.mark.asyncio
async def test_move_sprint_missing_404(client) -> None:
    resp = client.post(f"{PREFIX}/sprints/{_missing()}/move", json={"status": "active"})
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Sprint not found"


# --- delete -----------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_sprint_204_then_404(client) -> None:
    bid = await _seed_board()
    sid = await _seed_spec(bid)
    sprint_id = await _create_sprint(bid, sid)

    resp = client.delete(f"{PREFIX}/sprints/{sprint_id}")
    assert resp.status_code == 204, resp.text
    gone = client.delete(f"{PREFIX}/sprints/{sprint_id}")
    assert gone.status_code == 404
    assert gone.json()["detail"] == "Sprint not found"
    assert client.get(f"{PREFIX}/sprints/{sprint_id}").status_code == 404


@pytest.mark.asyncio
async def test_delete_sprint_missing_404(client) -> None:
    resp = client.delete(f"{PREFIX}/sprints/{_missing()}")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Sprint not found"


# --- assign / unassign ------------------------------------------------------


@pytest.mark.asyncio
async def test_assign_tasks_200_lane_envelope(client) -> None:
    bid = await _seed_board()
    sid = await _seed_spec(bid)
    card_id = await _seed_card(bid, sid)
    sprint_id = await _create_sprint(bid, sid)

    resp = client.post(f"{PREFIX}/sprints/{sprint_id}/assign-tasks", json={"card_ids": [card_id]})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["success"] is True
    assert body["assigned"] == 1
    assert body["assigned_count"] == 1
    assert body["lane_type"] == "normal"
    assert body["accepted_card_types"] == ["normal", "test", "bug"]


@pytest.mark.asyncio
async def test_assign_tasks_card_ids_required_400(client) -> None:
    bid = await _seed_board()
    sid = await _seed_spec(bid)
    sprint_id = await _create_sprint(bid, sid)

    resp = client.post(f"{PREFIX}/sprints/{sprint_id}/assign-tasks", json={})
    assert resp.status_code == 400
    assert resp.json()["detail"] == "card_ids required"


@pytest.mark.asyncio
async def test_assign_tasks_missing_card_400(client) -> None:
    bid = await _seed_board()
    sid = await _seed_spec(bid)
    sprint_id = await _create_sprint(bid, sid)

    resp = client.post(
        f"{PREFIX}/sprints/{sprint_id}/assign-tasks",
        json={"card_ids": [_missing()]},
    )
    assert resp.status_code == 400, resp.text
    detail = resp.json()["detail"]
    assert detail["code"] == "card_not_found"
    assert detail["facts"]["card_id"]


@pytest.mark.asyncio
async def test_assign_tasks_cross_spec_400(client) -> None:
    bid = await _seed_board()
    sid = await _seed_spec(bid)
    other_spec = await _seed_spec(bid)
    other_card = await _seed_card(bid, other_spec)
    sprint_id = await _create_sprint(bid, sid)

    resp = client.post(f"{PREFIX}/sprints/{sprint_id}/assign-tasks", json={"card_ids": [other_card]})
    assert resp.status_code == 400, resp.text
    assert "different spec" in str(resp.json()["detail"])


@pytest.mark.asyncio
async def test_unassign_tasks_clears_and_counts(client) -> None:
    bid = await _seed_board()
    sid = await _seed_spec(bid)
    card_id = await _seed_card(bid, sid)
    sprint_id = await _create_sprint(bid, sid)

    assigned = client.post(
        f"{PREFIX}/sprints/{sprint_id}/assign-tasks", json={"card_ids": [card_id]}
    )
    assert assigned.status_code == 200, assigned.text

    resp = client.post(
        f"{PREFIX}/sprints/{sprint_id}/unassign-tasks", json={"card_ids": [card_id]}
    )
    assert resp.status_code == 200, resp.text
    assert resp.json() == {"success": True, "unassigned": 1}

    # the clear really committed: a second unassign of the same card counts 0
    # (it no longer belongs to the sprint).
    again = client.post(
        f"{PREFIX}/sprints/{sprint_id}/unassign-tasks", json={"card_ids": [card_id]}
    )
    assert again.status_code == 200, again.text
    assert again.json()["unassigned"] == 0


@pytest.mark.asyncio
async def test_unassign_tasks_card_ids_required_400(client) -> None:
    bid = await _seed_board()
    sid = await _seed_spec(bid)
    sprint_id = await _create_sprint(bid, sid)

    resp = client.post(f"{PREFIX}/sprints/{sprint_id}/unassign-tasks", json={})
    assert resp.status_code == 400
    assert resp.json()["detail"] == "card_ids required"


# --- submit-evaluation wrong status + history + suggest ----------------------


@pytest.mark.asyncio
async def test_submit_evaluation_wrong_status_400_and_missing_404(client) -> None:
    bid = await _seed_board()
    sid = await _seed_spec(bid)
    sprint_id = await _create_sprint(bid, sid)  # draft, not review

    evaluation = {
        "breakdown_completeness": 90,
        "breakdown_justification": "ok",
        "granularity": 90,
        "granularity_justification": "ok",
        "dependency_coherence": 90,
        "dependency_justification": "ok",
        "test_coverage_quality": 90,
        "test_coverage_justification": "ok",
        "overall_score": 90,
        "overall_justification": "ok",
        "recommendation": "approve",
    }
    wrong = client.post(f"{PREFIX}/sprints/{sprint_id}/evaluations", json=evaluation)
    assert wrong.status_code == 400, wrong.text
    assert "review" in str(wrong.json()["detail"])

    miss = client.post(f"{PREFIX}/sprints/{_missing()}/evaluations", json=evaluation)
    assert miss.status_code == 404
    assert miss.json()["detail"] == "Sprint not found"


@pytest.mark.asyncio
async def test_list_history_200(client) -> None:
    bid = await _seed_board()
    sid = await _seed_spec(bid)
    sprint_id = await _create_sprint(bid, sid)

    resp = client.get(f"{PREFIX}/sprints/{sprint_id}/history")
    assert resp.status_code == 200, resp.text
    assert isinstance(resp.json(), list)
    # creation recorded one history row.
    assert any(h.get("action") == "created" for h in resp.json())


@pytest.mark.asyncio
async def test_suggest_sprints_200(client) -> None:
    bid = await _seed_board()
    sid = await _seed_spec(bid)

    resp = client.get(f"{PREFIX}/boards/{bid}/specs/{sid}/sprints/suggest")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "suggestions" in body
    assert body["count"] == len(body["suggestions"])


# --- full lifecycle through the migrated surface ----------------------------


@pytest.mark.asyncio
async def test_full_lifecycle_active_review_evaluate_close(client) -> None:
    """Drive assign → active → (TS passed) → review → submit-evaluation
    (``evaluation_id`` envelope) → (card done) → closed entirely through the
    migrated REST surface, proving the sprint state/coverage/evaluation gates
    survive the strangle."""
    ts_id = f"ts-{uuid.uuid4().hex[:8]}"
    bid = await _seed_board()
    sid = await _seed_spec(bid, ts_id=ts_id)
    card_id = await _seed_card(bid, sid)
    sprint_id = await _create_sprint(bid, sid, test_scenario_ids=[ts_id])

    assert client.post(
        f"{PREFIX}/sprints/{sprint_id}/assign-tasks", json={"card_ids": [card_id]}
    ).status_code == 200

    active = client.post(f"{PREFIX}/sprints/{sprint_id}/move", json={"status": "active"})
    assert active.status_code == 200, active.text
    assert active.json()["status"] == "active"

    await _mark_ts_passed(sid, ts_id)

    review = client.post(f"{PREFIX}/sprints/{sprint_id}/move", json={"status": "review"})
    assert review.status_code == 200, review.text
    assert review.json()["status"] == "review"

    evaluation = {
        "breakdown_completeness": 90,
        "breakdown_justification": "Tasks cover the sprint well",
        "granularity": 85,
        "granularity_justification": "Tasks are properly sized",
        "dependency_coherence": 80,
        "dependency_justification": "Dependencies make sense",
        "test_coverage_quality": 85,
        "test_coverage_justification": "Tests cover happy path",
        "overall_score": 85,
        "overall_justification": "Good overall quality",
        "recommendation": "approve",
    }
    submitted = client.post(f"{PREFIX}/sprints/{sprint_id}/evaluations", json=evaluation)
    assert submitted.status_code == 200, submitted.text
    sbody = submitted.json()
    assert sbody["success"] is True
    assert sbody["evaluation_id"]

    await _mark_card_done(card_id)

    closed = client.post(f"{PREFIX}/sprints/{sprint_id}/move", json={"status": "closed"})
    assert closed.status_code == 200, closed.text
    assert closed.json()["status"] == "closed"


# --- use case + AST ---------------------------------------------------------


@pytest.mark.asyncio
async def test_get_sprint_use_case_raises_for_missing_sprint() -> None:
    from okto_pulse.core.application.use_cases.base import ActorContext, EntityNotFoundError
    from okto_pulse.core.application.use_cases.sprints_crud import (
        GetSprintCommand,
        GetSprintUseCase,
    )
    from sqlalchemy_test_unit_of_work import SQLAlchemyUnitOfWorkFactory
    uowf = SQLAlchemyUnitOfWorkFactory(get_session_factory())
    actor = ActorContext(USER, "rest", realm_id=LOCAL_REALM_ID)
    with pytest.raises(EntityNotFoundError):
        async with uowf(actor=actor) as uow:
            await GetSprintUseCase().execute(
                GetSprintCommand(_missing()), actor=actor, uow=uow
            )


def test_fu7_s2_endpoints_take_uow_not_raw_session() -> None:
    for name in _ENDPOINTS:
        sig = inspect.signature(getattr(sprints_api, name))
        assert "db" not in sig.parameters, name
        assert "uow" in sig.parameters, name
        assert sig.parameters["uow"].default.dependency is get_unit_of_work, name


def test_sprints_router_has_no_endpoint_on_get_db() -> None:
    """FU7-S2 closes api/sprints.py: no REGISTERED route endpoint may depend on
    get_db / a raw AsyncSession anymore."""
    from okto_pulse.core.infra.database import get_db as _get_db

    checked = 0
    for route in sprints_router.routes:
        endpoint = getattr(route, "endpoint", None)
        if endpoint is None:
            continue
        checked += 1
        sig = inspect.signature(endpoint)
        assert "db" not in sig.parameters, f"{endpoint.__name__} still takes a db session"
        for param in sig.parameters.values():
            dep = getattr(param.default, "dependency", None)
            assert dep is not _get_db, f"{endpoint.__name__} still depends on get_db"
    assert checked > 0
