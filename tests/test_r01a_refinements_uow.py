"""Spec R01A REST-FU6-S3 — Refinements REST on the UnitOfWork.

Every ``api/refinements.py`` endpoint (refinement create/list/get/update/move/
delete/derive-spec/history; Q&A list/create/answer/delete; snapshot list/get;
knowledge list/get/create/delete) now routes through a transport-free use case
(``application/use_cases/refinements_crud.py``) + ``get_unit_of_work`` instead of
a raw ``AsyncSession``. The legacy behavior preserved here, end-to-end through
``TestClient`` (commit really persists across requests):

* refinement lifecycle create (+ done-required 400, missing-ideation 404) → list
  (+ "Ideation not found" 404) → get (+ 404) → update (+ 404) → move invalid
  transition 400 + missing 404 → delete (persisted, second delete 404)
* derive-spec done-gate 400 + missing 404 (the spec-derivation gate stays in the
  service)
* Q&A list → create (+ missing-refinement 404) → answer (happy, non-self-answer)
  + missing-Q&A 404 → delete (persisted, second delete 404)
* snapshot list + the ``Snapshot v{version} not found`` 404
* knowledge list → create (+ missing-refinement 404) → get (+ 404, + cross-refinement
  404) → delete (persisted, second delete 404)
* a direct use-case assertion (``GetRefinementUseCase`` raises
  ``EntityNotFoundError``), an AST signature guard proving every endpoint — and
  every registered route — takes ``uow`` (``get_unit_of_work``), not a raw
  ``AsyncSession``, and a Clean Core guard proving the use case module imports no
  ``okto_pulse.community.api`` and exposes no ``AsyncSession``/``select``/``get_db``.

No agent row is seeded for the test user, and the board is owned by the user, so
the refinement transition + critical-context gates resolve permissively; the
gate behavior under test is the transition/done ``ValueError`` mapping, which is
governance-independent.
"""

from __future__ import annotations

import inspect
import uuid

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.community.api import refinements as refinements_api
from okto_pulse.community.api.deps import get_unit_of_work
from okto_pulse.community.api.refinements import router as refinements_router
from okto_pulse.community.api.auth_deps import get_realm_id, require_user
from okto_pulse.core.domain.realm import LOCAL_REALM_ID
from okto_pulse.core.infra.database import get_db, get_session_factory
from knowledge_governance_test_data import valid_governance_metadata

USER = "r01a-fu6-s3-user"
OTHER = "r01a-fu6-s3-other"
PREFIX = "/api/v1"

_ENDPOINTS = (
    "create_refinement",
    "list_refinements",
    "get_refinement",
    "update_refinement",
    "move_refinement",
    "delete_refinement",
    "derive_spec",
    "list_refinement_history",
    "list_refinement_qa",
    "create_refinement_question",
    "answer_refinement_question",
    "delete_refinement_question",
    "list_refinement_snapshots",
    "get_refinement_snapshot",
    "list_refinement_knowledge",
    "get_refinement_knowledge",
    "create_refinement_knowledge",
    "delete_refinement_knowledge",
)


@pytest.fixture
def client():
    app = FastAPI()
    app.include_router(refinements_router, prefix=PREFIX)
    session_factory = get_session_factory()

    async def _override_db():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[require_user] = lambda: USER
    app.dependency_overrides[get_realm_id] = lambda: LOCAL_REALM_ID
    return TestClient(app)


def _missing() -> str:
    return f"missing-{uuid.uuid4().hex[:8]}"


async def _seed_ideation(*, status: str = "done", owner: str = USER) -> tuple[str, str]:
    """Seed a Board (owned by ``owner``) + an Ideation in ``status``. Returns
    ``(board_id, ideation_id)``."""
    from sqlalchemy_test_models import Board, Ideation, IdeationStatus

    board_id = f"board-fu6s3-{uuid.uuid4().hex[:8]}"
    ideation_id = f"ideation-fu6s3-{uuid.uuid4().hex[:8]}"
    async with get_session_factory()() as db:
        db.add(
            Board(
                id=board_id,
                name="fu6s3",
                owner_id=owner,
                realm_id=LOCAL_REALM_ID,
            )
        )
        db.add(
            Ideation(
                id=ideation_id,
                board_id=board_id,
                title=f"ideation-{uuid.uuid4().hex[:6]}",
                description="seeded ideation body",
                status=IdeationStatus(status),
                created_by=owner,
            )
        )
        await db.commit()
    return board_id, ideation_id


async def _seed_refinement(ideation_id: str) -> str:
    from okto_pulse.core.models.schemas import RefinementCreate
    from okto_pulse.core.services import RefinementService

    async with get_session_factory()() as db:
        refinement = await RefinementService(db).create_refinement(
            ideation_id,
            USER,
            RefinementCreate(ideation_id=ideation_id, title=f"refinement-{uuid.uuid4().hex[:6]}"),
        )
        await db.commit()
        return refinement.id


async def _seed_question(refinement_id: str, *, asked_by: str = OTHER) -> str:
    from okto_pulse.core.models.schemas import RefinementQACreate
    from okto_pulse.core.services import RefinementQAService

    async with get_session_factory()() as db:
        qa = await RefinementQAService(db).create_question(
            refinement_id, asked_by, RefinementQACreate(question="Why this scope?")
        )
        await db.commit()
        return qa.id


async def _seed_knowledge(refinement_id: str) -> str:
    from okto_pulse.core.models.schemas import RefinementKnowledgeCreate
    from okto_pulse.core.services import RefinementKnowledgeService

    async with get_session_factory()() as db:
        kb = await RefinementKnowledgeService(db).create_knowledge(
            refinement_id,
            USER,
            RefinementKnowledgeCreate(title="KB", content="kb body"),
        )
        await db.commit()
        return kb.id


async def _refinement_exists(refinement_id: str) -> bool:
    from okto_pulse.core.services import RefinementService

    async with get_session_factory()() as db:
        return await RefinementService(db).get_refinement(refinement_id) is not None


# --- create / list / get ----------------------------------------------------


@pytest.mark.asyncio
async def test_create_refinement_201_persists(client) -> None:
    _, ideation_id = await _seed_ideation(status="done")
    resp = client.post(
        f"{PREFIX}/ideations/{ideation_id}/refinements",
        json={"ideation_id": ideation_id, "title": "Refinement A"},
    )
    assert resp.status_code == 201, resp.text
    rid = resp.json()["id"]
    assert await _refinement_exists(rid)


@pytest.mark.asyncio
async def test_create_refinement_400_when_ideation_not_done(client) -> None:
    _, ideation_id = await _seed_ideation(status="draft")
    resp = client.post(
        f"{PREFIX}/ideations/{ideation_id}/refinements",
        json={"ideation_id": ideation_id, "title": "x"},
    )
    assert resp.status_code == 400, resp.text


@pytest.mark.asyncio
async def test_create_refinement_404_missing_ideation(client) -> None:
    ideation_id = _missing()
    resp = client.post(
        f"{PREFIX}/ideations/{ideation_id}/refinements",
        json={"ideation_id": ideation_id, "title": "x"},
    )
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Ideation not found or board not owned by user"


@pytest.mark.asyncio
async def test_list_refinements_200_and_404_missing_ideation(client) -> None:
    _, ideation_id = await _seed_ideation(status="done")
    rid = await _seed_refinement(ideation_id)

    ok = client.get(f"{PREFIX}/ideations/{ideation_id}/refinements")
    assert ok.status_code == 200, ok.text
    assert rid in {r["id"] for r in ok.json()}

    miss = client.get(f"{PREFIX}/ideations/{_missing()}/refinements")
    assert miss.status_code == 404
    assert miss.json()["detail"] == "Ideation not found"


@pytest.mark.asyncio
async def test_get_refinement_200_and_404(client) -> None:
    _, ideation_id = await _seed_ideation(status="done")
    rid = await _seed_refinement(ideation_id)

    ok = client.get(f"{PREFIX}/refinements/{rid}")
    assert ok.status_code == 200, ok.text
    assert ok.json()["id"] == rid

    miss = client.get(f"{PREFIX}/refinements/{_missing()}")
    assert miss.status_code == 404
    assert miss.json()["detail"] == "Refinement not found"


# --- update / move / delete -------------------------------------------------


@pytest.mark.asyncio
async def test_update_refinement_200_persists_and_404(client) -> None:
    _, ideation_id = await _seed_ideation(status="done")
    rid = await _seed_refinement(ideation_id)

    ok = client.patch(f"{PREFIX}/refinements/{rid}", json={"title": "renamed"})
    assert ok.status_code == 200, ok.text
    assert ok.json()["title"] == "renamed"
    assert ok.json()["version"] == 2

    scoped = client.patch(
        f"{PREFIX}/refinements/{rid}",
        json={"in_scope": ["included"], "out_of_scope": ["excluded"]},
    )
    assert scoped.status_code == 200, scoped.text
    assert scoped.json()["version"] == 3
    assert scoped.json()["in_scope"] == ["included"]
    assert scoped.json()["out_of_scope"] == ["excluded"]
    # persisted across a fresh request
    persisted = client.get(f"{PREFIX}/refinements/{rid}").json()
    assert persisted["title"] == "renamed"
    assert persisted["version"] == 3

    miss = client.patch(f"{PREFIX}/refinements/{_missing()}", json={"title": "x"})
    assert miss.status_code == 404
    assert miss.json()["detail"] == "Refinement not found"


@pytest.mark.asyncio
async def test_move_refinement_400_invalid_transition_and_404(client) -> None:
    _, ideation_id = await _seed_ideation(status="done")
    rid = await _seed_refinement(ideation_id)

    # draft → done is not an allowed transition → service ValueError → 400.
    bad = client.post(f"{PREFIX}/refinements/{rid}/move", json={"status": "done"})
    assert bad.status_code == 400, bad.text

    miss = client.post(f"{PREFIX}/refinements/{_missing()}/move", json={"status": "review"})
    assert miss.status_code == 404
    assert miss.json()["detail"] == "Refinement not found"


@pytest.mark.asyncio
async def test_delete_refinement_204_persists_and_404(client) -> None:
    _, ideation_id = await _seed_ideation(status="done")
    rid = await _seed_refinement(ideation_id)

    resp = client.delete(f"{PREFIX}/refinements/{rid}")
    assert resp.status_code == 204, resp.text
    assert not await _refinement_exists(rid)
    # delete really committed: a second delete is a 404.
    second = client.delete(f"{PREFIX}/refinements/{rid}")
    assert second.status_code == 404
    assert second.json()["detail"] == "Refinement not found"


# --- derive spec ------------------------------------------------------------


@pytest.mark.asyncio
async def test_derive_spec_400_when_refinement_not_done_and_404(client) -> None:
    _, ideation_id = await _seed_ideation(status="done")
    rid = await _seed_refinement(ideation_id)  # draft

    bad = client.post(f"{PREFIX}/refinements/{rid}/derive-spec")
    assert bad.status_code == 400, bad.text

    miss = client.post(f"{PREFIX}/refinements/{_missing()}/derive-spec")
    assert miss.status_code == 404
    assert miss.json()["detail"] == "Refinement not found"


# --- history ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_refinement_history_200(client) -> None:
    _, ideation_id = await _seed_ideation(status="done")
    rid = await _seed_refinement(ideation_id)
    resp = client.get(f"{PREFIX}/refinements/{rid}/history")
    assert resp.status_code == 200, resp.text
    assert isinstance(resp.json(), list)


# --- Q&A --------------------------------------------------------------------


@pytest.mark.asyncio
async def test_refinement_qa_create_list_answer_delete(client) -> None:
    _, ideation_id = await _seed_ideation(status="done")
    rid = await _seed_refinement(ideation_id)

    created = client.post(
        f"{PREFIX}/refinements/{rid}/qa", json={"question": "Why?"}
    )
    assert created.status_code == 201, created.text
    qa_id = created.json()["id"]

    listed = client.get(f"{PREFIX}/refinements/{rid}/qa")
    assert listed.status_code == 200
    assert qa_id in {q["id"] for q in listed.json()}

    deleted = client.delete(f"{PREFIX}/refinements/{rid}/qa/{qa_id}")
    assert deleted.status_code == 204, deleted.text
    # delete committed: a second delete is a 404 "Q&A item not found".
    second = client.delete(f"{PREFIX}/refinements/{rid}/qa/{qa_id}")
    assert second.status_code == 404
    assert second.json()["detail"] == "Q&A item not found"


@pytest.mark.asyncio
async def test_answer_refinement_question_200_persists(client) -> None:
    _, ideation_id = await _seed_ideation(status="done")
    rid = await _seed_refinement(ideation_id)
    # question asked by OTHER so USER answering it is NOT a self-answer.
    qa_id = await _seed_question(rid, asked_by=OTHER)

    resp = client.post(
        f"{PREFIX}/refinements/{rid}/qa/{qa_id}/answer", json={"answer": "Because scope."}
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["answer"] == "Because scope."
    # persisted across a fresh request
    listed = client.get(f"{PREFIX}/refinements/{rid}/qa").json()
    assert any(q["id"] == qa_id and q["answer"] == "Because scope." for q in listed)


@pytest.mark.asyncio
async def test_create_refinement_question_404_missing_refinement(client) -> None:
    resp = client.post(f"{PREFIX}/refinements/{_missing()}/qa", json={"question": "Why?"})
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Refinement not found"


@pytest.mark.asyncio
async def test_answer_refinement_question_404_missing_qa(client) -> None:
    _, ideation_id = await _seed_ideation(status="done")
    rid = await _seed_refinement(ideation_id)
    resp = client.post(
        f"{PREFIX}/refinements/{rid}/qa/{_missing()}/answer", json={"answer": "x"}
    )
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Q&A item not found"


# --- snapshots --------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_refinement_snapshots_200(client) -> None:
    _, ideation_id = await _seed_ideation(status="done")
    rid = await _seed_refinement(ideation_id)
    resp = client.get(f"{PREFIX}/refinements/{rid}/snapshots")
    assert resp.status_code == 200, resp.text
    assert isinstance(resp.json(), list)


@pytest.mark.asyncio
async def test_get_refinement_snapshot_404(client) -> None:
    _, ideation_id = await _seed_ideation(status="done")
    rid = await _seed_refinement(ideation_id)
    resp = client.get(f"{PREFIX}/refinements/{rid}/snapshots/99")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Snapshot v99 not found"


# --- knowledge --------------------------------------------------------------


@pytest.mark.asyncio
async def test_refinement_knowledge_create_list_get_delete(client) -> None:
    _, ideation_id = await _seed_ideation(status="done")
    rid = await _seed_refinement(ideation_id)

    created = client.post(
        f"{PREFIX}/refinements/{rid}/knowledge",
        json={"title": "KB A", "content": "body"},
    )
    assert created.status_code == 201, created.text
    kid = created.json()["id"]
    assert created.json()["governance"]["metadata_status"] == "legacy_incomplete"

    listed = client.get(f"{PREFIX}/refinements/{rid}/knowledge")
    assert listed.status_code == 200
    assert kid in {k["id"] for k in listed.json()}

    got = client.get(f"{PREFIX}/refinements/{rid}/knowledge/{kid}")
    assert got.status_code == 200, got.text
    assert got.json()["content"] == "body"

    deleted = client.delete(f"{PREFIX}/refinements/{rid}/knowledge/{kid}")
    assert deleted.status_code == 204, deleted.text
    # delete committed: a second delete is a 404.
    second = client.delete(f"{PREFIX}/refinements/{rid}/knowledge/{kid}")
    assert second.status_code == 404
    assert second.json()["detail"] == "Knowledge base item not found"


@pytest.mark.asyncio
async def test_refinement_governance_round_trip_and_invalid_write(client) -> None:
    _, ideation_id = await _seed_ideation(status="done")
    rid = await _seed_refinement(ideation_id)
    metadata = valid_governance_metadata()

    created = client.post(
        f"{PREFIX}/refinements/{rid}/knowledge",
        json={"title": "Governed", "content": "body", "governance_metadata": metadata},
    )
    assert created.status_code == 201, created.text
    assert created.json()["governance"]["metadata"] == metadata
    kid = created.json()["id"]
    assert client.get(
        f"{PREFIX}/refinements/{rid}/knowledge/{kid}"
    ).json()["governance"] == created.json()["governance"]

    rejected = client.post(
        f"{PREFIX}/refinements/{rid}/knowledge",
        json={"title": "Invalid", "content": "body", "governance_metadata": {}},
    )
    assert rejected.status_code == 422
    assert rejected.json()["code"] == "knowledge_governance_invalid_metadata"


@pytest.mark.asyncio
async def test_create_refinement_knowledge_404_missing_refinement(client) -> None:
    resp = client.post(
        f"{PREFIX}/refinements/{_missing()}/knowledge",
        json={"title": "KB", "content": "body"},
    )
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Refinement not found"


@pytest.mark.asyncio
async def test_get_refinement_knowledge_404_missing_and_cross_refinement(client) -> None:
    _, ideation_id = await _seed_ideation(status="done")
    rid = await _seed_refinement(ideation_id)
    other_rid = await _seed_refinement(ideation_id)
    kid = await _seed_knowledge(rid)

    miss = client.get(f"{PREFIX}/refinements/{rid}/knowledge/{_missing()}")
    assert miss.status_code == 404
    assert miss.json()["detail"] == "Knowledge base item not found"

    # the kb exists but belongs to a different refinement → 404 (cross-check)
    cross = client.get(f"{PREFIX}/refinements/{other_rid}/knowledge/{kid}")
    assert cross.status_code == 404
    assert cross.json()["detail"] == "Knowledge base item not found"


# --- use case + AST + Clean Core --------------------------------------------


@pytest.mark.asyncio
async def test_get_refinement_use_case_raises_for_missing_refinement() -> None:
    from okto_pulse.core.application.use_cases.base import ActorContext, EntityNotFoundError
    from okto_pulse.core.application.use_cases.refinements_crud import (
        GetRefinementCommand,
        GetRefinementUseCase,
    )
    from sqlalchemy_test_unit_of_work import SQLAlchemyUnitOfWorkFactory
    uowf = SQLAlchemyUnitOfWorkFactory(get_session_factory())
    actor = ActorContext(USER, "rest", realm_id=LOCAL_REALM_ID)
    with pytest.raises(EntityNotFoundError):
        async with uowf(actor=actor) as uow:
            await GetRefinementUseCase().execute(
                GetRefinementCommand(_missing()), actor=actor, uow=uow
            )


def test_fu6_s3_endpoints_take_uow_not_raw_session() -> None:
    for name in _ENDPOINTS:
        sig = inspect.signature(getattr(refinements_api, name))
        assert "db" not in sig.parameters, name
        assert "uow" in sig.parameters, name
        assert sig.parameters["uow"].default.dependency is get_unit_of_work, name


def test_refinements_router_has_no_endpoint_on_get_db() -> None:
    """FU6-S3 closes api/refinements.py: no REGISTERED route endpoint may depend on
    get_db / a raw AsyncSession anymore."""
    from okto_pulse.core.infra.database import get_db as _get_db

    checked = 0
    for route in refinements_router.routes:
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


def test_refinements_use_case_module_has_no_api_or_session_in_public_surface() -> None:
    """Clean Core: the use case module imports no okto_pulse.community.api and exposes
    no AsyncSession/select/get_db in its source."""
    import ast
    from pathlib import Path

    from okto_pulse.core.application.use_cases import refinements_crud

    src = Path(refinements_crud.__file__).read_text(encoding="utf-8")
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            assert not (node.module or "").startswith("okto_pulse.community.api"), node.module
    names = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}
    assert "AsyncSession" not in names
    assert "get_db" not in names
    assert "select" not in names
