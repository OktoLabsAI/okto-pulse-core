"""Spec R01A REST-FU3d-S4 — spec knowledge base / Q&A / evaluation on the UoW.

This card CLOSES ``api/specs.py``: after it, no endpoint depends on ``get_db``.
The ten endpoints (knowledge list/get/create/delete, qa list/create/answer/delete,
evaluation submit/list) now route through dedicated transport-free use cases +
``get_unit_of_work``; the adapter only maps the typed ``EntityNotFoundError``
back to each legacy 404 detail and re-maps the QA self-answer / gate / value
errors. The legacy behavior preserved here, end-to-end through ``TestClient``
(commit really persists across requests):

* knowledge create→get→list→delete lifecycle + the cross-spec / missing-spec 404s
* qa create (+ missing-spec 404), list, answer success, answer self-answer 403
  (with the ``{reason, message}`` detail and the commit-on-error side-effect),
  answer missing 404, delete + delete-missing 404
* evaluation submit missing-spec 404 + not-validated 409 (the propagated
  ``ValueError``), list 200 + missing 404
* a direct use-case assertion + an AST signature guard over all ten endpoints.
"""

from __future__ import annotations

import inspect
import uuid

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.core.api import specs as specs_api
from okto_pulse.core.api.deps import get_unit_of_work
from okto_pulse.core.api.specs import router as specs_router
from okto_pulse.core.infra.auth import require_user
from okto_pulse.core.infra.database import get_db, get_session_factory

USER = "r01a-fu3d-s4-user"
OTHER = "r01a-fu3d-s4-other"
PREFIX = "/api/v1"

_ENDPOINTS = (
    "list_spec_knowledge",
    "get_spec_knowledge",
    "create_spec_knowledge",
    "delete_spec_knowledge",
    "list_spec_qa",
    "create_spec_question",
    "answer_spec_question",
    "delete_spec_question",
    "submit_spec_evaluation",
    "list_spec_evaluations",
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


async def _seed_spec() -> str:
    from okto_pulse.core.models.db import Board
    from okto_pulse.core.models.schemas import SpecCreate
    from okto_pulse.core.services import SpecService

    bid = f"board-fu3ds4-{uuid.uuid4().hex[:8]}"
    async with get_session_factory()() as db:
        db.add(Board(id=bid, name="fu3ds4", owner_id=USER))
        await db.commit()
    async with get_session_factory()() as db:
        spec = await SpecService(db).create_spec(
            bid, USER, SpecCreate(title=f"fu3ds4-{uuid.uuid4().hex[:6]}")
        )
        await db.commit()
        return spec.id


async def _seed_question(spec_id: str, asked_by: str) -> str:
    """Insert a raw Q&A item so the answerer differs from the asker (a
    successful answer requires asked_by != answered_by under the default
    self-answer policy)."""
    from okto_pulse.core.models.db import SpecQAItem

    async with get_session_factory()() as db:
        qa = SpecQAItem(
            spec_id=spec_id,
            question="seeded?",
            question_type="text",
            asked_by=asked_by,
        )
        db.add(qa)
        await db.commit()
        return qa.id


def _missing() -> str:
    return f"missing-{uuid.uuid4().hex[:8]}"


def _kb_payload() -> dict:
    return {"title": "KB one", "content": "# body", "mime_type": "text/markdown"}


def _eval_payload() -> dict:
    return {
        "breakdown_completeness": 90,
        "breakdown_justification": "ACs are fully covered",
        "granularity": 85,
        "granularity_justification": "cards are right-sized",
        "dependency_coherence": 80,
        "dependency_justification": "deps form a clean DAG",
        "test_coverage_quality": 88,
        "test_coverage_justification": "scenarios map to ACs",
        "overall_score": 86,
        "overall_justification": "ready for execution",
        "recommendation": "approve",
    }


# --- knowledge --------------------------------------------------------------


@pytest.mark.asyncio
async def test_knowledge_lifecycle_create_get_list_delete(client) -> None:
    spec_id = await _seed_spec()

    created = client.post(f"{PREFIX}/specs/{spec_id}/knowledge", json=_kb_payload())
    assert created.status_code == 201, created.text
    kb_id = created.json()["id"]

    got = client.get(f"{PREFIX}/specs/{spec_id}/knowledge/{kb_id}")
    assert got.status_code == 200, got.text
    assert got.json()["content"] == "# body"

    listed = client.get(f"{PREFIX}/specs/{spec_id}/knowledge")
    assert listed.status_code == 200
    assert kb_id in {item["id"] for item in listed.json()}

    deleted = client.delete(f"{PREFIX}/specs/{spec_id}/knowledge/{kb_id}")
    assert deleted.status_code == 204
    # delete really committed: the item is gone on a fresh request.
    assert client.get(f"{PREFIX}/specs/{spec_id}/knowledge/{kb_id}").status_code == 404


@pytest.mark.asyncio
async def test_create_knowledge_missing_spec_404(client) -> None:
    resp = client.post(f"{PREFIX}/specs/{_missing()}/knowledge", json=_kb_payload())
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Spec not found"


@pytest.mark.asyncio
async def test_get_knowledge_cross_spec_404(client) -> None:
    spec_id = await _seed_spec()
    other_spec = await _seed_spec()
    kb_id = client.post(f"{PREFIX}/specs/{spec_id}/knowledge", json=_kb_payload()).json()["id"]
    # Right item id, wrong spec → the kb.spec_id mismatch maps to the legacy 404.
    resp = client.get(f"{PREFIX}/specs/{other_spec}/knowledge/{kb_id}")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Knowledge base item not found"


@pytest.mark.asyncio
async def test_delete_knowledge_missing_404(client) -> None:
    spec_id = await _seed_spec()
    resp = client.delete(f"{PREFIX}/specs/{spec_id}/knowledge/{_missing()}")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Knowledge base item not found"


@pytest.mark.asyncio
async def test_list_knowledge_unknown_spec_is_empty(client) -> None:
    # Legacy: no existence check — an unknown spec yields an empty list, not 404.
    resp = client.get(f"{PREFIX}/specs/{_missing()}/knowledge")
    assert resp.status_code == 200
    assert resp.json() == []


# --- q&a --------------------------------------------------------------------


@pytest.mark.asyncio
async def test_qa_create_list_and_missing_spec_404(client) -> None:
    spec_id = await _seed_spec()
    created = client.post(f"{PREFIX}/specs/{spec_id}/qa", json={"question": "why?"})
    assert created.status_code == 201, created.text
    qa_id = created.json()["id"]

    listed = client.get(f"{PREFIX}/specs/{spec_id}/qa")
    assert listed.status_code == 200
    assert qa_id in {item["id"] for item in listed.json()}

    miss = client.post(f"{PREFIX}/specs/{_missing()}/qa", json={"question": "x?"})
    assert miss.status_code == 404
    assert miss.json()["detail"] == "Spec not found"


@pytest.mark.asyncio
async def test_answer_self_answer_403_with_reason(client) -> None:
    """Default board policy forbids the asker from answering: the use case
    commits the audit side-effect and re-raises, the adapter returns the legacy
    403 with the ``{reason, message}`` detail."""
    spec_id = await _seed_spec()
    qa_id = client.post(f"{PREFIX}/specs/{spec_id}/qa", json={"question": "self?"}).json()["id"]
    resp = client.post(f"{PREFIX}/specs/{spec_id}/qa/{qa_id}/answer", json={"answer": "mine"})
    assert resp.status_code == 403, resp.text
    detail = resp.json()["detail"]
    assert detail["reason"] == "self_answering_not_allowed"
    assert "message" in detail


@pytest.mark.asyncio
async def test_answer_success_when_asker_differs(client) -> None:
    spec_id = await _seed_spec()
    qa_id = await _seed_question(spec_id, asked_by=OTHER)
    resp = client.post(
        f"{PREFIX}/specs/{spec_id}/qa/{qa_id}/answer", json={"answer": "the answer"}
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["answer"] == "the answer"
    assert body["answered_by"] == USER


@pytest.mark.asyncio
async def test_answer_missing_qa_404(client) -> None:
    spec_id = await _seed_spec()
    resp = client.post(
        f"{PREFIX}/specs/{spec_id}/qa/{_missing()}/answer", json={"answer": "x"}
    )
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Q&A item not found"


@pytest.mark.asyncio
async def test_delete_question_and_missing_404(client) -> None:
    spec_id = await _seed_spec()
    qa_id = client.post(f"{PREFIX}/specs/{spec_id}/qa", json={"question": "del?"}).json()["id"]
    ok = client.delete(f"{PREFIX}/specs/{spec_id}/qa/{qa_id}")
    assert ok.status_code == 204
    # commit really persisted the delete.
    assert client.delete(f"{PREFIX}/specs/{spec_id}/qa/{qa_id}").status_code == 404
    miss = client.delete(f"{PREFIX}/specs/{spec_id}/qa/{_missing()}")
    assert miss.status_code == 404
    assert miss.json()["detail"] == "Q&A item not found"


# --- evaluation -------------------------------------------------------------


@pytest.mark.asyncio
async def test_submit_evaluation_missing_spec_404(client) -> None:
    resp = client.post(f"{PREFIX}/specs/{_missing()}/evaluations", json=_eval_payload())
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Spec not found"


@pytest.mark.asyncio
async def test_submit_evaluation_not_validated_409(client) -> None:
    # A freshly-created spec is in draft, not 'validated' → the service ValueError
    # propagates and the adapter maps it to 409 (legacy contract).
    spec_id = await _seed_spec()
    resp = client.post(f"{PREFIX}/specs/{spec_id}/evaluations", json=_eval_payload())
    assert resp.status_code == 409, resp.text
    assert "validated" in str(resp.json()["detail"]).lower()


@pytest.mark.asyncio
async def test_list_evaluations_200_and_missing_404(client) -> None:
    spec_id = await _seed_spec()
    ok = client.get(f"{PREFIX}/specs/{spec_id}/evaluations")
    assert ok.status_code == 200, ok.text
    body = ok.json()
    assert body["spec_id"] == spec_id and "evaluations" in body

    miss = client.get(f"{PREFIX}/specs/{_missing()}/evaluations")
    assert miss.status_code == 404


# --- use case + AST ---------------------------------------------------------


@pytest.mark.asyncio
async def test_get_knowledge_use_case_raises_for_missing() -> None:
    from okto_pulse.core.application.use_cases import (
        GetSpecKnowledgeCommand,
        GetSpecKnowledgeUseCase,
    )
    from okto_pulse.core.application.use_cases.base import ActorContext, EntityNotFoundError
    from okto_pulse.core.repositories import SQLAlchemyUnitOfWorkFactory

    uowf = SQLAlchemyUnitOfWorkFactory(get_session_factory())
    actor = ActorContext(USER, "rest")
    with pytest.raises(EntityNotFoundError):
        async with uowf(actor=actor) as uow:
            await GetSpecKnowledgeUseCase().execute(
                GetSpecKnowledgeCommand(_missing(), _missing()), actor=actor, uow=uow
            )


def test_fu3d_s4_endpoints_take_uow_not_raw_session() -> None:
    for name in _ENDPOINTS:
        sig = inspect.signature(getattr(specs_api, name))
        assert "db" not in sig.parameters, name
        assert "uow" in sig.parameters, name
        assert sig.parameters["uow"].default.dependency is get_unit_of_work, name


def test_specs_router_has_no_endpoint_on_get_db() -> None:
    """FU3d-S4 closes api/specs.py: no REGISTERED route endpoint may depend on
    get_db / a raw AsyncSession anymore (helpers are out of scope)."""
    from okto_pulse.core.infra.database import get_db as _get_db

    checked = 0
    for route in specs_router.routes:
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
