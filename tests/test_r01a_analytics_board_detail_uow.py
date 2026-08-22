"""Spec R01A REST-FU2c — analytics board-detail inline-SQL endpoints on UoW.

The four inline-SQL endpoints (board_quality / board_validations /
board_spec_analytics / board_sprint_analytics) now route through transport-free
use cases + ``get_unit_of_work``; the inline ``select()`` queries moved to
``analytics_service`` readers (compute_quality / compute_validations /
compute_spec_analytics / compute_sprint_analytics) reusing the pure helpers
relocated in FU2b. Oracles: payload + board-ownership 404 + spec/sprint 404 +
golden parity (use case == reader) + AST.
"""

from __future__ import annotations

import inspect
import uuid

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.community.api import analytics as analytics_api
from okto_pulse.community.api.analytics import router as analytics_router
from okto_pulse.community.api.deps import get_unit_of_work
from okto_pulse.community.api.auth_deps import require_user
from okto_pulse.core.infra.database import get_db, get_session_factory

USER = "r01a-fu2c-user"
OTHER = "r01a-fu2c-other"
PREFIX = "/api/v1"
_ENDPOINTS = ("board_quality", "board_validations", "board_spec_analytics", "board_sprint_analytics")


def _client(user: str = USER) -> TestClient:
    app = FastAPI()
    app.include_router(analytics_router, prefix=PREFIX)
    session_factory = get_session_factory()

    async def _override_db():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[require_user] = lambda: user
    return TestClient(app)


async def _seed_board(owner: str = USER) -> str:
    from sqlalchemy_test_models import Board

    bid = f"board-fu2c-{uuid.uuid4().hex[:8]}"
    async with get_session_factory()() as db:
        db.add(Board(id=bid, name="fu2c", owner_id=owner))
        await db.commit()
    return bid


async def _seed_spec(board_id: str) -> str:
    from okto_pulse.core.models.schemas import SpecCreate
    from okto_pulse.core.services import SpecService

    async with get_session_factory()() as db:
        spec = await SpecService(db).create_spec(
            board_id,
            USER,
            SpecCreate(
                title=f"fu2c-{uuid.uuid4().hex[:6]}",
                delivery_context="brownfield",
            ),
        )
        await db.commit()
        return spec.id


async def _seed_sprint(board_id: str, spec_id: str) -> str:
    from sqlalchemy_test_models import Sprint

    sid = f"sprint-fu2c-{uuid.uuid4().hex[:8]}"
    async with get_session_factory()() as db:
        db.add(Sprint(
            id=sid, spec_id=spec_id, board_id=board_id, title="fu2c-sprint",
            created_by=USER, archived=False,
            skip_test_coverage=False, skip_rules_coverage=False,
            skip_qualitative_validation=False,
        ))
        await db.commit()
        return sid


# --- payload + board 404 ----------------------------------------------------


@pytest.mark.asyncio
async def test_quality_200_and_board_404() -> None:
    board_id = await _seed_board()
    ok = _client().get(f"{PREFIX}/boards/{board_id}/analytics/quality")
    assert ok.status_code == 200, ok.text
    body = ok.json()
    assert "conclusion_reported" in body and "validation_reported" in body
    miss = _client(OTHER).get(f"{PREFIX}/boards/{board_id}/analytics/quality")
    assert miss.status_code == 404 and miss.json()["detail"] == "Board not found"


@pytest.mark.asyncio
async def test_validations_200_and_board_404() -> None:
    board_id = await _seed_board()
    ok = _client().get(f"{PREFIX}/boards/{board_id}/analytics/validations")
    assert ok.status_code == 200, ok.text
    body = ok.json()
    assert "spec_validation_gate" in body and "task_validation_gate" in body
    miss = _client(OTHER).get(f"{PREFIX}/boards/{board_id}/analytics/validations")
    assert miss.status_code == 404


# --- spec/sprint: 200 + board 404 + entity 404 ------------------------------


@pytest.mark.asyncio
async def test_spec_analytics_200_board_404_spec_404() -> None:
    board_id = await _seed_board()
    spec_id = await _seed_spec(board_id)
    assert _client().get(f"{PREFIX}/boards/{board_id}/analytics/spec/{spec_id}").status_code == 200
    # missing spec on an owned board → "Spec not found"
    miss_spec = _client().get(
        f"{PREFIX}/boards/{board_id}/analytics/spec/missing-{uuid.uuid4().hex[:6]}"
    )
    assert miss_spec.status_code == 404 and miss_spec.json()["detail"] == "Spec not found"
    # non-owned board → "Board not found" (checked before the spec)
    miss_board = _client(OTHER).get(f"{PREFIX}/boards/{board_id}/analytics/spec/{spec_id}")
    assert miss_board.status_code == 404 and miss_board.json()["detail"] == "Board not found"


@pytest.mark.asyncio
async def test_sprint_analytics_200_and_sprint_404() -> None:
    board_id = await _seed_board()
    spec_id = await _seed_spec(board_id)
    sprint_id = await _seed_sprint(board_id, spec_id)
    assert _client().get(f"{PREFIX}/boards/{board_id}/analytics/sprint/{sprint_id}").status_code == 200
    miss = _client().get(
        f"{PREFIX}/boards/{board_id}/analytics/sprint/missing-{uuid.uuid4().hex[:6]}"
    )
    assert miss.status_code == 404 and miss.json()["detail"] == "Sprint not found"


# --- golden parity: use case == reader --------------------------------------


@pytest.mark.asyncio
async def test_quality_use_case_matches_reader() -> None:
    from okto_pulse.core.application.use_cases import BoardQualityCommand, BoardQualityUseCase
    from okto_pulse.core.application.use_cases.base import ActorContext
    from sqlalchemy_test_unit_of_work import SQLAlchemyUnitOfWorkFactory
    from okto_pulse.core.services.analytics_service import compute_quality

    board_id = await _seed_board()
    async with get_session_factory()() as db:
        baseline = await compute_quality(db, board_id, dt_from=None, dt_to=None)

    uowf = SQLAlchemyUnitOfWorkFactory(get_session_factory())
    actor = ActorContext(USER, "rest")
    async with uowf(actor=actor) as uow:
        result = await BoardQualityUseCase().execute(
            BoardQualityCommand(board_id), actor=actor, uow=uow
        )
    assert result.data == baseline


@pytest.mark.asyncio
async def test_spec_analytics_reader_returns_none_for_missing_spec() -> None:
    from okto_pulse.core.services.analytics_service import compute_spec_analytics

    board_id = await _seed_board()
    async with get_session_factory()() as db:
        assert await compute_spec_analytics(db, board_id, "no-such-spec") is None


# --- AST strangler proof ----------------------------------------------------


def test_board_detail_endpoints_take_uow_not_raw_session() -> None:
    for name in _ENDPOINTS:
        sig = inspect.signature(getattr(analytics_api, name))
        assert "db" not in sig.parameters, name
        assert "uow" in sig.parameters, name
        assert sig.parameters["uow"].default.dependency is get_unit_of_work, name
