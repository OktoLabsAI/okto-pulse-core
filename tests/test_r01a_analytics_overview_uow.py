"""Spec R01A REST-FU2b — analytics overview + export on the UnitOfWork path.

``analytics_overview`` (302-LOC inline SQL) became the transport-free reader
``analytics_service.compute_overview`` behind ``AnalyticsOverviewUseCase`` + the
MCP-less ``get_unit_of_work``; ``analytics_overview_export`` gets its data from the
SAME use case and keeps the CSV envelope. The pure aggregation helpers + the
velocity family moved to the service and are re-exported from ``api/analytics.py``
for the not-yet-migrated endpoints. Oracles: overview payload + cross-board
ownership scoping + from/to filters + CSV export shape + golden parity + AST.
"""

from __future__ import annotations

import inspect
import uuid

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.core.api import analytics as analytics_api
from okto_pulse.core.api.analytics import router as analytics_router
from okto_pulse.core.api.deps import get_unit_of_work
from okto_pulse.core.infra.auth import require_user
from okto_pulse.core.infra.database import get_db, get_session_factory

USER = "r01a-fu2b-user"
OTHER = "r01a-fu2b-other"
PREFIX = "/api/v1"
OVERVIEW = f"{PREFIX}/analytics/overview"
EXPORT = f"{PREFIX}/analytics/overview/export"


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


async def _seed_board(owner: str, name: str) -> str:
    from okto_pulse.core.models.db import Board

    bid = f"board-fu2b-{uuid.uuid4().hex[:8]}"
    async with get_session_factory()() as db:
        db.add(Board(id=bid, name=name, owner_id=owner))
        await db.commit()
    return bid


_OVERVIEW_KEYS = {
    "total_ideations", "total_specs", "total_cards_impl", "total_cards_test",
    "funnel", "velocity", "boards", "spec_validation_gate", "task_validation_gate",
    "spec_evaluation", "sprint_evaluation", "avg_completeness", "avg_drift",
}


@pytest.mark.asyncio
async def test_overview_payload_shape_and_owner_scoping() -> None:
    mine = await _seed_board(USER, "fu2b-mine")
    theirs = await _seed_board(OTHER, "fu2b-theirs")

    resp = _client(USER).get(OVERVIEW)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert _OVERVIEW_KEYS <= set(body), _OVERVIEW_KEYS - set(body)
    board_ids = {b["board_id"] for b in body["boards"]}
    assert mine in board_ids
    assert theirs not in board_ids  # strict owner-only scoping preserved


@pytest.mark.asyncio
async def test_overview_empty_user_returns_zeroed_payload() -> None:
    resp = _client(f"nobody-{uuid.uuid4().hex[:8]}").get(OVERVIEW)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["total_ideations"] == 0
    assert body["boards"] == []
    assert body["velocity"] == []


@pytest.mark.asyncio
async def test_overview_accepts_date_filters() -> None:
    await _seed_board(USER, "fu2b-dates")
    resp = _client(USER).get(OVERVIEW, params={"from": "2026-01-01", "to": "2026-12-31"})
    assert resp.status_code == 200, resp.text
    assert isinstance(resp.json(), dict)


@pytest.mark.asyncio
async def test_overview_use_case_matches_compute_overview() -> None:
    from okto_pulse.core.application.use_cases import (
        AnalyticsOverviewCommand,
        AnalyticsOverviewUseCase,
    )
    from okto_pulse.core.application.use_cases.base import ActorContext
    from okto_pulse.core.repositories import SQLAlchemyUnitOfWorkFactory
    from okto_pulse.core.services.analytics_service import compute_overview

    await _seed_board(USER, "fu2b-golden")
    async with get_session_factory()() as db:
        baseline = await compute_overview(db, USER, dt_from=None, dt_to=None)

    uowf = SQLAlchemyUnitOfWorkFactory(get_session_factory())
    actor = ActorContext(USER, "rest")
    async with uowf(actor=actor) as uow:
        result = await AnalyticsOverviewUseCase().execute(
            AnalyticsOverviewCommand(), actor=actor, uow=uow
        )
    assert result.data == baseline


@pytest.mark.asyncio
async def test_overview_export_csv_shape() -> None:
    await _seed_board(USER, "fu2b-export")
    resp = _client(USER).get(EXPORT)
    assert resp.status_code == 200, resp.text
    assert resp.headers["content-type"].startswith("text/csv")
    text = resp.text
    assert "Total Ideations" in text
    assert "Funnel Stage" in text
    assert "Board ID" in text


# --- strangler proofs -------------------------------------------------------


def test_overview_endpoints_take_uow_not_raw_session() -> None:
    for name in ("analytics_overview", "analytics_overview_export"):
        sig = inspect.signature(getattr(analytics_api, name))
        assert "db" not in sig.parameters, name
        assert "uow" in sig.parameters, name
        assert sig.parameters["uow"].default.dependency is get_unit_of_work, name


def test_moved_helpers_reexported_for_remaining_endpoints() -> None:
    """ac/guardrail: whatever pure helpers api/analytics.py still re-exports from
    the service (the set SHRINKS as more endpoints migrate — FU2c/FU2d) must
    resolve to the service implementation, never a divergent copy."""
    import ast
    from pathlib import Path

    from okto_pulse.core.services import analytics_service

    tree = ast.parse(Path(analytics_api.__file__).read_text(encoding="utf-8"))
    reexported = [
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        and node.module == "okto_pulse.core.services.analytics_service"
        for alias in node.names
        if alias.name.startswith("_")
    ]
    # The set shrinks to empty once the adapter is fully strangled (FU2f) — that is
    # the terminal, correct state. Whatever (if anything) is still re-exported must
    # resolve to the single service implementation, never a divergent copy.
    for name in reexported:
        assert getattr(analytics_api, name) is getattr(analytics_service, name), name


def test_analytics_service_has_no_api_dependency() -> None:
    """Clean Core structural guard (FU2b rework, val_5b1fe35f): the analytics
    service layer must NOT import the HTTP adapter — no ``okto_pulse.core.api``
    import (top-level OR lazy) anywhere in analytics_service.py. The velocity
    family + ``_load_lifecycle_moves`` now live in the service, so the previous
    service→api coupling is gone and must not reappear."""
    import ast
    from pathlib import Path

    from okto_pulse.core.services import analytics_service

    tree = ast.parse(Path(analytics_service.__file__).read_text(encoding="utf-8"))
    offenders: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and (node.module or "").startswith(
            "okto_pulse.core.api"
        ):
            offenders.append((node.lineno, node.module))
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.startswith("okto_pulse.core.api"):
                    offenders.append((node.lineno, alias.name))
    assert offenders == [], offenders
