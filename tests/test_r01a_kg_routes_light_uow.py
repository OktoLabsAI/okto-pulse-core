"""Spec R01A REST-FU5-S2 — KG dashboard "light" endpoints on the UnitOfWork.

The seven ``api/kg_routes.py`` endpoints that still bound a raw request session —
the two readers (``list_audit``, ``global_search``) and the five governance
delegators (``start_historical``, ``cancel_historical_endpoint``,
``historical_progress_endpoint``, ``delete_board_kg``, ``get_settings``) — now
route through the ``kg_routes_crud`` use cases + ``get_unit_of_work``; the inline
``select`` of the two readers moved to ``kg/dashboard_readers.py`` (a service
reader), and each governance delegate calls the existing ``kg.governance``
function (which commits the request session internally, exactly as the legacy
endpoint relied on). Oracles exercise the migrated status codes + bodies (audit
200 with/without rows, global-search 200 envelope + invalid-layer 400, start /
cancel / progress 200, delete 204, settings 200), the reader/use case running
transport-free over a ``PulseUnitOfWork``, an AST signature check proving every
migrated endpoint takes ``uow`` (not a raw ``AsyncSession``), and the
relational-boundary gate proving the new use case file holds no ``select`` /
``AsyncSession`` / ORM coupling.
"""

from __future__ import annotations

import inspect
import uuid
from datetime import datetime, timezone

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.community.api import kg_routes as kg_routes_api
from okto_pulse.community.api.kg_routes import router as kg_router
from okto_pulse.community.api.deps import get_unit_of_work
from okto_pulse.core.infra.database import get_db, get_session_factory
from okto_pulse.community.api.auth_deps import get_current_user, get_realm_id, require_user

PREFIX = "/api/v1"
ACTOR = "local-user"

# Exactly the seven "light" endpoints migrated by FU5-S2 (the out-of-scope
# list_pending / list_pending_tree / retry_pending_entry / boost_node endpoints
# still bind a raw session and are NOT asserted here).
_MIGRATED_ENDPOINTS = (
    "list_audit",
    "global_search",
    "start_historical",
    "cancel_historical_endpoint",
    "historical_progress_endpoint",
    "delete_board_kg",
    "get_settings",
)


@pytest.fixture
def client():
    app = FastAPI()
    app.include_router(kg_router, prefix=PREFIX)
    session_factory = get_session_factory()

    async def _override_db():
        async with session_factory() as session:
            yield session

    async def _override_user():
        return {"sub": ACTOR, "roles": ["admin"]}

    def _override_user_id():
        return ACTOR

    async def _override_realm():
        return None

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[get_current_user] = _override_user
    app.dependency_overrides[require_user] = _override_user_id
    app.dependency_overrides[get_realm_id] = _override_realm
    return TestClient(app)


async def _seed_board(name: str = "fu5s2") -> str:
    from sqlalchemy_test_models import Board

    bid = f"board-fu5s2-{uuid.uuid4().hex[:8]}"
    async with get_session_factory()() as db:
        db.add(Board(id=bid, name=name, owner_id=ACTOR))
        await db.commit()
        return bid


async def _seed_board_with_done_spec() -> str:
    from sqlalchemy_test_models import Board, Spec, SpecStatus

    bid = f"board-fu5s2-{uuid.uuid4().hex[:8]}"
    async with get_session_factory()() as db:
        db.add(Board(id=bid, name="fu5s2", owner_id=ACTOR))
        db.add(Spec(
            id=str(uuid.uuid4()),
            board_id=bid,
            title="fu5s2-spec",
            status=SpecStatus.DONE,
            archived=False,
            created_by=ACTOR,
        ))
        await db.commit()
        return bid


async def _seed_audit_row(board_id: str) -> str:
    from sqlalchemy_test_models import ConsolidationAudit

    session_id = f"kgses_{uuid.uuid4().hex[:16]}"
    now = datetime.now(timezone.utc)
    async with get_session_factory()() as db:
        db.add(ConsolidationAudit(
            session_id=session_id,
            board_id=board_id,
            artifact_id=str(uuid.uuid4()),
            artifact_type="spec",
            agent_id="agent-fu5s2",
            started_at=now,
            committed_at=now,
            nodes_added=2,
            content_hash="hash-fu5s2",
            undo_status="none",
        ))
        await db.commit()
        return session_id


# --- list_audit -------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_audit_200_returns_committed_entry(client) -> None:
    board_id = await _seed_board()
    session_id = await _seed_audit_row(board_id)
    resp = client.get(f"{PREFIX}/kg/boards/{board_id}/audit")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["next_cursor"] is None
    entry = next(e for e in body["entries"] if e["session_id"] == session_id)
    assert entry["board_id"] == board_id
    assert entry["nodes_added"] == 2
    assert entry["undo_status"] == "none"


@pytest.mark.asyncio
async def test_list_audit_empty_200(client) -> None:
    board_id = await _seed_board()
    resp = client.get(f"{PREFIX}/kg/boards/{board_id}/audit")
    assert resp.status_code == 200, resp.text
    assert resp.json() == {"entries": [], "next_cursor": None}


# --- global_search ----------------------------------------------------------


@pytest.mark.asyncio
async def test_global_search_200_envelope(client) -> None:
    await _seed_board()
    resp = client.get(f"{PREFIX}/kg/global/search", params={"q": "auth design"})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["graph_layer"] == "canonical"
    assert body["total"] == len(body["results"])
    assert isinstance(body["results"], list)


@pytest.mark.asyncio
async def test_global_search_invalid_layer_400(client) -> None:
    # normalize_graph_layer raises KGToolError(invalid_param) → _handle_kg_error
    # maps it to 400, preserving the legacy error path.
    resp = client.get(
        f"{PREFIX}/kg/global/search",
        params={"q": "x", "graph_layer": "bogus"},
    )
    assert resp.status_code == 400, resp.text


# --- historical consolidation: start / cancel / progress --------------------


@pytest.mark.asyncio
async def test_start_historical_200(client) -> None:
    board_id = await _seed_board_with_done_spec()
    resp = client.post(
        f"{PREFIX}/kg/boards/{board_id}/historical-consolidation/start"
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "queueing"
    assert body["board_id"] == board_id
    assert body["total_artifacts"] >= 1


@pytest.mark.asyncio
async def test_cancel_historical_200(client) -> None:
    board_id = await _seed_board_with_done_spec()
    client.post(f"{PREFIX}/kg/boards/{board_id}/historical-consolidation/start")
    resp = client.post(
        f"{PREFIX}/kg/boards/{board_id}/historical-consolidation/cancel"
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "cancelled"
    assert body["removed"] >= 1


@pytest.mark.asyncio
async def test_historical_progress_200(client) -> None:
    board_id = await _seed_board_with_done_spec()
    client.post(f"{PREFIX}/kg/boards/{board_id}/historical-consolidation/start")
    resp = client.get(
        f"{PREFIX}/kg/boards/{board_id}/historical-consolidation/progress"
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["total"] >= 1
    assert body["enabled"] is True


# --- delete_board_kg (right-to-erasure) -------------------------------------


@pytest.mark.asyncio
async def test_delete_board_kg_204(client) -> None:
    board_id = await _seed_board()
    resp = client.delete(f"{PREFIX}/kg/boards/{board_id}/kg")
    assert resp.status_code == 204, resp.text


# --- get_settings -----------------------------------------------------------


@pytest.mark.asyncio
async def test_get_settings_200(client) -> None:
    board_id = await _seed_board()
    resp = client.get(f"{PREFIX}/kg/boards/{board_id}/settings")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["consolidation_enabled"] is True
    assert body["kg_initialized"] is False  # no graph bootstrapped for this board
    assert "embedding_provider" in body
    assert "graph_store" in body


# --- service reader + use case (transport-free) -----------------------------


@pytest.mark.asyncio
async def test_list_all_board_ids_reader_picks_up_seeded_board() -> None:
    from okto_pulse.core.kg.dashboard_readers import list_all_board_ids

    board_id = await _seed_board()
    async with get_session_factory()() as db:
        # The reader's default ``limit=100`` is an UNORDERED legacy-parity cut
        # (``select(Board).limit(100)``), not a stable lookup — asserting a freshly
        # seeded board sits inside it is flaky once the shared test DB holds >100
        # boards. Lift the cap so the membership check is deterministic regardless
        # of insertion order...
        unlimited = await list_all_board_ids(db, limit=10_000_000)
        assert board_id in unlimited
        # ...and separately prove the ``limit`` param still caps the projection
        # (the behavior we must preserve from the legacy global_search lookup).
        assert len(await list_all_board_ids(db, limit=3)) <= 3


@pytest.mark.asyncio
async def test_list_audit_use_case_runs_over_unit_of_work() -> None:
    from okto_pulse.core.application.use_cases.base import ActorContext
    from okto_pulse.core.application.use_cases.kg_routes_crud import (
        ListAuditCommand,
        ListAuditUseCase,
    )
    from sqlalchemy_test_unit_of_work import SQLAlchemyUnitOfWorkFactory
    board_id = await _seed_board()
    session_id = await _seed_audit_row(board_id)

    uowf = SQLAlchemyUnitOfWorkFactory(get_session_factory())
    actor = ActorContext(ACTOR, "rest")
    async with uowf(actor=actor) as uow:
        result = await ListAuditUseCase().execute(
            ListAuditCommand(board_id, limit=50), actor=actor, uow=uow
        )
    assert session_id in {e["session_id"] for e in result.entries}


# --- AST signature + relational-boundary gate -------------------------------


def test_fu5_s2_endpoints_take_uow_not_raw_session() -> None:
    for name in _MIGRATED_ENDPOINTS:
        sig = inspect.signature(getattr(kg_routes_api, name))
        assert "db" not in sig.parameters, name
        assert "uow" in sig.parameters, name
        assert sig.parameters["uow"].default.dependency is get_unit_of_work, name


def test_kg_routes_crud_use_case_is_relational_boundary_clean() -> None:
    """The migrated use case file must hold NO direct relational coupling
    (select / AsyncSession / get_db / ORM import) — the inline SQL lives in the
    kg/dashboard_readers service reader instead."""
    from okto_pulse.core.repositories.relational_boundary_gate import (
        run_relational_boundary_gate,
    )

    report = run_relational_boundary_gate()
    offenders = [
        v for v in report.violations if "kg_routes_crud.py" in v.file.replace("\\", "/")
    ]
    assert offenders == [], [v.symbol for v in offenders]
