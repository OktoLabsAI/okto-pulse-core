"""Spec R01A REST-FU5-S3 — KG pending-queue endpoints on the UnitOfWork.

The three ``api/kg_routes.py`` endpoints that still bound a raw request session —
``list_pending`` / ``list_pending_tree`` (readers) and ``retry_pending_entry``
(write) — now route through the ``kg_routes_crud`` use cases + ``get_unit_of_work``.
The inline ``select`` of the two readers moved to ``kg/dashboard_readers.py``
(``list_pending_entries`` / ``build_pending_tree``); the retry mutation + recursive
descendant sweep + commit + worker signal moved to
``kg.governance.retry_pending_entry`` (which commits the request session
internally, exactly as the legacy endpoint relied on).

Oracles exercise the migrated status codes + bodies (pending 200 envelope with /
without rows, tree 200 hierarchical shape + level counters, retry 200 reset, retry
404 "queue entry not found", retry recursive reopens descendants), the readers /
use cases running transport-free over a ``PulseUnitOfWork``, an AST signature check
proving every migrated endpoint takes ``uow`` (not a raw ``AsyncSession``), and the
relational-boundary gate proving the appended use cases hold no ``select`` /
``AsyncSession`` / ORM coupling.
"""

from __future__ import annotations

import inspect
import uuid

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.community.api import kg_routes as kg_routes_api
from okto_pulse.community.api.kg_routes import router as kg_router
from okto_pulse.community.api.deps import get_unit_of_work
from okto_pulse.community.api.auth_deps import get_current_user, get_realm_id, require_user
from okto_pulse.core.domain.realm import LOCAL_REALM_ID, RealmScope
from okto_pulse.core.infra.database import get_db, get_session_factory

PREFIX = "/api/v1"
ACTOR = "local-user"

# Exactly the three pending-queue endpoints migrated by FU5-S3 (boost_node and the
# already-migrated S2 light endpoints are NOT asserted here).
_MIGRATED_ENDPOINTS = (
    "list_pending",
    "list_pending_tree",
    "retry_pending_entry",
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
        return {
            "sub": ACTOR,
            "roles": ["admin"],
            "permissions": {
                "kg": {
                    "operations": {
                        "queue": {"read": True, "reprocess": True},
                    },
                    "admin": {
                        "settings_read": True,
                        "settings_write": True,
                    },
                }
            },
        }

    def _override_user_id():
        return ACTOR

    async def _override_realm():
        return LOCAL_REALM_ID

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[get_current_user] = _override_user
    app.dependency_overrides[require_user] = _override_user_id
    app.dependency_overrides[get_realm_id] = _override_realm
    return TestClient(app)


async def _seed_board(name: str = "fu5s3") -> str:
    from sqlalchemy_test_models import Board

    bid = f"board-fu5s3-{uuid.uuid4().hex[:8]}"
    async with get_session_factory()() as db:
        db.add(
            Board(
                id=bid,
                name=name,
                owner_id=ACTOR,
                realm_id=LOCAL_REALM_ID,
            )
        )
        await db.commit()
        return bid


async def _seed_pending_entry(
    board_id: str,
    *,
    artifact_type: str = "spec",
    status: str = "pending",
    source: str = "historical_backfill",
    claimed_by_session_id: str | None = None,
) -> str:
    from sqlalchemy_test_models import ConsolidationQueue

    entry = ConsolidationQueue(
        board_id=board_id,
        artifact_type=artifact_type,
        artifact_id=str(uuid.uuid4()),
        status=status,
        priority="low",
        source=source,
        claimed_by_session_id=claimed_by_session_id,
    )
    async with get_session_factory()() as db:
        db.add(entry)
        await db.commit()
        return entry.id


async def _seed_chain() -> dict[str, str]:
    """Seed a single ideation→refinement→spec→sprint→card chain. Returns the ids."""
    from sqlalchemy_test_models import (
        Board, Card, Ideation, Refinement, Spec, Sprint,
    )

    suffix = uuid.uuid4().hex[:8]
    ids = {
        "board": f"b-fu5s3-{suffix}",
        "ideation": f"i-fu5s3-{suffix}",
        "refinement": f"r-fu5s3-{suffix}",
        "spec": f"s-fu5s3-{suffix}",
        "sprint": f"sp-fu5s3-{suffix}",
        "card": f"c-fu5s3-{suffix}",
    }
    async with get_session_factory()() as db:
        db.add(
            Board(
                id=ids["board"],
                name="b",
                description="",
                owner_id=ACTOR,
                realm_id=LOCAL_REALM_ID,
            )
        )
        await db.flush()
        db.add(Ideation(
            id=ids["ideation"], board_id=ids["board"], title="Idea",
            description="", problem_statement="", proposed_approach="",
            created_by=ACTOR,
        ))
        await db.flush()
        db.add(Refinement(
            id=ids["refinement"], board_id=ids["board"], ideation_id=ids["ideation"],
            title="R", description="", created_by=ACTOR,
        ))
        await db.flush()
        db.add(Spec(
            id=ids["spec"], board_id=ids["board"], refinement_id=ids["refinement"],
            title="S", description="", created_by=ACTOR,
        ))
        await db.flush()
        db.add(Sprint(
            id=ids["sprint"], board_id=ids["board"], spec_id=ids["spec"],
            title="Sprint 1", created_by=ACTOR,
        ))
        await db.flush()
        db.add(Card(
            id=ids["card"], board_id=ids["board"], spec_id=ids["spec"],
            sprint_id=ids["sprint"], title="Card", created_by=ACTOR,
        ))
        await db.commit()
    return ids


# --- list_pending -----------------------------------------------------------


@pytest.mark.asyncio
async def test_list_pending_200_returns_seeded_entry(client) -> None:
    board_id = await _seed_board()
    entry_id = await _seed_pending_entry(board_id)
    resp = client.get(f"{PREFIX}/kg/boards/{board_id}/pending")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["count"] == 1
    entry = body["entries"][0]
    assert entry["id"] == entry_id
    assert entry["board_id"] == board_id
    assert entry["status"] == "pending"
    assert entry["source"] == "historical_backfill"
    assert entry["triggered_at"] is not None


@pytest.mark.asyncio
async def test_list_pending_empty_200(client) -> None:
    board_id = await _seed_board()
    resp = client.get(f"{PREFIX}/kg/boards/{board_id}/pending")
    assert resp.status_code == 200, resp.text
    assert resp.json() == {"entries": [], "count": 0}


# --- list_pending_tree ------------------------------------------------------


@pytest.mark.asyncio
async def test_pending_tree_empty_board_200(client) -> None:
    board_id = await _seed_board()
    resp = client.get(f"{PREFIX}/kg/boards/{board_id}/pending/tree")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["board_id"] == board_id
    assert body["tree"] == []
    assert body["total_pending"] == 0
    assert set(body["levels"].keys()) == {
        "ideations", "refinements", "specs", "sprints", "cards",
    }


@pytest.mark.asyncio
async def test_pending_tree_hierarchical_shape_200(client) -> None:
    ids = await _seed_chain()
    resp = client.get(f"{PREFIX}/kg/boards/{ids['board']}/pending/tree")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert len(body["tree"]) == 1
    ideation_node = body["tree"][0]
    assert ideation_node["id"] == ids["ideation"]
    assert ideation_node["type"] == "ideation"
    ref_node = ideation_node["children"][0]
    assert ref_node["type"] == "refinement"
    spec_node = ref_node["children"][0]
    assert spec_node["type"] == "spec"
    sprint_node = spec_node["children"][0]
    assert sprint_node["type"] == "sprint"
    card_node = sprint_node["children"][0]
    assert card_node["type"] == "card"
    assert card_node["id"] == ids["card"]


@pytest.mark.asyncio
async def test_pending_tree_counters_track_queue_status_200(client) -> None:
    ids = await _seed_chain()
    # Queue the spec as pending — the spec level counter must reflect it.
    from sqlalchemy_test_models import ConsolidationQueue

    async with get_session_factory()() as db:
        db.add(ConsolidationQueue(
            board_id=ids["board"], artifact_type="spec", artifact_id=ids["spec"],
            status="pending", priority="low", source="historical_backfill",
        ))
        await db.commit()

    resp = client.get(f"{PREFIX}/kg/boards/{ids['board']}/pending/tree")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["levels"]["specs"]["pending"] == 1
    assert body["total_pending"] == 1


@pytest.mark.asyncio
async def test_pending_tree_depth_limits_children_200(client) -> None:
    ids = await _seed_chain()
    resp = client.get(
        f"{PREFIX}/kg/boards/{ids['board']}/pending/tree", params={"depth": 2}
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    ref_node = body["tree"][0]["children"][0]
    # depth=2 stops at refinement — no spec children.
    assert ref_node["children"] == []


# --- retry_pending_entry ----------------------------------------------------


@pytest.mark.asyncio
async def test_retry_404_when_entry_missing(client) -> None:
    board_id = await _seed_board()
    resp = client.post(
        f"{PREFIX}/kg/boards/{board_id}/pending/does-not-exist/retry"
    )
    assert resp.status_code == 404, resp.text
    assert resp.json()["detail"] == "queue entry not found"


@pytest.mark.asyncio
async def test_retry_200_resets_failed_entry(client) -> None:
    from sqlalchemy_test_models import ConsolidationQueue

    board_id = await _seed_board()
    entry_id = await _seed_pending_entry(
        board_id, status="failed", claimed_by_session_id="prev_session",
    )
    resp = client.post(
        f"{PREFIX}/kg/boards/{board_id}/pending/{entry_id}/retry"
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["reopened_count"] == 1
    assert body["reopened_ids"] == [entry_id]
    assert body["recursive"] is False

    async with get_session_factory()() as db:
        refreshed = await db.get(ConsolidationQueue, entry_id)
        assert refreshed.status == "pending"
        assert refreshed.claimed_at is None
        assert refreshed.claimed_by_session_id is None
        assert refreshed.source == "retry_from_ui"


@pytest.mark.asyncio
async def test_retry_recursive_reopens_descendants_200(client) -> None:
    from sqlalchemy_test_models import ConsolidationQueue

    ids = await _seed_chain()
    async with get_session_factory()() as db:
        spec_entry = ConsolidationQueue(
            board_id=ids["board"], artifact_type="spec", artifact_id=ids["spec"],
            status="failed", source="initial",
        )
        sprint_entry = ConsolidationQueue(
            board_id=ids["board"], artifact_type="sprint", artifact_id=ids["sprint"],
            status="done", source="initial",
        )
        card_entry = ConsolidationQueue(
            board_id=ids["board"], artifact_type="card", artifact_id=ids["card"],
            status="done", source="initial",
        )
        db.add_all([spec_entry, sprint_entry, card_entry])
        await db.commit()
        spec_entry_id = spec_entry.id
        sprint_entry_id = sprint_entry.id
        card_entry_id = card_entry.id

    resp = client.post(
        f"{PREFIX}/kg/boards/{ids['board']}/pending/{spec_entry_id}/retry",
        params={"recursive": "true"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["recursive"] is True
    assert body["reopened_count"] >= 3
    for eid in (spec_entry_id, sprint_entry_id, card_entry_id):
        assert eid in body["reopened_ids"]

    async with get_session_factory()() as db:
        for eid in (spec_entry_id, sprint_entry_id, card_entry_id):
            refreshed = await db.get(ConsolidationQueue, eid)
            assert refreshed.status == "pending"


# --- service reader + use case (transport-free) -----------------------------


@pytest.mark.asyncio
async def test_list_pending_entries_reader_picks_up_seeded_entry() -> None:
    from okto_pulse.core.kg.dashboard_readers import list_pending_entries

    board_id = await _seed_board()
    entry_id = await _seed_pending_entry(board_id)
    async with get_session_factory()() as db:
        entries = await list_pending_entries(db, board_id)
    assert entry_id in {e["id"] for e in entries}


@pytest.mark.asyncio
async def test_pending_tree_use_case_runs_over_unit_of_work() -> None:
    from okto_pulse.core.application.use_cases.base import ActorContext
    from okto_pulse.core.application.use_cases.kg_routes_crud import (
        ListPendingTreeCommand,
        ListPendingTreeUseCase,
    )
    from sqlalchemy_test_unit_of_work import SQLAlchemyUnitOfWorkFactory
    ids = await _seed_chain()
    uowf = SQLAlchemyUnitOfWorkFactory(get_session_factory())
    actor = ActorContext(ACTOR, "rest", realm_scope=RealmScope.local())
    async with uowf(actor=actor) as uow:
        result = await ListPendingTreeUseCase().execute(
            ListPendingTreeCommand(ids["board"], depth=5), actor=actor, uow=uow
        )
    assert result.payload["board_id"] == ids["board"]
    assert result.payload["tree"][0]["id"] == ids["ideation"]


@pytest.mark.asyncio
async def test_retry_use_case_raises_not_found_for_missing_entry() -> None:
    from okto_pulse.core.application.use_cases.base import (
        ActorContext,
        EntityNotFoundError,
    )
    from okto_pulse.core.application.use_cases.kg_routes_crud import (
        RetryPendingEntryCommand,
        RetryPendingEntryUseCase,
    )
    from sqlalchemy_test_unit_of_work import SQLAlchemyUnitOfWorkFactory
    board_id = await _seed_board()
    uowf = SQLAlchemyUnitOfWorkFactory(get_session_factory())
    actor = ActorContext(ACTOR, "rest", realm_scope=RealmScope.local())
    with pytest.raises(EntityNotFoundError):
        async with uowf(actor=actor) as uow:
            await RetryPendingEntryUseCase().execute(
                RetryPendingEntryCommand(board_id, "does-not-exist", recursive=False),
                actor=actor,
                uow=uow,
            )


# --- AST signature + relational-boundary gate -------------------------------


def test_fu5_s3_endpoints_take_uow_not_raw_session() -> None:
    for name in _MIGRATED_ENDPOINTS:
        sig = inspect.signature(getattr(kg_routes_api, name))
        assert "db" not in sig.parameters, name
        assert "uow" in sig.parameters, name
        assert sig.parameters["uow"].default.dependency is get_unit_of_work, name


def test_kg_routes_crud_use_case_is_relational_boundary_clean() -> None:
    """The migrated use case file must hold NO direct relational coupling
    (select / AsyncSession / get_db / ORM import) — the inline SQL lives in the
    kg/dashboard_readers + kg/governance service modules instead."""
    from okto_pulse.core.repositories.relational_boundary_gate import (
        run_relational_boundary_gate,
    )

    report = run_relational_boundary_gate()
    offenders = [
        v for v in report.violations if "kg_routes_crud.py" in v.file.replace("\\", "/")
    ]
    assert offenders == [], [v.symbol for v in offenders]
