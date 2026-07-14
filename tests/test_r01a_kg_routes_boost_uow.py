"""Spec R01A REST-FU5-S4 — KG ``boost_node`` endpoint on the UnitOfWork
(+ bug 547a2aa8 regression — boost_node audit row persistence).

``api/kg_routes.boost_node`` was the LAST ``kg_routes.py`` endpoint still binding a
raw request session (``db: AsyncSession = Depends(get_db)``). It now routes through
the ``BoostNodeUseCase`` + ``get_unit_of_work``; the graph read/SET and the
``ConsolidationAudit`` staging moved to ``kg.governance.boost_node`` (so the use case
holds no ``select`` / ``AsyncSession`` / ORM coupling), and the use case commits the
staged audit via the UoW.

Bug 547a2aa8 fix (BUG R01A - boost_node audit row is silently dropped): the legacy
staged audit row omitted the NOT-NULL ``artifact_type``/``started_at`` columns, so its
commit always raised IntegrityError and was swallowed by the best-effort guard — the
row NEVER persisted while the boost still returned 200. The fix populates
``artifact_type="boost"`` + ``started_at`` and gives ``session_id`` a uuid suffix so a
second boost of the same node in the same second no longer collides on the audit PK.
The commit stays best-effort ONLY for a genuinely unexpected failure on the
already-boosted graph (split-brain), preserving the legacy 200/404/500 contract.

Oracles exercise: the migrated status codes + bodies (boost 200 envelope with the
+0.3/clamp arithmetic, repeated boosts stacking to the 1.5 clamp, the 404 RFC 7807
problem body when the node is absent, the 500 ``kuzu_error`` problem on a failed SET);
the bug regression (a successful boost persists its ``boost-*`` audit row with the
required NOT-NULL fields populated and orderable; N repeated boosts persist N distinct
rows; an audit-commit failure still preserves the boosted graph + 200); the use case
running transport-free over a ``PulseUnitOfWork``; an AST signature check proving the
endpoint takes ``uow`` (not a raw ``AsyncSession``); and the relational-boundary gate
proving the appended use case holds no relational symbol.
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
from okto_pulse.core.infra.database import get_db, get_session_factory
from okto_pulse.core.domain.realm import LOCAL_REALM_ID

PREFIX = "/api/v1"
ACTOR = "local-user"

# Exactly the one endpoint migrated by FU5-S4 (the S2/S3 endpoints are NOT
# re-asserted here).
_MIGRATED_ENDPOINTS = ("boost_node",)


@pytest.fixture(autouse=True)
def _require_real_community_graph(_kg_registry_test_fakes):
    from kg_registry_testing import configure_real_graph_test_kg_registry

    configure_real_graph_test_kg_registry()


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


@pytest.fixture(autouse=True)
def _kg_teardown():
    """Release per-board Kùzu handles + the pool between tests so Windows file
    locks (single-writer embedded store) do not bleed across cases."""
    yield
    from okto_pulse.community.adapters.graph_connection_pool import reset_connection_pool_for_tests
    from kg_schema_testing import close_all_connections

    close_all_connections()
    reset_connection_pool_for_tests()


async def _seed_board(name: str = "fu5s4") -> str:
    from sqlalchemy_test_models import Board

    bid = f"board-fu5s4-{uuid.uuid4().hex[:8]}"
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


def _bootstrap_empty_graph(board_id: str) -> None:
    """Bootstrap an EMPTY per-board graph (node tables exist, no rows) so the
    boost read loop runs over real tables and finds nothing — the 404 path."""
    from kg_schema_testing import bootstrap_board_graph, close_all_connections

    bootstrap_board_graph(board_id)
    close_all_connections(board_id)


def _seed_kg_node(board_id: str, node_id: str, *, relevance_score: float = 0.5) -> None:
    """Bootstrap the board graph and CREATE one ``Entity`` node carrying a known
    ``relevance_score``, then close the seeding connection so the endpoint opens a
    fresh scope (single-writer embedded store)."""
    from kg_schema_testing import (
        bootstrap_board_graph,
        close_all_connections,
        open_board_connection,
    )

    bootstrap_board_graph(board_id)
    with open_board_connection(board_id) as (_db, conn):
        conn.execute(
            "CREATE (n:Entity {id: $id, title: $t, content: $c, "
            "context: '', justification: '', source_artifact_ref: '', "
            "source_session_id: 'sess-boost', "
            "created_at: timestamp('2026-04-19T12:00:00'), "
            "created_by_agent: 'agent-boost', "
            "source_confidence: 0.5, relevance_score: $rel, "
            "query_hits: 0, last_queried_at: NULL, "
            "priority_boost: 0.0, "
            "embedding: $emb})",
            {
                "id": node_id,
                "t": "t",
                "c": "c",
                "rel": relevance_score,
                "emb": [0.1] * 384,
            },
        )
    close_all_connections(board_id)


async def _audit_rows(board_id: str):
    from sqlalchemy import select

    from sqlalchemy_test_models import ConsolidationAudit

    async with get_session_factory()() as db:
        return (
            await db.execute(
                select(ConsolidationAudit).where(
                    ConsolidationAudit.board_id == board_id
                )
            )
        ).scalars().all()


# --- boost 200 --------------------------------------------------------------


@pytest.mark.asyncio
async def test_boost_200_persists_audit_row(client) -> None:
    board_id = await _seed_board()
    node_id = f"e-boost-{uuid.uuid4().hex[:8]}"
    _seed_kg_node(board_id, node_id, relevance_score=0.5)

    resp = client.post(f"{PREFIX}/kg/boards/{board_id}/nodes/{node_id}/boost")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["node_id"] == node_id
    assert body["node_type"] == "Entity"
    assert body["score_before"] == pytest.approx(0.5)
    assert body["score_after"] == pytest.approx(0.8)  # 0.5 + 0.3
    assert body["boosted_by"] == "local-user"
    assert body["boosted_at"]

    # Bug 547a2aa8 fix: the boost now PERSISTS its ConsolidationAudit row. The legacy
    # code staged the row WITHOUT the NOT-NULL ``artifact_type``/``started_at`` columns,
    # so its commit raised IntegrityError and was silently swallowed (200 with no
    # ``boost-*`` row). The REST 200 path must now land exactly one boost audit row
    # with those columns populated (full field-level assertions live in the dedicated
    # regression ``test_boost_persists_audit_row_with_required_not_null_fields``).
    rows = await _audit_rows(board_id)
    boost_rows = [r for r in rows if r.session_id.startswith("boost-")]
    assert len(boost_rows) == 1
    assert boost_rows[0].artifact_type == "boost"
    assert boost_rows[0].started_at is not None


@pytest.mark.asyncio
async def test_boost_stacks_until_clamp(client) -> None:
    """Idempotency is NOT enforced: each call stacks +0.3 until the 1.5 clamp."""
    board_id = await _seed_board()
    node_id = f"e-clamp-{uuid.uuid4().hex[:8]}"
    _seed_kg_node(board_id, node_id, relevance_score=0.5)

    last_after = 0.5
    # 0.5 -> 0.8 -> 1.1 -> 1.4 -> 1.5 (clamped) -> 1.5 (stays clamped)
    for _ in range(6):
        resp = client.post(
            f"{PREFIX}/kg/boards/{board_id}/nodes/{node_id}/boost"
        )
        assert resp.status_code == 200, resp.text
        last_after = resp.json()["score_after"]
    assert last_after == pytest.approx(1.5)


# --- boost 404 (RFC 7807 problem) -------------------------------------------


@pytest.mark.asyncio
async def test_boost_404_problem_body_when_node_absent(client) -> None:
    board_id = await _seed_board()
    _bootstrap_empty_graph(board_id)
    missing = "does-not-exist"

    resp = client.post(f"{PREFIX}/kg/boards/{board_id}/nodes/{missing}/boost")
    assert resp.status_code == 404, resp.text
    body = resp.json()
    # Preserves the legacy RFC 7807 problem shape (NOT the {"detail": ...} adapter
    # default): _problem(404, "Node not found", ..., "not_found").
    assert body["type"] == "/errors/not_found"
    assert body["title"] == "Node not found"
    assert body["status"] == 404
    assert missing in body["detail"]
    assert board_id in body["detail"]


# --- use case (transport-free over the UnitOfWork) --------------------------


@pytest.mark.asyncio
async def test_boost_use_case_runs_over_unit_of_work() -> None:
    from okto_pulse.core.application.use_cases.base import ActorContext
    from okto_pulse.core.application.use_cases.kg_routes_crud import (
        BoostNodeCommand,
        BoostNodeUseCase,
    )
    from sqlalchemy_test_unit_of_work import SQLAlchemyUnitOfWorkFactory
    board_id = await _seed_board()
    node_id = f"e-uow-{uuid.uuid4().hex[:8]}"
    _seed_kg_node(board_id, node_id, relevance_score=0.4)

    uowf = SQLAlchemyUnitOfWorkFactory(get_session_factory())
    actor = ActorContext(ACTOR, "rest", realm_id=LOCAL_REALM_ID)
    async with uowf(actor=actor) as uow:
        result = await BoostNodeUseCase().execute(
            BoostNodeCommand(board_id, node_id), actor=actor, uow=uow
        )
    assert result.payload["node_id"] == node_id
    assert result.payload["score_before"] == pytest.approx(0.4)
    assert result.payload["score_after"] == pytest.approx(0.7)

    # Bug 547a2aa8 fix: the use case now PERSISTS the audit row (the staged row
    # carries the NOT-NULL artifact_type/started_at columns, so the commit succeeds).
    rows = await _audit_rows(board_id)
    boost_rows = [
        r for r in rows
        if r.artifact_id == node_id and r.session_id.startswith("boost-")
    ]
    assert len(boost_rows) == 1
    assert boost_rows[0].artifact_type == "boost"
    assert boost_rows[0].started_at is not None


@pytest.mark.asyncio
async def test_boost_use_case_raises_not_found_for_missing_node() -> None:
    from okto_pulse.core.application.use_cases.base import (
        ActorContext,
        EntityNotFoundError,
    )
    from okto_pulse.core.application.use_cases.kg_routes_crud import (
        BoostNodeCommand,
        BoostNodeUseCase,
    )
    from sqlalchemy_test_unit_of_work import SQLAlchemyUnitOfWorkFactory
    board_id = await _seed_board()
    _bootstrap_empty_graph(board_id)

    uowf = SQLAlchemyUnitOfWorkFactory(get_session_factory())
    actor = ActorContext(ACTOR, "rest", realm_id=LOCAL_REALM_ID)
    with pytest.raises(EntityNotFoundError):
        async with uowf(actor=actor) as uow:
            await BoostNodeUseCase().execute(
                BoostNodeCommand(board_id, "does-not-exist"), actor=actor, uow=uow
            )


# --- bug 547a2aa8 regression: boost audit row persistence -------------------


@pytest.mark.asyncio
async def test_boost_persists_audit_row_with_required_not_null_fields(client) -> None:
    """Bug 547a2aa8: a successful boost MUST persist its ConsolidationAudit row with
    the previously-dropped NOT-NULL columns populated — no IntegrityError swallowed
    behind a 200. Pre-fix this FAILS (the staged row omitted ``artifact_type`` /
    ``started_at`` so the commit raised and was swallowed → zero ``boost-*`` rows).
    Covers Codex criteria: boost-* row persisted, artifact_type stable + asserted,
    started_at/committed_at filled and orderable."""
    board_id = await _seed_board()
    node_id = f"e-audit-{uuid.uuid4().hex[:8]}"
    _seed_kg_node(board_id, node_id, relevance_score=0.5)

    resp = client.post(f"{PREFIX}/kg/boards/{board_id}/nodes/{node_id}/boost")
    assert resp.status_code == 200, resp.text

    rows = await _audit_rows(board_id)
    boost_rows = [r for r in rows if r.session_id.startswith("boost-")]
    assert len(boost_rows) == 1, (
        f"expected exactly one persisted boost audit row, got {len(boost_rows)}"
    )
    row = boost_rows[0]
    # The two NOT-NULL columns the legacy bug dropped:
    assert row.artifact_type == "boost"
    assert row.started_at is not None
    # ... and the rest of the audit contract:
    assert row.artifact_id == node_id
    assert row.agent_id == "local-user"
    assert row.board_id == board_id
    assert row.committed_at is not None
    # Orderable: the operation started no later than it committed.
    assert row.started_at <= row.committed_at
    # A boost is not an artifact consolidation — it must not inflate node/edge counts.
    assert row.nodes_added == 0
    assert row.edges_added == 0


@pytest.mark.asyncio
async def test_repeated_boosts_persist_distinct_audit_rows(client) -> None:
    """Repeated boosts of the SAME node (rapid, same-second) must EACH persist a
    DISTINCT audit row. ``session_id`` is the audit PK; without the uuid suffix a
    second same-second boost would collide on the PK and be silently swallowed by the
    best-effort guard — re-dropping the audit row (bug 547a2aa8 in a new form). N
    boosts → N distinct ``boost-*`` rows (Codex's session_id-collision catch)."""
    board_id = await _seed_board()
    node_id = f"e-repeat-{uuid.uuid4().hex[:8]}"
    _seed_kg_node(board_id, node_id, relevance_score=0.0)

    n = 3
    for _ in range(n):
        resp = client.post(f"{PREFIX}/kg/boards/{board_id}/nodes/{node_id}/boost")
        assert resp.status_code == 200, resp.text

    rows = await _audit_rows(board_id)
    boost_rows = [r for r in rows if r.session_id.startswith("boost-")]
    assert len(boost_rows) == n, (
        f"expected {n} distinct boost audit rows, got {len(boost_rows)}"
    )
    # Distinct PKs (no collision, nothing swallowed):
    assert len({r.session_id for r in boost_rows}) == n
    assert all(
        r.artifact_type == "boost" and r.started_at is not None for r in boost_rows
    )


@pytest.mark.asyncio
async def test_boost_audit_commit_failure_preserves_kg_mutation(
    client, monkeypatch
) -> None:
    """Split-brain protection: if the audit commit fails for a genuinely unexpected
    reason, the boost — already written to the embedded graph — must still succeed
    (200) and the node score must stay bumped; the best-effort guard rolls back only
    the audit-only row. No rollback may undo the successful KG mutation (Codex
    criterion)."""
    import okto_pulse.core.application.use_cases.kg_routes_crud as crud

    board_id = await _seed_board()
    node_id = f"e-split-{uuid.uuid4().hex[:8]}"
    _seed_kg_node(board_id, node_id, relevance_score=0.5)

    async def _boom(_uow):
        raise RuntimeError("audit commit blew up")

    # Force the audit commit (and only that) to fail inside the use case's
    # best-effort guard.
    monkeypatch.setattr(crud, "commit", _boom)

    resp = client.post(f"{PREFIX}/kg/boards/{board_id}/nodes/{node_id}/boost")
    assert resp.status_code == 200, resp.text
    assert resp.json()["score_after"] == pytest.approx(0.8)  # arithmetic intact

    # The audit-only row was rolled back (best-effort) — nothing persisted.
    rows = await _audit_rows(board_id)
    assert not any(r.session_id.startswith("boost-") for r in rows)

    # The KG node score is DURABLY bumped: re-reading the graph shows 0.8, not 0.5,
    # proving the audit rollback did NOT undo the successful graph SET.
    from kg_schema_testing import close_all_connections, open_board_connection

    close_all_connections(board_id)
    with open_board_connection(board_id) as (_db, conn):
        res = conn.execute(
            "MATCH (n:Entity {id: $id}) RETURN n.relevance_score", {"id": node_id}
        )
        assert res.has_next()
        assert float(res.get_next()[0]) == pytest.approx(0.8)
    close_all_connections(board_id)


@pytest.mark.asyncio
async def test_boost_persist_error_maps_to_legacy_500(client, monkeypatch) -> None:
    """A failed graph SET (``BoostPersistError``) still maps to the legacy 500
    ``kuzu_error`` RFC 7807 problem body — unchanged by the audit-persistence fix
    (Codex criterion: 500 legacy preserved)."""
    from sqlalchemy_test_models import Board
    from okto_pulse.core.kg.governance import BoostPersistError

    async with get_session_factory()() as db:
        db.add(Board(id="board-x", name="boom", owner_id=ACTOR, realm_id=LOCAL_REALM_ID))
        await db.commit()

    class _BoomUseCase:
        async def execute(self, *_a, **_k):
            raise BoostPersistError("Failed to persist boost: kuzu down")

    monkeypatch.setattr(kg_routes_api, "BoostNodeUseCase", _BoomUseCase)

    resp = client.post(
        f"{PREFIX}/kg/boards/board-x/nodes/whatever/boost"
    )
    assert resp.status_code == 500, resp.text
    body = resp.json()
    assert body["type"] == "/errors/kuzu_error"
    assert body["title"] == "Boost persist failed"
    assert body["status"] == 500


# --- AST signature + relational-boundary gate -------------------------------


def test_fu5_s4_endpoint_takes_uow_not_raw_session() -> None:
    for name in _MIGRATED_ENDPOINTS:
        sig = inspect.signature(getattr(kg_routes_api, name))
        assert "db" not in sig.parameters, name
        assert "uow" in sig.parameters, name
        assert sig.parameters["uow"].default.dependency is get_unit_of_work, name


def test_kg_routes_crud_use_case_is_relational_boundary_clean() -> None:
    """The migrated use case file must hold NO direct relational coupling
    (select / AsyncSession / get_db / ORM import) — the graph read/SET + audit
    write live in the kg/governance service module instead."""
    from okto_pulse.core.repositories.relational_boundary_gate import (
        run_relational_boundary_gate,
    )

    report = run_relational_boundary_gate()
    offenders = [
        v for v in report.violations if "kg_routes_crud.py" in v.file.replace("\\", "/")
    ]
    assert offenders == [], [v.symbol for v in offenders]
