"""Spec R01A IMP4 — REST batch (read-only inspectors + agents.py cache) on UoW.

Migrated handlers:
- api/kg_stale_canonical_parity.py: ``list_stale_canonical_parity_endpoint``
- api/queue_health.py: ``get_kg_queue_health``, ``get_kg_queue_drilldown``
- api/agents.py: ``update_agent``, ``update_board_overrides`` (cache-invalidating)

Each now obtains a request-scoped ``PulseUnitOfWork`` via ``get_unit_of_work`` and
calls a transport-free use case; the handler no longer takes a raw
``AsyncSession``. The MCP permission-cache invalidation stays in ``agents.py``
exactly on ``update_agent`` / ``update_board_overrides`` (ts_4160b5c5 /
ac_8e695cf2); grant/revoke/delete do NOT gain a new invalidation.
"""

from __future__ import annotations

import ast
import inspect
import uuid
from pathlib import Path
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.community.api import agents as agents_api
from okto_pulse.community.api import kg_stale_canonical_parity as stale_api
from okto_pulse.community.api import queue_health as queue_api
from okto_pulse.community.api.agents import router as agents_router
from okto_pulse.community.api.deps import get_unit_of_work
from okto_pulse.community.api.kg_stale_canonical_parity import router as stale_router
from okto_pulse.community.api.queue_health import router as queue_router
from okto_pulse.community.api.auth_deps import (
    get_realm_id,
    require_principal,
    require_user,
)
from okto_pulse.core.domain.realm import LOCAL_REALM_ID
from okto_pulse.core.infra.database import get_db, get_session_factory
from okto_pulse.core.ports.authentication import Principal
from okto_pulse.core.ports.permission_policy import registered_permission_flags
from okto_pulse.core.repositories.relational_boundary_gate import (
    default_use_cases_path,
    run_relational_boundary_gate,
)
from okto_pulse.core.repositories.relational_consumer_inventory import (
    build_relational_consumer_inventory,
)

USER = "r01a-imp4-user"

MIGRATED_HANDLERS = {
    "list_stale_canonical_parity_endpoint": stale_api.list_stale_canonical_parity_endpoint,
    "get_kg_queue_health": queue_api.get_kg_queue_health,
    "get_kg_queue_drilldown": queue_api.get_kg_queue_drilldown,
    "update_agent": agents_api.update_agent,
    "update_board_overrides": agents_api.update_board_overrides,
}


def _client(*router_prefixes) -> TestClient:
    app = FastAPI()
    for router, prefix in router_prefixes:
        app.include_router(router, prefix=prefix)
    session_factory = get_session_factory()

    async def _override_db():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[require_user] = lambda: USER
    app.dependency_overrides[get_realm_id] = lambda: LOCAL_REALM_ID
    app.dependency_overrides[require_principal] = lambda: Principal(
        USER,
        realm_id=LOCAL_REALM_ID,
        actor_kind="human",
        claims={
            "roles": ["admin"],
            "permissions": registered_permission_flags(),
        },
    )
    return TestClient(app)


# --- read-only parity via the REAL endpoints -------------------------------


@pytest.mark.asyncio
async def test_stale_canonical_parity_endpoint_payload() -> None:
    from sqlalchemy_test_models import Board

    board_id = f"board-{uuid.uuid4().hex[:8]}"
    async with get_session_factory()() as db:
        db.add(Board(id=board_id, name="R01A parity", owner_id=USER))
        await db.commit()

    client = _client((stale_router, "/api/v1"))
    resp = client.get(
        f"/api/v1/kg/{board_id}/stale-canonical-parity",
        params={"limit": 25, "offset": 0},
    )
    assert resp.status_code == 200, resp.text
    assert isinstance(resp.json(), dict)


@pytest.mark.asyncio
@pytest.mark.parametrize("foreign", [False, True], ids=["missing", "foreign"])
async def test_stale_canonical_parity_board_denial_is_non_enumerable(
    foreign: bool,
) -> None:
    from sqlalchemy_test_models import Board

    board_id = f"board-{uuid.uuid4().hex[:8]}"
    if foreign:
        async with get_session_factory()() as db:
            db.add(Board(id=board_id, name="Foreign parity", owner_id="other-user"))
            await db.commit()

    response = _client((stale_router, "/api/v1")).get(
        f"/api/v1/kg/{board_id}/stale-canonical-parity",
        params={"limit": 25, "offset": 0},
    )

    assert response.status_code == 404
    assert response.json() == {"detail": "Board not found"}


def test_queue_health_endpoints_payload() -> None:
    client = _client((queue_router, "/api/v1"))
    health = client.get("/api/v1/kg/queue/health")
    assert health.status_code == 200, health.text
    body = health.json()
    assert "queue_depth" in body and "dead_letter_count" in body
    drill = client.get("/api/v1/kg/queue/drilldown")
    assert drill.status_code == 200, drill.text
    assert isinstance(drill.json(), dict)


# --- agents.py write parity + cache preservation ---------------------------


@pytest.mark.asyncio
async def test_update_agent_preserves_behavior_and_invalidates_cache() -> None:
    """The migrated PATCH /agents/{id} still updates the owner's agent (200 +
    new name) and STILL invalidates the MCP permission cache exactly once."""
    from okto_pulse.core.models import AgentCreate
    from okto_pulse.core.services import AgentService

    factory = get_session_factory()
    async with factory() as db:
        agent, _key = await AgentService(db).create_agent(
            USER, AgentCreate(name="r01a-imp4-agent")
        )
        await db.commit()
        agent_id = agent.id

    client = _client((agents_router, "/api/v1/agents"))
    with patch("okto_pulse.core.mcp.server.invalidate_agent_cache") as mock_inval:
        resp = client.patch(f"/api/v1/agents/{agent_id}", json={"name": "renamed-imp4"})

    assert resp.status_code == 200, resp.text
    assert resp.json()["name"] == "renamed-imp4"
    mock_inval.assert_called_once_with(agent_id)


@pytest.mark.asyncio
async def test_update_agent_404_for_non_owner_does_not_invalidate() -> None:
    """Permission/404 baseline preserved: a missing/non-owned agent returns the
    legacy 404 'Agent not found' and the cache is NOT invalidated."""
    client = _client((agents_router, "/api/v1/agents"))
    with patch("okto_pulse.core.mcp.server.invalidate_agent_cache") as mock_inval:
        resp = client.patch(
            f"/api/v1/agents/missing-{uuid.uuid4().hex[:8]}", json={"name": "x"}
        )
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Agent not found"
    mock_inval.assert_not_called()


# --- AST / signature strangler proofs --------------------------------------


def test_migrated_handlers_take_uow_not_raw_session() -> None:
    for name, fn in MIGRATED_HANDLERS.items():
        sig = inspect.signature(fn)
        assert "db" not in sig.parameters, name
        assert "uow" in sig.parameters, name
        assert sig.parameters["uow"].default.dependency is get_unit_of_work, name


def _funcs_using(module, symbol: str) -> set[str]:
    tree = ast.parse(Path(module.__file__).read_text(encoding="utf-8"))
    out: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef)):
            for n in ast.walk(node):
                if isinstance(n, ast.Name) and n.id == symbol:
                    out.add(node.name)
                if isinstance(n, ast.Call):
                    fn_name = n.func.id if isinstance(n.func, ast.Name) else getattr(n.func, "attr", None)
                    if fn_name == symbol:
                        out.add(node.name)
    return out


def test_migrated_agents_endpoints_have_no_relational_coupling() -> None:
    for symbol in ("get_db_for_mcp", "get_db"):
        users = _funcs_using(agents_api, symbol)
        assert "update_agent" not in users, symbol
        assert "update_board_overrides" not in users, symbol


def test_agents_cache_invalidation_points_unchanged() -> None:
    """ts_4160b5c5 / ac_8e695cf2: update_agent + update_board_overrides invalidate
    the cache; grant/revoke/delete do NOT gain a new invalidation."""
    invalidating = _funcs_using(agents_api, "invalidate_agent_cache")
    assert invalidating == {"update_agent", "update_board_overrides"}, invalidating


# --- inventory delta (R01A IMP1 strangler ledger) --------------------------


def test_read_only_routers_fully_strangled_in_inventory() -> None:
    inv = build_relational_consumer_inventory()
    for f in ("core/api/kg_stale_canonical_parity.py", "core/api/queue_health.py"):
        rows = [c for c in inv.consumers if c.file == f]
        assert rows == [], (f, [c.symbol for c in rows])


def test_use_cases_layer_stays_relationally_clean() -> None:
    report = run_relational_boundary_gate(root=default_use_cases_path())
    assert report.ok, [(v.file, v.symbol) for v in report.violations]
