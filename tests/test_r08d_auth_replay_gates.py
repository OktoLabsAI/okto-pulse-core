"""R08-D — MCP authentication-gate + agent-lifecycle REPLAY HARNESS.

A validation/harness spec on top of R08-A (port) + R08-B (AuthContext bridge). It
drives the REAL MCP ASGI auth path — ``ApiKeySessionMiddleware`` (the actual
transport->_active_api_key ContextVar shim) wrapping a probe app that calls the
REAL ``_get_authenticated_agent`` / ``_get_agent_ctx`` / KG ``_get_user_boards``
— via ``httpx.ASGITransport`` (NOT unit mocks of the port). Everything runs in a
single event loop per test so the async SQLAlchemy engine + the ASGI requests
share one loop (TR1).

Scenario mapping:
  ts_722505d6 (TS01) — replay authenticated via query param + X-API-Key + Bearer
       -> SAME agent, equivalent 200 result.
  ts_f7f329c5 (TS02, negative) — absent/invalid/inactive credential -> 401;
       agent without board access -> 403; each fail-closed NON-MUTATING (agent
       entities + permission cache snapshot identical before/after).
  ts_722a1f06 (TS03) — agent lifecycle through the REAL AgentService: create
       preserves api_key (+ api_key_hash persistence + hash lookup), list shape,
       regenerate invalidates the old key.
  ts_8cf72513 (TS04) — a KG/MCP board-ACL path consumes AuthContext/MCPAuthContext
       and applies get_accessible_boards() (no bypass).
  ts_336479f7 (TS05) — 12 OVERLAPPING concurrent requests with distinct agents do
       not leak identity/ACL (the _active_api_key ContextVar is task-isolated).
  ts_54bec4d3 (TS06) — ApplicationPurityGate + mcp_credential_usage_gate +
       AntiSingletonGate as regression evidence (only the pre-existing _worker
       singleton is flagged — not introduced by R08-D).
"""

from __future__ import annotations

import asyncio
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import httpx
import pytest
from sqlalchemy import select
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route

import okto_pulse.core.app as _core_app  # noqa: F401  (register ORM models)
import okto_pulse.core.infra.database as _db_mod
import okto_pulse.core.mcp.server as server
from okto_pulse.core.kg.providers.embedded.mcp_auth_context import (
    create_mcp_auth_factory,
)
from okto_pulse.core.services.main import AgentService

_HASH = AgentService.hash_api_key


@pytest.fixture
def _harness_env():
    """Isolate settings / db engine+factory / registry / permission cache / env.
    The DB + ASGI requests are created INSIDE each test's single asyncio.run so the
    aiosqlite engine and the requests share one event loop."""
    import okto_pulse.core.infra.config as _config
    from okto_pulse.core.infra.config import CoreSettings
    from okto_pulse.core.kg.interfaces import registry as _reg

    saved = dict(
        settings=_config._settings_instance,
        engine=_db_mod._engine,
        factory=_db_mod._session_factory,
        reg=(_reg._registry, _reg._configured),
        mcp_sf=server._mcp_session_factory,
        data=os.environ.get("DATA_DIR"),
    )
    cache_snapshot = dict(server._permission_cache)
    tmp = tempfile.mkdtemp()
    os.environ["DATA_DIR"] = tmp
    _config.configure_settings(CoreSettings())
    _reg.reset_registry_for_tests()
    server._permission_cache.clear()
    try:
        yield tmp
    finally:
        # Dispose the temp engine SYNCHRONOUSLY (no asyncio.run: it would leave
        # the thread without a current event loop and break session-scoped
        # pytest-asyncio tests that run afterwards). Restore the saved refs.
        try:
            if _db_mod._engine is not None:
                _db_mod._engine.sync_engine.dispose()
        except Exception:
            pass
        _config._settings_instance = saved["settings"]
        _db_mod._engine = saved["engine"]
        _db_mod._session_factory = saved["factory"]
        _reg._registry, _reg._configured = saved["reg"]
        server._mcp_session_factory = saved["mcp_sf"]
        server._permission_cache.clear()
        server._permission_cache.update(cache_snapshot)
        if saved["data"] is None:
            os.environ.pop("DATA_DIR", None)
        else:
            os.environ["DATA_DIR"] = saved["data"]


async def _seed(tmp: str) -> None:
    """Create the DB, register the MCP session factory, seed agents + boards, and
    register the AuthContext factory bound to the REAL server agent/db providers."""
    from kg_registry_testing import configure_test_kg_registry
    from okto_pulse.core.models.db import Agent, AgentBoard, Board

    _db_mod.create_database(f"sqlite+aiosqlite:///{Path(tmp) / 'r08d.db'}")
    await _db_mod.init_db()
    server.register_session_factory(_db_mod.get_session_factory())

    now = datetime.now(timezone.utc)
    async with _db_mod.get_session_factory()() as s:
        s.add(Board(id="B1", name="Board 1", owner_id="A1", created_at=now, updated_at=now))
        s.add(Board(id="B2", name="Board 2", owner_id="A2", created_at=now, updated_at=now))
        for aid, key, active in (("A1", "kA1", True), ("A2", "kA2", True), ("IN", "kInact", False)):
            s.add(Agent(
                id=aid, name=aid, api_key=key, api_key_hash=_HASH(key),
                is_active=active, created_by="owner", created_at=now,
                permission_flags=None, permissions=[],
            ))
        s.add(AgentBoard(id="AB1", agent_id="A1", board_id="B1", granted_by="owner", granted_at=now))
        s.add(AgentBoard(id="AB2", agent_id="A2", board_id="B2", granted_by="owner", granted_at=now))
        await s.commit()

    # R08-B: AuthContext factory bound to the REAL MCP providers.
    configure_test_kg_registry(
        auth_context_factory=create_mcp_auth_factory(
            server._get_authenticated_agent, server.get_db_for_mcp
        )
    )


def _build_app() -> object:
    """The REAL ApiKeySessionMiddleware wrapping a probe app that calls the REAL
    server auth helpers (transport extraction + ContextVar + DB lookup + ACL)."""

    async def whoami(request):
        agent = await server._get_authenticated_agent()
        if agent is None:
            return JSONResponse({"error": "unauthorized"}, status_code=401)
        return JSONResponse({"agent_id": agent.id})

    async def board_access(request):
        board_id = request.path_params["board_id"]
        agent = await server._get_authenticated_agent()
        if agent is None:
            return JSONResponse({"error": "unauthorized"}, status_code=401)
        ctx = await server._get_agent_ctx(board_id)
        if ctx is None:
            return JSONResponse({"error": "forbidden"}, status_code=403)
        return JSONResponse({"agent_id": ctx.agent_id, "board_id": ctx.board_id})

    async def kg_acl(request):
        # TS04: a KG/MCP tool path that resolves agent + boards via the AuthContext
        # port (the registered MCPAuthContext factory) — no ACL bypass.
        from okto_pulse.core.mcp.kg_query_tools import _get_user_boards

        board_id = request.path_params["board_id"]
        agent, boards = await _get_user_boards()  # AuthContext path (factory set)
        if agent is None:
            return JSONResponse({"error": "unauthorized"}, status_code=401)
        if board_id not in boards:
            return JSONResponse({"error": "forbidden", "boards": boards}, status_code=403)
        return JSONResponse({"agent_id": agent.id, "boards": boards})

    probe = Starlette(routes=[
        Route("/whoami", whoami),
        Route("/board/{board_id}", board_access),
        Route("/kg/{board_id}", kg_acl),
    ])
    return server.ApiKeySessionMiddleware(probe)


def _client(app):
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t")


async def _agent_full_snapshot():
    """Every observable agent field, incl. the last_used_at audit timestamp."""
    async with _db_mod.get_session_factory()() as s:
        from okto_pulse.core.models.db import Agent

        rows = (await s.execute(select(Agent))).scalars().all()
        return {
            a.id: (a.api_key_hash, a.is_active, a.last_used_at, a.name, a.permission_flags)
            for a in rows
        }


async def _agent_security_snapshot():
    """Security-relevant agent state only (the credential hash, active flag and
    permission flags) — EXCLUDES last_used_at, which get_agent_by_key updates on
    any VALID credential by design (a proper audit write, not a security
    side-effect)."""
    async with _db_mod.get_session_factory()() as s:
        from okto_pulse.core.models.db import Agent

        rows = (await s.execute(select(Agent))).scalars().all()
        return {a.id: (a.api_key_hash, a.is_active, a.permission_flags) for a in rows}


# ===========================================================================
# TS01 (ts_722505d6) — transport replay: query / X-API-Key / Bearer -> same agent.
# ===========================================================================
async def test_ts_722505d6_transport_replay_same_agent(_harness_env):
    async def scenario():
        await _seed(_harness_env)
        app = _build_app()
        async with _client(app) as c:
            via_query = await c.get("/whoami", params={"api_key": "kA1"})
            via_header = await c.get("/whoami", headers={"X-API-Key": "kA1"})
            via_bearer = await c.get("/whoami", headers={"Authorization": "Bearer kA1"})
        return via_query, via_header, via_bearer

    q, h, b = await scenario()
    assert q.status_code == h.status_code == b.status_code == 200
    # SAME agent + equivalent result across all three transports.
    assert q.json() == h.json() == b.json() == {"agent_id": "A1"}


# ===========================================================================
# TS02 (ts_f7f329c5) — negatives, fail-closed + NON-MUTATING.
# ===========================================================================
async def test_ts_f7f329c5_auth_negatives_401_zero_mutation(_harness_env):
    """absent / invalid / inactive credential -> 401, matching NO agent, so the
    fail-closed path is fully NON-MUTATING (full snapshot + cache identical)."""

    async def scenario():
        await _seed(_harness_env)
        app = _build_app()
        before = await _agent_full_snapshot()
        before_cache = dict(server._permission_cache)
        async with _client(app) as c:
            absent = await c.get("/whoami")
            invalid = await c.get("/whoami", params={"api_key": "bogus-key"})
            inactive = await c.get("/whoami", params={"api_key": "kInact"})
        after = await _agent_full_snapshot()
        after_cache = dict(server._permission_cache)
        return absent, invalid, inactive, before, after, before_cache, after_cache

    absent, invalid, inactive, before, after, before_c, after_c = await scenario()
    assert absent.status_code == 401  # no credential
    assert invalid.status_code == 401  # unknown key
    assert inactive.status_code == 401  # get_agent_by_key filters is_active
    # TR2: nothing matched an agent -> ZERO mutation (incl. last_used_at) + no cache.
    assert after == before
    assert after_c == before_c == {}


async def test_ts_f7f329c5_no_board_access_403_no_privilege_escalation(_harness_env):
    """A VALID agent (A2) denied at the board ACL -> 403. No privilege escalation:
    the permission cache is NOT populated and the security-relevant agent state is
    unchanged (last_used_at MAY update — a proper audit write on a valid key)."""

    async def scenario():
        await _seed(_harness_env)
        app = _build_app()
        before_sec = await _agent_security_snapshot()
        before_cache = dict(server._permission_cache)
        async with _client(app) as c:
            no_board = await c.get("/board/B1", params={"api_key": "kA2"})  # A2 != B1
        after_sec = await _agent_security_snapshot()
        after_cache = dict(server._permission_cache)
        return no_board, before_sec, after_sec, before_cache, after_cache

    no_board, before_sec, after_sec, before_c, after_c = await scenario()
    assert no_board.status_code == 403  # authenticated but no board ACL
    # No privilege escalation: security state unchanged, cache NOT populated for
    # the denied (agent, board) pair.
    assert after_sec == before_sec
    assert after_c == before_c == {}
    assert ("A2", "B1") not in server._permission_cache


# ===========================================================================
# TS03 (ts_722a1f06) — agent lifecycle via the REAL AgentService.
# ===========================================================================
async def test_ts_722a1f06_agent_lifecycle_api_key_preserved(_harness_env):
    from okto_pulse.core.models.schemas import AgentCreate, AgentResponse

    async def scenario():
        await _seed(_harness_env)
        sf = _db_mod.get_session_factory()
        async with sf() as db:
            svc = AgentService(db)
            agent, raw_key = await svc.create_agent("owner", AgentCreate(name="LC"))
            await db.commit()
            agent_id = agent.id
            # AgentResponse exposes api_key; persisted hash captured BEFORE
            # regenerate (regenerate mutates the same ORM object's hash).
            resp = AgentResponse.model_validate(agent)
            created_hash = agent.api_key_hash
            found = await svc.get_agent_by_key(raw_key)
            listed = await svc.list_agents_for_user("owner")
            # regenerate invalidates the old key.
            _agent2, new_key = await svc.regenerate_key(agent_id)
            await db.commit()
            old_after = await svc.get_agent_by_key(raw_key)
            new_after = await svc.get_agent_by_key(new_key)
            return (agent_id, raw_key, resp, created_hash, found, listed,
                    old_after, new_after, new_key)

    agent_id, raw_key, resp, created_hash, found, listed, old_after, new_after, new_key = (
        await scenario()
    )
    # api_key preserved on response + raw key persisted as hash (pre-regenerate).
    assert resp.api_key == raw_key
    assert created_hash == _HASH(raw_key)
    # hash lookup resolves the agent.
    assert found is not None and found.id == agent_id
    # list shape includes the agent (no drift).
    assert any(a.id == agent_id for a in listed)
    assert all(hasattr(a, "api_key_hash") and hasattr(a, "id") for a in listed)
    # regenerate: old key invalid, new key valid.
    assert old_after is None
    assert new_after is not None and new_after.id == agent_id
    assert new_key != raw_key


# ===========================================================================
# TS04 (ts_8cf72513) — KG board ACL via AuthContext (no bypass).
# ===========================================================================
async def test_ts_8cf72513_kg_board_acl_via_auth_context(_harness_env):
    async def scenario():
        await _seed(_harness_env)
        app = _build_app()
        async with _client(app) as c:
            allowed = await c.get("/kg/B1", params={"api_key": "kA1"})   # A1 -> B1 member
            denied = await c.get("/kg/B2", params={"api_key": "kA1"})    # A1 not in B2
            unauth = await c.get("/kg/B1")                               # no credential
        return allowed, denied, unauth

    allowed, denied, unauth = await scenario()
    assert allowed.status_code == 200
    assert allowed.json() == {"agent_id": "A1", "boards": ["B1"]}  # via AuthContext
    assert denied.status_code == 403
    assert denied.json()["boards"] == ["B1"]  # ACL surfaced, B2 excluded (no bypass)
    assert unauth.status_code == 401


# ===========================================================================
# TS05 (ts_336479f7) — overlapping concurrency: no identity/ACL leak.
# ===========================================================================
async def test_ts_336479f7_overlapping_concurrency_no_identity_leak(_harness_env):
    async def scenario():
        await _seed(_harness_env)
        app = _build_app()
        async with _client(app) as c:
            async def call(key):
                r = await c.get("/whoami", params={"api_key": key})
                return r.status_code, r.json().get("agent_id")

            # 12 OVERLAPPING requests (gather -> separate tasks -> separate
            # ContextVar contexts) alternating two distinct agents.
            plan = [("kA1", "A1"), ("kA2", "A2")] * 6
            results = await asyncio.gather(*(call(k) for k, _ in plan))
            return plan, results

    plan, results = await scenario()
    assert len(results) == 12
    for (_, expected), (status, agent_id) in zip(plan, results):
        assert status == 200
        # Each concurrent request resolved its OWN agent — no ContextVar leak.
        assert agent_id == expected


# ===========================================================================
# TS06 (ts_54bec4d3) — purity + credential-usage + singleton gates (evidence).
# ===========================================================================
def test_ts_54bec4d3_purity_and_singleton_gates():
    from okto_pulse.core.application.boundary.mcp_credential_usage_gate import (
        run_mcp_credential_usage_gate,
    )
    from okto_pulse.core.application.boundary.singleton_gate import AntiSingletonGate
    from okto_pulse.core.application.purity_gate import run_application_purity_gate

    # ApplicationPurityGate: no app-layer module imports the forbidden symbols.
    purity = run_application_purity_gate()
    assert purity.ok is True, purity.violations

    # mcp_credential_usage_gate: no _active_api_key / get_agent_by_key use outside
    # the documented allowlisted shims.
    cred = run_mcp_credential_usage_gate()
    assert cred.ok is True, [f.file for f in cred.violations]

    # AntiSingletonGate: R08-D introduces NO new singleton. The only flagged
    # singleton is the PRE-EXISTING `_worker` (kg/workers/cognitive_closeout.py,
    # an untracked earlier card) — not introduced here.
    sg = AntiSingletonGate().run()
    new_names = {n["name"] for n in sg.evidence["new_singletons"]}
    assert new_names <= {"_worker"}, f"R08-D introduced a new singleton: {new_names}"
