"""R-P2-09 — MCP auth facades use request-scoped credentials.

The leverage migration: re-pointing _get_agent_ctx / _get_authenticated_agent at
the active_api_key_credential() request-scope shim migrates the ~227 facade
call-sites automatically. The former _active_api_key ContextVar is removed. The
full per-transport family golden replay is the R08-D net
(tests/test_r08d_auth_replay_gates.py, re-run green after this migration).

Scenario mapping:
  TS01 — the facades resolve the credential via active_api_key_credential() (the
         McpCredential port DTO), not via a direct global carrier; the facade
         still resolves the same agent (behavioural + structural).
  TS02 — golden replay of the migrated facades: success / auth-failure /
         permission-denied preserve outcome with no response/side-effect change.
  TS03 — mcp_credential_usage_gate is ALIAS-AWARE and covers the facades: real
         core ok; aliased / qualified / assignment-chain bypass BLOCKED.
  TS04 — _active_api_key removed: server.py no longer defines/imports the
         ContextVar and the singleton ledger no longer baselines it.
  TS05 — request-scoped credentials are isolated under concurrency (no leak).
  TS06 — AntiSingletonGate adds zero new singletons (only pre-existing _worker);
         ApplicationPurityGate + mcp_credential_usage_gate green.
"""

from __future__ import annotations

import ast
import asyncio
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pytest

import okto_pulse.core.app as _core_app  # noqa: F401 (register ORM models)
import okto_pulse.core.infra.database as _db_mod
import okto_pulse.core.mcp.server as server
from okto_pulse.core.services.main import AgentService
from okto_pulse.core.ports import McpCredential

_HASH = AgentService.hash_api_key


@pytest.fixture
def _seeded():
    """Temp SQLite DB seeded with A1->B1, A2->B2, plus an inactive agent; the MCP
    session factory registered. Restores settings/engine/factory/cache/env."""
    import okto_pulse.core.infra.config as _config
    from okto_pulse.core.infra.config import CoreSettings

    saved = dict(
        settings=_config._settings_instance,
        engine=_db_mod._engine,
        factory=_db_mod._session_factory,
        mcp_sf=server._mcp_session_factory,
        data=os.environ.get("DATA_DIR"),
    )
    cache = dict(server._permission_cache)
    tmp = tempfile.mkdtemp()
    os.environ["DATA_DIR"] = tmp
    _config.configure_settings(CoreSettings())
    server._permission_cache.clear()
    try:
        yield tmp
    finally:
        try:
            if _db_mod._engine is not None:
                _db_mod._engine.sync_engine.dispose()
        except Exception:
            pass
        _config._settings_instance = saved["settings"]
        _db_mod._engine = saved["engine"]
        _db_mod._session_factory = saved["factory"]
        server._mcp_session_factory = saved["mcp_sf"]
        server._permission_cache.clear()
        server._permission_cache.update(cache)
        if saved["data"] is None:
            os.environ.pop("DATA_DIR", None)
        else:
            os.environ["DATA_DIR"] = saved["data"]


async def _seed(tmp: str) -> None:
    from okto_pulse.core.models.db import Agent, AgentBoard, Board

    _db_mod.create_database(f"sqlite+aiosqlite:///{Path(tmp) / 'r08c.db'}")
    await _db_mod.init_db()
    server.register_session_factory(_db_mod.get_session_factory())
    now = datetime.now(timezone.utc)
    async with _db_mod.get_session_factory()() as s:
        s.add(Board(id="B1", name="B1", owner_id="A1", created_at=now, updated_at=now))
        s.add(Board(id="B2", name="B2", owner_id="A2", created_at=now, updated_at=now))
        for aid, key, active in (("A1", "kA1", True), ("A2", "kA2", True), ("IN", "kIn", False)):
            s.add(Agent(id=aid, name=aid, api_key=key, api_key_hash=_HASH(key),
                        is_active=active, created_by="o", created_at=now,
                        permission_flags=None, permissions=[]))
        s.add(AgentBoard(id="AB1", agent_id="A1", board_id="B1", granted_by="o", granted_at=now))
        s.add(AgentBoard(id="AB2", agent_id="A2", board_id="B2", granted_by="o", granted_at=now))
        await s.commit()


class _Req:
    def __init__(self, credential=None):
        self.scope = {}
        self.query_params = {}
        self.headers = {}
        if credential is not None:
            self.scope[server._MCP_CREDENTIAL_SCOPE_KEY] = credential


def _set_request_scope(monkeypatch, key: str | None):
    """Simulate FastMCP exposing the current HTTP request to server.py."""
    import fastmcp.server.dependencies as deps

    credential = (
        McpCredential(source="query_param", value=key) if key is not None else None
    )
    monkeypatch.setattr(deps, "get_http_request", lambda: _Req(credential))


# ===========================================================================
# TS01 — facades resolve via the request-scope shim, not a direct global read.
# ===========================================================================
def test_ts01_facades_route_credential_through_shim_structural():
    src = Path(server.__file__).read_text(encoding="utf-8")
    tree = ast.parse(src)
    facades = {
        f.name: f
        for f in ast.walk(tree)
        if isinstance(f, ast.AsyncFunctionDef)
        and f.name in {"_get_authenticated_agent", "_get_agent_ctx"}
    }
    assert set(facades) == {"_get_authenticated_agent", "_get_agent_ctx"}
    for name, fn in facades.items():
        calls = {
            n.func.id
            for n in ast.walk(fn)
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
        }
        # routes through the request-scope shim...
        assert "active_api_key_credential" in calls, name
        # ...and does NOT read the old global carrier directly in its body.
        names = {n.id for n in ast.walk(fn) if isinstance(n, ast.Name)}
        assert "_active_api_key" not in names, name


async def test_ts01_facade_resolves_same_agent_behavioural(_seeded, monkeypatch):
    await _seed(_seeded)
    _set_request_scope(monkeypatch, "kA1")
    agent = await server._get_authenticated_agent()
    ctx = await server._get_agent_ctx("B1")
    assert agent is not None and agent.id == "A1"
    assert ctx is not None and ctx.agent_id == "A1" and ctx.board_id == "B1"


# ===========================================================================
# TS02 — golden replay of the migrated facades (success / auth-fail / denied).
# ===========================================================================
async def test_ts02_golden_replay_success_authfail_denied(_seeded, monkeypatch):
    await _seed(_seeded)

    async def _ctx(key, board):
        _set_request_scope(monkeypatch, key)
        return await server._get_agent_ctx(board)

    # success: A1 -> B1
    ok = await _ctx("kA1", "B1")
    assert ok is not None and ok.agent_id == "A1"
    # auth failure: unknown + inactive key -> None
    assert await _ctx("bogus", "B1") is None
    assert await _ctx("kIn", "B1") is None
    # permission denied: valid A2 but not a member of B1 -> None (no bypass)
    assert await _ctx("kA2", "B1") is None
    # no credential at all -> None
    _set_request_scope(monkeypatch, None)
    assert await server._get_agent_ctx("B1") is None


# ===========================================================================
# ts_3e57f597 (spec TS02 / AC2) — DEDICATED inventory of migrated call-sites:
# the families are migrated AND there is no new concrete-credential use outside
# the allowlisted files.
# ===========================================================================
def test_ts_3e57f597_inventory_of_migrated_call_sites():
    core = Path(server.__file__).resolve().parents[1]  # .../okto_pulse/core

    # (1) ZERO direct CODE uses of `_active_api_key` (ast.Name / ast.Attribute)
    # across production core. String literals in gates (ast.Constant) are stale
    # reintroduction guards, NOT credential reads.
    code_use_files: dict[str, int] = {}
    for py in core.rglob("*.py"):
        if "__pycache__" in py.parts:
            continue
        try:
            tree = ast.parse(py.read_text(encoding="utf-8"))
        except (SyntaxError, UnicodeDecodeError):
            continue
        rel = py.relative_to(core).as_posix()
        for n in ast.walk(tree):
            is_name = isinstance(n, ast.Name) and n.id == "_active_api_key"
            is_attr = isinstance(n, ast.Attribute) and n.attr == "_active_api_key"
            if is_name or is_attr:
                code_use_files[rel] = code_use_files.get(rel, 0) + 1
    assert code_use_files == {}, code_use_files

    # (2) server.py does not import contextvars nor define the old carrier.
    server_tree = ast.parse(Path(server.__file__).read_text(encoding="utf-8"))
    assert not any(
        isinstance(n, ast.ImportFrom) and n.module == "contextvars"
        for n in ast.walk(server_tree)
    )
    assert not any(
        isinstance(n, (ast.Assign, ast.AnnAssign))
        and (
            any(isinstance(t, ast.Name) and t.id == "_active_api_key" for t in getattr(n, "targets", ()))
            or isinstance(getattr(n, "target", None), ast.Name)
            and getattr(n, "target").id == "_active_api_key"
        )
        for n in ast.walk(server_tree)
    )

    # (3) The credential-usage gate inventory classifies every sensitive-symbol
    # finding as allowlisted -> ZERO debt outside the allowlist (AC2).
    from okto_pulse.core.application.boundary.mcp_credential_usage_gate import (
        run_mcp_credential_usage_gate,
    )

    report = run_mcp_credential_usage_gate()
    assert report.ok is True
    assert report.violations == ()
    # Every recorded finding is inside an allowlisted file (the shim/definition).
    assert {f.file for f in report.findings} <= set(report.allowlist)
    # And the facades themselves ARE inventoried (the migrated credential path is
    # present ONLY in the allowlisted server.py).
    facade_files = {f.file for f in report.findings if f.symbol == "_get_agent_ctx"}
    assert facade_files == {"okto_pulse/core/mcp/server.py"}


# ===========================================================================
# TS03 — alias-aware credential-usage gate covering the facades.
# ===========================================================================
def test_ts03_gate_alias_aware_real_core_ok_and_blocks_bypass(tmp_path):
    from okto_pulse.core.application.boundary.mcp_credential_usage_gate import (
        SENSITIVE_SYMBOLS,
        run_mcp_credential_usage_gate,
    )

    # The facades are now part of the restricted set.
    assert "_get_agent_ctx" in SENSITIVE_SYMBOLS
    assert "_get_authenticated_agent" in SENSITIVE_SYMBOLS

    # Real core passes (server.py + services/main.py allowlisted).
    assert run_mcp_credential_usage_gate().ok is True

    # Aliased / qualified / assignment-chain bypass in a NEW external file blocks.
    svc = tmp_path / "okto_pulse" / "core" / "services"
    svc.mkdir(parents=True)
    (svc / "rogue.py").write_text(
        "from okto_pulse.core.mcp.server import _get_agent_ctx as f\n"
        "async def g(b):\n    return await f(b)\n",
        encoding="utf-8",
    )
    (svc / "rogue2.py").write_text(
        "from okto_pulse.core.mcp.server import _active_api_key as ak\n"
        "x = ak.get()\n",
        encoding="utf-8",
    )
    report = run_mcp_credential_usage_gate(source_root=tmp_path)
    assert report.ok is False
    flagged = {(v.file, v.symbol) for v in report.violations}
    assert ("okto_pulse/core/services/rogue.py", "_get_agent_ctx") in flagged
    assert ("okto_pulse/core/services/rogue2.py", "_active_api_key") in flagged


# ===========================================================================
# TS04 — _active_api_key removed from runtime + singleton baseline.
# ===========================================================================
def test_ts04_context_var_removed_and_unledgered():
    from okto_pulse.core.application.boundary.singleton_gate import (
        BASELINE_SINGLETONS,
        SINGLETON_LEDGER,
    )

    assert not hasattr(server, "_active_api_key")
    src = Path(server.__file__).read_text(encoding="utf-8")
    assert "from contextvars import ContextVar" not in src
    assert "_active_api_key: ContextVar" not in src

    assert "_active_api_key" not in SINGLETON_LEDGER
    assert "okto_pulse/core/mcp/server.py::_active_api_key" not in BASELINE_SINGLETONS


# ===========================================================================
# TS05 — request-scoped credentials are task-isolated under concurrency.
# ===========================================================================
async def test_ts05_request_scope_credentials_no_leak(_seeded):
    await _seed(_seeded)

    async def resolve(key):
        credential = McpCredential(source="query_param", value=key)
        # yield control so the gathered tasks genuinely interleave
        await asyncio.sleep(0)
        agent = await server._authenticate_mcp_credential(credential)
        return agent.id if agent else None

    plan = [("kA1", "A1"), ("kA2", "A2")] * 6  # 12 overlapping
    results = await asyncio.gather(*(resolve(k) for k, _ in plan))
    for (_, expected), got in zip(plan, results):
        assert got == expected  # no global identity carrier leak across tasks


# ===========================================================================
# TS06 — gates: zero new singletons + purity + credential-usage green.
# ===========================================================================
def test_ts06_singleton_purity_credential_gates():
    from okto_pulse.core.application.boundary.mcp_credential_usage_gate import (
        run_mcp_credential_usage_gate,
    )
    from okto_pulse.core.application.boundary.singleton_gate import AntiSingletonGate
    from okto_pulse.core.application.purity_gate import run_application_purity_gate

    assert run_application_purity_gate().ok is True
    assert run_mcp_credential_usage_gate().ok is True

    sg = AntiSingletonGate().run()
    new = {n["name"] for n in sg.evidence["new_singletons"]}
    # R08-C introduces NO new singleton; only the pre-existing _worker remains.
    assert new <= {"_worker"}, f"R08-C introduced a new singleton: {new}"
