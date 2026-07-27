"""R08-B/R06 — AuthSession/AgentAuthSession (R08-A) <-> KG AuthContext.

Scenario mapping:
  ts_68899123 — the Community MCPAuthContext bridge CONFORMS
                to AuthContext: runtime_checkable isinstance AND behavioural
                (the 3 methods resolve agent_id/boards via AgentService — no
                bypass — and has_admin_role()).
  ts_7f7bbdfd — the KG query tools (_get_user_boards) resolve agent_id + boards
                via the registered AuthContext factory, board ACL through
                AgentService.list_boards_for_agent (an agent without membership
                gets [] — no bypass).
  ts_8c088b7d — the old get_agent/get_uow fallback now fails closed; server
                facades (_get_agent_ctx + _permission_cache) are NOT removed.
  ts_0f6f619e — the adapter readiness inventory ledgers mcp_auth_context as
                Community-owned readiness, not a core concrete.
"""

from __future__ import annotations

import asyncio
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path

import pytest

import okto_pulse.community.app as _core_app  # noqa: F401  (registers ORM models)
import okto_pulse.community.adapters.sqlalchemy_database as _db_mod
from okto_pulse.core.kg.interfaces.auth_context import AuthContext
from okto_pulse.core.ports.mcp_auth import AgentAuthSession

from kg_registry_testing import configure_test_kg_registry


@pytest.fixture
def _seeded_db():
    """Temp SQLite DB seeded with: agent A1 -> board B1 (member), agent A2 -> no
    membership. Yields (session_factory, ids). Restores settings/engine/factory."""
    import okto_pulse.core.infra.config as _config
    from okto_pulse.core.infra.config import CoreSettings
    from okto_pulse.core.runtime_context import (
        capture_runtime_values_for_tests,
        restore_runtime_values_for_tests,
    )

    saved_runtime = capture_runtime_values_for_tests()
    saved_data = os.environ.get("DATA_DIR")
    import tempfile

    tmp = tempfile.mkdtemp()
    runtime_swapped = False
    try:
        os.environ["DATA_DIR"] = tmp
        _config.configure_settings(CoreSettings())

        from sqlalchemy_test_models import Agent, AgentBoard, Board

        now = datetime.now(timezone.utc)

        async def _setup():
            await _db_mod.init_db()
            async with _db_mod.get_session_factory()() as s:
                s.add(
                    Board(
                        id="B1",
                        name="Board One",
                        owner_id="A1",
                        realm_id="local",
                        created_at=now,
                        updated_at=now,
                    )
                )
                s.add(Agent(id="A1", name="Agent One", api_key="k1", api_key_hash="h1",
                            is_active=True, created_by="owner", created_at=now))
                s.add(Agent(id="A2", name="Agent Two", api_key="k2", api_key_hash="h2",
                            is_active=True, created_by="owner", created_at=now))
                s.add(AgentBoard(id="AB1", agent_id="A1", board_id="B1",
                                 granted_by="owner", granted_at=now))
                await s.commit()

        _db_mod.configure_community_database(
            f"sqlite+aiosqlite:///{Path(tmp) / 'r08b.db'}"
        )
        from okto_pulse.community.adapters.relational_schema_lifecycle import (
            register_community_relational_schema_lifecycle,
        )

        register_community_relational_schema_lifecycle()
        runtime_swapped = True
        asyncio.run(_setup())
        yield _db_mod.get_session_factory()
    finally:
        if runtime_swapped:
            try:
                asyncio.run(_db_mod.close_db())
            except Exception:
                pass
        restore_runtime_values_for_tests(saved_runtime)
        if saved_data is None:
            os.environ.pop("DATA_DIR", None)
        else:
            os.environ["DATA_DIR"] = saved_data
        shutil.rmtree(tmp, ignore_errors=True)


class _Agent:
    def __init__(self, id):
        self.id = id


def _community_bridge():
    return pytest.importorskip("okto_pulse.community.adapters.mcp_auth")


# ===========================================================================
# ts_68899123 — conformance: structural + behavioural (no ACL bypass).
# ===========================================================================
def test_ts68899123_mcp_auth_context_conforms_structurally():
    import okto_pulse.core.kg.providers.embedded.mcp_auth_context as core_bridge

    assert core_bridge.AuthContext is AuthContext
    assert not hasattr(core_bridge, "MCPAuthContext")
    assert not hasattr(core_bridge, "create_mcp_auth_factory")

    bridge = _community_bridge()
    # AuthContext is @runtime_checkable -> structural isinstance is meaningful.
    ctx = bridge.MCPAuthContext(get_agent=lambda: None, get_db=lambda: None)
    assert isinstance(ctx, AuthContext)
    # A type missing a method is rejected (the check has teeth).

    class _Broken:
        async def get_agent_id(self):
            return None

    assert not isinstance(_Broken(), AuthContext)


def test_ts68899123_mcp_auth_context_behavioural_no_bypass(_seeded_db):
    bridge = _community_bridge()
    sf = _seeded_db

    async def _agent():
        return _Agent("A1")

    ctx = bridge.MCPAuthContext(get_agent=_agent, get_db=sf)

    async def drive():
        return (
            await ctx.get_agent_id(),
            await ctx.get_accessible_boards(),
            ctx.has_admin_role(),
        )

    agent_id, boards, admin = asyncio.run(drive())
    assert agent_id == "A1"
    # Boards came through AgentService.list_boards_for_agent (no bypass).
    assert boards == ["B1"]
    assert admin is False


def test_ts68899123_bridge_session_to_auth_context(_seeded_db):
    bridge = _community_bridge()
    sf = _seeded_db
    # Bridge a resolved R08-A session -> AuthContext (reusing MCPAuthContext).
    bridged = bridge.auth_context_from_session(
        AgentAuthSession(agent_id="A1", agent_name="Agent One", is_active=True), sf
    )
    assert isinstance(bridged, bridge.MCPAuthContext)
    assert isinstance(bridged, AuthContext)

    async def drive():
        return await bridged.get_agent_id(), await bridged.get_accessible_boards()

    agent_id, boards = asyncio.run(drive())
    assert agent_id == "A1" and boards == ["B1"]  # ACL via AgentService, no bypass

    # Fail-closed: inactive / absent session -> unauthenticated.
    inactive = bridge.auth_context_from_session(
        AgentAuthSession("A1", "Agent One", is_active=False), sf
    )
    assert asyncio.run(inactive.get_agent_id()) is None
    assert asyncio.run(bridge.auth_context_from_session(None, sf).get_agent_id()) is None


# ===========================================================================
# ts_7f7bbdfd — KG query tools resolve via the registered AuthContext (no bypass).
# ===========================================================================
def test_ts7f7bbdfd_kg_query_tools_use_auth_context_no_bypass(_seeded_db):
    bridge = _community_bridge()
    sf = _seeded_db
    from okto_pulse.core.kg.interfaces import registry as reg
    from okto_pulse.core.mcp.kg_query_tools import _get_user_boards

    saved = reg.capture_registry_state_for_tests()
    try:
        async def _agent_a1():
            return _Agent("A1")

        reg.reset_registry_for_tests()
        configure_test_kg_registry(
            auth_context_factory=bridge.create_mcp_auth_factory(_agent_a1, sf)
        )
        agent, boards = asyncio.run(_get_user_boards())
        # Resolved THROUGH the AuthContext port (factory registered), ACL via
        # AgentService -> A1 is a member of B1 only.
        assert agent is not None and agent.id == "A1"
        assert boards == ["B1"]

        # An agent WITHOUT membership gets [] — ACL enforced, no bypass.
        async def _agent_a2():
            return _Agent("A2")

        reg.reset_registry_for_tests()
        configure_test_kg_registry(
            auth_context_factory=bridge.create_mcp_auth_factory(_agent_a2, sf)
        )
        agent2, boards2 = asyncio.run(_get_user_boards())
        assert agent2.id == "A2" and boards2 == []
    finally:
        reg.restore_registry_state_for_tests(saved)


# ===========================================================================
# ts_8c088b7d — legacy facade path preserved (same ACL, facades not removed).
# ===========================================================================
def test_ts8c088b7d_missing_auth_context_fails_closed(_seeded_db):
    from okto_pulse.core.kg.interfaces import registry as reg
    from okto_pulse.core.mcp.kg_query_tools import _get_user_boards

    saved = reg.capture_registry_state_for_tests()
    try:
        # NO auth_context_factory registered -> fail closed. The injected
        # get_agent/get_uow callables must not be consulted.
        reg.reset_registry_for_tests()
        configure_test_kg_registry()

        async def _agent_a1():
            raise AssertionError("R06 forbids get_agent fallback")

        agent, boards = asyncio.run(
            _get_user_boards(
                get_agent=_agent_a1,
                get_uow=lambda: (_ for _ in ()).throw(
                    AssertionError("R06 forbids get_uow fallback")
                ),
            )
        )
        assert agent is None and boards == []
    finally:
        reg.restore_registry_state_for_tests(saved)


def test_ts8c088b7d_server_facades_not_removed():
    # R08-B is register-HALF: the legacy server facades + permission cache stay.
    import okto_pulse.core.mcp.server as server

    src = Path(server.__file__).read_text(encoding="utf-8")
    assert "async def _get_authenticated_agent" in src
    assert "async def _get_agent_ctx" in src
    assert "_permission_cache" in src  # permission cache facade preserved


def test_ts8c088b7d_api_key_surface_secret_free():
    # TR1/TR5: R08-B does NOT remove/alter Agent.api_key/api_key_hash, while
    # AgentResponse stays secret-free.
    from sqlalchemy_test_models import Agent
    from okto_pulse.core.models.schemas import AgentResponse

    agent_cols = {c.name for c in Agent.__table__.columns}
    assert {"api_key", "api_key_hash"} <= agent_cols
    assert "api_key" not in AgentResponse.model_fields


# ===========================================================================
# ts_0f6f619e — ledger: mcp_auth_context fail-closed in-transition.
# ===========================================================================
def test_ts0f6f619e_inventory_ledgers_mcp_auth_context_fail_closed():
    from okto_pulse.core.application.boundary.adapter_readiness_inventory import (
        build_adapter_inventory,
    )

    entry = next(
        e for e in build_adapter_inventory() if e.adapter_key == "mcp_auth_context"
    )
    # Fail-closed in-transition: actionable, but NOT ready/removed.
    assert entry.status == "blocked"
    assert entry.owner == "okto-pulse-community/inbound-mcp"
    assert entry.current_module == "okto_pulse/community/adapters/mcp_auth.py"
    assert entry.port_ref == "AuthContext / McpAuthenticator"
    assert "boards_acl_resolution" in entry.oracles_required
    # The removal criterion records Community ownership and read-time fail-closed
    # without promoting auth_context_factory into configure-time _missing.
    crit = entry.removal_criterion
    assert "okto_pulse.community.adapters.mcp_auth" in crit
    assert "fail closed" in crit
    assert "auth_context_factory" in crit
    assert "_missing" in crit
