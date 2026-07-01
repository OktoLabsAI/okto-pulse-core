"""R06 — MCP AuthContext concrete belongs to Community, core fails closed."""

from __future__ import annotations

import asyncio
from pathlib import Path

from kg_registry_testing import configure_test_kg_registry


def _inventory_entry():
    from okto_pulse.core.application.boundary.adapter_readiness_inventory import (
        build_adapter_inventory,
    )

    return next(e for e in build_adapter_inventory() if e.adapter_key == "mcp_auth_context")


def test_core_mcp_auth_context_module_is_contract_tombstone_only() -> None:
    import okto_pulse.core.kg.providers.embedded.mcp_auth_context as core_bridge

    src = Path(core_bridge.__file__).read_text(encoding="utf-8")
    assert hasattr(core_bridge, "AuthContext")
    assert not hasattr(core_bridge, "MCPAuthContext")
    assert not hasattr(core_bridge, "create_mcp_auth_factory")
    assert "AgentService" not in src
    assert "list_boards_for_agent" not in src


def test_kg_query_tools_missing_auth_context_fails_closed_without_fallback() -> None:
    from okto_pulse.core.kg.interfaces import registry as reg
    from okto_pulse.core.mcp.kg_query_tools import _get_user_boards

    saved = (reg._registry, reg._configured)
    try:
        reg.reset_registry_for_tests()
        configure_test_kg_registry()

        async def forbidden_agent():
            raise AssertionError("get_agent fallback must not be called")

        def forbidden_uow():
            raise AssertionError("get_uow fallback must not be called")

        agent, boards = asyncio.run(
            _get_user_boards(get_agent=forbidden_agent, get_uow=forbidden_uow)
        )
        assert agent is None
        assert boards == []
    finally:
        reg._registry, reg._configured = saved


def test_auth_gate_blocks_core_concrete_bridge_and_acl_fallback(tmp_path) -> None:
    from okto_pulse.core.application.boundary.mcp_credential_usage_gate import (
        SENSITIVE_SYMBOLS,
        run_mcp_credential_usage_gate,
    )

    assert "MCPAuthContext" in SENSITIVE_SYMBOLS
    assert "create_mcp_auth_factory" in SENSITIVE_SYMBOLS
    assert "AgentService" in SENSITIVE_SYMBOLS
    assert "list_boards_for_agent" in SENSITIVE_SYMBOLS
    assert run_mcp_credential_usage_gate().ok is True

    rogue = tmp_path / "okto_pulse" / "core" / "mcp" / "rogue_acl.py"
    rogue.parent.mkdir(parents=True, exist_ok=True)
    rogue.write_text(
        "from okto_pulse.core.kg.providers.embedded.mcp_auth_context import MCPAuthContext\n"
        "from okto_pulse.core.services.main import AgentService\n"
        "async def f(db, agent):\n"
        "    ctx = MCPAuthContext(lambda: agent, lambda: db)\n"
        "    return await AgentService(db).list_boards_for_agent(agent.id), ctx\n",
        encoding="utf-8",
    )
    report = run_mcp_credential_usage_gate(source_root=tmp_path)
    flagged = {(v.file, v.symbol) for v in report.violations}
    assert ("okto_pulse/core/mcp/rogue_acl.py", "MCPAuthContext") in flagged
    assert ("okto_pulse/core/mcp/rogue_acl.py", "AgentService") in flagged
    assert ("okto_pulse/core/mcp/rogue_acl.py", "list_boards_for_agent") in flagged


def test_readiness_inventory_tracks_community_owner_and_evidence_gate() -> None:
    from okto_pulse.core.application.boundary.adapter_readiness_inventory import (
        REQUIRED_EVIDENCE,
        AdapterEvidence,
        evaluate_adapter_readiness,
    )

    entry = _inventory_entry()
    assert entry.owner == "okto-pulse-community/inbound-mcp"
    assert entry.current_module == "okto_pulse/community/adapters/mcp_auth.py"
    assert "auth_context_factory" in entry.removal_criterion
    assert "_missing" in entry.removal_criterion

    full = {name: True for name in REQUIRED_EVIDENCE}
    without_community = AdapterEvidence(**{**full, "community_registered": False})
    blocked = evaluate_adapter_readiness(entry, without_community)
    assert blocked.status == "blocked"
    assert "community_registered" in blocked.failed_evidence

    ready = evaluate_adapter_readiness(entry, AdapterEvidence(**full))
    assert ready.is_ready is True


def test_telemetry_dependency_audit_covers_kg_query_auth_surface() -> None:
    from okto_pulse.core.application.boundary.telemetry_dependency_audit import (
        R08_AUTH_MODULES,
    )

    assert "mcp/kg_query_tools.py" in R08_AUTH_MODULES
