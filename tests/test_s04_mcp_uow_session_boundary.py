"""S04 boundary: legacy MCP service calls use an edition UoW scope."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest


def _server_tree() -> ast.Module:
    source = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "okto_pulse"
        / "core"
        / "mcp"
        / "server.py"
    ).read_text(encoding="utf-8")
    return ast.parse(source)


def test_mcp_catalog_has_no_direct_database_scope_openers() -> None:
    calls = [
        node
        for node in ast.walk(_server_tree())
        if isinstance(node, ast.AsyncWith)
        for item in node.items
        if isinstance(item.context_expr, ast.Call)
        and isinstance(item.context_expr.func, ast.Name)
        and item.context_expr.func.id == "get_db_for_mcp"
    ]
    uow_scopes = [
        node
        for node in ast.walk(_server_tree())
        if isinstance(node, ast.AsyncWith)
        for item in node.items
        if isinstance(item.context_expr, ast.Call)
        and isinstance(item.context_expr.func, ast.Name)
        and item.context_expr.func.id == "get_uow_session_for_mcp"
    ]

    assert calls == []
    assert len(uow_scopes) >= 36


@pytest.mark.asyncio
async def test_mcp_uow_scope_yields_only_the_context_from_registered_factory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.mcp import server

    session = object()

    class _Uow:
        def __init__(self) -> None:
            self.session = session

    class _Scope:
        async def __aenter__(self):
            return _Uow()

        async def __aexit__(self, *_args):
            return None

    monkeypatch.setattr(server, "get_unit_of_work_factory_for_mcp", lambda: lambda: _Scope())

    async with server.get_uow_session_for_mcp() as resolved:
        assert resolved is session
    async with server.get_db_for_mcp() as legacy_resolved:
        assert legacy_resolved is session


def test_mcp_host_calls_are_absent_from_the_core_catalog() -> None:
    source = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "okto_pulse"
        / "core"
        / "mcp"
        / "server.py"
    ).read_text(encoding="utf-8")

    assert "mcp.http_app(" not in source
    assert "import uvicorn" not in source
    assert "from starlette.requests" not in source
    assert "from starlette.types" not in source

    tree = ast.parse(source)
    imported_roots = {
        alias.name.split(".", 1)[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    imported_roots.update(
        (node.module or "").split(".", 1)[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.level == 0
    )
    assert "fastmcp" not in imported_roots


def test_core_catalog_reads_request_credentials_only_through_the_host_port(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.mcp import server
    from okto_pulse.core.ports import McpCredential

    credential = McpCredential(source="x_api_key_header", value="host-only")

    class _Host:
        def active_credential(self):
            return credential

    monkeypatch.setattr(server, "get_mcp_host_provider", lambda: _Host())

    assert server.active_api_key_credential() is credential
