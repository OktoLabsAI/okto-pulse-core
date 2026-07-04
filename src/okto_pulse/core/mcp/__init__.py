"""Public MCP composition facade."""

from __future__ import annotations

from typing import Any

from okto_pulse.core.mcp.server import (
    build_mcp_asgi_app,
    mcp,
    mount_mcp,
    register_session_factory,
    run_mcp_server,
)


async def get_authenticated_agent_for_mcp() -> Any:
    """Return the current MCP-authenticated agent through a public hook."""
    from okto_pulse.core.mcp.server import _get_authenticated_agent

    return await _get_authenticated_agent()


def get_db_for_current_mcp_request() -> Any:
    """Return the current MCP request DB/session provider through a public hook."""
    from okto_pulse.core.mcp.server import get_db_for_mcp

    return get_db_for_mcp()


def effective_resource_catalog() -> Any:
    """Return the effective MCP resource catalog through the public facade."""
    from okto_pulse.core.mcp.server import effective_resource_catalog as _catalog

    return _catalog()


def register_resource_catalog(catalog: Any) -> None:
    """Register an edition MCP resource catalog through the public facade."""
    from okto_pulse.core.mcp.server import register_resource_catalog as _register

    _register(catalog)


def freeze_resource_catalog() -> None:
    """Freeze the effective MCP resource catalog through the public facade."""
    from okto_pulse.core.mcp.server import freeze_resource_catalog as _freeze

    _freeze()


__all__ = [
    "build_mcp_asgi_app",
    "effective_resource_catalog",
    "freeze_resource_catalog",
    "get_authenticated_agent_for_mcp",
    "get_db_for_current_mcp_request",
    "mcp",
    "mount_mcp",
    "register_resource_catalog",
    "register_session_factory",
    "run_mcp_server",
]
