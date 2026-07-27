"""Public MCP composition facade."""

from __future__ import annotations

from typing import Any

from okto_pulse.core.mcp.server import (
    build_mcp_asgi_app,
    freeze_instruction_providers,
    has_instruction_provider,
    mcp,
    mount_mcp,
    register_instruction_provider,
    register_mcp_authenticator,
    register_scheduler_control_for_mcp,
)


async def get_authenticated_agent_for_mcp() -> Any:
    """Return the current MCP-authenticated agent through a public hook."""
    from okto_pulse.core.mcp.server import _get_authenticated_agent

    return await _get_authenticated_agent()


def effective_resource_catalog() -> Any:
    """Return the effective MCP resource catalog through the public facade."""
    from okto_pulse.core.mcp.server import effective_resource_catalog as _catalog

    return _catalog()


def effective_resource_manifest() -> Any:
    """Return the canonical manifest of the frozen resource projection."""

    from okto_pulse.core.mcp.server import effective_resource_manifest as _manifest

    return _manifest()


def resource_catalog_is_frozen() -> bool:
    """Return whether resource composition has published its frozen snapshot."""

    from okto_pulse.core.mcp.server import resource_catalog_is_frozen as _is_frozen

    return _is_frozen()


def register_resource_catalog(catalog: Any) -> None:
    """Register an edition MCP resource catalog through the public facade."""
    from okto_pulse.core.mcp.server import register_resource_catalog as _register

    _register(catalog)


def freeze_resource_catalog() -> None:
    """Freeze the effective MCP resource catalog through the public facade."""
    from okto_pulse.core.mcp.server import freeze_resource_catalog as _freeze

    _freeze()


def invalidate_agent_cache(agent_id: str) -> None:
    """Invalidate one agent through the public MCP composition facade."""

    from okto_pulse.core.mcp.server import invalidate_agent_cache as _invalidate

    _invalidate(agent_id)


__all__ = [
    "build_mcp_asgi_app",
    "effective_resource_catalog",
    "effective_resource_manifest",
    "freeze_resource_catalog",
    "freeze_instruction_providers",
    "get_authenticated_agent_for_mcp",
    "has_instruction_provider",
    "invalidate_agent_cache",
    "mcp",
    "mount_mcp",
    "register_instruction_provider",
    "register_mcp_authenticator",
    "register_scheduler_control_for_mcp",
    "register_resource_catalog",
    "resource_catalog_is_frozen",
]
