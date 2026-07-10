"""Host boundary for the MCP command catalog.

Core defines and validates the command catalog but does not choose an ASGI
runtime, middleware stack or listener.  Editions compose that transport through
this port; Community supplies the Local First FastMCP host and a SaaS edition
can supply its own authenticated gateway.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


class McpHostProviderMissing(RuntimeError):
    """Raised when an MCP host is requested before edition composition."""

    code = "mcp_host_provider_missing"

    def __init__(self) -> None:
        super().__init__(
            "mcp_host_provider_missing: the edition composition root must "
            "register an MCP host provider before serving the command catalog"
        )


@runtime_checkable
class McpHostProvider(Protocol):
    """Edition-owned ASGI host for a Core MCP command catalog."""

    def active_credential(self) -> Any | None:
        """Return the current transport credential, or ``None`` outside a request."""
        ...

    def build_asgi_app(self, catalog: Any, *, trace_sink: Any | None = None) -> Any:
        ...

    def wrap_session_middleware(self, app: Any) -> Any:
        ...

    def mount(
        self,
        app: Any,
        catalog: Any,
        *,
        mount_path: str,
        trace_sink: Any | None = None,
    ) -> None:
        ...


_mcp_host_provider: McpHostProvider | None = None


def register_mcp_host_provider(provider: McpHostProvider) -> None:
    """Register the MCP host selected by the edition composition root."""

    global _mcp_host_provider
    _mcp_host_provider = provider


def get_mcp_host_provider() -> McpHostProvider:
    """Resolve the composed host, failing closed outside an edition runtime."""

    if _mcp_host_provider is None:
        raise McpHostProviderMissing()
    return _mcp_host_provider


def reset_mcp_host_provider_for_tests() -> None:
    """Clear explicit test composition."""

    global _mcp_host_provider
    _mcp_host_provider = None


__all__ = [
    "McpHostProvider",
    "McpHostProviderMissing",
    "get_mcp_host_provider",
    "register_mcp_host_provider",
    "reset_mcp_host_provider_for_tests",
]
