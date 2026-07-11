"""Host boundary for the MCP command catalog.

Core defines and validates the command catalog but does not choose an ASGI
runtime, middleware stack or listener.  Editions compose that transport through
this port; Community supplies the Local First FastMCP host and a SaaS edition
can supply its own authenticated gateway.
"""

from __future__ import annotations

from okto_pulse.core.runtime_context import register_runtime_value, reset_runtime_values, resolve_runtime_value

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


_RUNTIME_KEY = "ports.mcp_host.provider"


def register_mcp_host_provider(provider: McpHostProvider) -> None:
    """Register the MCP host selected by the edition composition root."""

    register_runtime_value(_RUNTIME_KEY, provider)


def get_mcp_host_provider() -> McpHostProvider:
    """Resolve the composed host, failing closed outside an edition runtime."""

    provider = resolve_runtime_value(_RUNTIME_KEY)
    if provider is None:
        raise McpHostProviderMissing()
    return provider


def reset_mcp_host_provider_for_tests() -> None:
    """Clear explicit test composition."""

    reset_runtime_values(_RUNTIME_KEY)


__all__ = [
    "McpHostProvider",
    "McpHostProviderMissing",
    "get_mcp_host_provider",
    "register_mcp_host_provider",
    "reset_mcp_host_provider_for_tests",
]
