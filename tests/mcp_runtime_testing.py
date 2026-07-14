"""Test-only MCP runtime composition helpers."""

from __future__ import annotations

from okto_pulse.core.mcp.server import (
    register_mcp_authenticator,
    register_scheduler_control_for_mcp,
)
from okto_pulse.core.ports.relational_application import (
    require_relational_application_adapter,
)


class _RelationalTestMcpAuthenticator:
    def __init__(self, relational_scope_factory) -> None:
        self._relational_scope_factory = relational_scope_factory

    async def authenticate(self, credential):
        if credential is None or not getattr(credential, "value", None):
            return None
        async with self._relational_scope_factory() as relational_scope:
            gateway = require_relational_application_adapter().agent_authentication(
                relational_scope
            )
            return await gateway.authenticate_agent_by_api_key(
                credential.value,
                credential_source=str(getattr(credential, "source", "mcp")),
            )


def register_mcp_test_runtime(
    relational_scope_factory,
    *,
    scheduler_control=None,
    mcp_authenticator=None,
) -> None:
    """Compose MCP ports for SQLAlchemy-backed integration tests."""

    register_mcp_authenticator(
        mcp_authenticator
        if mcp_authenticator is not None
        else _RelationalTestMcpAuthenticator(relational_scope_factory)
    )
    register_scheduler_control_for_mcp(scheduler_control)


__all__ = ["register_mcp_test_runtime"]
