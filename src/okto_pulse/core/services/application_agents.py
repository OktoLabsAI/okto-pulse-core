"""Application-facing agent helpers.

Public facade for agent credential, authentication and ACL primitives that
Community needs without importing the legacy ``services.main`` module.
"""

from __future__ import annotations

import hashlib
from typing import Any

from okto_pulse.core.ports import AgentAuthSession
from okto_pulse.core.ports.relational_application import (
    AgentPermissionContext,
    require_relational_application_adapter,
)


def hash_api_key(key: str) -> str:
    """Hash an API key without selecting a persistence adapter."""

    return hashlib.sha256(key.encode()).hexdigest()


def credential_marker(key_hash: str) -> str:
    """Return the non-recoverable compatibility marker for legacy rows."""

    return f"sha256:{key_hash[:57]}"


async def authenticate_agent_by_api_key(
    db: Any,
    api_key: str,
    *,
    credential_source: str = "unknown",
) -> AgentAuthSession | None:
    """Authenticate an agent API key and return the public MCP auth DTO.

    This read path deliberately avoids touching ``last_used_at`` so MCP request
    authentication does not dirty/commit a SQLAlchemy session solely for audit
    metadata. Call an explicit usage-recording method when that write is wanted.
    """

    return await require_relational_application_adapter().agent_authentication(
        db
    ).authenticate_agent_by_api_key(
        api_key,
        credential_source=credential_source,
    )


async def list_accessible_board_ids_for_agent(db: Any, agent_id: str) -> list[str]:
    """Return board ids visible to an agent through the canonical ACL policy."""

    return await require_relational_application_adapter().agent_authentication(
        db
    ).list_accessible_board_ids_for_agent(agent_id)


async def agent_has_board_access(db: Any, agent_id: str, board_id: str) -> bool:
    """Return whether an agent is explicitly granted to a board."""

    return await require_relational_application_adapter().agent_authentication(
        db
    ).agent_has_board_access(agent_id, board_id)


async def resolve_agent_permission_context(
    db: Any,
    agent_id: str,
    *,
    board_id: str | None = None,
) -> AgentPermissionContext | None:
    """Resolve an agent's effective MCP permission context by identity.

    ``board_id`` applies the same board override layer the previous MCP bootstrap
    helper applied. It intentionally authenticates by ``agent_id`` only: raw MCP
    credentials must already have been resolved by the registered
    ``McpAuthenticator`` port.
    """

    return await require_relational_application_adapter().agent_authentication(
        db
    ).resolve_agent_permission_context(
        agent_id,
        board_id=board_id,
    )


__all__ = [
    "AgentPermissionContext",
    "agent_has_board_access",
    "authenticate_agent_by_api_key",
    "credential_marker",
    "hash_api_key",
    "list_accessible_board_ids_for_agent",
    "resolve_agent_permission_context",
]
