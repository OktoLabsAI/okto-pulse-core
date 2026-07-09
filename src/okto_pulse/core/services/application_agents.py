"""Application-facing agent helpers.

Public facade for agent credential, authentication and ACL primitives that
Community needs without importing the legacy ``services.main`` module.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from okto_pulse.core.ports import AgentAuthSession


@dataclass(frozen=True)
class AgentPermissionContext:
    """Resolved agent identity plus effective MCP permissions."""

    agent_id: str
    agent_name: str
    permissions: Any


def hash_api_key(key: str) -> str:
    from okto_pulse.core.services.main import AgentService

    return AgentService.hash_api_key(key)


def credential_marker(key_hash: str) -> str:
    from okto_pulse.core.services.main import AgentService

    return AgentService.credential_marker(key_hash)


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

    from okto_pulse.core.services.main import AgentService

    agent = await AgentService(db).get_agent_by_key(api_key, touch_last_used_at=False)
    if agent is None:
        return None
    return AgentAuthSession(
        agent_id=agent.id,
        agent_name=agent.name,
        is_active=bool(getattr(agent, "is_active", True)),
        metadata={"credential_source": credential_source},
    )


async def list_accessible_board_ids_for_agent(db: Any, agent_id: str) -> list[str]:
    """Return board ids visible to an agent through the canonical ACL policy."""

    from okto_pulse.core.services.main import AgentService

    boards = await AgentService(db).list_boards_for_agent(agent_id)
    await db.commit()
    return [board.id for board in boards]


async def agent_has_board_access(db: Any, agent_id: str, board_id: str) -> bool:
    """Return whether an agent is explicitly granted to a board."""

    from okto_pulse.core.services.main import AgentService

    return await AgentService(db).agent_has_board_access(agent_id, board_id)


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

    from sqlalchemy import select

    from okto_pulse.core.infra.permissions import resolve_permissions
    from okto_pulse.core.models.db import Agent, AgentBoard, PermissionPreset

    agent = await db.get(Agent, agent_id)
    if agent is None or not bool(getattr(agent, "is_active", True)):
        return None

    agent_board = None
    if board_id:
        result = await db.execute(
            select(AgentBoard).where(
                AgentBoard.agent_id == agent.id,
                AgentBoard.board_id == board_id,
            )
        )
        agent_board = result.scalar_one_or_none()
        if agent_board is None:
            return None

    agent_flags = getattr(agent, "permission_flags", None)
    if agent_flags is not None:
        preset_flags = None
        preset_id = getattr(agent, "preset_id", None)
        if preset_id:
            preset = await db.get(PermissionPreset, preset_id)
            if preset:
                preset_flags = preset.flags
        board_overrides = (
            getattr(agent_board, "permission_overrides", None)
            if agent_board is not None
            else None
        )
        permissions = resolve_permissions(agent_flags, preset_flags, board_overrides)
    else:
        permissions = agent.permissions

    return AgentPermissionContext(
        agent_id=agent.id,
        agent_name=agent.name,
        permissions=permissions,
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
