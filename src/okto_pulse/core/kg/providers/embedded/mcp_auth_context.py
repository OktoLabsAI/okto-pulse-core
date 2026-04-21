"""MCPAuthContext — satisfies AuthContext Protocol for the MCP server.

Lazily resolves agent identity and board ACL from the get_agent/get_db
callables provided by server.py. Results are cached per-instance (one
instance per MCP tool call).
"""

from __future__ import annotations

from typing import Any, Callable


class MCPAuthContext:
    def __init__(self, get_agent: Callable, get_db: Callable):
        self._get_agent = get_agent
        self._get_db = get_db
        self._agent: Any = _UNSET
        self._boards: list[str] | None = None

    async def _resolve_agent(self):
        if self._agent is _UNSET:
            self._agent = await self._get_agent()
        return self._agent

    async def get_agent_id(self) -> str | None:
        agent = await self._resolve_agent()
        return agent.id if agent else None

    async def get_accessible_boards(self) -> list[str]:
        if self._boards is not None:
            return self._boards
        agent = await self._resolve_agent()
        if agent is None:
            self._boards = []
            return self._boards
        async with self._get_db() as db:
            from okto_pulse.core.services.main import AgentService

            svc = AgentService(db)
            boards = await svc.list_boards_for_agent(agent.id)
            await db.commit()
            self._boards = [b.id for b in boards]
        return self._boards

    def has_admin_role(self) -> bool:
        return False


_UNSET = object()


def create_mcp_auth_factory(get_agent: Callable, get_db: Callable) -> Callable:
    """Build an auth_context_factory for the MCP server bootstrap."""

    def factory() -> MCPAuthContext:
        return MCPAuthContext(get_agent, get_db)

    return factory
