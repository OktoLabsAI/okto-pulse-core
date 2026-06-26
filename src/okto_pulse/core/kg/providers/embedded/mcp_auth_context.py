"""MCPAuthContext — satisfies AuthContext Protocol for the MCP server.

Lazily resolves agent identity and board ACL from the get_agent/get_db
callables provided by server.py. Results are cached per-instance (one
instance per MCP tool call).

R08-B (bridge): ``auth_context_from_session`` adapts an R08-A resolved
``AuthSession`` / ``AgentAuthSession`` (identity only — never the api_key) to the
KG ``AuthContext`` by REUSING this same ``MCPAuthContext`` (FR1 — no second
context factory). The session supplies the agent identity; board ACL still
resolves through ``AgentService.list_boards_for_agent`` (no bypass).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from okto_pulse.core.ports.mcp_auth import AuthSession


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


@dataclass(frozen=True)
class _SessionAgent:
    """Minimal agent holder carrying ONLY the id that MCPAuthContext reads from a
    resolved R08-A AuthSession — no api_key, no ORM/Agent coupling."""

    id: str


def auth_context_from_session(
    session: "AuthSession | None", get_db: Callable
) -> MCPAuthContext:
    """Bridge an R08-A ``AuthSession`` (resolved agent identity, NEVER the
    api_key) to the KG ``AuthContext``, REUSING ``MCPAuthContext`` (FR1 — a single
    context factory, no parallel implementation).

    The session supplies the agent identity via a synthesized ``get_agent``;
    board ACL still resolves through ``MCPAuthContext`` ->
    ``AgentService.list_boards_for_agent`` (no ACL bypass). Fail-closed: an absent
    or inactive session yields an unauthenticated context (``get_agent`` -> None,
    so ``get_agent_id`` -> None and ``get_accessible_boards`` -> [])."""

    async def _get_agent():
        if session is None or not getattr(session, "is_active", False):
            return None
        return _SessionAgent(session.agent_id)

    return MCPAuthContext(_get_agent, get_db)
