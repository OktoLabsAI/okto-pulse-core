"""AuthContext Protocol — async authentication/authorization contract."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from okto_pulse.core.domain.realm import RealmScope


@runtime_checkable
class AuthContext(Protocol):
    async def get_agent_id(self) -> str | None:
        """Return the authenticated agent's ID, or None if unauthenticated."""
        ...

    async def get_accessible_boards(self) -> list[str]:
        """Return board IDs the authenticated agent can access."""
        ...

    async def get_realm_scope(self) -> RealmScope:
        """Return the non-optional tenant boundary for KG operations."""
        ...

    def has_admin_role(self) -> bool:
        """Whether the current context has admin privileges."""
        ...
