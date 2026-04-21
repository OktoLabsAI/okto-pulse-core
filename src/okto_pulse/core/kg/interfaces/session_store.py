"""SessionStore Protocol — async session lifecycle contract."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from okto_pulse.core.kg.session_manager import ConsolidationSession


@runtime_checkable
class SessionStore(Protocol):
    @property
    def default_ttl_seconds(self) -> int: ...

    async def create(
        self,
        *,
        session_id: str,
        board_id: str,
        artifact_id: str,
        artifact_type: str,
        agent_id: str,
        raw_content: str,
        ttl_seconds: int | None = None,
    ) -> ConsolidationSession: ...

    async def get(self, session_id: str) -> ConsolidationSession | None: ...

    async def remove(self, session_id: str) -> None: ...

    async def sweep_expired(self) -> int: ...

    async def active_count(self) -> int: ...
