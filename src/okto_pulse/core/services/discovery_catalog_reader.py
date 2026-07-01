"""Discovery catalog reader service (SaaS Refactor spec R01A REST-FU7-S4).

Houses the inline SQL that the legacy ``api/discovery.py`` GET/execute endpoints
ran directly on the request session — the active intent catalog, a board's saved
searches, a user's recent search history, and the single-intent lookup driving
``execute_discovery_intent`` — plus the REST selector board/spec read policy
(``DiscoverySelectorRestAccessPolicy``) that moved out of the router so the
``application/use_cases`` layer can construct it without importing the transport.

The relational coupling (``select``/``AsyncSession``/ORM models) belongs HERE in
the service layer; the use cases only call these reader methods so they stay
clean under the relational ratchet gate.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.models.db import (
    DiscoveryIntent,
    DiscoverySavedSearch,
    DiscoverySearchHistory,
    Spec,
)
from okto_pulse.core.services.discovery_selector_catalog import SelectorAccessPolicy
from okto_pulse.core.services.main import ShareService


class DiscoverySelectorRestAccessPolicy(SelectorAccessPolicy):
    """Board/spec read policy used by the selector REST flow.

    Moved verbatim from ``api/discovery.py`` so the selector use case can build it
    without the inbound layer reaching back into the transport package.
    """

    async def can_read_board(
        self,
        db: AsyncSession,
        identity: Any,
        board_id: str,
    ) -> bool:
        user_id = str(identity or "")
        if not user_id:
            return False
        permission = await ShareService(db).get_user_permission(board_id, user_id)
        return permission is not None

    async def can_read_spec(
        self,
        db: AsyncSession,
        identity: Any,
        spec: Spec,
    ) -> bool:
        board_id = getattr(spec, "board_id", None)
        if not board_id:
            return False
        return await self.can_read_board(db, identity, str(board_id))


class DiscoveryCatalogReader:
    """Read-only accessor over the Discovery catalog / saved-search / history
    tables. Each method replicates the exact query the legacy REST endpoint ran
    inline on the request session."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def list_active_intents(self) -> list[DiscoveryIntent]:
        """Active catalog of user-facing Discovery intents, ordered by
        category then label (legacy ``list_discovery_intents``)."""
        result = await self.session.execute(
            select(DiscoveryIntent)
            .where(DiscoveryIntent.active == True)  # noqa: E712
            .order_by(DiscoveryIntent.category, DiscoveryIntent.label)
        )
        return list(result.scalars().all())

    async def list_saved_searches(self, board_id: str) -> list[DiscoverySavedSearch]:
        """A board's saved searches, newest first (legacy ``list_saved_searches``)."""
        result = await self.session.execute(
            select(DiscoverySavedSearch)
            .where(DiscoverySavedSearch.board_id == board_id)
            .order_by(DiscoverySavedSearch.created_at.desc())
        )
        return list(result.scalars().all())

    async def list_search_history(
        self, board_id: str, user_id: str
    ) -> list[DiscoverySearchHistory]:
        """The current user's last 50 search entries on a board, newest first
        (legacy ``list_search_history``)."""
        result = await self.session.execute(
            select(DiscoverySearchHistory)
            .where(
                DiscoverySearchHistory.board_id == board_id,
                DiscoverySearchHistory.user_id == user_id,
            )
            .order_by(DiscoverySearchHistory.searched_at.desc())
            .limit(50)
        )
        return list(result.scalars().all())

    async def get_intent(self, intent_id: str) -> DiscoveryIntent | None:
        """Resolve a single intent by id (legacy ``execute_discovery_intent``
        lookup). The active-state gate stays with the caller, exactly as the
        legacy endpoint did."""
        result = await self.session.execute(
            select(DiscoveryIntent).where(DiscoveryIntent.id == intent_id)
        )
        return result.scalar_one_or_none()
