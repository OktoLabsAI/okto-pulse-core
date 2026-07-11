"""Storage-neutral Discovery catalog reader and access policy."""

from __future__ import annotations

from typing import Any

from okto_pulse.core.ports.discovery_catalog import (
    DiscoveryIntentRecord,
    DiscoverySavedSearchRecord,
    DiscoverySearchHistoryRecord,
    get_discovery_catalog_read_port,
)
from okto_pulse.core.services.discovery_selector_catalog import SelectorAccessPolicy


class DiscoverySelectorRestAccessPolicy(SelectorAccessPolicy):
    async def can_read_board(
        self,
        db: object,
        identity: Any,
        board_id: str,
    ) -> bool:
        user_id = str(identity or "")
        if not user_id:
            return False
        return await get_discovery_catalog_read_port().can_read_board(
            db,
            board_id=board_id,
            user_id=user_id,
        )

    async def can_read_spec(
        self,
        db: object,
        identity: Any,
        spec: object,
    ) -> bool:
        board_id = getattr(spec, "board_id", None)
        if not board_id:
            return False
        return await self.can_read_board(db, identity, str(board_id))


class DiscoveryCatalogReader:
    def __init__(self, session: object) -> None:
        self.session = session

    async def list_active_intents(self) -> list[DiscoveryIntentRecord]:
        rows = await get_discovery_catalog_read_port().list_active_intents(
            self.session
        )
        return list(rows)

    async def list_saved_searches(
        self, board_id: str
    ) -> list[DiscoverySavedSearchRecord]:
        rows = await get_discovery_catalog_read_port().list_saved_searches(
            self.session,
            board_id=board_id,
        )
        return list(rows)

    async def list_search_history(
        self, board_id: str, user_id: str
    ) -> list[DiscoverySearchHistoryRecord]:
        rows = await get_discovery_catalog_read_port().list_search_history(
            self.session,
            board_id=board_id,
            user_id=user_id,
            limit=50,
        )
        return list(rows)

    async def get_intent(self, intent_id: str) -> DiscoveryIntentRecord | None:
        return await get_discovery_catalog_read_port().get_intent(
            self.session,
            intent_id=intent_id,
        )


__all__ = ["DiscoveryCatalogReader", "DiscoverySelectorRestAccessPolicy"]
