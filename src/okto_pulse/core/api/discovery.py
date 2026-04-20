"""Discovery REST router — exposes the intent catalog, saved searches
and per-user search history to the Global Discovery screen.

v1 is read-only for the intent catalog (GET). Admin CRUD on intents
(POST/PUT/DELETE) is deferred to a follow-up card tracked alongside the
parent spec. Saved-search and search-history write endpoints are also
future work — v1 only ships the GET surface needed by the redesigned
GlobalSearchView.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.infra.database import get_db
from okto_pulse.core.infra.auth import require_user
from okto_pulse.core.models.db import (
    DiscoveryIntent,
    DiscoverySavedSearch,
    DiscoverySearchHistory,
)
from okto_pulse.core.models.schemas import (
    DiscoveryIntentResponse,
    DiscoverySavedSearchResponse,
    DiscoverySearchHistoryResponse,
)

router = APIRouter()


@router.get(
    "/discovery/intents",
    response_model=list[DiscoveryIntentResponse],
)
async def list_discovery_intents(
    _user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
) -> list[DiscoveryIntent]:
    """Return the active catalog of user-facing Discovery intents."""
    result = await db.execute(
        select(DiscoveryIntent)
        .where(DiscoveryIntent.active == True)  # noqa: E712
        .order_by(DiscoveryIntent.category, DiscoveryIntent.label)
    )
    return list(result.scalars().all())


@router.get(
    "/discovery/boards/{board_id}/saved-searches",
    response_model=list[DiscoverySavedSearchResponse],
)
async def list_saved_searches(
    board_id: str,
    _user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
) -> list[DiscoverySavedSearch]:
    """Return the saved searches for a board (shared with all members)."""
    result = await db.execute(
        select(DiscoverySavedSearch)
        .where(DiscoverySavedSearch.board_id == board_id)
        .order_by(DiscoverySavedSearch.created_at.desc())
    )
    return list(result.scalars().all())


@router.get(
    "/discovery/boards/{board_id}/search-history",
    response_model=list[DiscoverySearchHistoryResponse],
)
async def list_search_history(
    board_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
) -> list[DiscoverySearchHistory]:
    """Return the current user's last 50 search entries on this board."""
    result = await db.execute(
        select(DiscoverySearchHistory)
        .where(
            DiscoverySearchHistory.board_id == board_id,
            DiscoverySearchHistory.user_id == user_id,
        )
        .order_by(DiscoverySearchHistory.searched_at.desc())
        .limit(50)
    )
    return list(result.scalars().all())
