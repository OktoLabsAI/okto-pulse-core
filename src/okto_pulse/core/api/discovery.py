"""Discovery REST router — exposes the intent catalog, saved searches
and per-user search history to the Global Discovery screen.

v1 is read-only for the intent catalog (GET). Admin CRUD on intents
(POST/PUT/DELETE) is deferred to a follow-up card tracked alongside the
parent spec. Saved-search and search-history write endpoints are also
future work — v1 only ships the GET surface needed by the redesigned
GlobalSearchView.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException
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
from okto_pulse.core.services.discovery_executor import execute_intent

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


@router.post("/discovery/intents/{intent_id}/execute")
async def execute_discovery_intent(
    intent_id: str,
    payload: dict[str, Any] = Body(default_factory=dict),
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    """Execute the real tool bound to an intent and return a normalized payload.

    Ideação a4f526df — this is what closes the "semantic fallback masking
    real tool" gap observed in the v1 catalog. Body shape::

        {"board_id": "<uuid>", "params": {"topic": "...", ...}}

    Missing required params listed by `intent.params_schema` yield 400 with
    the specific field name. Unknown tool_binding yields 400 too so drift is
    caught at runtime (a CI parity test is tracked in a separate ideation).
    """
    board_id = (payload or {}).get("board_id")
    if not board_id:
        raise HTTPException(status_code=400, detail="board_id is required")

    intent = (
        await db.execute(
            select(DiscoveryIntent).where(DiscoveryIntent.id == intent_id)
        )
    ).scalar_one_or_none()
    if intent is None or not intent.active:
        raise HTTPException(status_code=404, detail="Intent not found")

    params = (payload or {}).get("params") or {}
    try:
        result = await execute_intent(db, user_id, board_id, intent, params)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    result["intent_id"] = intent.id
    result["intent_name"] = intent.name
    return result
