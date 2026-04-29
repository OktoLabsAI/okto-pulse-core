"""KG decay tick controllability endpoints (spec 54399628 — Wave 2 NC f9732afc).

Endpoint manual `POST /api/v1/kg/tick/run-now` — operador (ou agente via
MCP gemellar) dispara um tick imediato sem esperar o cron periódico.

Pattern compartilha o mesmo `get_async_lock("kg_daily_tick", "global")`
do `_emit_daily_tick` em `core/app.py` — primeiro a chegar ganha; segundo
recebe HTTP 409. Resposta 202 + tick_id é retornada imediatamente; o tick
roda em background via `asyncio.create_task` para não bloquear o request.

`force_full_rebuild=true` zera `last_recomputed_at` de todos nodes do
escopo (board_id se fornecido, todos boards caso contrário) ANTES do
tick, forçando recompute completo ignorando staleness threshold.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.events import publish as event_publish
from okto_pulse.core.events.types import KGDailyTick
from okto_pulse.core.infra.auth import require_user
from okto_pulse.core.infra.database import get_db
from okto_pulse.core.kg.workers.advisory_lock import get_async_lock

logger = logging.getLogger("okto_pulse.api.kg_tick")
router = APIRouter()


class TickRunNowRequest(BaseModel):
    board_id: str | None = None
    force_full_rebuild: bool = False


class TickRunNowResponse(BaseModel):
    tick_id: str
    status: str  # "running"
    scheduled_at: str  # ISO


@router.post(
    "/kg/tick/run-now",
    response_model=TickRunNowResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def run_tick_now(
    payload: TickRunNowRequest,
    user: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
) -> TickRunNowResponse:
    """Trigger the KG decay tick manually (idempotent — concurrent calls
    return 409 until the in-flight tick releases the advisory lock).

    Body:
        - ``board_id`` (optional): scope the tick to a single board. When
          omitted, the tick runs globally (same scope as the cron schedule).
        - ``force_full_rebuild`` (optional, default false): zero out
          ``last_recomputed_at`` for nodes in scope BEFORE the tick, forcing
          recompute even of fresh nodes (ignores staleness threshold).

    Returns 202 with tick_id immediately; the tick runs in background.
    Operator monitors progress via KGHealthView snapshot polling (30s).

    Returns 409 when the advisory lock is already held by the cron OR
    another manual trigger.
    """
    lock = get_async_lock("kg_daily_tick", "global")
    if lock.locked():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error": "tick_already_running",
                "message": "Tick already running, retry shortly",
            },
        )

    tick_id = str(uuid.uuid4())
    scheduled_at = datetime.now(timezone.utc).isoformat()

    # Audit log — emit BEFORE the background task starts so the trigger
    # is recorded even if the task crashes immediately.
    logger.info(
        "kg.tick.manual_triggered tick_id=%s user=%s board=%s force=%s",
        tick_id, user, payload.board_id, payload.force_full_rebuild,
        extra={
            "event": "kg.tick.manual_triggered",
            "tick_id": tick_id,
            "triggered_by_user_id": user,
            "board_id": payload.board_id,
            "force_full_rebuild": payload.force_full_rebuild,
        },
    )

    asyncio.create_task(
        _dispatch_manual_tick(
            tick_id=tick_id,
            board_id=payload.board_id,
            force_full_rebuild=payload.force_full_rebuild,
        )
    )

    return TickRunNowResponse(
        tick_id=tick_id,
        status="running",
        scheduled_at=scheduled_at,
    )


async def _dispatch_manual_tick(
    *,
    tick_id: str,
    board_id: str | None,
    force_full_rebuild: bool,
) -> None:
    """Background task — reset last_recomputed_at if force_full_rebuild,
    then publish KGDailyTick event for the existing handler to pick up.

    Failures are logged but not raised — the request already returned 202
    so there's nothing to bubble back to the caller.
    """
    try:
        if force_full_rebuild:
            await _reset_last_recomputed_at(board_id)

        # Publish the standard event — the same handler the cron uses
        # picks it up. Keeps the manual path consistent with the periodic
        # path (no separate code paths to maintain).
        await event_publish(
            KGDailyTick(
                tick_id=tick_id,
                scheduled_at=datetime.now(timezone.utc).isoformat(),
                board_id=board_id or "*",
                actor_id="manual-trigger",
                actor_type="user",
            ),
            session=None,
        )
    except Exception as exc:
        logger.warning(
            "kg.tick.manual_dispatch_failed tick_id=%s err=%s",
            tick_id, exc,
            extra={
                "event": "kg.tick.manual_dispatch_failed",
                "tick_id": tick_id,
            },
        )


async def _reset_last_recomputed_at(board_id: str | None) -> None:
    """Zero out `last_recomputed_at` for nodes in scope (board or global).

    Called when the operator passes ``force_full_rebuild=true`` — bypasses
    the staleness filter on the next tick run by making every node appear
    "stale" again.

    Iterates the per-board Kùzu graph(s) and sets the column to NULL via
    Cypher SET. Soft-fails per board so a single broken graph doesn't
    block the rest.
    """
    from okto_pulse.core.infra.database import get_session_factory
    from okto_pulse.core.kg.schema import (
        VECTOR_INDEX_TYPES,
        open_board_connection,
    )
    from okto_pulse.core.models.db import Board
    from sqlalchemy import select

    factory = get_session_factory()
    async with factory() as session:
        if board_id:
            board_ids = [board_id]
        else:
            rows = (await session.execute(select(Board.id))).scalars().all()
            board_ids = list(rows)

    for bid in board_ids:
        try:
            conn = open_board_connection(bid)
            with conn as (_kdb, kconn):
                for node_type in VECTOR_INDEX_TYPES:
                    try:
                        kconn.execute(
                            f"MATCH (n:{node_type}) SET n.last_recomputed_at = NULL"
                        )
                    except Exception:
                        # column may not exist on legacy boards (NC-10);
                        # ignore — nothing to reset.
                        continue
        except Exception as exc:
            logger.warning(
                "kg.tick.reset_failed board=%s err=%s",
                bid, exc,
                extra={
                    "event": "kg.tick.reset_failed",
                    "board_id": bid,
                },
            )
