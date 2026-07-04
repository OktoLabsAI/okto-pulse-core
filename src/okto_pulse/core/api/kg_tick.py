"""KG decay tick controllability endpoints (spec 54399628 — Wave 2 NC f9732afc).

Manual endpoint `POST /api/v1/kg/tick/run-now` lets an operator or MCP
agent schedule an immediate tick without waiting for the periodic cron.

Pattern compartilha a mesma `LeaseProvider` do `_emit_daily_tick` — primeiro
a chegar ganha; segundo
recebe HTTP 409. Resposta 202 + tick_id só é retornada depois que o evento
e suas execuções de handler foram gravados e commitados.

`force_full_rebuild=true` zera `last_recomputed_at` de todos nodes do
escopo (board_id se fornecido, todos boards caso contrário) ANTES do
tick, forçando recompute completo ignorando staleness threshold.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Callable
from datetime import datetime, timezone
from typing import AsyncContextManager

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.infra.auth import require_user
from okto_pulse.core.infra.database import get_db
from okto_pulse.core.api.deps import scheduler_control_from_request
from okto_pulse.core.kg.backpressure import _RISK_STATE_HARD_REJECT
from okto_pulse.core.ports.coordination import (
    CoordinationProviderMissing,
    get_lease_provider,
)
from okto_pulse.core.ports.scheduler import SchedulerControl
from okto_pulse.core.services.kg_health_service import get_kg_health

logger = logging.getLogger("okto_pulse.api.kg_tick")
router = APIRouter()

SessionScopeFactory = Callable[[], AsyncContextManager[AsyncSession]]


class TickRunNowRequest(BaseModel):
    board_id: str | None = None
    force_full_rebuild: bool = False


class TickRunNowResponse(BaseModel):
    tick_id: str
    status: str  # "running"
    scheduled_at: str  # ISO


async def _refuse_tick_if_degraded(
    board_id: str | None,
    db: AsyncSession,
    *,
    scheduler_control: SchedulerControl | None = None,
) -> dict | None:
    """Shared tick-admission gate (F17). Returns a structured refusal payload
    when a CONCRETE board's KG is degraded (``graph_state`` in the shared
    ``_RISK_STATE_HARD_REJECT`` set), else ``None``.

    A global tick (``board_id is None``) is NOT health-gated — there is no single
    board to probe and a global tick must not be blocked by one board's health
    (FR9). Both the REST endpoint and the MCP twin call this ONE gate, so the
    refusal originates from a single place and the degraded predicate is never
    duplicated (FR10/FR11).
    """
    if board_id is None:
        return None
    health = await get_kg_health(
        board_id,
        db,
        scheduler_control=scheduler_control,
    )
    graph_state = health.get("graph_state")
    if graph_state in _RISK_STATE_HARD_REJECT:
        return {
            "error": "graph_recovery_needed",
            "graph_state": graph_state,
            "board_id": board_id,
            "message": (
                f"KG for board {board_id} is {graph_state}; a manual tick is "
                "refused until recovery completes. Use the explicit KG Health "
                "recovery flow."
            ),
        }
    return None


@router.post(
    "/kg/tick/run-now",
    response_model=TickRunNowResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def run_tick_now(
    payload: TickRunNowRequest,
    request: Request,
    user: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
) -> TickRunNowResponse:
    """Trigger the KG decay tick manually (idempotent — concurrent calls
    return 409 until the in-flight tick releases the lease).

    Body:
        - ``board_id`` (optional): scope the tick to a single board. When
          omitted, the tick runs globally (same scope as the cron schedule).
        - ``force_full_rebuild`` (optional, default false): zero out
          ``last_recomputed_at`` for nodes in scope BEFORE the tick, forcing
          recompute even of fresh nodes (ignores staleness threshold).

    Returns 202 after the tick event has been durably scheduled.
    Operator monitors progress via KGHealthView snapshot polling (30s).

    Returns 409 when the tick lease is already held by the cron OR
    another manual trigger.
    """
    try:
        lease_provider = get_lease_provider()
    except CoordinationProviderMissing as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "error": exc.code,
                "provider": exc.provider_key,
                "message": "Tick lease provider is not configured",
            },
        ) from exc

    lease = await lease_provider.try_acquire("kg_daily_tick", ttl_seconds=300)
    if lease is None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error": "tick_already_running",
                "message": "Tick already running, retry shortly",
            },
        )

    # F17 admission gate: refuse a manual tick on a degraded CONCRETE board with
    # a structured 409 — AFTER the lease check (so tick_already_running keeps
    # priority, TR7) and BEFORE any tick_id is allocated (no doomed tick). The
    # global tick (board_id is None) is not health-gated (FR9).
    refusal = await _refuse_tick_if_degraded(
        payload.board_id,
        db,
        scheduler_control=scheduler_control_from_request(request),
    )
    if refusal is not None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=refusal,
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

    try:
        try:
            await _dispatch_manual_tick(
                tick_id=tick_id,
                board_id=payload.board_id,
                force_full_rebuild=payload.force_full_rebuild,
                session=db,
            )
            await db.commit()
        except Exception as exc:
            await db.rollback()
            logger.error(
                "kg.tick.manual_schedule_failed tick_id=%s err=%s",
                tick_id, exc,
                extra={
                    "event": "kg.tick.manual_schedule_failed",
                    "tick_id": tick_id,
                    "board_id": payload.board_id,
                    "force_full_rebuild": payload.force_full_rebuild,
                    "error": str(exc),
                },
            )
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail={
                    "error": "tick_schedule_failed",
                    "message": (
                        "Failed to persist the KG tick event. "
                        "No background tick was scheduled."
                    ),
                    "detail": str(exc),
                },
            ) from exc
    finally:
        await lease_provider.release(lease)

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
    session: AsyncSession | None = None,
    session_scope_factory: SessionScopeFactory | None = None,
) -> None:
    """Persist KGDailyTick event(s) through the same path as the cron.

    Fan-out por board real (campo 2026-06-10): o sentinel global ``'*'``
    violava a FK de ``domain_events.board_id`` com PRAGMA foreign_keys=ON
    (runtime community) — o agendamento falhava com IntegrityError. O
    ``tick_id`` do response vira correlação de log; cada evento do fan-out
    tem o próprio uuid.

    When ``session`` is provided, the caller owns commit/rollback. MCP and
    other non-request callers may omit it only when composition injects a
    ``session_scope_factory``; this helper never imports the concrete database
    factory itself.
    """
    from okto_pulse.core.events.handlers.kg_decay_tick import (
        publish_tick_events,
    )

    scheduled_at = datetime.now(timezone.utc).isoformat()

    if session is not None:
        if force_full_rebuild:
            await _reset_last_recomputed_at(board_id, session=session)
        await publish_tick_events(
            session,
            board_id=board_id,
            actor_id="manual-trigger",
            actor_type="user",
            scheduled_at=scheduled_at,
        )
        return

    if session_scope_factory is None:
        raise RuntimeError(
            "session_scope_factory is required when _dispatch_manual_tick is "
            "called without an explicit session"
        )

    async with session_scope_factory() as owned_session:
        if force_full_rebuild:
            await _reset_last_recomputed_at(board_id, session=owned_session)
        await publish_tick_events(
            owned_session,
            board_id=board_id,
            actor_id="manual-trigger",
            actor_type="user",
            scheduled_at=scheduled_at,
        )
        await owned_session.commit()


async def _reset_last_recomputed_at(
    board_id: str | None, *, session: AsyncSession | None = None
) -> None:
    """Zero out `last_recomputed_at` for nodes in scope (board or global).

    Called when the operator passes ``force_full_rebuild=true`` — bypasses
    the staleness filter on the next tick run by making every node appear
    "stale" again.

    Iterates the per-board Kùzu graph(s) and sets the column to NULL via
    Cypher SET. Soft-fails per board so a single broken graph doesn't
    block the rest.
    """
    from okto_pulse.core.kg.interfaces.registry import get_kg_registry
    from okto_pulse.core.kg.schema_contract import VECTOR_INDEX_TYPES
    from okto_pulse.core.kg.write_barrier import require_write_token
    from okto_pulse.core.models.db import Board
    from sqlalchemy import select

    if board_id:
        board_ids = [board_id]
    else:
        if session is None:
            raise RuntimeError(
                "session is required to reset all boards for force_full_rebuild"
            )
        rows = (await session.execute(select(Board.id))).scalars().all()
        board_ids = list(rows)

    for bid in board_ids:
        # KG-01.3.1 boundary: force_full_rebuild is a write path against
        # graph.lbug. require_write_token() raises in STRICT mode if no
        # safe-write guard is active; in SOFT mode (default) it logs and
        # bumps kg_unguarded_write_total. Production wires the caller to
        # enter KGSafeWriteLifecycle before invoking this path.
        require_write_token(bid)
        try:
            # R05-C: write through the #06 GraphTransaction port. _kdb was
            # unused; the SET is a plain scope.execute — behaviour-identical to
            # the old (db, conn) tuple.
            async with await get_kg_registry().graph_transaction.begin(bid) as scope:
                for node_type in VECTOR_INDEX_TYPES:
                    try:
                        scope.execute(
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
