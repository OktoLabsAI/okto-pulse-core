"""Transport-neutral policy and orchestration for manual KG ticks."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import Any, AsyncContextManager

from okto_pulse.core.kg.backpressure import _RISK_STATE_HARD_REJECT
from okto_pulse.core.ports.kg_operational import get_kg_operational_read_model_port
from okto_pulse.core.ports.scheduler import SchedulerControl
from okto_pulse.core.repositories import PulseUnitOfWork

logger = logging.getLogger("okto_pulse.application.kg_tick")
SessionScopeFactory = Callable[[], AsyncContextManager[Any]]
HealthProbe = Callable[..., Awaitable[dict[str, Any]]]


async def get_kg_health(
    board_id: str,
    uow: PulseUnitOfWork,
    *,
    scheduler_control: SchedulerControl | None = None,
) -> dict[str, Any]:
    """Default health probe through the edition-composed service catalog."""

    return await uow.services.kg.health(
        board_id,
        scheduler_control=scheduler_control,
    )


async def refuse_tick_if_degraded(
    board_id: str | None,
    uow: PulseUnitOfWork,
    *,
    scheduler_control: SchedulerControl | None = None,
    health_probe: HealthProbe | None = None,
) -> dict[str, object] | None:
    if board_id is None:
        return None
    probe = health_probe or get_kg_health
    health = await probe(
        board_id,
        uow,
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


async def dispatch_manual_tick(
    *,
    tick_id: str,
    board_id: str | None,
    force_full_rebuild: bool,
    session: Any | None = None,
    session_scope_factory: SessionScopeFactory | None = None,
) -> None:
    """Persist manual tick events through the same path as the scheduled tick."""

    from okto_pulse.core.events.handlers.kg_decay_tick import publish_tick_events

    scheduled_at = datetime.now(timezone.utc).isoformat()
    if session is not None:
        if force_full_rebuild:
            await reset_last_recomputed_at(board_id, session=session)
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
            "session_scope_factory is required when dispatch_manual_tick is "
            "called without an explicit session"
        )
    async with session_scope_factory() as owned_session:
        if force_full_rebuild:
            await reset_last_recomputed_at(board_id, session=owned_session)
        await publish_tick_events(
            owned_session,
            board_id=board_id,
            actor_id="manual-trigger",
            actor_type="user",
            scheduled_at=scheduled_at,
        )
        await owned_session.commit()


async def reset_last_recomputed_at(
    board_id: str | None, *, session: Any | None = None
) -> None:
    from okto_pulse.core.kg.interfaces.registry import get_kg_registry
    from okto_pulse.core.kg.schema_contract import VECTOR_INDEX_TYPES
    from okto_pulse.core.kg.write_barrier import require_write_token

    if board_id:
        board_ids = [board_id]
    else:
        if session is None:
            raise RuntimeError(
                "session is required to reset all boards for force_full_rebuild"
            )
        board_ids = list(
            await get_kg_operational_read_model_port().list_all_board_ids(
                session,
                limit=10_000,
            )
        )
    for current_board_id in board_ids:
        require_write_token(current_board_id)
        try:
            transaction = get_kg_registry().graph_transaction
            async with await transaction.begin(current_board_id) as scope:
                for node_type in VECTOR_INDEX_TYPES:
                    try:
                        scope.execute(
                            f"MATCH (n:{node_type}) "
                            "SET n.last_recomputed_at = NULL"
                        )
                    except Exception:
                        continue
        except Exception as exc:
            logger.warning(
                "kg.tick.reset_failed board=%s err=%s",
                current_board_id,
                exc,
                extra={
                    "event": "kg.tick.reset_failed",
                    "board_id": current_board_id,
                },
            )


__all__ = [
    "SessionScopeFactory",
    "HealthProbe",
    "dispatch_manual_tick",
    "get_kg_health",
    "refuse_tick_if_degraded",
    "reset_last_recomputed_at",
]
