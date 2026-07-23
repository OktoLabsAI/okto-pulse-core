"""Transport-neutral policy and orchestration for manual KG ticks."""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from okto_pulse.core.kg.backpressure import _RISK_STATE_HARD_REJECT
from okto_pulse.core.ports.kg_operational import get_kg_operational_read_model_port
from okto_pulse.core.ports.relational_effects import get_relational_effects_port
from okto_pulse.core.ports.scheduler import SchedulerControl
from okto_pulse.core.repositories import PulseUnitOfWork

logger = logging.getLogger("okto_pulse.application.kg_tick")
HealthProbe = Callable[..., Awaitable[dict[str, Any]]]
KG_TICK_FULL_REBUILD_OPERATION = "kg_tick_full_rebuild_reset"
_KG_TICK_FULL_REBUILD_LIFECYCLE_STEPS = (
    "checkpoint",
    "flush",
    "fsync",
)


class KGTickAdmissionDeferred(RuntimeError):
    """A global recovery fence safely deferred KG maintenance."""

    code = "kg_tick_deferred_for_global_recovery"
    retryable = True

    def __init__(self, *, reason_code: str) -> None:
        self.reason_code = str(reason_code)
        super().__init__(f"{self.code}:{self.reason_code}")


@dataclass(frozen=True, slots=True)
class KGTickFullRebuildResetFailure:
    """One bounded failure observed while preparing a full-rebuild tick."""

    board_id: str | None
    phase: str
    node_type: str | None
    error_type: str
    detail: str


class KGTickFullRebuildResetFailed(RuntimeError):
    """The full-rebuild reset was incomplete and the tick must be retried."""

    code = "kg_tick_full_rebuild_reset_failed"
    retryable = True

    def __init__(
        self,
        failures: Iterable[KGTickFullRebuildResetFailure],
    ) -> None:
        materialized = tuple(failures)
        if not materialized:
            raise ValueError("at least one reset failure is required")
        self.failures = materialized
        boards = sorted(
            {failure.board_id or "*" for failure in materialized},
        )
        super().__init__(
            f"{self.code}: failures={len(materialized)} "
            f"boards={','.join(boards)}"
        )


def _full_rebuild_reset_failure(
    *,
    board_id: str | None,
    phase: str,
    exc: BaseException,
    node_type: str | None = None,
) -> KGTickFullRebuildResetFailure:
    return KGTickFullRebuildResetFailure(
        board_id=board_id,
        phase=phase,
        node_type=node_type,
        error_type=type(exc).__name__,
        detail=str(exc)[:512],
    )


async def require_kg_tick_admission(
    relational_context: object,
    *,
    trigger: str,
    fence_publication: bool = False,
) -> None:
    """Fail closed before any tick source, graph, outbox, or run mutation."""

    try:
        port = get_relational_effects_port()
        if fence_publication:
            active = await port.fence_kg_tick_publication(relational_context)
        else:
            active = await port.is_global_recovery_active(relational_context)
    except Exception as exc:
        logger.warning(
            "kg.tick.deferred reason=recovery_guard_unavailable trigger=%s "
            "error_type=%s",
            trigger,
            type(exc).__name__,
            extra={
                "event": "kg.tick.deferred",
                "reason": "recovery_guard_unavailable",
                "trigger": trigger,
                "error_type": type(exc).__name__,
            },
        )
        raise KGTickAdmissionDeferred(reason_code="recovery_guard_unavailable") from exc
    if active:
        logger.info(
            "kg.tick.deferred reason=global_recovery_active trigger=%s",
            trigger,
            extra={
                "event": "kg.tick.deferred",
                "reason": "global_recovery_active",
                "trigger": trigger,
            },
        )
        raise KGTickAdmissionDeferred(reason_code="global_recovery_active")


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
    relational_context: object,
    scheduled_at: str | None = None,
) -> list[str]:
    """Persist manual tick events through the same path as the scheduled tick."""

    from okto_pulse.core.events.handlers.kg_decay_tick import publish_tick_events

    effective_scheduled_at = (
        scheduled_at or datetime.now(timezone.utc).isoformat()
    )
    return await publish_tick_events(
        relational_context,
        board_id=board_id,
        actor_id="manual-trigger",
        actor_type="user",
        scheduled_at=effective_scheduled_at,
        force_full_rebuild=force_full_rebuild,
        tick_id=tick_id,
    )


async def _reset_board_last_recomputed_at(
    current_board_id: str,
    *,
    mutation_ref: str | None,
) -> tuple[list[KGTickFullRebuildResetFailure], Exception | None]:
    """Reset one board while retaining writer ownership through cleanup.

    The caller owns this complete acquire/mutate/lifecycle/release coroutine
    through a cancellation-atomic task.  Keeping the owner-token assignment
    and its exact-token release in the same child lifetime prevents a parent
    cancellation from orphaning a cross-process writer manifest.
    """

    from okto_pulse.core.kg.interfaces.registry import get_kg_registry
    from okto_pulse.core.kg.schema_contract import VECTOR_INDEX_TYPES
    from okto_pulse.core.kg.safe_write_lifecycle import (
        HealthProbe as LifecycleHealthProbe,
        KGSafeWriteLifecycle,
        LockOwnerProbe,
        SafeWriteLifecycleStatus,
    )
    from okto_pulse.core.kg.write_barrier import (
        require_write_token,
        under_safe_write,
    )
    from okto_pulse.core.kg.single_writer_lock import (
        KGSingleWriterLock,
        SingleWriterLockError,
        SingleWriterLockErrorCode,
    )

    failures: list[KGTickFullRebuildResetFailure] = []
    last_failure: Exception | None = None
    single_writer = KGSingleWriterLock()
    owner_token: str | None = None
    active_node_type: str | None = None
    phase = "writer_acquire"
    try:
        acquisition = await asyncio.to_thread(
            single_writer.acquire,
            board_id=current_board_id,
            operation=KG_TICK_FULL_REBUILD_OPERATION,
            owner_id=(
                f"kg-tick-full-rebuild:"
                f"{mutation_ref or uuid.uuid4().hex}"
            ),
        )
        if not acquisition.acquired or acquisition.owner_token is None:
            raise SingleWriterLockError(
                SingleWriterLockErrorCode.LOCK_CONTENTION,
                retryable=True,
                reason=(
                    "kg tick full rebuild writer is busy "
                    f"current_owner={acquisition.current_owner}"
                ),
            )
        owner_token = acquisition.owner_token
        phase = "write_token"
        with under_safe_write(
            current_board_id,
            owner_token,
            KG_TICK_FULL_REBUILD_OPERATION,
        ):
            require_write_token(
                current_board_id,
                expected_owner_token=owner_token,
            )

            registry = get_kg_registry()
            phase = "graph_open"
            graph_scope = await registry.graph_transaction.begin(
                current_board_id
            )

            phase = "graph_transaction"
            async with graph_scope as scope:
                for node_type in VECTOR_INDEX_TYPES:
                    active_node_type = node_type
                    phase = "statement"
                    scope.execute(
                        f"MATCH (n:{node_type}) "
                        "SET n.last_recomputed_at = NULL"
                    )
                    active_node_type = None
                phase = "graph_transaction"

            phase = "lifecycle"
            lifecycle = KGSafeWriteLifecycle(
                step_adapter=registry.graph_lifecycle.apply_step,
                owner_probe=LockOwnerProbe(
                    is_active_owner=lambda candidate_board, candidate_token: (
                        candidate_board == current_board_id
                        and single_writer.is_owner(
                            candidate_board,
                            candidate_token,
                        )
                    )
                ),
                health_probe=LifecycleHealthProbe(
                    classify=lambda _board, _graph, status, _step: (
                        "healthy"
                        if status is SafeWriteLifecycleStatus.APPLIED
                        else "recovery_needed"
                    )
                ),
            )
            lifecycle_result = await asyncio.to_thread(
                lifecycle.apply,
                board_id=current_board_id,
                graph_type="board_graph",
                operation=KG_TICK_FULL_REBUILD_OPERATION,
                owner_token=owner_token,
                mutation_ref=(
                    mutation_ref
                    or f"kg-tick-full-rebuild:{current_board_id}"
                ),
                required_steps=_KG_TICK_FULL_REBUILD_LIFECYCLE_STEPS,
            )
            if (
                lifecycle_result.status
                is not SafeWriteLifecycleStatus.APPLIED
            ):
                raise RuntimeError(
                    "kg_tick_full_rebuild_lifecycle_incomplete "
                    f"failed_step={lifecycle_result.failed_step} "
                    f"health_state_after="
                    f"{lifecycle_result.health_state_after}"
                )
    except Exception as exc:
        failures.append(
            _full_rebuild_reset_failure(
                board_id=current_board_id,
                phase=phase,
                node_type=active_node_type,
                exc=exc,
            )
        )
        last_failure = exc
    finally:
        if owner_token is not None:
            try:
                released = await asyncio.to_thread(
                    single_writer.release,
                    board_id=current_board_id,
                    owner_token=owner_token,
                )
                if not released:
                    raise RuntimeError(
                        "kg_tick_full_rebuild_writer_release_failed"
                    )
            except Exception as exc:
                failures.append(
                    _full_rebuild_reset_failure(
                        board_id=current_board_id,
                        phase="writer_release",
                        exc=exc,
                    )
                )
                last_failure = exc
    return failures, last_failure


async def reset_last_recomputed_at(
    board_id: str | None,
    *,
    relational_context: object | None = None,
    mutation_ref: str | None = None,
) -> None:
    from okto_pulse.core.kg.primitives import run_cancellation_atomic

    if board_id:
        board_ids = [board_id]
    else:
        if relational_context is None:
            exc = RuntimeError("relational context is required to reset all boards")
            raise KGTickFullRebuildResetFailed(
                (
                    _full_rebuild_reset_failure(
                        board_id=None,
                        phase="input_validation",
                        exc=exc,
                    ),
                )
            ) from exc
        try:
            board_ids = list(
                await get_kg_operational_read_model_port().list_all_board_ids(
                    relational_context,
                    limit=10_000,
                )
            )
        except Exception as exc:
            raise KGTickFullRebuildResetFailed(
                (
                    _full_rebuild_reset_failure(
                        board_id=None,
                        phase="board_inventory",
                        exc=exc,
                    ),
                )
            ) from exc

    failures: list[KGTickFullRebuildResetFailure] = []
    last_failure: Exception | None = None
    for current_board_id in board_ids:
        board_failures, board_last_failure = await run_cancellation_atomic(
            _reset_board_last_recomputed_at(
                current_board_id,
                mutation_ref=mutation_ref,
            ),
            task_name="core.kg.tick_full_rebuild_reset",
        )
        failures.extend(board_failures)
        if board_last_failure is not None:
            last_failure = board_last_failure

    if failures:
        error = KGTickFullRebuildResetFailed(failures)
        logger.warning(
            "kg.tick.reset_failed failure_count=%s boards=%s",
            len(failures),
            sorted({failure.board_id or "*" for failure in failures}),
            extra={
                "event": "kg.tick.reset_failed",
                "failure_count": len(failures),
                "failures": [
                    {
                        "board_id": failure.board_id,
                        "phase": failure.phase,
                        "node_type": failure.node_type,
                        "error_type": failure.error_type,
                    }
                    for failure in failures
                ],
            },
        )
        raise error from last_failure


__all__ = [
    "HealthProbe",
    "KGTickAdmissionDeferred",
    "KGTickFullRebuildResetFailed",
    "KGTickFullRebuildResetFailure",
    "dispatch_manual_tick",
    "get_kg_health",
    "refuse_tick_if_degraded",
    "require_kg_tick_admission",
    "reset_last_recomputed_at",
]
