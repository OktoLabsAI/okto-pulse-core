"""KGDailyTickHandler — daily decay tick for the KG (Ideação #4, IMPL-D).

Reacts to ``kg.tick.daily`` (emitted by the active scheduler adapter using the
``kg_decay_tick_interval_minutes`` policy configured in ``config.py``) by
walking every active board and recomputing the relevance_score of nodes that
haven't been recomputed in
``KG_DECAY_TICK_STALENESS_DAYS`` days.

Cursor scan keeps memory bounded: results stream in batches of
``KG_DECAY_TICK_BATCH_SIZE`` ordered by ``id ASC`` so a tick never revisits
a node in the same run. Failure of one node never aborts the loop (BR14).
Board-level failures (corrupt/locked graph) are caught and counted
(``boards_failed``) so the rest of the fleet continues — FR1.

The tick run is persisted through the registered relational effects port so
kg_health can surface ``last_decay_tick_at`` and
``nodes_recomputed_in_last_tick`` without core owning SQL dialect mechanics.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime, timedelta, timezone

from okto_pulse.core.events.bus import register_handler
from okto_pulse.core.events.types import KGDailyTick, KGDeliveryRedriveTick
from okto_pulse.core.kg.async_bridge import run_async_blocking
from okto_pulse.core.kg.interfaces import get_kg_registry
from okto_pulse.core.kg.schema_contract import NODE_TYPES
from okto_pulse.core.kg.scoring import _recompute_relevance_batch
from okto_pulse.core.ports.delivery_ledger import (
    DeliveryLedgerPort,
    DeliveryMaintenanceReceipt,
    get_delivery_ledger_port,
)
from okto_pulse.core.ports.relational_effects import (
    KGTickRunUpsert,
    get_relational_effects_port,
)
from okto_pulse.core.ports.runtime_workers import WorkerClockPort
from okto_pulse.core.ports.stale_sweep import (
    StaleSweepPort,
    StaleSweepScheduleReceipt,
    StaleSweepScheduleRequest,
    get_stale_sweep_port,
)
from okto_pulse.core.ports.takedown_telemetry import (
    TakedownSloEvaluation,
    TakedownTelemetryReadPort,
    get_takedown_telemetry_read_port,
)

logger = logging.getLogger(__name__)


KG_DECAY_TICK_BATCH_SIZE = 200
KG_DECAY_TICK_STALENESS_DAYS = 7
KG_DELIVERY_WATCHDOG_LIMIT = 50
KG_DELIVERY_REDRIVE_LIMIT = 50
KG_STALE_SWEEP_BUDGET = 50


class DeliveryMaintenanceFailed(RuntimeError):
    """Abort the relational UoW when tick-owned delivery maintenance fails."""


class StaleSweepMaintenanceFailed(RuntimeError):
    """Abort the relational UoW when durable sweep scheduling fails."""


def _clock_now(clock: WorkerClockPort | None) -> datetime:
    """Read one UTC worker timestamp while preserving legacy construction."""

    return clock.now() if clock is not None else datetime.now(timezone.utc)


def _optional_delivery_ledger_port() -> DeliveryLedgerPort | None:
    """Resolve Card 7 maintenance without breaking legacy Core test runtimes."""

    try:
        return get_delivery_ledger_port()
    except RuntimeError as exc:
        if str(exc) == "delivery_ledger_port_not_configured":
            return None
        raise


def _optional_stale_sweep_port() -> StaleSweepPort | None:
    """Resolve Card 8 scheduling without breaking legacy Core test runtimes."""

    try:
        return get_stale_sweep_port()
    except RuntimeError as exc:
        if str(exc) == "stale_sweep_port_not_configured":
            return None
        raise


def _optional_takedown_telemetry_port() -> TakedownTelemetryReadPort | None:
    """Resolve the edition-owned periodic monitor for legacy runtimes."""

    try:
        return get_takedown_telemetry_read_port()
    except RuntimeError as exc:
        if str(exc) == "takedown_telemetry_read_port_not_configured":
            return None
        raise


async def _evaluate_takedown_slo(
    *,
    port: TakedownTelemetryReadPort | None,
    session: object,
    board_id: str,
    now: datetime,
    transaction_state: str,
    correlation_id: str | None = None,
    correlation_kind: str | None = None,
) -> TakedownSloEvaluation | None:
    """Run the optional monitor without changing delivery transaction fate."""

    correlation = {
        "correlation_id": correlation_id,
        "correlation_kind": correlation_kind,
    }
    if port is None:
        logger.warning(
            "kg.takedown.slo_monitor_unavailable board_id=%s",
            board_id,
            extra={
                "event": "kg.takedown.slo_monitor_unavailable",
                "board_id": board_id,
                "observed_at": now.isoformat(),
                "transaction_state": transaction_state,
                "monitor_status": "unavailable",
                **correlation,
            },
        )
        return None
    try:
        evaluation = await port.evaluate_takedown_slo(
            session,
            board_id=board_id,
            now=now,
            transaction_state=transaction_state,
        )
        if not isinstance(evaluation, TakedownSloEvaluation):
            raise RuntimeError("takedown_slo_evaluation_invalid")
        if (
            evaluation.board_id != board_id
            or evaluation.observed_at != now
            or evaluation.transaction_state != transaction_state
        ):
            raise RuntimeError("takedown_slo_evaluation_identity_mismatch")
    except Exception as exc:  # monitoring must not roll back delivered work
        logger.exception(
            "kg.takedown.slo_evaluation_failed board_id=%s error=%s",
            board_id,
            str(exc),
            extra={
                "event": "kg.takedown.slo_evaluation_failed",
                "board_id": board_id,
                "observed_at": now.isoformat(),
                "transaction_state": transaction_state,
                "monitor_status": "failed",
                "error": str(exc),
                "error_class": type(exc).__name__,
                **correlation,
            },
        )
        return None

    payload = evaluation.to_dict()
    if evaluation.status.value == "insufficient_data":
        event = "kg.takedown.slo_evaluation_insufficient_data"
        log = logger.warning
    else:
        event = "kg.takedown.slo_evaluated"
        log = logger.info
    log(
        "%s board_id=%s status=%s breached=%s",
        event,
        board_id,
        evaluation.status.value,
        evaluation.breached,
        extra={
            "event": event,
            "monitor_status": "evaluated",
            **payload,
            **correlation,
        },
    )
    return evaluation


async def _schedule_stale_sweep(
    *,
    port: StaleSweepPort,
    session: object,
    board_id: str,
    budget: int,
    now: datetime,
) -> StaleSweepScheduleReceipt:
    """Stage one board coordinator after the decay graph scope is closed."""

    receipt = await port.schedule_stale_sweep(
        session,
        StaleSweepScheduleRequest(
            board_id=board_id,
            budget=budget,
            now=now,
        ),
    )
    if receipt.scheduled:
        event = "kg.stale_sweep.scheduled.staged"
    elif receipt.board_present:
        event = "kg.stale_sweep.schedule_deduplicated"
    else:
        event = "kg.stale_sweep.schedule_skipped_board_absent"
    logger.info(
        "%s board=%s sweep_id=%s cursor=%s budget=%d attempt=%d "
        "board_present=%s transaction_state=%s commit_owner=%s",
        event,
        receipt.board_id,
        receipt.sweep_id,
        receipt.cursor,
        receipt.budget,
        receipt.attempt,
        receipt.board_present,
        "pending_caller_commit",
        "dispatcher",
        extra={
            "event": event,
            "board_id": receipt.board_id,
            "sweep_id": receipt.sweep_id,
            "cursor": receipt.cursor,
            "budget": receipt.budget,
            "attempt": receipt.attempt,
            "board_present": receipt.board_present,
            "transaction_state": "pending_caller_commit",
            "commit_owner": "dispatcher",
        },
    )
    return receipt


def _log_delivery_maintenance_receipt(
    *,
    operation: str,
    board_id: str,
    limit: int,
    receipt: DeliveryMaintenanceReceipt,
) -> None:
    """Emit the bounded, structured progress receipt for one maintenance pass."""

    event = f"kg.tick.delivery_{operation}.staged"
    fields = {
        "event": event,
        "operation": operation,
        "board_id": board_id,
        "limit": limit,
        "scanned": receipt.scanned,
        "transitioned": receipt.transitioned,
        "emitted": receipt.emitted,
        "concurrency_lost": receipt.concurrency_lost,
        "has_more": receipt.has_more,
        "oldest_debt_age_seconds": receipt.oldest_debt_age_seconds,
        "checkpoint_version": receipt.checkpoint_version,
        "resume_board_id": receipt.resume_board_id,
        "transaction_state": "pending_caller_commit",
        "commit_owner": "dispatcher",
    }
    logger.info(
        "%s board_id=%s scanned=%d transitioned=%d emitted=%d "
        "concurrency_lost=%d has_more=%s oldest_debt_age_seconds=%s "
        "checkpoint_version=%d resume_board_id=%s transaction_state=%s "
        "commit_owner=%s",
        event,
        board_id,
        receipt.scanned,
        receipt.transitioned,
        receipt.emitted,
        receipt.concurrency_lost,
        receipt.has_more,
        receipt.oldest_debt_age_seconds,
        receipt.checkpoint_version,
        receipt.resume_board_id,
        "pending_caller_commit",
        "dispatcher",
        extra=fields,
    )


def _delivery_redrive_continuation_id(checkpoint_version: int) -> str:
    """Return the replay-stable identity for one committed checkpoint."""

    if (
        isinstance(checkpoint_version, bool)
        or not isinstance(checkpoint_version, int)
        or checkpoint_version < 1
    ):
        raise ValueError("delivery_redrive_checkpoint_version_invalid")
    return str(
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"okto-pulse:global-delivery-redrive:{checkpoint_version}",
        )
    )


async def _publish_delivery_redrive_continuation(
    *,
    session: object,
    receipt: DeliveryMaintenanceReceipt,
    scheduled_at: datetime,
) -> str | None:
    """Persist the next bounded run in the same UoW as this run's checkpoint."""

    if not receipt.has_more:
        return None
    if receipt.resume_board_id is None:
        raise ValueError("delivery_redrive_resume_board_missing")

    from okto_pulse.core.events import publish as event_publish

    run_id = _delivery_redrive_continuation_id(receipt.checkpoint_version)
    await event_publish(
        KGDeliveryRedriveTick(
            event_id=run_id,
            board_id=receipt.resume_board_id,
            actor_id=None,
            actor_type="system",
            run_id=run_id,
            scheduled_at=scheduled_at.isoformat(),
            checkpoint_version=receipt.checkpoint_version,
        ),
        session=session,
    )
    logger.info(
        "kg.tick.delivery_redrive.continuation_staged "
        "run_id=%s checkpoint_version=%d board_id=%s",
        run_id,
        receipt.checkpoint_version,
        receipt.resume_board_id,
        extra={
            "event": "kg.tick.delivery_redrive.continuation_staged",
            "run_id": run_id,
            "checkpoint_version": receipt.checkpoint_version,
            "board_id": receipt.resume_board_id,
            "transaction_state": "pending_caller_commit",
            "commit_owner": "dispatcher",
        },
    )
    return run_id


async def _run_delivery_redrive_pass(
    *,
    port: DeliveryLedgerPort,
    session: object,
    board_id: str,
    now: datetime,
    redrive_limit: int,
) -> DeliveryMaintenanceReceipt:
    """Stage one global, budgeted redrive run and its durable continuation."""

    receipt = await port.redrive_delivery_debt(
        session,
        now=now,
        limit=redrive_limit,
    )
    _log_delivery_maintenance_receipt(
        operation="redrive",
        board_id=board_id,
        limit=redrive_limit,
        receipt=receipt,
    )
    await _publish_delivery_redrive_continuation(
        session=session,
        receipt=receipt,
        scheduled_at=now,
    )
    return receipt


async def _run_delivery_maintenance(
    *,
    port: DeliveryLedgerPort,
    session: object,
    board_id: str,
    watchdog_limit: int,
    redrive_limit: int,
    clock: WorkerClockPort | None = None,
) -> DeliveryMaintenanceReceipt:
    """Run a board watchdog, then one global fair redrive page."""

    now = _clock_now(clock)
    watchdog = await port.reconcile_orphaned_attempts(
        session,
        board_id=board_id,
        now=now,
        limit=watchdog_limit,
    )
    _log_delivery_maintenance_receipt(
        operation="watchdog",
        board_id=board_id,
        limit=watchdog_limit,
        receipt=watchdog,
    )
    return await _run_delivery_redrive_pass(
        port=port,
        session=session,
        board_id=board_id,
        now=now,
        redrive_limit=redrive_limit,
    )


async def publish_tick_events(
    session,
    *,
    board_id: str | None = None,
    actor_id: str | None = None,
    actor_type: str | None = None,
    scheduled_at: str | None = None,
    clock: WorkerClockPort | None = None,
) -> list[str]:
    """Publica ``KGDailyTick`` com FAN-OUT por board real. Retorna os tick_ids.

    Campo 2026-06-10: ``domain_events.board_id`` é NOT NULL com FK para
    ``boards.id``, e o runtime de produção (community ``serve``) liga
    ``PRAGMA foreign_keys=ON`` desde sempre — o sentinel global ``'*'``
    violava a constraint no INSERT do evento, então NENHUM tick (cron OU
    manual) chegava a ser agendado em produção (IntegrityError →
    ``tick_schedule_failed``). Os testes do core não ligam o PRAGMA, por
    isso o bug nunca apareceu na suíte.

    O fan-out publica um evento POR board existente: a FK passa, o
    isolamento por board vira total (falha de um board não toca os outros
    nem na enumeração) e eventos de boards deletados são limpos pelo
    ON DELETE CASCADE. O handler já suportava ``board_id`` concreto.
    """
    from okto_pulse.core.events import publish as event_publish

    if board_id and board_id != "*":
        board_ids = [board_id]
    else:
        board_ids = list(await get_relational_effects_port().list_board_ids(session))

    when = scheduled_at or _clock_now(clock).isoformat()
    extra_kwargs: dict[str, str] = {}
    if actor_id is not None:
        extra_kwargs["actor_id"] = actor_id
    if actor_type is not None:
        extra_kwargs["actor_type"] = actor_type

    tick_ids: list[str] = []
    for bid in board_ids:
        tid = str(uuid.uuid4())
        await event_publish(
            KGDailyTick(
                board_id=bid,
                tick_id=tid,
                scheduled_at=when,
                **extra_kwargs,
            ),
            session=session,
        )
        tick_ids.append(tid)
    return tick_ids


def _fetch_stale_nodes(
    conn,
    node_type: str,
    cutoff_iso: str,
    cursor_id: str | None,
    *,
    limit: int,
) -> list[tuple[str, str]]:
    """Return up to ``limit`` (node_type, node_id) pairs needing recompute.

    A node is "stale" when ``last_recomputed_at IS NULL`` or strictly less
    than ``cutoff_iso`` (ISO datetime). Pagination uses a strictly
    increasing ``id > cursor_id`` keyset so a tick never revisits the same
    node within one scan, even when later writes shift the candidate set.
    """
    res = None
    rows: list[tuple[str, str]] = []
    try:
        if cursor_id is None:
            res = conn.execute(
                f"MATCH (n:{node_type}) "
                f"WHERE (n.last_recomputed_at IS NULL "
                f"       OR n.last_recomputed_at < $cutoff) "
                f"RETURN n.id "
                f"ORDER BY n.id ASC "
                f"LIMIT $limit",
                {"cutoff": cutoff_iso, "limit": limit},
            )
        else:
            res = conn.execute(
                f"MATCH (n:{node_type}) "
                f"WHERE (n.last_recomputed_at IS NULL "
                f"       OR n.last_recomputed_at < $cutoff) "
                f"  AND n.id > $cursor "
                f"RETURN n.id "
                f"ORDER BY n.id ASC "
                f"LIMIT $limit",
                {"cutoff": cutoff_iso, "cursor": cursor_id, "limit": limit},
            )
        for row in res.rows:
            rows.append((node_type, str(row[0])))
    except Exception as exc:
        logger.warning(
            "kg.tick.fetch_stale_failed node_type=%s err=%s", node_type, exc,
        )
    return rows


def _count_stale_nodes_pre_tick(conn, cutoff_iso: str) -> int:
    """Count nodes meeting the stale criterion BEFORE the tick processes them.

    Spec 28583299 (Ideação #4, AC41/TS42): the structured log
    ``kg.relevance.tick.completed`` exposes ``nodes_with_stale_score_pre_tick``
    so operators / agents can spot drift in the recompute mechanism — a
    sustained high value means hit-flush / boost-change recompute aren't
    keeping pace and the tick is doing more catch-up work than expected.
    """
    total = 0
    for node_type in NODE_TYPES:
        res = None
        try:
            res = conn.execute(
                f"MATCH (n:{node_type}) "
                f"WHERE (n.last_recomputed_at IS NULL "
                f"       OR n.last_recomputed_at < $cutoff) "
                f"RETURN count(n) AS c",
                {"cutoff": cutoff_iso},
            )
            if res.rows:
                row = res.rows[0]
                total += int(row[0] or 0)
        except Exception as exc:
            logger.debug(
                "kg.tick.stale_count_failed node_type=%s err=%s",
                node_type, exc,
            )
    return total


def _process_board_sync(
    board_id: str, cutoff_iso: str, *, batch_size: int,
) -> tuple[int, int]:
    """Drain stale nodes for one board. Returns (recomputed, stale_pre_count).

    FR7: opening the graph transaction port happens before the per-node loop.
    If the concrete backend raises (e.g. corrupt or locked graph), the exception
    propagates to ``_run_daily_tick`` so only that board increments
    ``boards_failed``.
    """
    registry = get_kg_registry()
    if not registry.graph_runtime_store.exists(board_id):
        return (0, 0)

    async def _run() -> tuple[int, int]:
        total = 0
        async with await registry.graph_transaction.begin(board_id) as scope:
            stale_pre_count = _count_stale_nodes_pre_tick(scope, cutoff_iso)
            for node_type in NODE_TYPES:
                cursor: str | None = None
                while True:
                    stale = _fetch_stale_nodes(
                        scope, node_type, cutoff_iso, cursor,
                        limit=batch_size,
                    )
                    if not stale:
                        break
                    try:
                        persisted = _recompute_relevance_batch(
                            scope, board_id, stale, trigger="daily_tick",
                        )
                        total += persisted
                    except Exception as exc:
                        # BR14 — a single batch failure does not abort the tick.
                        logger.warning(
                            "kg.tick.batch_failed board=%s node_type=%s err=%s",
                            board_id, node_type, exc,
                        )
                    cursor = stale[-1][1]
                    if len(stale) < batch_size:
                        break
        return (total, stale_pre_count)

    return run_async_blocking(_run())


async def _persist_tick_run(
    session,
    *,
    tick_id: str,
    started_at: datetime,
    completed_at: datetime,
    nodes_recomputed: int,
    duration_ms: float,
    boards_processed: int,
    boards_failed: int = 0,
    error: str | None = None,
) -> None:
    """Insert or update the latest tick state through the relational port."""

    await get_relational_effects_port().upsert_kg_tick_run(
        session,
        KGTickRunUpsert(
            tick_id=tick_id,
            started_at=started_at,
            completed_at=completed_at,
            nodes_recomputed=nodes_recomputed,
            duration_ms=duration_ms,
            boards_processed=boards_processed,
            boards_failed=boards_failed,
            error=error,
        ),
    )


async def _run_daily_tick(
    *,
    tick_id: str,
    session,
    board_id: str | None = None,
    batch_size: int | None = None,
    staleness_days: int | None = None,
    delivery_watchdog_limit: int | None = None,
    delivery_redrive_limit: int | None = None,
    stale_sweep_budget: int | None = None,
    clock: WorkerClockPort | None = None,
) -> dict:
    """Execute the full tick cycle and persist the run row.

    Returns a summary dict suitable for structured logging / tests:
    ``{tick_id, nodes_recomputed, boards_processed, boards_failed,
    duration_ms, nodes_with_stale_score_pre_tick}``.

    FR1/TR2: each board iteration is wrapped in an independent try/except.
    A board whose graph transaction open raises (corrupt/locked graph)
    increments ``boards_failed`` and emits a ``kg.tick.board_failed`` warning
    without aborting the remaining boards.
    """
    from okto_pulse.core.infra.config import get_settings

    settings = get_settings()
    if batch_size is None:
        batch_size = int(
            getattr(settings, "kg_decay_tick_batch_size", KG_DECAY_TICK_BATCH_SIZE)
        )
    if staleness_days is None:
        staleness_days = int(
            getattr(
                settings,
                "kg_decay_tick_staleness_days",
                KG_DECAY_TICK_STALENESS_DAYS,
            )
        )
    if delivery_watchdog_limit is None:
        delivery_watchdog_limit = KG_DELIVERY_WATCHDOG_LIMIT
    if delivery_redrive_limit is None:
        delivery_redrive_limit = KG_DELIVERY_REDRIVE_LIMIT
    if stale_sweep_budget is None:
        stale_sweep_budget = int(
            getattr(settings, "kg_stale_sweep_budget", KG_STALE_SWEEP_BUDGET)
        )
    delivery_watchdog_limit = int(delivery_watchdog_limit)
    delivery_redrive_limit = int(delivery_redrive_limit)
    stale_sweep_budget = int(stale_sweep_budget)
    if delivery_watchdog_limit < 1:
        raise ValueError("delivery_watchdog_limit_must_be_positive")
    if delivery_redrive_limit < 1:
        raise ValueError("delivery_redrive_limit_must_be_positive")
    if stale_sweep_budget < 1:
        raise ValueError("stale_sweep_budget_must_be_positive")

    started_at = _clock_now(clock)
    cutoff_iso = (started_at - timedelta(days=staleness_days)).isoformat()
    boards_processed = 0
    boards_failed = 0
    total_recomputed = 0

    nodes_with_stale_score_pre_tick = 0
    if board_id and board_id != "*":
        boards = [board_id]
    else:
        boards = list(await get_relational_effects_port().list_board_ids(session))
    delivery_ledger_port = _optional_delivery_ledger_port()
    stale_sweep_port = _optional_stale_sweep_port()
    takedown_telemetry_port = _optional_takedown_telemetry_port()

    for bid in boards:
        try:
            recomputed, stale_pre = await asyncio.to_thread(
                _process_board_sync, bid, cutoff_iso, batch_size=batch_size,
            )
            boards_processed += 1
            total_recomputed += recomputed
            nodes_with_stale_score_pre_tick += stale_pre
        except Exception as exc:
            # FR1/TR2: any exception (RuntimeError from graph transaction open,
            # asyncio wrapper, etc.) is caught here so the fleet continues.
            boards_failed += 1
            logger.warning(
                "kg.tick.board_failed board_id=%s error=%s",
                bid, str(exc),
                extra={
                    "event": "kg.tick.board_failed",
                    "board_id": bid,
                    "error": str(exc),
                },
            )
        # Card 8: this is deliberately after ``_process_board_sync`` has
        # returned (or failed), so no decay graph transaction overlaps the
        # durable coordinator. Its graph work is later owned by the queue
        # processor under a separate advisory lock and transaction.
        if stale_sweep_port is not None:
            try:
                await _schedule_stale_sweep(
                    port=stale_sweep_port,
                    session=session,
                    board_id=bid,
                    budget=stale_sweep_budget,
                    now=_clock_now(clock),
                )
            except Exception as exc:
                logger.warning(
                    "kg.stale_sweep.schedule_failed board_id=%s error=%s",
                    bid,
                    str(exc),
                    extra={
                        "event": "kg.stale_sweep.schedule_failed",
                        "board_id": bid,
                        "error": str(exc),
                    },
                )
                raise StaleSweepMaintenanceFailed(
                    f"stale_sweep_schedule_failed:{bid}"
                ) from exc
        # Card 7 ownership maintenance deliberately runs after the board graph
        # scope has closed.  It must still run when graph open/apply failed:
        # delivery truth is relational and the tick is its sole redrive owner.
        if delivery_ledger_port is not None:
            try:
                await _run_delivery_maintenance(
                    port=delivery_ledger_port,
                    session=session,
                    board_id=bid,
                    watchdog_limit=delivery_watchdog_limit,
                    redrive_limit=delivery_redrive_limit,
                    clock=clock,
                )
            except Exception as exc:
                # The adapter may have staged its ledger CAS before detecting
                # an outbox collision.  Propagation is load-bearing: the
                # handler must let its caller roll back the entire relational
                # UoW instead of committing a half-redrive.  Production tick
                # events are already fanned out one per board.
                logger.warning(
                    "kg.tick.delivery_maintenance_failed board_id=%s error=%s",
                    bid,
                    str(exc),
                    extra={
                        "event": "kg.tick.delivery_maintenance_failed",
                        "board_id": bid,
                        "error": str(exc),
                    },
                )
                raise DeliveryMaintenanceFailed(
                    f"delivery_maintenance_failed:{bid}"
                ) from exc
        await _evaluate_takedown_slo(
            port=takedown_telemetry_port,
            session=session,
            board_id=bid,
            now=_clock_now(clock),
            transaction_state="pending_caller_commit",
            correlation_id=tick_id,
            correlation_kind="kg.tick.daily",
        )

    completed_at = _clock_now(clock)
    duration_ms = (completed_at - started_at).total_seconds() * 1000.0
    await _persist_tick_run(
        session,
        tick_id=tick_id,
        started_at=started_at,
        completed_at=completed_at,
        nodes_recomputed=total_recomputed,
        duration_ms=duration_ms,
        boards_processed=boards_processed,
        boards_failed=boards_failed,
    )

    summary = {
        "tick_id": tick_id,
        "nodes_recomputed": total_recomputed,
        "boards_processed": boards_processed,
        "boards_failed": boards_failed,
        "duration_ms": duration_ms,
        "nodes_with_stale_score_pre_tick": nodes_with_stale_score_pre_tick,
    }
    logger.info(
        "kg.relevance.tick.completed",
        extra={"event": "kg.relevance.tick.completed", **summary},
    )
    return summary


@register_handler("kg.tick.daily")
class KGDailyTickHandler:
    def __init__(self, *, clock: WorkerClockPort | None = None) -> None:
        self._clock = clock

    async def handle(self, event: KGDailyTick, session: object) -> None:
        started_at = _clock_now(self._clock)
        try:
            await _run_daily_tick(
                tick_id=event.tick_id,
                session=session,
                board_id=event.board_id,
                clock=self._clock,
            )
        except (DeliveryMaintenanceFailed, StaleSweepMaintenanceFailed):
            # Cards 7/8: never attempt an error upsert in the tainted
            # transaction and never let the dispatcher ACK/commit partial
            # maintenance or scheduling effects. Its normal exception path
            # owns the rollback and retry.
            logger.exception(
                "kg.tick.delivery_maintenance_rollback tick_id=%s board_id=%s",
                event.tick_id,
                event.board_id,
                extra={
                    "event": "kg.tick.delivery_maintenance_rollback",
                    "tick_id": event.tick_id,
                    "board_id": event.board_id,
                },
            )
            raise
        except Exception as exc:
            # FR3: on error, persist the tick_run with the error string and
            # return normally. The persisted row IS the idempotent signal; the
            # dispatcher marks the handler execution done. Do NOT re-raise.
            completed_at = _clock_now(self._clock)
            try:
                await _persist_tick_run(
                    session,
                    tick_id=event.tick_id,
                    started_at=started_at,
                    completed_at=completed_at,
                    nodes_recomputed=0,
                    duration_ms=(
                        completed_at - started_at
                    ).total_seconds() * 1000.0,
                    boards_processed=0,
                    boards_failed=0,
                    error=str(exc),
                )
            except Exception:
                logger.exception(
                    "kg.relevance.tick.failure_persist_failed tick_id=%s",
                    event.tick_id,
                )


@register_handler("kg.tick.delivery_redrive")
class KGDeliveryRedriveTickHandler:
    """Execute one durable continuation without repeating graph decay."""

    def __init__(self, *, clock: WorkerClockPort | None = None) -> None:
        self._clock = clock

    async def handle(
        self,
        event: KGDeliveryRedriveTick,
        session: object,
    ) -> None:
        port = _optional_delivery_ledger_port()
        if port is None:
            logger.warning(
                "kg.tick.delivery_redrive.port_unavailable run_id=%s",
                event.run_id,
                extra={
                    "event": "kg.tick.delivery_redrive.port_unavailable",
                    "run_id": event.run_id,
                    "checkpoint_version": event.checkpoint_version,
                },
            )
            raise DeliveryMaintenanceFailed(
                f"delivery_redrive_port_unavailable:{event.run_id}"
            )
        try:
            now = _clock_now(self._clock)
            await _run_delivery_redrive_pass(
                port=port,
                session=session,
                board_id=event.board_id,
                now=now,
                redrive_limit=KG_DELIVERY_REDRIVE_LIMIT,
            )
            await _evaluate_takedown_slo(
                port=_optional_takedown_telemetry_port(),
                session=session,
                board_id=event.board_id,
                now=now,
                transaction_state="pending_caller_commit",
                correlation_id=event.run_id,
                correlation_kind="kg.tick.delivery_redrive",
            )
        except Exception as exc:
            logger.warning(
                "kg.tick.delivery_redrive.rollback run_id=%s error=%s",
                event.run_id,
                str(exc),
                extra={
                    "event": "kg.tick.delivery_redrive.rollback",
                    "run_id": event.run_id,
                    "board_id": event.board_id,
                    "checkpoint_version": event.checkpoint_version,
                    "error": str(exc),
                },
            )
            raise DeliveryMaintenanceFailed(
                f"delivery_redrive_failed:{event.run_id}"
            ) from exc


__all__ = [
    "DeliveryMaintenanceFailed",
    "StaleSweepMaintenanceFailed",
    "KGDailyTickHandler",
    "KGDeliveryRedriveTickHandler",
    "KG_DECAY_TICK_BATCH_SIZE",
    "KG_DECAY_TICK_STALENESS_DAYS",
    "KG_DELIVERY_REDRIVE_LIMIT",
    "KG_DELIVERY_WATCHDOG_LIMIT",
    "KG_STALE_SWEEP_BUDGET",
    "_fetch_stale_nodes",
    "_evaluate_takedown_slo",
    "_process_board_sync",
    "_run_delivery_maintenance",
    "_run_delivery_redrive_pass",
    "_run_daily_tick",
    "_schedule_stale_sweep",
    "_persist_tick_run",
]
