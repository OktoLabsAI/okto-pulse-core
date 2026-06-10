"""Core application factory."""

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import AsyncGenerator, Callable, Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from okto_pulse.core.infra.auth import AuthProvider, configure_auth
from okto_pulse.core.infra.config import CoreSettings, configure_settings
from okto_pulse.core.infra.database import create_database, init_db, close_db, get_session_factory
from okto_pulse.core.infra.storage import StorageProvider, configure_storage
from okto_pulse.core.api import api_router
from okto_pulse.core.telemetry.service import TelemetryService

logger = logging.getLogger(__name__)


class _TelemetryASGIMiddleware:
    """Telemetria HTTP como ASGI puro (sem BaseHTTPMiddleware).

    Intercepta ``http.response.start`` para capturar o status e registra o
    evento quando o downstream conclui (para streams, ao fim do stream —
    mesma semântica da versão BaseHTTPMiddleware). Não cria task group nem
    cancel scope em volta da resposta, o que mantém o caminho de
    cancelamento de SSE/streaming idêntico ao do servidor puro.
    """

    def __init__(self, app, settings: CoreSettings) -> None:
        self.app = app
        self.settings = settings

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http" or not scope.get("path", "").startswith("/api/"):
            await self.app(scope, receive, send)
            return

        started = time.perf_counter()
        status_holder = {"status": 500}
        error_class = None

        async def send_wrapper(message) -> None:
            if message["type"] == "http.response.start":
                status_holder["status"] = message["status"]
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        except Exception as exc:
            error_class = exc.__class__.__name__
            raise
        finally:
            # O router popula scope["route"] durante o dispatch — disponível
            # aqui depois que o downstream rodou.
            route = scope.get("route")
            route_template = getattr(route, "path", scope.get("path", ""))
            payload = {
                "method": scope.get("method", ""),
                "route_template": route_template,
                "status_code": status_holder["status"],
                "duration_ms": int((time.perf_counter() - started) * 1000),
            }
            if error_class:
                payload["error_class"] = error_class
            try:
                TelemetryService(self.settings).record_event("http", payload)
            except Exception:
                logger.debug("telemetry.record_failed", exc_info=True)


def create_app(
    settings: CoreSettings,
    auth_provider: AuthProvider,
    storage_provider: StorageProvider,
    *,
    cors_origins: list[str] | None = None,
    lifespan: Optional[Callable] = None,
) -> FastAPI:
    """Create and configure the FastAPI application.

    Args:
        settings: Application settings (CoreSettings or subclass)
        auth_provider: Authentication provider implementation
        storage_provider: File storage provider implementation
        cors_origins: List of allowed CORS origins
    """
    if auth_provider is None:
        raise TypeError("auth_provider is required")
    if storage_provider is None:
        raise TypeError("storage_provider is required")

    # Register providers
    configure_settings(settings)
    configure_auth(auth_provider)
    configure_storage(storage_provider)

    # Initialize database
    create_database(settings.database_url, echo=settings.debug)

    @asynccontextmanager
    async def _default_lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
        # Ensure the AppSetting model is registered with Base before init_db
        # creates the schema. Side-effect import only.
        from okto_pulse.core.services import settings_service as _settings_svc  # noqa: F401

        await init_db()

        # Apply persisted runtime settings BEFORE any module opens a Kùzu
        # Database instance. _open_kuzu_db reads CoreSettings at call time,
        # so we just need configure_settings() to be updated by then.
        try:
            from okto_pulse.core.services.settings_service import (
                apply_persisted_settings_to_core_settings,
            )
            await apply_persisted_settings_to_core_settings()
        except Exception:
            # Fresh install, table may not exist yet — that's fine, defaults
            # already cover the safe budget.
            pass

        # Import events package BEFORE dispatcher.start — side-effect of
        # importing handlers is @register_handler populating the registry.
        # Dispatcher relies on the registry being complete when it drains.
        from okto_pulse.core import events as _events  # noqa: F401
        from okto_pulse.core.events.dispatcher import EventDispatcher, set_dispatcher

        event_dispatcher = EventDispatcher(get_session_factory())
        await event_dispatcher.start()
        set_dispatcher(event_dispatcher)

        # Start the deterministic KG consolidation worker. The dispatcher
        # only enqueues consolidation work; this worker is the component that
        # drains consolidation_queue into graph.lbug. Without it, DLQ
        # reprocess and rebuild backlogs remain pending until an ad-hoc MCP
        # process_now call runs a one-off batch.
        consolidation_worker = None
        try:
            from okto_pulse.core.kg.workers.consolidation import (
                get_consolidation_worker,
            )

            consolidation_worker = get_consolidation_worker()
            await consolidation_worker.start()
        except Exception as exc:
            logger.warning(
                "kg.consolidation_worker.start_failed err=%s",
                exc,
                extra={
                    "event": "kg.consolidation_worker.start_failed",
                    "error": str(exc),
                },
            )

        # NC-10 fix: migrate per-board KG schemas idempotently on boot.
        # Boards created before SCHEMA_VERSION 0.3.3 lack the
        # ``last_recomputed_at`` column on every node type, which floods
        # the daily tick with ``Cannot find property last_recomputed_at``
        # warnings and silently skips those boards' decay recompute.
        # ``apply_schema_to_connection`` is idempotent (CREATE NODE TABLE
        # IF NOT EXISTS, ALTER TABLE ADD COLUMN IF NOT EXISTS) so this is
        # safe to run on every startup; soft-fail per board so a single
        # broken Kùzu file does not block the app from booting.
        try:
            from sqlalchemy import select as _select
            from okto_pulse.core.models.db import Board as _Board
            from okto_pulse.core.kg.schema import (
                board_kuzu_path as _board_kuzu_path,
                open_board_connection as _open_board_connection,
            )

            factory = get_session_factory()
            async with factory() as _session:
                board_ids = (
                    await _session.execute(_select(_Board.id))
                ).scalars().all()

            def _sweep_one(_bid: str) -> bool:
                """Abre/fecha a BoardConnection (migração idempotente).

                Roda via asyncio.to_thread — abrir um Kùzu DB é I/O pesado
                (até 6.2s de retry em lock contention) e este loop rodava
                SÍNCRONO no event loop, congelando o servidor inteiro no
                startup quando havia boards lentos/em recuperação.
                """
                bc = _open_board_connection(_bid)
                bc.close()
                return True

            migrated = 0
            for _bid in board_ids:
                if not _board_kuzu_path(_bid).exists():
                    continue
                try:
                    await asyncio.to_thread(_sweep_one, _bid)
                    migrated += 1
                except Exception as _exc:
                    logger.warning(
                        "kg.schema.migration_failed board=%s err=%s",
                        _bid, _exc,
                        extra={
                            "event": "kg.schema.migration_failed",
                            "board_id": _bid,
                            "error": str(_exc),
                        },
                    )
            logger.info(
                "kg.schema.migration_swept boards=%d", migrated,
                extra={
                    "event": "kg.schema.migration_swept",
                    "boards_swept": migrated,
                },
            )
        except Exception as _exc:
            # Tabela ainda não existe em fresh install ou Kùzu não
            # instalado — não bloqueia boot.
            logger.debug(
                "kg.schema.migration_skipped err=%s", _exc,
                extra={"event": "kg.schema.migration_skipped"},
            )

        # Start the KG session cleanup worker if enabled. Safe to call even
        # when the KG layer is unused — the worker just sweeps an empty
        # SessionManager and costs one asyncio.sleep per interval.
        cleanup_worker = None
        if getattr(settings, "kg_cleanup_enabled", True):
            from okto_pulse.core.kg.workers import get_cleanup_worker

            cleanup_worker = get_cleanup_worker()
            await cleanup_worker.start()
        # Start the global discovery outbox worker. Populates the meta-graph
        # from GlobalUpdateOutbox events so cross-board search works.
        outbox_worker = None
        try:
            from okto_pulse.core.kg.global_discovery.outbox_worker import get_outbox_worker
            outbox_worker = get_outbox_worker()
            await outbox_worker.start()
        except Exception:
            # Kùzu may not be installed — log and continue
            pass

        # spec 28583299 (Ideação #4, IMPL-D, dec_bc0eaeec): start the daily
        # decay tick scheduler. APScheduler in-process is the chosen
        # vehicle — fits FastAPI lifespan, no external broker. Multi-replica
        # safety relies on the in-process advisory lock pattern (single
        # process today; documented as needing pg_try_advisory_lock for a
        # real multi-replica deploy — see open_for_spec_phase D-1).
        scheduler = None
        if os.getenv("KG_DAILY_TICK_DISABLED") != "1":
            try:
                from apscheduler.schedulers.asyncio import AsyncIOScheduler
                from apscheduler.triggers.interval import IntervalTrigger

                from okto_pulse.core.infra.config import get_settings as _get_settings
                from okto_pulse.core.kg.scheduler_singleton import set_scheduler

                _interval_minutes = _get_settings().kg_decay_tick_interval_minutes
                scheduler = AsyncIOScheduler(timezone=timezone.utc)
                # Catch-up no boot (campo 2026-06-10): IntervalTrigger só
                # dispara o PRIMEIRO tick um intervalo COMPLETO após o
                # start — com interval de 24h e um processo que reinicia
                # (deploys/crashes), o tick nunca rodava. O next_run_time
                # explícito honra o último tick persistido: vencido →
                # dispara em ~2min; senão → no vencimento real.
                _job_kwargs: dict = {}
                try:
                    _next_run = await _compute_tick_catch_up_next_run(
                        _interval_minutes
                    )
                    if _next_run is not None:
                        _job_kwargs["next_run_time"] = _next_run
                except Exception as _exc:
                    logger.warning(
                        "kg.tick.catch_up_compute_failed err=%s", _exc,
                        extra={"event": "kg.tick.catch_up_compute_failed"},
                    )
                scheduler.add_job(
                    _emit_daily_tick,
                    # Spec 54399628 (Wave 2 NC f9732afc): IntervalTrigger
                    # com `kg_decay_tick_interval_minutes` permite operador
                    # ajustar via PUT /api/v1/settings/runtime sem rebuild.
                    # Hot-reload via scheduler.reschedule_job (settings_service).
                    trigger=IntervalTrigger(
                        minutes=_interval_minutes,
                        timezone=timezone.utc,
                    ),
                    id="kg_daily_tick",
                    replace_existing=True,
                    max_instances=1,
                    coalesce=True,
                    **_job_kwargs,
                )
                scheduler.start()
                set_scheduler(scheduler)  # expose for hot-reload
                logger.info(
                    "kg.tick.scheduler_started interval_minutes=%d",
                    _interval_minutes,
                    extra={
                        "event": "kg.tick.scheduler_started",
                        "interval_minutes": _interval_minutes,
                    },
                )
            except Exception as exc:
                # APScheduler not installed (e.g. minimal test env) or
                # event loop oddities — log and continue without the tick.
                logger.warning(
                    "kg.tick.scheduler_failed err=%s", exc,
                    extra={"event": "kg.tick.scheduler_failed"},
                )
                scheduler = None
        try:
            yield
        finally:
            # Reverse order: stop dispatcher first so in-flight handlers
            # finish before the downstream workers they depend on exit.
            if scheduler is not None:
                try:
                    scheduler.shutdown(wait=False)
                except Exception:
                    pass
            # Para os pollers SSE antes do DB fechar — eles abrem sessões
            # próprias fora do escopo de qualquer request.
            try:
                from okto_pulse.core.api.kg_events_hub import shutdown_kg_events_hub

                await shutdown_kg_events_hub()
            except Exception:
                pass
            await event_dispatcher.stop(timeout=5.0)
            set_dispatcher(None)
            if consolidation_worker is not None:
                await consolidation_worker.stop()
            if cleanup_worker is not None:
                await cleanup_worker.stop()
            if outbox_worker is not None:
                await outbox_worker.stop()
            # Release LadybugDB handles explicitly on graceful shutdown.
            # Relying on interpreter teardown can leave WAL sidecars as the
            # only holder of recent writes; a later bootstrap probe may then
            # see a corrupt WAL and previously attempted an automatic purge.
            try:
                from okto_pulse.core.kg.schema import close_all_connections

                # to_thread: o close drena leitores (até 5s por board via o
                # close guard) — síncrono no loop, congelava o shutdown.
                await asyncio.to_thread(close_all_connections)
            except Exception as exc:
                logger.warning(
                    "kg.shutdown.close_connections_failed err=%s",
                    exc,
                    extra={
                        "event": "kg.shutdown.close_connections_failed",
                        "error": str(exc),
                    },
                )
            await close_db()

    app = FastAPI(
        title=settings.app_name,
        version=settings.app_version,
        lifespan=lifespan if lifespan else _default_lifespan,
    )

    # CORS
    if cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=cors_origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    # Root-cause fix (2026-06-09): este middleware era um
    # ``@app.middleware("http")`` (= BaseHTTPMiddleware). BaseHTTPMiddleware
    # consome respostas de streaming dentro de um task group do anyio; quando
    # o cliente de um SSE desconectava, o cancel scope cancelava o generator
    # com hard-cancel no meio de awaits de DB — vazando conexões do pool
    # (exaustão → "travamento"). Como ASGI puro, a desconexão fecha o
    # generator pelo caminho normal (aclose), sem cancel scope atravessando
    # o cleanup.
    app.add_middleware(_TelemetryASGIMiddleware, settings=settings)

    # Health check
    @app.get("/health")
    async def health_check():
        return {"status": "healthy", "version": settings.app_version}

    # API routes
    app.include_router(api_router)

    return app


def _tick_next_run_from_last(
    last_completed_at: datetime | None,
    interval_minutes: int,
    now: datetime,
) -> datetime:
    """Próximo disparo do tick honrando o histórico (função pura).

    Sem histórico ou com último tick vencido → ~2min após o boot (dá tempo
    do app estabilizar); senão → no vencimento real (last + interval)."""
    floor = now + timedelta(seconds=120)
    if last_completed_at is None:
        return floor
    if last_completed_at.tzinfo is None:
        last_completed_at = last_completed_at.replace(tzinfo=timezone.utc)
    due = last_completed_at + timedelta(minutes=interval_minutes)
    return max(due, floor)


async def _compute_tick_catch_up_next_run(
    interval_minutes: int,
) -> datetime | None:
    """Lê o último tick persistido e devolve o next_run_time do job."""
    from sqlalchemy import select

    from okto_pulse.core.models.db import KGTickRun

    factory = get_session_factory()
    async with factory() as session:
        last = (
            await session.execute(
                select(KGTickRun.completed_at)
                .order_by(KGTickRun.completed_at.desc())
                .limit(1)
            )
        ).scalars().first()
    return _tick_next_run_from_last(
        last, interval_minutes, datetime.now(timezone.utc)
    )


async def _emit_daily_tick() -> None:
    """APScheduler callback — emits KGDailyTick if this replica owns the lock.

    Acquires the in-process advisory lock keyed ``("kg_daily_tick", "global")``;
    if another emitter already holds it on this loop, returns silently. The
    handler picks up the event and runs the actual tick body.
    """
    from okto_pulse.core.kg.workers.advisory_lock import get_async_lock

    lock = get_async_lock("kg_daily_tick", "global")
    if lock.locked():
        logger.info(
            "kg.tick.skipped reason=non_leader",
            extra={"event": "kg.tick.skipped", "reason": "non_leader"},
        )
        return
    async with lock:
        try:
            factory = get_session_factory()
        except AssertionError:
            logger.warning(
                "kg.tick.no_session_factory",
                extra={"event": "kg.tick.no_session_factory"},
            )
            return
        scheduled_at = datetime.now(timezone.utc).isoformat()
        try:
            # Fan-out por board real (campo 2026-06-10): o evento global
            # board_id='*' violava a FK de domain_events com
            # PRAGMA foreign_keys=ON (runtime community) — nenhum tick
            # chegava a ser agendado em produção.
            from okto_pulse.core.events.handlers.kg_decay_tick import (
                publish_tick_events,
            )

            async with factory() as session:
                tick_ids = await publish_tick_events(
                    session, scheduled_at=scheduled_at,
                )
                await session.commit()
            logger.info(
                "kg.tick.emitted",
                extra={
                    "event": "kg.tick.emitted",
                    "tick_count": len(tick_ids),
                    "scheduled_at": scheduled_at,
                },
            )
        except Exception as exc:
            logger.error(
                "kg.tick.emit_failed err=%s", exc,
                extra={"event": "kg.tick.emit_failed", "error": str(exc)},
            )
