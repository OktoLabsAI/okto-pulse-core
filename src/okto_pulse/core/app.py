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
from okto_pulse.core.composition import (
    REQUIRED_OWNED_PROVIDERS,
    RuntimeComposition,
    RuntimeProviderMissing,
    validate_required_providers,
)
from okto_pulse.core.api import api_router
from okto_pulse.core.telemetry.http_policy import safe_route_template, should_count_http
from okto_pulse.core.telemetry.telemetry_port_registry import get_telemetry_port

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
        # R5A-C: explicit, testable HTTP telemetry policy (counts /api + /mcp; excludes
        # /health /docs /openapi.json /redoc; no lookalike false positives) replaces
        # the old implicit startswith('/api/') filter.
        if scope["type"] != "http" or not should_count_http(scope.get("path", "")):
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
            # aqui depois que o downstream rodou. safe_route_template prefere o
            # PADRÃO da rota resolvida (placeholders) e nunca emite path concreto.
            route = scope.get("route")
            route_template = safe_route_template(route, scope.get("path", ""))
            payload = {
                "method": scope.get("method", ""),
                "route_template": route_template,
                "status_code": status_holder["status"],
                "duration_ms": int((time.perf_counter() - started) * 1000),
            }
            if error_class:
                payload["error_class"] = error_class
            try:
                get_telemetry_port(self.settings).record_event("http", payload)
            except Exception:
                logger.debug("telemetry.record_failed", exc_info=True)


async def shutdown_kg_then_db(
    close_db: Callable[[], "object"],
    *,
    logger: logging.Logger,
    graph_lifecycle_provider: Callable[[], object] | None = None,
    run_blocking: Callable[[Callable[[], object]], object] | None = None,
) -> None:
    """Release KG graph connections, then close the DB (R16-D, tr_5c727d9b).

    Closes the board graphs via the spec #06 ``GraphLifecycle`` port
    (``graph_lifecycle.close(None)`` replaces ``kg.schema.close_all_connections``),
    driven OFF the event loop because the embedded adapter is synchronous under
    the async port (the close drains readers up to ~5s/board and would freeze
    shutdown if run on the loop).

    A KG-close (or registry-resolution) failure is logged as
    ``kg.shutdown.close_connections_failed`` and NEVER blocks the DB close —
    ``close_db()`` ALWAYS runs afterwards. ``graph_lifecycle_provider`` /
    ``run_blocking`` are injectable for deterministic tests.
    """
    try:
        if graph_lifecycle_provider is None:
            from okto_pulse.core.kg.interfaces import get_kg_registry

            graph_lifecycle = get_kg_registry().graph_lifecycle
        else:
            graph_lifecycle = graph_lifecycle_provider()
        if run_blocking is None:
            await asyncio.to_thread(
                lambda: asyncio.run(graph_lifecycle.close(None))
            )
        else:
            await run_blocking(lambda: graph_lifecycle.close(None))
    except Exception as exc:  # noqa: BLE001 — never block the DB close
        logger.warning(
            "kg.shutdown.close_connections_failed err=%s",
            exc,
            extra={
                "event": "kg.shutdown.close_connections_failed",
                "error": str(exc),
            },
        )
    await close_db()


def create_app(
    settings: CoreSettings,
    auth_provider: AuthProvider,
    storage_provider: StorageProvider,
    *,
    cors_origins: list[str] | None = None,
    lifespan: Optional[Callable] = None,
    composition: RuntimeComposition | None = None,
    strict_runtime: bool = False,
) -> FastAPI:
    """Create and configure the FastAPI application.

    Args:
        settings: Application settings (CoreSettings or subclass)
        auth_provider: Authentication provider implementation
        storage_provider: File storage provider implementation
        cors_origins: List of allowed CORS origins
        composition: Optional explicit :class:`RuntimeComposition` (spec #03). When
            omitted, the historical wiring is preserved (backward compatible).
        strict_runtime: When True (spec #03, tr_f9d2f84f) a #03-owned provider that
            is absent fails fast with ``runtime_provider_missing`` — no silent
            fallback to a concrete default. The KG registry stays deferred to #05.
    """
    if auth_provider is None:
        raise TypeError("auth_provider is required")
    if storage_provider is None:
        raise TypeError("storage_provider is required")

    # spec #03 (tr_f9d2f84f): strict_runtime requires an explicit composition and
    # rejects any missing required owned provider deterministically. Default
    # (strict_runtime=False) keeps the historical behaviour intact.
    if strict_runtime:
        if composition is None:
            raise RuntimeProviderMissing("composition", missing=list(REQUIRED_OWNED_PROVIDERS))
        validate_required_providers(composition)

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

        # Self-heal: Q&A respondidas herdadas sem answered_at viravam
        # falso-abertas no badge open_qa_count (a herança não copiava o
        # timestamp). Idempotente — só carimba linhas respondidas órfãs.
        try:
            from okto_pulse.core.services.main import backfill_qa_answered_at

            factory = get_session_factory()
            async with factory() as _qa_session:
                _qa_fixed = await backfill_qa_answered_at(_qa_session)
            if _qa_fixed:
                logger.info(
                    "qa.answered_at.backfilled %s", _qa_fixed,
                    extra={
                        "event": "qa.answered_at.backfilled",
                        "fixed": _qa_fixed,
                    },
                )
        except Exception as _exc:
            logger.warning(
                "qa.answered_at.backfill_failed err=%s", _exc,
                extra={"event": "qa.answered_at.backfill_failed"},
            )

        # Self-heal AFG (investigacao 2026-06-10): findings de arquitetura
        # so nasciam em saves pos-feature; 83% dos designs nunca tiveram
        # run e o gate avaliava tabela vazia. O sweep re-avalia os designs
        # (payloads re-hidratados) em BACKGROUND para nao atrasar o boot.
        async def _afg_backfill_task() -> None:
            try:
                from okto_pulse.core.services.architecture import (
                    backfill_architecture_finding_runs,
                )

                factory = get_session_factory()
                async with factory() as _afg_session:
                    _afg_stats = await backfill_architecture_finding_runs(
                        _afg_session
                    )
                logger.info(
                    "architecture.finding_backfill.completed %s", _afg_stats,
                    extra={
                        "event": "architecture.finding_backfill.completed",
                        **_afg_stats,
                    },
                )
            except Exception as _afg_exc:
                logger.warning(
                    "architecture.finding_backfill.failed err=%s", _afg_exc,
                    extra={"event": "architecture.finding_backfill.failed"},
                )

        _afg_task = asyncio.create_task(_afg_backfill_task())

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

        # RKG-03: the dedicated cognitive-closeout worker drains PENDING cognitive
        # closeout from the ledger and persists Alternative/Assumption/Learning to
        # graph.lbug OUTSIDE the event drain (never inside the deterministic Layer-1
        # worker, never inside the event drain transaction).
        cognitive_closeout_worker = None
        try:
            from okto_pulse.core.kg.workers.cognitive_closeout import (
                get_cognitive_closeout_worker,
            )

            cognitive_closeout_worker = get_cognitive_closeout_worker()
            await cognitive_closeout_worker.start()
        except Exception as exc:
            logger.warning(
                "kg.cognitive_closeout_worker.start_failed err=%s",
                exc,
                extra={
                    "event": "kg.cognitive_closeout_worker.start_failed",
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
            from okto_pulse.core.kg.interfaces import get_kg_registry
            from okto_pulse.core.kg.startup_schema_sweep import (
                sweep_board_schemas,
            )

            factory = get_session_factory()
            async with factory() as _session:
                board_ids = (
                    await _session.execute(_select(_Board.id))
                ).scalars().all()

            # R16-D: the per-board KG schema sweep now goes through the spec #06
            # ports — GraphPathResolver.exists (skip-missing) +
            # GraphSchemaManager.ensure_bootstrapped (idempotent per-board
            # migration) — instead of kg.schema.board_kuzu_path /
            # open_board_connection. The embedded adapter runs the synchronous
            # Kùzu work, so the helper offloads each board off the event loop,
            # preserving the original non-blocking boot. Same skip-missing /
            # soft-fail-per-board / count / log (migration_swept/_failed)
            # semantics; an outer failure (table absent on fresh install / Kùzu
            # missing) still degrades to migration_skipped below.
            _kg_reg = get_kg_registry()
            await sweep_board_schemas(
                board_ids,
                graph_path_resolver=_kg_reg.graph_path_resolver,
                graph_schema_manager=_kg_reg.graph_schema_manager,
                logger=logger,
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
            if cognitive_closeout_worker is not None:
                await cognitive_closeout_worker.stop()
            if cleanup_worker is not None:
                await cleanup_worker.stop()
            if outbox_worker is not None:
                await outbox_worker.stop()
            # Release LadybugDB handles explicitly on graceful shutdown.
            # Relying on interpreter teardown can leave WAL sidecars as the
            # only holder of recent writes; a later bootstrap probe may then
            # see a corrupt WAL and previously attempted an automatic purge.
            #
            # R16-D: extracted to shutdown_kg_then_db (tr_5c727d9b) — closes via
            # the spec #06 GraphLifecycle port off the loop; a KG-close failure
            # is logged (kg.shutdown.close_connections_failed) and never blocks
            # the DB close (close_db always runs).
            await shutdown_kg_then_db(close_db, logger=logger)

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

    # MockupDesignSystemGate (spec 3a006f65 / card 0192f58d): the gate runs inside the
    # service-layer entity update methods, so a bulk screen_mockups REST update that
    # violates a blocking Design System gate raises DesignSystemError. Translate it to a
    # clean, actionable structured response (FR8) on every REST surface in one place.
    from fastapi.responses import JSONResponse

    from okto_pulse.core.services.design_system import DesignSystemError

    @app.exception_handler(DesignSystemError)
    async def _design_system_error_handler(_request, exc: DesignSystemError):
        return JSONResponse(status_code=exc.status_code, content=exc.to_dict())

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
