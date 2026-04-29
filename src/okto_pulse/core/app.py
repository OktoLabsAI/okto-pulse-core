"""Core application factory."""

import logging
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import AsyncGenerator, Callable, Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from okto_pulse.core.infra.auth import AuthProvider, configure_auth
from okto_pulse.core.infra.config import CoreSettings, configure_settings
from okto_pulse.core.infra.database import create_database, init_db, close_db, get_session_factory
from okto_pulse.core.infra.storage import StorageProvider, configure_storage
from okto_pulse.core.api import api_router

logger = logging.getLogger(__name__)


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
            migrated = 0
            for _bid in board_ids:
                if not _board_kuzu_path(_bid).exists():
                    continue
                try:
                    bc = _open_board_connection(_bid)
                    bc.close()
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
            await event_dispatcher.stop(timeout=5.0)
            set_dispatcher(None)
            if cleanup_worker is not None:
                await cleanup_worker.stop()
            if outbox_worker is not None:
                await outbox_worker.stop()
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

    # Health check
    @app.get("/health")
    async def health_check():
        return {"status": "healthy", "version": settings.app_version}

    # API routes
    app.include_router(api_router)

    return app


async def _emit_daily_tick() -> None:
    """APScheduler callback — emits KGDailyTick if this replica owns the lock.

    Acquires the in-process advisory lock keyed ``("kg_daily_tick", "global")``;
    if another emitter already holds it on this loop, returns silently. The
    handler picks up the event and runs the actual tick body.
    """
    from okto_pulse.core.events import publish as event_publish
    from okto_pulse.core.events.types import KGDailyTick
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
        tick_id = str(uuid.uuid4())
        scheduled_at = datetime.now(timezone.utc).isoformat()
        try:
            async with factory() as session:
                await event_publish(
                    KGDailyTick(
                        board_id="*",
                        tick_id=tick_id,
                        scheduled_at=scheduled_at,
                    ),
                    session=session,
                )
                await session.commit()
            logger.info(
                "kg.tick.emitted",
                extra={
                    "event": "kg.tick.emitted",
                    "tick_id": tick_id,
                    "scheduled_at": scheduled_at,
                },
            )
        except Exception as exc:
            logger.error(
                "kg.tick.emit_failed err=%s", exc,
                extra={"event": "kg.tick.emit_failed", "error": str(exc)},
            )
