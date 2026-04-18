"""Core application factory."""

from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from okto_pulse.core.infra.auth import AuthProvider, configure_auth
from okto_pulse.core.infra.config import CoreSettings, configure_settings
from okto_pulse.core.infra.database import create_database, init_db, close_db, get_session_factory
from okto_pulse.core.infra.storage import StorageProvider, configure_storage
from okto_pulse.core.api import api_router


def create_app(
    settings: CoreSettings,
    auth_provider: AuthProvider,
    storage_provider: StorageProvider,
    *,
    cors_origins: list[str] | None = None,
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
    async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
        await init_db()
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
        try:
            yield
        finally:
            if cleanup_worker is not None:
                await cleanup_worker.stop()
            if outbox_worker is not None:
                await outbox_worker.stop()
            await close_db()

    app = FastAPI(
        title=settings.app_name,
        version=settings.app_version,
        lifespan=lifespan,
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
