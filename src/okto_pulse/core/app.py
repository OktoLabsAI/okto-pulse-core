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
        yield
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
