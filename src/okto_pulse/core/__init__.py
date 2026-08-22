"""Okto Pulse Core — shared logic for all editions."""

# ruff: noqa: F401  # TYPE_CHECKING re-exports are intentional (lazy public API)

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

__version__ = "0.3.2"

# Public names are resolved LAZILY (PEP 562 module ``__getattr__``) so importing
# ``okto_pulse.core`` — or any submodule, which runs this ``__init__`` first —
# does NOT eagerly pull ``infra.database`` / SQLAlchemy / auth / storage. The
# the first access imports the owning module on demand. This keeps the
# pure ports (e.g. ``core.ports.relational_schema_migrator``, R16-A) importable
# without dragging the relational/ORM stack. Boundary fix only — it does NOT
# touch edition-owned schema metadata or runtime wiring.
_LAZY_EXPORTS: dict[str, str] = {
    # authentication port and registration seam
    "AuthProvider": "okto_pulse.core.ports.authentication",
    "AuthenticationError": "okto_pulse.core.ports.authentication",
    "AuthenticationPort": "okto_pulse.core.ports.authentication",
    "AuthorizationDenied": "okto_pulse.core.ports.authentication",
    "Credential": "okto_pulse.core.ports.authentication",
    "InvalidCredential": "okto_pulse.core.ports.authentication",
    "MissingCredential": "okto_pulse.core.ports.authentication",
    "Principal": "okto_pulse.core.ports.authentication",
    "configure_auth": "okto_pulse.core.infra.auth",
    "get_auth_provider": "okto_pulse.core.infra.auth",
    "reset_auth_for_tests": "okto_pulse.core.infra.auth",
    # infra.config
    "CoreSettings": "okto_pulse.core.infra.config",
    "get_settings": "okto_pulse.core.infra.config",
    "configure_settings": "okto_pulse.core.infra.config",
    "register_package_version_provider": "okto_pulse.core.infra.config",
    "reset_package_version_provider_for_tests": "okto_pulse.core.infra.config",
    # relational runtime port
    "get_db": "okto_pulse.core.ports.relational_runtime",
    "get_db_session": "okto_pulse.core.ports.relational_runtime",
    "init_db": "okto_pulse.core.ports.relational_runtime",
    "close_db": "okto_pulse.core.ports.relational_runtime",
    # infra.permissions
    "Permissions": "okto_pulse.core.infra.permissions",
    "check_permission": "okto_pulse.core.infra.permissions",
    "has_permission": "okto_pulse.core.infra.permissions",
    # infra.storage
    "DEFAULT_STREAM_CHUNK_SIZE": "okto_pulse.core.infra.storage",
    "StorageProvider": "okto_pulse.core.infra.storage",
    "StorageObjectStat": "okto_pulse.core.infra.storage",
    "configure_storage": "okto_pulse.core.infra.storage",
    "get_storage_provider": "okto_pulse.core.infra.storage",
    # relational schema lifecycle port
    "RelationalSchemaLifecycleOrchestrator": "okto_pulse.core.ports.schema_lifecycle",
    "register_relational_schema_lifecycle_orchestrator": "okto_pulse.core.ports.schema_lifecycle",
    "resolve_relational_schema_lifecycle_orchestrator": "okto_pulse.core.ports.schema_lifecycle",
    "reset_relational_schema_lifecycle_orchestrator": "okto_pulse.core.ports.schema_lifecycle",
}

__all__ = ["__version__", *_LAZY_EXPORTS]

if TYPE_CHECKING:  # static type-checkers / import resolvers see the real symbols
    from okto_pulse.core.infra.auth import (
        configure_auth,
        get_auth_provider,
        reset_auth_for_tests,
    )
    from okto_pulse.core.ports.authentication import (
        AuthProvider,
        AuthenticationError,
        AuthenticationPort,
        AuthorizationDenied,
        Credential,
        InvalidCredential,
        MissingCredential,
        Principal,
    )
    from okto_pulse.core.infra.config import (
        CoreSettings,
        configure_settings,
        get_settings,
        register_package_version_provider,
        reset_package_version_provider_for_tests,
    )
    from okto_pulse.core.ports.relational_runtime import (
        close_db,
        get_db,
        get_db_session,
        init_db,
    )
    from okto_pulse.core.infra.permissions import (
        Permissions,
        check_permission,
        has_permission,
    )
    from okto_pulse.core.infra.storage import (
        DEFAULT_STREAM_CHUNK_SIZE,
        StorageProvider,
        StorageObjectStat,
        configure_storage,
        get_storage_provider,
    )
    from okto_pulse.core.ports.schema_lifecycle import (
        RelationalSchemaLifecycleOrchestrator,
        register_relational_schema_lifecycle_orchestrator,
        reset_relational_schema_lifecycle_orchestrator,
        resolve_relational_schema_lifecycle_orchestrator,
    )


def __getattr__(name: str):
    module = _LAZY_EXPORTS.get(name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(importlib.import_module(module), name)
    globals()[name] = value  # cache: subsequent access skips __getattr__
    return value


def __dir__() -> list[str]:
    return sorted({*globals(), *_LAZY_EXPORTS})
