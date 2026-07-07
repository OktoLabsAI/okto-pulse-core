"""Okto Pulse Core — shared logic for all editions."""

# ruff: noqa: F401  # TYPE_CHECKING re-exports are intentional (lazy public API)

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

__version__ = "0.3.0"

# Public names are resolved LAZILY (PEP 562 module ``__getattr__``) so importing
# ``okto_pulse.core`` — or any submodule, which runs this ``__init__`` first —
# does NOT eagerly pull ``infra.database`` / SQLAlchemy / auth / storage. The
# established API is unchanged: ``from okto_pulse.core import Base`` (etc.) still
# works; the first access imports the owning module on demand. This keeps the
# pure ports (e.g. ``core.ports.relational_schema_migrator``, R16-A) importable
# without dragging the relational/ORM stack. Boundary fix only — it does NOT
# touch infra.database, init_db, the migrations, Base.metadata/create_all or any
# runtime wiring.
_LAZY_EXPORTS: dict[str, str] = {
    # infra.auth
    "AuthProvider": "okto_pulse.core.infra.auth",
    "configure_auth": "okto_pulse.core.infra.auth",
    "get_auth_provider": "okto_pulse.core.infra.auth",
    "get_current_user": "okto_pulse.core.infra.auth",
    "get_current_user_id": "okto_pulse.core.infra.auth",
    "get_realm_id": "okto_pulse.core.infra.auth",
    "require_user": "okto_pulse.core.infra.auth",
    # infra.config
    "CoreSettings": "okto_pulse.core.infra.config",
    "get_settings": "okto_pulse.core.infra.config",
    "configure_settings": "okto_pulse.core.infra.config",
    "register_package_version_provider": "okto_pulse.core.infra.config",
    "reset_package_version_provider_for_tests": "okto_pulse.core.infra.config",
    # infra.database
    "Base": "okto_pulse.core.infra.database",
    "get_db": "okto_pulse.core.infra.database",
    "get_db_session": "okto_pulse.core.infra.database",
    "init_db": "okto_pulse.core.infra.database",
    "close_db": "okto_pulse.core.infra.database",
    # infra.permissions
    "Permissions": "okto_pulse.core.infra.permissions",
    "check_permission": "okto_pulse.core.infra.permissions",
    "has_permission": "okto_pulse.core.infra.permissions",
    # infra.storage
    "StorageProvider": "okto_pulse.core.infra.storage",
    "configure_storage": "okto_pulse.core.infra.storage",
    "get_storage_provider": "okto_pulse.core.infra.storage",
}

__all__ = ["__version__", *_LAZY_EXPORTS]

if TYPE_CHECKING:  # static type-checkers / import resolvers see the real symbols
    from okto_pulse.core.infra.auth import (
        AuthProvider,
        configure_auth,
        get_auth_provider,
        get_current_user,
        get_current_user_id,
        get_realm_id,
        require_user,
    )
    from okto_pulse.core.infra.config import (
        CoreSettings,
        configure_settings,
        get_settings,
        register_package_version_provider,
        reset_package_version_provider_for_tests,
    )
    from okto_pulse.core.infra.database import (
        Base,
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
        StorageProvider,
        configure_storage,
        get_storage_provider,
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
