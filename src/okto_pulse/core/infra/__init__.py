"""Core infrastructure compatibility facade with lazy public exports.

Importing a pure policy helper must not initialize the relational runtime just
because it lives next to infrastructure modules.  Concrete modules remain
available through the historical ``okto_pulse.core.infra`` names on first use.
"""

from __future__ import annotations

import importlib


_LAZY_EXPORTS: dict[str, str] = {
    "AuthProvider": "okto_pulse.core.infra.auth",
    "AuthenticationPort": "okto_pulse.core.infra.auth",
    "configure_auth": "okto_pulse.core.infra.auth",
    "get_auth_provider": "okto_pulse.core.infra.auth",
    "reset_auth_for_tests": "okto_pulse.core.infra.auth",
    "CoreSettings": "okto_pulse.core.infra.config",
    "MCPSettings": "okto_pulse.core.infra.config",
    "configure_settings": "okto_pulse.core.infra.config",
    "get_mcp_settings": "okto_pulse.core.infra.config",
    "get_settings": "okto_pulse.core.infra.config",
    "close_db": "okto_pulse.core.infra.database",
    "configure_database_runtime": "okto_pulse.core.infra.database",
    "create_database": "okto_pulse.core.infra.database",
    "get_db": "okto_pulse.core.infra.database",
    "get_db_session": "okto_pulse.core.infra.database",
    "get_engine": "okto_pulse.core.infra.database",
    "get_session_factory": "okto_pulse.core.infra.database",
    "init_db": "okto_pulse.core.infra.database",
    "is_database_runtime_configured": "okto_pulse.core.infra.database",
    "reset_database_runtime_for_tests": "okto_pulse.core.infra.database",
    "Permissions": "okto_pulse.core.infra.permissions",
    "check_permission": "okto_pulse.core.infra.permissions",
    "has_permission": "okto_pulse.core.infra.permissions",
    "StorageProvider": "okto_pulse.core.infra.storage",
    "configure_storage": "okto_pulse.core.infra.storage",
    "get_storage_provider": "okto_pulse.core.infra.storage",
}

__all__ = [*_LAZY_EXPORTS]


def __getattr__(name: str):
    """Resolve a historical infrastructure export only when it is requested."""

    module = _LAZY_EXPORTS.get(name)
    if module is not None:
        value = getattr(importlib.import_module(module), name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted({*globals(), *_LAZY_EXPORTS})
