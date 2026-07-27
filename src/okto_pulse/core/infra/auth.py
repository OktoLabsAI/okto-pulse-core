"""Authentication registration seam with no transport dependency.

The provider contract lives in :mod:`okto_pulse.core.ports.authentication`.
FastAPI dependencies live in ``okto_pulse.community.api.auth_deps`` so a Core
use case, worker, or future SaaS composition can import this module without
loading an edition transport.
"""

from __future__ import annotations

from okto_pulse.core.ports.authentication import AuthenticationPort
from okto_pulse.core.runtime_context import register_runtime_value, reset_runtime_values, resolve_runtime_value

# Compatibility type name retained for external composition roots.  It is a
# structural, transport-free Protocol rather than the former FastAPI-coupled ABC.
AuthProvider = AuthenticationPort

_RUNTIME_KEY = "infra.auth.provider"


def configure_auth(provider: AuthenticationPort) -> None:
    """Register the edition-owned authentication port at startup."""
    register_runtime_value(_RUNTIME_KEY, provider)


def reset_auth_for_tests() -> None:
    """Clear the registered provider for isolated composition tests."""
    reset_runtime_values(_RUNTIME_KEY)


def get_auth_provider() -> AuthenticationPort:
    """Return the registered port or fail closed before processing work."""
    from okto_pulse.core.composition import (
        current_runtime_composition,
    )

    composition = current_runtime_composition()
    if composition is not None and composition.auth_provider is not None:
        return composition.auth_provider
    provider = resolve_runtime_value(_RUNTIME_KEY)
    if provider is None:
        raise RuntimeError(
            "AuthenticationPort not configured. Call configure_auth() from the "
            "edition composition root first."
        )
    return provider


__all__ = [
    "AuthProvider",
    "AuthenticationPort",
    "configure_auth",
    "get_auth_provider",
    "reset_auth_for_tests",
]
