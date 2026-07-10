"""Authentication registration seam with no transport dependency.

The provider contract lives in :mod:`okto_pulse.core.ports.authentication`.
FastAPI dependencies moved to ``core.api.auth_deps`` so a Core use case, worker
or future SaaS composition can import this module without loading FastAPI.
"""

from __future__ import annotations

from typing import Any

from okto_pulse.core.ports.authentication import AuthenticationPort

# Compatibility type name retained for external composition roots.  It is a
# structural, transport-free Protocol rather than the former FastAPI-coupled ABC.
AuthProvider = AuthenticationPort

_auth_provider: AuthenticationPort | None = None


def configure_auth(provider: AuthenticationPort) -> None:
    """Register the edition-owned authentication port at startup."""
    global _auth_provider
    _auth_provider = provider


def reset_auth_for_tests() -> None:
    """Clear the registered provider for isolated composition tests."""
    global _auth_provider
    _auth_provider = None


def get_auth_provider() -> AuthenticationPort:
    """Return the registered port or fail closed before processing work."""
    if _auth_provider is None:
        raise RuntimeError(
            "AuthenticationPort not configured. Call configure_auth() from the "
            "edition composition root first."
        )
    return _auth_provider


def __getattr__(name: str) -> Any:
    """Serve deprecated REST dependency imports without re-coupling this seam.

    Existing callers can still import these symbols from ``infra.auth`` while
    receiving the exact functions owned by the inbound FastAPI adapter.  New
    code must import them from ``core.api.auth_deps`` directly.
    """
    if name in {
        "get_current_principal",
        "get_current_user",
        "get_current_user_id",
        "get_realm_id",
        "require_principal",
        "require_user",
        "security",
    }:
        from okto_pulse.core.api import auth_deps

        return getattr(auth_deps, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "AuthProvider",
    "AuthenticationPort",
    "configure_auth",
    "get_auth_provider",
    "reset_auth_for_tests",
]
