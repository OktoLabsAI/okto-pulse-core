"""Transport-free authentication contract for edition adapters.

REST, MCP and background workers may extract credentials differently, but the
application only needs a normalized credential and resolved principal.  This
module deliberately uses stdlib types only so a SaaS edition can supply an
identity provider without importing FastAPI or Community code.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Protocol, runtime_checkable


class AuthenticationError(Exception):
    """Base class for secret-free authentication outcomes."""


class MissingCredential(AuthenticationError):
    """No credential was accepted for a protected operation."""


class InvalidCredential(AuthenticationError):
    """The supplied credential could not be authenticated."""


class AuthorizationDenied(AuthenticationError):
    """An authenticated principal lacks access to the requested operation."""


@dataclass(frozen=True, repr=False)
class Credential:
    """A credential extracted at an inbound edge.

    ``value`` is intentionally redacted from ``repr``.  ``metadata`` may carry
    bounded, secret-free transport hints such as the authentication scheme.
    """

    value: str
    source: str = "unknown"
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __repr__(self) -> str:
        return (
            f"Credential(source={self.source!r}, "
            f"value=<redacted len={len(self.value)}>, "
            f"metadata={dict(self.metadata)!r})"
        )


@dataclass(frozen=True)
class Principal:
    """Authenticated identity consumed by application policies.

    Claims remain adapter-provided data.  The Core only relies on ``subject``
    and the optional ``realm_id``; REST compatibility can project claims at the
    edge without leaking a framework or provider-specific object inward.
    """

    subject: str
    realm_id: str | None = None
    claims: Mapping[str, Any] = field(default_factory=dict)

    def legacy_user(self) -> dict[str, Any]:
        """Return the historical REST user mapping without sharing mutable state."""
        user = dict(self.claims)
        user["sub"] = self.subject
        return user


@runtime_checkable
class AuthenticationPort(Protocol):
    """Edition-owned credential resolver.

    Return ``None`` for an unrecognized credential or raise one of the typed
    outcomes above.  A local-first adapter may intentionally resolve an absent
    credential to its single local principal.
    """

    async def authenticate(self, credential: Credential | None) -> Principal | None:
        """Resolve an authenticated principal or fail closed."""
        ...


# Historical public name retained for edition composition roots.
AuthProvider = AuthenticationPort


__all__ = [
    "AuthProvider",
    "AuthenticationError",
    "AuthenticationPort",
    "AuthorizationDenied",
    "Credential",
    "InvalidCredential",
    "MissingCredential",
    "Principal",
]
