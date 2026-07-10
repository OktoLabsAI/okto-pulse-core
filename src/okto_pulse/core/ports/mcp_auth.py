"""McpAuthenticator port (spec R08-A, api_… / tr_247ff5be).

Pure, contract-only boundary for MCP credential authentication. It defines the
``McpAuthenticator`` Protocol and its typed DTOs — ``McpCredential``,
``AuthSession`` / ``AgentAuthSession`` — so the inbound MCP server can resolve an
agent identity from a transport-extracted credential WITHOUT the port knowing
the concrete persistence (``AgentService`` / ``Agent`` / SQLAlchemy) or the
transport (FastAPI / Starlette ``Request`` / ASGI scope details).

STRICT scope (R08-A): a compatible contract + adapter that PRESERVES the current
Community behaviour. It does NOT introduce a CredentialStore, JWT, realms/scopes
or user-auth; it does NOT touch ``Agent.api_key`` / ``Agent.api_key_hash`` or
``AgentResponse``. The concrete Community adapter (R08-B/this card) reproduces
``AgentService.get_agent_by_key`` behind this port.

Fail-closed: an absent/invalid/inactive credential yields ``None`` — the
authenticator NEVER raises for a bad credential, and the raw secret is never
logged (``McpCredential`` redacts ``value`` in its ``repr``).

Pure: stdlib ``dataclasses`` / ``typing`` only. It does NOT import FastAPI,
SQLAlchemy, ``AgentService``, ``Agent``, ``Request``, ``ContextVar``,
``okto_pulse.community`` or ``mcp.server`` (tr_f200464a import gate).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Mapping, Protocol, runtime_checkable

from .authentication import Principal

#: Where a credential was extracted from. ``unknown`` is kept for backward
#: compatibility with callers that cannot preserve the originating transport.
McpCredentialSource = Literal[
    "query_param",
    "x_api_key_header",
    "authorization_bearer",
    "unknown",
]
MCP_CREDENTIAL_SOURCES: tuple[McpCredentialSource, ...] = (
    "query_param",
    "x_api_key_header",
    "authorization_bearer",
    "unknown",
)

#: Shared ASGI scope key.  The concrete host writes it and the Core catalog
#: reads it only through the host port; keeping the literal here avoids a
#: transport-specific module dependency in Core.
MCP_CREDENTIAL_SCOPE_KEY = "okto_pulse.mcp_credential"

#: Bounded, secret-free failure reasons for observability. ``authenticate``
#: returns ``None`` (fail-closed) on any of these — the reason set is for
#: telemetry/logging and never carries the raw credential.
McpAuthFailureReason = Literal[
    "absent_credential",
    "invalid_credential",
    "inactive_agent",
    "backend_error",
]
MCP_AUTH_FAILURE_REASONS: frozenset[str] = frozenset(
    {"absent_credential", "invalid_credential", "inactive_agent", "backend_error"}
)


class McpAuthError(Exception):
    """Structured, fail-closed wiring error — raised ONLY for a missing/
    unregistered authenticator (``require_authenticator``), never for a bad
    credential (that is ``None``). The message is kept secret-free."""

    def __init__(self, failure_reason: str, *, remediation: str | None = None) -> None:
        self.failure_reason = failure_reason
        self.remediation = remediation
        super().__init__(failure_reason)


@dataclass(frozen=True, repr=False)
class McpCredential:
    """A transport-extracted MCP credential.

    ``value`` holds the RAW credential, encapsulated: it is never rendered in
    ``repr`` (redacted to length only) so logs/tracebacks cannot leak the secret.
    ``metadata`` is a bounded, secret-free map (e.g. transport hints) — it MUST
    NOT contain the credential.
    """

    source: McpCredentialSource
    value: str
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __repr__(self) -> str:  # secret-free representation
        return (
            f"McpCredential(source={self.source!r}, "
            f"value=<redacted len={len(self.value)}>, "
            f"metadata={dict(self.metadata)!r})"
        )


@runtime_checkable
class AuthSession(Protocol):
    """Structural shape of a resolved auth session. Carries identity only —
    NEVER the api_key. The concrete DTO is :class:`AgentAuthSession`."""

    agent_id: str
    agent_name: str
    is_active: bool


@dataclass(frozen=True)
class AgentAuthSession:
    """Canonical resolved session for an authenticated agent (satisfies
    :class:`AuthSession`). ``metadata`` is bounded and secret-free — it MUST NOT
    contain the api_key."""

    agent_id: str
    agent_name: str
    is_active: bool
    metadata: Mapping[str, Any] = field(default_factory=dict)


@runtime_checkable
class McpAuthenticator(Protocol):
    """Runtime-agnostic MCP authentication contract.

    Implementations (e.g. the Community adapter) own the concrete agent lookup;
    this port never references it. ``authenticate`` is fail-closed: an
    absent/invalid/inactive credential returns ``None`` and it MUST NOT raise for
    a bad credential.
    """

    async def authenticate(self, credential: McpCredential | None) -> AuthSession | None:
        """Resolve the agent identity for ``credential`` or ``None`` (fail-closed)."""
        ...


def principal_from_auth_session(session: AuthSession | None) -> Principal | None:
    """Project an active MCP auth session to the shared pure principal DTO."""
    if session is None or not session.is_active:
        return None
    return Principal(
        subject=session.agent_id,
        claims={
            "agent_name": session.agent_name,
            "auth_channel": "mcp",
        },
    )


def mcp_credential_from_sources(
    *,
    query_param: str | None,
    x_api_key_header: str | None,
    authorization_header: str | None,
) -> McpCredential | None:
    """Build an :class:`McpCredential` applying the CANONICAL transport
    precedence — query param > ``X-API-Key`` header > ``Authorization: Bearer`` —
    mirroring ``mcp.server.ApiKeySessionMiddleware`` WITHOUT importing
    FastAPI/Starlette. Returns ``None`` when no source yields a non-empty value.
    """
    if query_param:
        return McpCredential(source="query_param", value=query_param)
    if x_api_key_header:
        return McpCredential(source="x_api_key_header", value=x_api_key_header)
    bearer = (authorization_header or "").removeprefix("Bearer ").strip()
    if bearer:
        return McpCredential(source="authorization_bearer", value=bearer)
    return None


def require_authenticator(
    authenticator: McpAuthenticator | None,
) -> McpAuthenticator:
    """Fail-closed resolver for an absent authenticator.

    A missing/unregistered :class:`McpAuthenticator` raises a structured
    :class:`McpAuthError` — it NEVER silently degrades to an unauthenticated
    pass. Concrete registries (the Community composition) call this before
    authenticating.
    """
    if authenticator is None:
        raise McpAuthError(
            "authenticator_absent",
            remediation=(
                "Register/resolve an McpAuthenticator (e.g. the Community MCP "
                "authenticator) before authenticating MCP credentials."
            ),
        )
    return authenticator


__all__ = [
    "MCP_CREDENTIAL_SCOPE_KEY",
    "MCP_CREDENTIAL_SOURCES",
    "MCP_AUTH_FAILURE_REASONS",
    "McpCredentialSource",
    "McpAuthFailureReason",
    "McpAuthError",
    "McpCredential",
    "AuthSession",
    "AgentAuthSession",
    "McpAuthenticator",
    "mcp_credential_from_sources",
    "principal_from_auth_session",
    "require_authenticator",
]
