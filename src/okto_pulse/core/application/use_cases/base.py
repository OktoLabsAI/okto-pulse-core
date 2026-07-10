"""Transport-neutral building blocks for application use cases (spec #09).

Defines the ``UseCase`` protocol, the transport-neutral ``ActorContext``, the
command/result markers, the domain error hierarchy used by inbound adapters to
map failures, and the transitional UnitOfWork bridge.

This module lives inside the purity-gated ``application/use_cases`` package and
therefore imports no transport framework. It deliberately avoids importing
SQLAlchemy: the UnitOfWork is duck-typed as ``Any`` so the application layer
stays free of the relational adapter (that boundary is closed by spec #04).
"""

from __future__ import annotations

from typing import Any, Literal, Mapping, Protocol, TypeVar, runtime_checkable

from okto_pulse.core.ports.authentication import Principal

Source = Literal["rest", "mcp", "system"]


class ActorContext:
    """Transport-neutral actor for a use case execution (tr_b18aefe5).

    Carries identity and authorization data only — never a framework object.
    ``source`` records the inbound surface as plain data, not as a dependency
    on FastAPI/MCP. Immutable by convention.
    """

    __slots__ = (
        "actor_id",
        "source",
        "actor_name",
        "board_id",
        "realm_id",
        "permissions",
        "roles",
    )

    def __init__(
        self,
        actor_id: str,
        source: Source,
        *,
        actor_name: str | None = None,
        board_id: str | None = None,
        realm_id: str | None = None,
        permissions: Mapping[str, Any] | None = None,
        roles: tuple[str, ...] = (),
    ) -> None:
        self.actor_id = actor_id
        self.source: Source = source
        # Display name for audit, when the transport already resolved it (e.g. the
        # MCP agent name). None lets the service resolve it (the REST behavior).
        self.actor_name = actor_name
        self.board_id = board_id
        self.realm_id = realm_id
        self.permissions = permissions
        self.roles = roles

    def __repr__(self) -> str:  # pragma: no cover - debug aid
        return (
            f"ActorContext(actor_id={self.actor_id!r}, source={self.source!r}, "
            f"actor_name={self.actor_name!r}, board_id={self.board_id!r}, "
            f"realm_id={self.realm_id!r})"
        )


def actor_context_from_principal(
    principal: Principal,
    *,
    source: Source,
    board_id: str | None = None,
) -> ActorContext:
    """Normalize a transport-free principal for application authorization.

    Claim interpretation is intentionally centralized here so REST, MCP and a
    future SaaS worker feed identical identity, realm, role and permission data
    into board/state-transition policies.
    """
    claims = principal.claims
    raw_roles = claims.get("roles", claims.get("role", ()))
    if isinstance(raw_roles, str):
        roles = (raw_roles,)
    elif isinstance(raw_roles, (list, tuple, set, frozenset)):
        roles = tuple(str(role) for role in raw_roles if role)
    else:
        roles = ()

    permissions: Any = ()
    for key in ("permissions", "permission_flags", "flags"):
        if key in claims:
            permissions = claims[key]
            break

    actor_name = claims.get("name", claims.get("agent_name"))
    return ActorContext(
        principal.subject,
        source,
        actor_name=str(actor_name) if actor_name else None,
        board_id=board_id,
        realm_id=principal.realm_id,
        permissions=permissions,
        roles=roles,
    )


CommandT = TypeVar("CommandT", contravariant=True)
ResultT = TypeVar("ResultT", covariant=True)


@runtime_checkable
class UseCase(Protocol[CommandT, ResultT]):
    """A transport-free application operation (tr_3d5b5204).

    ``uow`` is typed ``Any`` as a transitional bridge while the concrete
    ``PulseUnitOfWork`` port remains external to this package. Adapters pass a
    UnitOfWork exposing ``.session``/``.commit()``; bare relational sessions are
    rejected at this boundary.
    """

    async def execute(self, command: CommandT, *, actor: ActorContext, uow: Any) -> ResultT:
        ...


# --- Domain error hierarchy (adapters map these to transport responses) ------


class UseCaseError(Exception):
    """Base class for transport-neutral use case errors."""


class CommandValidationError(UseCaseError):
    """Invalid command payload (inbound adapters map to HTTP 400 / MCP error)."""


class EntityNotFoundError(UseCaseError):
    """A referenced entity does not exist (adapters map to HTTP 404)."""

    def __init__(self, entity_type: str, entity_id: str) -> None:
        self.entity_type = entity_type
        self.entity_id = entity_id
        super().__init__(f"{entity_type} not found: {entity_id}")


class ConflictError(UseCaseError):
    """A request conflicts with existing state (adapters map to HTTP 409)."""

    def __init__(self, entity_type: str, entity_id: str) -> None:
        self.entity_type = entity_type
        self.entity_id = entity_id
        super().__init__(f"{entity_type} conflict: {entity_id}")


class PermissionDeniedError(UseCaseError):
    """The actor lacks a required permission (adapters map to HTTP 403). ``message``
    is the human-readable reason the adapter surfaces as the 403 detail."""

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)


# --- Transitional UnitOfWork bridge (removed when spec #04 closes the port) ---


def session_of(uow: Any) -> Any:
    """Return the relational session for ``uow``.

    Accepts only a UnitOfWork exposing a non-null ``.session``. Bare
    relational sessions must be wrapped by the registered UnitOfWorkFactory.
    """
    missing = object()
    session = getattr(uow, "session", missing)
    if session is missing:
        raise TypeError("uow must expose .session; bare sessions are not accepted")
    if session is None:
        raise TypeError("uow.session must not be None")
    return session


async def commit(uow: Any) -> None:
    """Commit the transaction owned by ``uow``."""
    commit_fn = getattr(uow, "commit", None)
    if commit_fn is None:
        raise TypeError("uow must provide an awaitable commit()")
    await commit_fn()


def relational_context_from_uow(value: Any) -> Any:
    """Expose an adapter-neutral relational context to legacy Core services.

    Migrated inbound adapters receive a ``PulseUnitOfWork``.  A small number of
    pre-strangler Core application services still accept their relational
    context directly; this bridge unwraps the UoW without importing a concrete
    session type.  Passing a context directly remains supported for non-HTTP
    callers while MCP finishes its own UoW migration.
    """

    return getattr(value, "session", value)
