"""Transport-neutral building blocks for application use cases (spec #09).

Defines the ``UseCase`` protocol, the transport-neutral ``ActorContext``, the
command/result markers, and the domain error hierarchy used by inbound adapters
to map failures.

This module lives inside the purity-gated ``application/use_cases`` package and
therefore imports no transport framework or persistence implementation. Use
cases depend only on the typed ``PulseUnitOfWork`` port.
"""

from __future__ import annotations

from typing import Any, Literal, Mapping, Protocol, TypeVar, runtime_checkable

from okto_pulse.core.ports.authentication import Principal, PrincipalKind
from okto_pulse.core.domain.realm import (
    LOCAL_REALM_ID,
    RealmScope,
    require_realm_scope,
)
from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork

Source = Literal["rest", "mcp", "system"]


class ActorContext:
    """Transport-neutral actor for a use case execution (tr_b18aefe5).

    Carries identity and authorization data only — never a framework object.
    ``source`` records the inbound surface; ``actor_kind`` records the
    independently authenticated identity class.  Neither is inferred from the
    other inside application policy. Immutable by convention.
    """

    __slots__ = (
        "actor_id",
        "source",
        "actor_name",
        "actor_kind",
        "board_id",
        "realm_id",
        "realm_scope",
        "permissions",
        "roles",
    )

    def __init__(
        self,
        actor_id: str,
        source: Source,
        *,
        actor_name: str | None = None,
        actor_kind: PrincipalKind = "unknown",
        board_id: str | None = None,
        realm_id: str | None = None,
        realm_scope: RealmScope | None = None,
        permissions: Mapping[str, Any] | None = None,
        roles: tuple[str, ...] = (),
    ) -> None:
        self.actor_id = actor_id
        self.source: Source = source
        # Display name for audit, when the transport already resolved it (e.g. the
        # MCP agent name). None lets the service resolve it (the REST behavior).
        self.actor_name = actor_name
        self.actor_kind: PrincipalKind = actor_kind
        self.board_id = board_id
        if realm_scope is None and realm_id:
            realm_scope = (
                RealmScope.local()
                if realm_id == LOCAL_REALM_ID
                else RealmScope.tenant(realm_id)
            )
        self.realm_scope = realm_scope
        self.realm_id = realm_scope.realm_id if realm_scope is not None else realm_id
        self.permissions = permissions
        self.roles = roles

    def __repr__(self) -> str:  # pragma: no cover - debug aid
        return (
            f"ActorContext(actor_id={self.actor_id!r}, source={self.source!r}, "
            f"actor_name={self.actor_name!r}, actor_kind={self.actor_kind!r}, "
            f"board_id={self.board_id!r}, "
            f"realm_id={self.realm_id!r})"
        )

    def require_realm_scope(self) -> RealmScope:
        return require_realm_scope(self.realm_scope)


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
        actor_kind=principal.actor_kind,
        board_id=board_id,
        realm_scope=(
            RealmScope.local()
            if principal.realm_id == LOCAL_REALM_ID
            else RealmScope.tenant(principal.realm_id or "")
        ),
        permissions=permissions,
        roles=roles,
    )


CommandT = TypeVar("CommandT", contravariant=True)
ResultT = TypeVar("ResultT", covariant=True)


@runtime_checkable
class UseCase(Protocol[CommandT, ResultT]):
    """A transport-free application operation (tr_3d5b5204).

    Adapters supply the ``PulseUnitOfWork`` port; no relational implementation
    detail is exposed to the application layer.
    """

    async def execute(
        self,
        command: CommandT,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ) -> ResultT:
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


async def commit(uow: PulseUnitOfWork) -> None:
    """Commit the transaction owned by ``uow``."""
    commit_fn = getattr(uow, "commit", None)
    if commit_fn is None:
        raise TypeError("uow must provide an awaitable commit()")
    await commit_fn()
