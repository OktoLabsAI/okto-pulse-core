"""Central, transport-neutral authorization helpers for Core use cases.

The synchronous decision function operates only on permission data already
carried by an actor (or supplied explicitly).  The async guards add the one
edition-neutral lookup needed by REST actors whose board permissions have not
yet been resolved.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, TypeAlias

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    PermissionDeniedError,
)
from okto_pulse.core.domain.permissions import (
    ALL_FLAGS,
    DefaultPermissionPolicy,
    PermissionContext,
    PermissionDecision,
    PermissionSet,
)

if TYPE_CHECKING:
    from okto_pulse.core.ports.permission_policy import PermissionPolicyPort
    from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork


PermissionInput = PermissionSet | Mapping[str, Any] | list[str] | tuple[str, ...] | None
NormalizedPermissionInput = PermissionSet | list[str] | tuple[str, ...] | None


class _UnsetPermissionInput:
    __slots__ = ()


PermissionOverride: TypeAlias = PermissionInput | _UnsetPermissionInput
_UNSET = _UnsetPermissionInput()


@dataclass(frozen=True, slots=True)
class PermissionRequirement:
    """One canonical operation and its optional compatibility/state context."""

    operation: str
    legacy_operation: str | None = None
    entity: str | None = None
    state: str | None = None


def _plain_mapping(value: Mapping[str, Any]) -> dict[str, Any]:
    return {
        str(key): _plain_mapping(item) if isinstance(item, Mapping) else item
        for key, item in value.items()
    }


def normalize_permission_input(
    permissions: PermissionInput,
) -> NormalizedPermissionInput:
    """Normalize permission documents without widening their authority.

    A mapping is resolved permission data, so it becomes a ``PermissionSet``.
    Flat lists/tuples deliberately retain legacy-token support.  ``None`` is
    preserved as the historical trusted/full-access sentinel; the actor-aware
    decision layer limits that sentinel to legacy MCP/system callers.
    """

    if permissions is None or isinstance(permissions, PermissionSet):
        return permissions
    if isinstance(permissions, Mapping):
        return PermissionSet(_plain_mapping(permissions))
    if isinstance(permissions, list):
        return permissions
    if isinstance(permissions, tuple):
        return permissions
    # Malformed claim input is authority-bearing data and therefore fails
    # closed.  An empty tuple cannot satisfy a registered permission.
    return ()


def decide_authorization(
    actor: ActorContext,
    requirement: PermissionRequirement,
    permissions: PermissionOverride = _UNSET,
    policy: PermissionPolicyPort | None = None,
) -> PermissionDecision:
    """Make a synchronous decision from actor-carried or explicit permissions."""

    invalid_context = _validate_requirement_context(requirement)
    if invalid_context is not None:
        return invalid_context

    if permissions is _UNSET:
        raw_permissions = actor.permissions
    elif permissions is None and actor.permissions is not None:
        # An explicit null override cannot erase restrictions already carried
        # by the actor and thereby elevate it to historical trusted access.
        raw_permissions = ()
    else:
        raw_permissions = permissions
    normalized = normalize_permission_input(raw_permissions)
    if normalized is None and actor.source not in ("mcp", "system"):
        normalized = ()
    evaluator = policy or DefaultPermissionPolicy()
    return evaluator.evaluate(
        PermissionContext(
            operation=requirement.operation,
            permissions=normalized,
            entity=requirement.entity,
            state=requirement.state,
            legacy_operation=requirement.legacy_operation,
        )
    )


def _validate_requirement_context(
    requirement: PermissionRequirement,
) -> PermissionDecision | None:
    operation = requirement.operation.strip()
    if operation not in ALL_FLAGS:
        return PermissionDecision.deny(
            operation,
            json.dumps(
                {
                    "error": "Permission denied",
                    "reason": "unknown_permission",
                    "required_permission": operation,
                    "detail": (
                        f"The permission '{operation}' is not registered by the "
                        "Core permission policy."
                    ),
                }
            ),
        )

    has_entity = requirement.entity is not None
    has_state = requirement.state is not None
    if has_entity != has_state:
        return _invalid_context_decision(
            requirement,
            "State-aware authorization requires both entity and state.",
        )
    if not has_entity:
        return None
    if not operation.startswith(f"{requirement.entity}."):
        return _invalid_context_decision(
            requirement,
            (
                f"Operation '{operation}' does not belong to state-gated entity "
                f"'{requirement.entity}'."
            ),
        )

    state_permission = f"{requirement.entity}.interact_in.{requirement.state}"
    if state_permission not in ALL_FLAGS:
        return _invalid_context_decision(
            requirement,
            f"The state permission '{state_permission}' is not registered.",
        )
    return None


def _invalid_context_decision(
    requirement: PermissionRequirement,
    detail: str,
) -> PermissionDecision:
    return PermissionDecision.deny(
        requirement.operation.strip(),
        json.dumps(
            {
                "error": "Permission denied",
                "reason": "invalid_permission_context",
                "required_permission": requirement.operation.strip(),
                "detail": detail,
            }
        ),
    )


async def resolve_actor_permissions(
    actor: ActorContext,
    uow: PulseUnitOfWork | None = None,
    board_id: str | None = None,
) -> NormalizedPermissionInput:
    """Resolve missing REST board permissions through the UoW service catalog."""

    target_board_id = board_id if board_id is not None else actor.board_id
    scope_mismatch = board_id is not None and board_id != actor.board_id

    if actor.permissions is not None and not scope_mismatch:
        return normalize_permission_input(actor.permissions)

    if actor.source not in ("mcp", "system", "rest"):
        return ()
    if actor.source != "rest":
        # ``None`` is the deliberate legacy trusted sentinel only for an
        # unscoped MCP/system actor.  Board-target mismatch never inherits it.
        return () if scope_mismatch else None

    if uow is None or target_board_id is None:
        return ()
    services = getattr(uow, "services", None)
    resolver = getattr(services, "resolve_user_permissions", None)
    if not callable(resolver):
        return ()

    resolved = await resolver(actor.actor_id, target_board_id)
    if resolved is None:
        # A REST lookup that cannot establish authority is not trusted access.
        return ()
    return normalize_permission_input(resolved)


def _structured_denial(decision: PermissionDecision) -> str:
    reason = decision.reason
    if reason:
        try:
            decoded = json.loads(reason)
        except (TypeError, ValueError):
            decoded = None
        if isinstance(decoded, Mapping):
            return reason
    return json.dumps(
        {
            "error": "permission_denied",
            "reason": "permission_missing",
            "required_permission": decision.required_permission,
            **({"detail": reason} if reason else {}),
        }
    )


def _raise_if_denied(decision: PermissionDecision) -> PermissionDecision:
    if not decision.allowed:
        raise PermissionDeniedError(_structured_denial(decision))
    return decision


def _precheck_board_scope(actor: ActorContext, board_id: str | None) -> None:
    """Deny non-REST actors targeting a board outside their authenticated scope."""

    if board_id is None or board_id == actor.board_id or actor.source == "rest":
        return
    raise PermissionDeniedError(
        json.dumps(
            {
                "error": "permission_denied",
                "reason": "board_scope_mismatch",
                "required_permission": "board_scope",
            }
        )
    )


async def require_authorization(
    actor: ActorContext,
    requirement: PermissionRequirement,
    *,
    uow: PulseUnitOfWork | None = None,
    board_id: str | None = None,
    permissions: PermissionOverride = _UNSET,
    policy: PermissionPolicyPort | None = None,
) -> PermissionDecision:
    """Require one permission, resolving a missing REST permission set if needed."""

    _precheck_board_scope(actor, board_id)
    effective_permissions = permissions
    if effective_permissions is _UNSET:
        effective_permissions = await resolve_actor_permissions(actor, uow, board_id)
    return _raise_if_denied(
        decide_authorization(actor, requirement, effective_permissions, policy)
    )


async def require_all(
    actor: ActorContext,
    *requirements: PermissionRequirement,
    uow: PulseUnitOfWork | None = None,
    board_id: str | None = None,
    permissions: PermissionOverride = _UNSET,
    policy: PermissionPolicyPort | None = None,
) -> tuple[PermissionDecision, ...]:
    """Require every operation, resolving actor permissions at most once."""

    _precheck_board_scope(actor, board_id)
    effective_permissions = permissions
    if effective_permissions is _UNSET:
        effective_permissions = await resolve_actor_permissions(actor, uow, board_id)
    decisions = tuple(
        decide_authorization(actor, requirement, effective_permissions, policy)
        for requirement in requirements
    )
    for decision in decisions:
        _raise_if_denied(decision)
    return decisions


async def require_any_authority(
    actor: ActorContext,
    *requirements: PermissionRequirement,
    roles: Sequence[str] = (),
    uow: PulseUnitOfWork | None = None,
    board_id: str | None = None,
    permissions: PermissionOverride = _UNSET,
    policy: PermissionPolicyPort | None = None,
) -> PermissionDecision:
    """Require an explicit role or any one permission requirement.

    Roles never bypass permission policy implicitly: callers must enumerate the
    accepted roles for the specific operation.
    """

    _precheck_board_scope(actor, board_id)

    accepted_roles = {str(role).strip().casefold() for role in roles if str(role).strip()}
    actor_roles = {
        str(role).strip().casefold() for role in actor.roles if str(role).strip()
    }
    matched_roles = accepted_roles.intersection(actor_roles)
    if matched_roles:
        return PermissionDecision.allow(f"role:{sorted(matched_roles)[0]}")

    effective_permissions = permissions
    if effective_permissions is _UNSET:
        effective_permissions = await resolve_actor_permissions(actor, uow, board_id)
    decisions = tuple(
        decide_authorization(actor, requirement, effective_permissions, policy)
        for requirement in requirements
    )
    for decision in decisions:
        if decision.allowed:
            return decision

    if decisions:
        raise PermissionDeniedError(_structured_denial(decisions[0]))
    raise PermissionDeniedError(
        json.dumps(
            {
                "error": "permission_denied",
                "reason": "authority_missing",
                "required_permission": "any_authority",
            }
        )
    )


__all__ = [
    "PermissionRequirement",
    "decide_authorization",
    "normalize_permission_input",
    "require_all",
    "require_any_authority",
    "require_authorization",
    "resolve_actor_permissions",
]
