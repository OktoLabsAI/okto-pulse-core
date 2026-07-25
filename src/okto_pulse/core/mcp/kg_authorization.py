"""Small permission bridge for board-scoped KG MCP adapters."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from okto_pulse.core.domain.permissions import (
    PermissionSet,
    Permissions,
    check_permission,
)


def principal_id(principal: Any) -> str | None:
    value = getattr(
        principal,
        "agent_id",
        getattr(principal, "id", None),
    )
    return str(value) if value else None


def kg_permission_error(
    context: Any,
    required_permission: str,
    *,
    legacy_fallback: str | None = Permissions.BOARD_READ,
) -> str | None:
    """Check a canonical KG flag while preserving explicit legacy ACLs.

    Board-scoped ``PermissionSet`` is authoritative and therefore observes
    board overrides. Legacy flat permission lists predate KG flags; an explicit
    ``board:read`` retains their historical board-authorized behavior. Admin
    callers pass ``legacy_fallback=None`` so an old read grant never becomes an
    implicit administrative grant.
    """

    permissions = getattr(context, "permissions", None)
    if isinstance(permissions, Mapping):
        permissions = PermissionSet(dict(permissions))
    if isinstance(permissions, PermissionSet):
        return check_permission(permissions, required_permission)
    if permissions is None:
        # Canonical permission APIs deliberately retain this established
        # compatibility meaning for principals created before permission flags.
        return None
    if required_permission in permissions:
        return None
    if legacy_fallback is not None and legacy_fallback in permissions:
        return None
    return f"Permission denied: requires '{required_permission}'"


__all__ = [
    "kg_permission_error",
    "principal_id",
]
