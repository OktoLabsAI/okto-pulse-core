"""Small permission bridge for board-scoped KG MCP adapters."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from okto_pulse.core.domain.permissions import (
    PermissionSet,
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
) -> str | None:
    """Check one canonical KG operation under the resolved Board authority."""

    permissions = getattr(context, "permissions", None)
    if isinstance(permissions, Mapping):
        permissions = PermissionSet(dict(permissions))
    if isinstance(permissions, (list, tuple, set)) and "*" in permissions:
        permissions = None
    return check_permission(permissions, required_permission)



__all__ = [
    "kg_permission_error",
    "principal_id",
]
