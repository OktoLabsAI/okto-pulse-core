"""Application-facing permission policy facade."""

from __future__ import annotations

from typing import Any

from okto_pulse.core.infra.permissions import PermissionSet, Permissions

__all__ = [
    "PermissionSet",
    "Permissions",
    "check_permission",
    "check_story_state_permission",
]


def check_permission(permission_set: Any, permission: str) -> str | None:
    """Evaluate a permission through the concrete policy module.

    Import at call time so existing tests and adapters that patch the concrete
    permission evaluator keep observing the same seam.
    """
    from okto_pulse.core.infra.permissions import check_permission as _check

    return _check(permission_set, permission)


def check_story_state_permission(
    permissions: Any,
    granular: str,
    legacy: str | None,
    story: Any,
    *,
    story_state: str,
) -> str | None:
    """Mirror the MCP story-state permission check behind a service facade."""
    if not granular:
        return None
    if isinstance(permissions, PermissionSet):
        return permissions.check_with_state(granular, "story", story_state)
    if permissions is None:
        return None
    if granular in permissions:
        return None
    if legacy and legacy in permissions:
        return None
    return f"Permission denied: requires '{granular}'"
