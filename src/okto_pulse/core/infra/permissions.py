"""Deprecated compatibility facade for the pure Core permission policy.

New production code imports :mod:`okto_pulse.core.domain.permissions` or the
public permission port.  This module remains import-compatible for existing
extensions and tests while carrying no implementation of its own.
"""

from __future__ import annotations

from okto_pulse.core.domain import permissions as _domain_permissions
from okto_pulse.core.domain.permissions import *  # noqa: F403
from okto_pulse.core.ports.permission_policy import PermissionPolicyPort


PERMISSION_POLICY: PermissionPolicyPort = _domain_permissions.DefaultPermissionPolicy()


def __getattr__(name: str):
    """Delegate legacy private helper imports during the compatibility window."""

    return getattr(_domain_permissions, name)


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(dir(_domain_permissions)))


__all__ = [
    name for name in dir(_domain_permissions) if not name.startswith("_")
] + ["PERMISSION_POLICY", "PermissionPolicyPort"]
