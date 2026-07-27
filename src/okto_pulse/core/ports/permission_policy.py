"""Public, persistence-free permission policy contract and helpers.

Adapters may use these functions to apply the same policy as Core without
reaching into a private infrastructure module.  This module and its domain
dependencies are standard-library-only so a Core-only installation can define
and validate a SaaS adapter without importing Community.
"""

from __future__ import annotations

import copy
from typing import Any, Protocol, runtime_checkable

from okto_pulse.core.domain.permissions import (
    DefaultPermissionPolicy,
    InvalidPermissionContext,
    PermissionContext,
    PermissionContractViolation,
    PermissionDecision,
    PermissionFlags,
    PermissionPolicyError,
    PermissionSet,
    PERMISSION_REGISTRY,
    _flatten_registry,
    _get_nested,
    _match_builtin_preset_name,
    _set_nested,
    evaluate_permission,
    get_builtin_presets,
    map_legacy_permissions,
    merge_missing_flags,
    resolve_permissions,
)


@runtime_checkable
class PermissionPolicyPort(Protocol):
    """Edition-neutral permission policy boundary.

    Implementations may load flags from any edition-owned source, but they must
    return the Core value objects and preserve the canonical ceiling model.
    """

    def resolve(
        self,
        agent_flags: PermissionFlags | None,
        preset_flags: PermissionFlags | None,
        board_overrides: PermissionFlags | None,
    ) -> PermissionSet:
        """Resolve effective permissions for one application scope."""
        ...

    def evaluate(self, context: PermissionContext) -> PermissionDecision:
        """Evaluate one canonical permission operation."""
        ...


def flatten_permission_flags(flags: PermissionFlags) -> list[str]:
    """Return the registered flag paths present in a partial override payload."""

    return _flatten_registry(flags)


def get_permission_flag(flags: PermissionFlags, path: str) -> Any:
    """Read a nested permission flag by its canonical dotted path."""

    return _get_nested(flags, path)


def set_permission_flag(flags: PermissionFlags, path: str, value: Any) -> None:
    """Set a nested permission flag by its canonical dotted path."""

    _set_nested(flags, path, value)


def builtin_preset_name(flags: PermissionFlags) -> str | None:
    """Return the matching built-in preset name, if the policy has one."""

    return _match_builtin_preset_name(flags)


def legacy_permissions_to_flags(values: list[str]) -> PermissionFlags:
    """Normalize legacy flat permissions into the canonical flag tree."""

    return map_legacy_permissions(values)


def resolve_effective_permissions(
    agent_flags: PermissionFlags | None,
    preset_flags: PermissionFlags | None,
    board_overrides: PermissionFlags | None,
) -> PermissionSet:
    """Apply the canonical policy merge used by all editions."""

    return resolve_permissions(agent_flags, preset_flags, board_overrides)


def builtin_permission_presets() -> list[dict[str, Any]]:
    """Return canonical built-in preset definitions for edition bootstrap."""

    return copy.deepcopy(get_builtin_presets())


def registered_permission_flags() -> PermissionFlags:
    """Return an isolated copy of the canonical permission registry."""

    return copy.deepcopy(PERMISSION_REGISTRY)


def merge_permission_registry_defaults(
    stored: PermissionFlags,
) -> tuple[PermissionFlags, int]:
    """Backfill missing canonical flags without overwriting stored values."""

    return merge_missing_flags(stored, PERMISSION_REGISTRY)


__all__ = [
    "DefaultPermissionPolicy",
    "InvalidPermissionContext",
    "PermissionContext",
    "PermissionContractViolation",
    "PermissionDecision",
    "PermissionFlags",
    "PermissionPolicyError",
    "PermissionPolicyPort",
    "PermissionSet",
    "builtin_permission_presets",
    "builtin_preset_name",
    "evaluate_permission",
    "flatten_permission_flags",
    "get_permission_flag",
    "legacy_permissions_to_flags",
    "merge_permission_registry_defaults",
    "registered_permission_flags",
    "resolve_effective_permissions",
    "set_permission_flag",
]
