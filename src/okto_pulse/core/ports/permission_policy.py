"""Public, persistence-free permission policy helpers.

Adapters may use these functions to apply the same policy as Core without
reaching into the private ``infra.permissions`` module.
"""

from __future__ import annotations

from typing import Any

from okto_pulse.core.infra.permissions import (
    _flatten_registry,
    _get_nested,
    _match_builtin_preset_name,
    _set_nested,
    map_legacy_permissions,
    resolve_permissions,
)


def flatten_permission_flags(flags: dict[str, Any]) -> list[str]:
    """Return the registered flag paths present in a partial override payload."""

    return _flatten_registry(flags)


def get_permission_flag(flags: dict[str, Any], path: str) -> Any:
    """Read a nested permission flag by its canonical dotted path."""

    return _get_nested(flags, path)


def set_permission_flag(flags: dict[str, Any], path: str, value: Any) -> None:
    """Set a nested permission flag by its canonical dotted path."""

    _set_nested(flags, path, value)


def builtin_preset_name(flags: dict[str, Any]) -> str | None:
    """Return the matching built-in preset name, if the policy has one."""

    return _match_builtin_preset_name(flags)


def legacy_permissions_to_flags(values: list[str]) -> dict[str, Any]:
    """Normalize legacy flat permissions into the canonical flag tree."""

    return map_legacy_permissions(values)


def resolve_effective_permissions(
    agent_flags: dict[str, Any] | None,
    preset_flags: dict[str, Any] | None,
    board_overrides: dict[str, Any] | None,
) -> Any:
    """Apply the canonical policy merge used by all editions."""

    return resolve_permissions(agent_flags, preset_flags, board_overrides)


__all__ = [
    "builtin_preset_name",
    "flatten_permission_flags",
    "get_permission_flag",
    "legacy_permissions_to_flags",
    "resolve_effective_permissions",
    "set_permission_flag",
]
