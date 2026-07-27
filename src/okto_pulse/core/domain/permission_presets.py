"""Pure built-in permission-preset reconciliation policy."""

from __future__ import annotations

import copy
from dataclasses import dataclass
from enum import Enum
from typing import Iterable

from okto_pulse.core.domain.permissions import (
    PermissionFlags,
    get_builtin_presets,
)


class PermissionPresetReconciliationError(ValueError):
    """The catalog or persisted built-in identity is ambiguous."""


@dataclass(frozen=True)
class PermissionPresetDefinition:
    """Canonical Core definition of one immutable built-in preset."""

    name: str
    description: str
    flags: PermissionFlags
    base_preset_name: str | None = None

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise PermissionPresetReconciliationError("preset name must not be empty")
        object.__setattr__(self, "flags", copy.deepcopy(self.flags))


@dataclass(frozen=True)
class PersistedPermissionPreset:
    """Storage-neutral snapshot consumed by the reconciliation planner."""

    id: str
    name: str
    description: str | None
    is_builtin: bool
    flags: PermissionFlags
    owner_id: str | None = None
    base_preset_name: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "flags", copy.deepcopy(self.flags))


class ReconciliationAction(str, Enum):
    INSERT = "insert"
    UPDATE = "update"


@dataclass(frozen=True)
class ReconciliationCommand:
    """Idempotent repository command emitted by Core policy."""

    action: ReconciliationAction
    definition: PermissionPresetDefinition
    preset_id: str | None = None

    def __post_init__(self) -> None:
        if self.action is ReconciliationAction.INSERT and self.preset_id is not None:
            raise PermissionPresetReconciliationError(
                "insert command must not target an existing preset id"
            )
        if self.action is ReconciliationAction.UPDATE and not self.preset_id:
            raise PermissionPresetReconciliationError(
                "update command requires an existing preset id"
            )


def builtin_permission_preset_definitions() -> tuple[PermissionPresetDefinition, ...]:
    """Project the canonical permission catalog into typed domain values."""

    return tuple(
        PermissionPresetDefinition(
            name=item["name"],
            description=item["description"],
            flags=item["flags"],
            base_preset_name=item.get("base_preset_name"),
        )
        for item in get_builtin_presets()
    )


def _same_definition(
    persisted: PersistedPermissionPreset,
    definition: PermissionPresetDefinition,
) -> bool:
    return (
        persisted.description == definition.description
        and persisted.flags == definition.flags
        and persisted.base_preset_name == definition.base_preset_name
    )


def plan_permission_preset_reconciliation(
    catalog: Iterable[PermissionPresetDefinition],
    persisted: Iterable[PersistedPermissionPreset],
) -> tuple[ReconciliationCommand, ...]:
    """Return deterministic writes needed to converge built-ins to the catalog.

    Custom presets are never command targets, including a custom preset whose
    name matches a built-in.  Stale built-ins are preserved for compatibility;
    catalog removal is a separate product decision rather than an implicit
    destructive bootstrap action.
    """

    definitions = tuple(catalog)
    by_name: dict[str, PermissionPresetDefinition] = {}
    for definition in definitions:
        if definition.name in by_name:
            raise PermissionPresetReconciliationError(
                f"duplicate built-in catalog name: {definition.name}"
            )
        by_name[definition.name] = definition

    persisted_builtins: dict[str, PersistedPermissionPreset] = {}
    for preset in persisted:
        if not preset.is_builtin:
            continue
        if preset.name in persisted_builtins:
            raise PermissionPresetReconciliationError(
                f"duplicate persisted built-in name: {preset.name}"
            )
        persisted_builtins[preset.name] = preset

    commands: list[ReconciliationCommand] = []
    for definition in definitions:
        current = persisted_builtins.get(definition.name)
        if current is None:
            commands.append(
                ReconciliationCommand(
                    action=ReconciliationAction.INSERT,
                    definition=definition,
                )
            )
        elif not _same_definition(current, definition):
            commands.append(
                ReconciliationCommand(
                    action=ReconciliationAction.UPDATE,
                    definition=definition,
                    preset_id=current.id,
                )
            )
    return tuple(commands)


__all__ = [
    "PermissionPresetDefinition",
    "PermissionPresetReconciliationError",
    "PersistedPermissionPreset",
    "ReconciliationAction",
    "ReconciliationCommand",
    "builtin_permission_preset_definitions",
    "plan_permission_preset_reconciliation",
]
