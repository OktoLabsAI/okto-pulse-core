"""Core use case for deterministic built-in permission-preset bootstrap."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from okto_pulse.core.domain.permission_presets import (
    PermissionPresetDefinition,
    ReconciliationCommand,
    builtin_permission_preset_definitions,
    plan_permission_preset_reconciliation,
)
from okto_pulse.core.ports.permission_preset_reconciliation import (
    PermissionPresetReconciliationRepository,
)


@dataclass(frozen=True)
class ReconcilePermissionPresetsResult:
    commands: tuple[ReconciliationCommand, ...]

    @property
    def changed(self) -> bool:
        return bool(self.commands)


class ReconcilePermissionPresetsUseCase:
    """Plan in Core and delegate one atomic write batch to the edition."""

    def __init__(
        self,
        catalog: Sequence[PermissionPresetDefinition] | None = None,
    ) -> None:
        self._catalog = tuple(catalog or builtin_permission_preset_definitions())

    async def execute(
        self,
        repository: PermissionPresetReconciliationRepository,
    ) -> ReconcilePermissionPresetsResult:
        persisted = await repository.list_permission_presets()
        commands = plan_permission_preset_reconciliation(self._catalog, persisted)
        if commands:
            await repository.apply_permission_preset_commands(commands)
        return ReconcilePermissionPresetsResult(commands=commands)


__all__ = [
    "ReconcilePermissionPresetsResult",
    "ReconcilePermissionPresetsUseCase",
]
